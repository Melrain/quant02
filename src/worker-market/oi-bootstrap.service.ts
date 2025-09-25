/* eslint-disable @typescript-eslint/no-unsafe-assignment */
/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-argument */
import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { RedisStreamsService } from 'src/redis-streams/redis-streams.service';
import { parseSymbolsFromEnv } from 'src/utils/utils';

/**
 * OiBootstrapService
 *
 * 目标：
 *  - 冷启动时通过 OKX REST 拉取一批 5m OI 历史，写入与 WS 相同的 Stream/Hash
 *  - 这样 MarketEnvUpdater 马上就有足够样本，不用等 5m/15m 桶滚出来
 *  - 后续实时更新仍由现有 WS → StreamsBridgeService.handleOi 完成
 *
 * 行为：
 *  - 启动时按 symbols 逐个拉取历史（默认 period=5m，拉 200 条左右）
 *  - 去重策略：只写入 ts > 已有最新 ts 的点，避免重复
 *  - 同时刷新 state:oi:{sym} 为最新一条（TTL 1h），与 WS 写法保持一致
 *
 * 配置（环境变量）：
 *  - OKX_REST_BASE     默认 https://www.okx.com
 *  - OI_BOOTSTRAP_ON   默认 "1"（开启），设为 "0" 可关闭预热
 *  - OI_BOOTSTRAP_N    默认 200（拉多少条，OKX最多返回固定窗口，这里尽量多留冗余）
 *  - OI_BOOTSTRAP_TF   默认 "5m"（OKX支持 5m/1H/1D 等）
 *  - OKX_ASSETS / OKX_SYMBOLS 见你现有 parseSymbolsFromEnv
 */
@Injectable()
export class OiBootstrapService implements OnModuleInit {
  private readonly logger = new Logger(OiBootstrapService.name);
  private readonly base =
    process.env.OKX_REST_BASE?.trim() || 'https://www.okx.com';
  private readonly enabled = (process.env.OI_BOOTSTRAP_ON ?? '1') !== '0';
  private readonly wantN = Math.max(
    10,
    Number(process.env.OI_BOOTSTRAP_N ?? 200),
  );
  private readonly period = process.env.OI_BOOTSTRAP_TF?.trim() || '5m';
  private readonly symbols = parseSymbolsFromEnv();

  constructor(private readonly streams: RedisStreamsService) {}

  async onModuleInit() {
    if (!this.enabled) {
      this.logger.log('OI bootstrap disabled (OI_BOOTSTRAP_ON=0)');
      return;
    }
    // 一个个跑，尽量减少突发请求（也可 Promise.all + 简单节流）
    for (const sym of this.symbols) {
      try {
        const added = await this.bootstrapOne(sym);
        this.logger.log(
          `[oi.bootstrap] ${sym} added=${added.count} range=[${added.firstTs ?? '-'}, ${added.lastTs ?? '-'}]`,
        );
      } catch (e) {
        this.logger.warn(
          `[oi.bootstrap] ${sym} failed: ${(e as Error).message}`,
        );
      }
      // 简单间隔，降低被限频风险
      await this.sleep(250);
    }
  }

  private sleep(ms: number) {
    return new Promise((r) => setTimeout(r, ms));
  }

  /** 单标的预热：读取最近 wantN 条，过滤掉 <= 最新已写 ts 的点，再写入 */
  private async bootstrapOne(sym: string): Promise<{
    count: number;
    firstTs?: number;
    lastTs?: number;
  }> {
    // 1) 读取本地已存在的最新一条（取 payload.ts）
    const oiKey = this.streams.buildKey(sym, 'oi');
    const latest = await this.streams.readLatestAsObjects(oiKey, 1);
    const lastTsInStream = Number(latest?.[0]?.ts ?? 0);

    // 2) REST 拉历史
    const rows = await this.fetchOkxOiHistory(sym, this.period);
    if (!rows.length) return { count: 0 };

    // 3) 过滤老数据，按旧→新写入
    const fresh = rows
      .filter((r) => Number(r.ts) > lastTsInStream)
      .sort((a, b) => Number(a.ts) - Number(b.ts));

    if (!fresh.length) return { count: 0 };

    // 4) 写入 Stream（与 WS 写法保持字段一致），并刷新 state:oi
    let firstTs: number | undefined;
    let lastTs: number | undefined;

    for (const r of fresh) {
      const ts = Number(r.ts);
      firstTs ??= ts;
      lastTs = ts;

      await this.streams.xadd(
        oiKey,
        {
          type: 'market.oi',
          src: 'okx',
          instId: sym,
          ts: String(ts),
          oi: r.oi ?? '',
          oiCcy: r.oiCcy ?? '',
          recvTs: String(Date.now()),
          ingestId: 'bootstrap',
        },
        { maxlenApprox: 5000 },
      );
    }

    // 刷新 state:oi（仅用最新点）
    const stateKey = this.streams.buildOutKey(sym, 'state:oi');
    const last = fresh[fresh.length - 1];
    await this.streams.hset(stateKey, {
      ts: String(last.ts),
      oi: last.oi ?? '',
      oiCcy: last.oiCcy ?? '',
      updatedTs: String(Date.now()),
      source: 'bootstrap', // 标注来源，便于区分
    });
    await this.streams.expire(stateKey, 60 * 60); // 1h TTL

    return { count: fresh.length, firstTs, lastTs };
  }

  /**
   * 拉取 OKX OI 历史（旧→新）
   * 优先尝试 /public/open-interest-history；若不可用则回退到 /public/open-interest?period=5m
   * 返回标准化结构：[{ ts, oi, oiCcy }]
   */
  private async fetchOkxOiHistory(
    instId: string,
    period: string,
  ): Promise<Array<{ ts: number; oi?: string; oiCcy?: string }>> {
    // A) 尝试 open-interest-history（更语义化的历史端点）
    const tryA = await this.tryFetch(
      `${this.base}/api/v5/public/open-interest-history?instId=${encodeURIComponent(instId)}&period=${encodeURIComponent(period)}`,
    );
    const rowsA = this.normalizeHistory(tryA);
    if (rowsA.length) {
      return rowsA.slice(-this.wantN);
    }

    // B) Fallback: open-interest（有的环境 period 也能返回时间序列）
    const tryB = await this.tryFetch(
      `${this.base}/api/v5/public/open-interest?instId=${encodeURIComponent(instId)}&period=${encodeURIComponent(period)}`,
    );
    const rowsB = this.normalizeHistory(tryB);
    if (rowsB.length) {
      return rowsB.slice(-this.wantN);
    }

    // C) 再退化一次：不带 period（取当前多点？部分环境仅返回一条，这里也收下）
    const tryC = await this.tryFetch(
      `${this.base}/api/v5/public/open-interest?instId=${encodeURIComponent(instId)}`,
    );
    const rowsC = this.normalizeHistory(tryC);
    if (rowsC.length) {
      return rowsC.slice(-this.wantN);
    }

    this.logger.warn(
      `[oi.bootstrap] ${instId} fetch empty from REST (history/fallback failed)`,
    );
    return [];
  }

  /** 通用 fetch（带 6s 超时），返回 { code, data } 或 {} */
  private async tryFetch(url: string): Promise<any> {
    const ctl = new AbortController();
    const timer = setTimeout(() => ctl.abort(), 6000);
    try {
      const res = await fetch(url, { signal: ctl.signal });
      if (!res.ok) return {};
      const json = await res.json();
      return json;
    } catch {
      return {};
    } finally {
      clearTimeout(timer);
    }
  }

  /**
   * 适配 OKX 返回结构，统一到 [{ts:number, oi?:string, oiCcy?:string}]，旧→新
   * 兼容：
   *  - { code:"0", data:[ { ts, oi, oiCcy, ...}, ... ] }
   *  - data 可能是按新→旧，统一反转为旧→新
   */
  private normalizeHistory(resp: any): Array<{
    ts: number;
    oi?: string;
    oiCcy?: string;
  }> {
    const data = Array.isArray(resp?.data) ? resp.data : [];
    if (!data.length) return [];
    // 尝试判断顺序：若 ts 递减则反转
    const sorted = [...data].sort(
      (a, b) => Number(a.ts ?? 0) - Number(b.ts ?? 0),
    );
    return sorted
      .map((d) => ({
        ts: Number(d.ts ?? 0),
        oi: d.oi != null ? String(d.oi) : undefined,
        oiCcy: d.oiCcy != null ? String(d.oiCcy) : undefined,
      }))
      .filter((x) => Number.isFinite(x.ts) && x.ts > 0);
  }
}
