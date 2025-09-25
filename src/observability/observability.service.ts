// src/observability/observability.service.ts
/* eslint-disable @typescript-eslint/no-unused-vars */
import { Injectable, Logger } from '@nestjs/common';
import { RedisStreamsService } from 'src/redis-streams/redis-streams.service';
import { parseSymbolsFromEnv } from 'src/utils/utils';

type DynGateSnapshot = {
  sym: string;
  updatedAt: string | null;
  params: {
    effMin0: number;
    minNotional3s: number;
    minMoveBp: number;
    minMoveAtrRatio: number;
    cooldownMs: number;
    dedupMs: number;
    breakoutBandPct: number;

    volPct: number;
    liqPct: number;
    rateExc: number;
    eventFlag: number;
    oiRegime: number;

    updated_at: number;
    version: string;
  };
};

@Injectable()
export class ObservabilityService {
  private readonly logger = new Logger(ObservabilityService.name);

  // 按你的约定：从环境解析交易标的
  private readonly symbols = parseSymbolsFromEnv();

  // 注意这里实例名叫 redis（不是 stream）
  constructor(private readonly redis: RedisStreamsService) {}

  /* ------------------------- 工具：数值化/四舍五入 ------------------------- */
  private num(v: any, d = 0): number {
    const n = Number(v);
    return Number.isFinite(n) ? n : d;
  }
  private round(x: number, digits = 3): number {
    return Number.isFinite(x) ? Number(x.toFixed(digits)) : x;
  }

  /* -------------------------- 1) 动态门槛快照 --------------------------- */
  async getDynGate(sym: string): Promise<DynGateSnapshot> {
    // 与 writer 对齐：MarketEnvUpdaterService 用的是 dyn:gate:{sym}
    const key = `dyn:gate:{${sym}}`;
    const res = await this.redis.hgetall(key); // 返回 Record<string,string> | {}

    // 兜底（没有的话返回空骨架）
    if (!res || Object.keys(res).length === 0) {
      return {
        sym,
        updatedAt: null,
        params: {
          effMin0: 0,
          minNotional3s: 0,
          minMoveBp: 0,
          minMoveAtrRatio: 0,
          cooldownMs: 0,
          dedupMs: 0,
          breakoutBandPct: 0,
          volPct: 0,
          liqPct: 0,
          rateExc: 0,
          eventFlag: 0,
          oiRegime: 0,
          updated_at: 0,
          version: 'n/a',
        },
      };
    }

    const updatedAtNum = this.num(res.updated_at);
    const snapshot: DynGateSnapshot = {
      sym,
      updatedAt: updatedAtNum ? new Date(updatedAtNum).toISOString() : null,
      params: {
        effMin0: this.num(res.effMin0),
        minNotional3s: this.num(res.minNotional3s),
        minMoveBp: this.num(res.minMoveBp),
        minMoveAtrRatio: this.num(res.minMoveAtrRatio),
        cooldownMs: this.num(res.cooldownMs),
        dedupMs: this.num(res.dedupMs),
        breakoutBandPct: this.num(res.breakoutBandPct),

        volPct: this.num(res.volPct),
        liqPct: this.num(res.liqPct),
        rateExc: this.num(res.rateExc),
        eventFlag: this.num(res.eventFlag),
        oiRegime: this.num(res.oiRegime),

        updated_at: updatedAtNum,
        version: String(res.version ?? 'v1'),
      },
    };

    // 可选：把小数统一到合理精度
    snapshot.params.effMin0 = this.round(snapshot.params.effMin0, 2);
    snapshot.params.minMoveAtrRatio = this.round(
      snapshot.params.minMoveAtrRatio,
      3,
    );
    snapshot.params.breakoutBandPct = this.round(
      snapshot.params.breakoutBandPct,
      4,
    );
    snapshot.params.volPct = this.round(snapshot.params.volPct, 2);
    snapshot.params.liqPct = this.round(snapshot.params.liqPct, 2);

    return snapshot;
  }

  /* -------------------------- 2) 信号统计面板 --------------------------- */
  /**
   * 读取每个标的的信号统计。
   * 默认先读 signal:final:{sym}；若不存在则回退读 signal:detected:{sym}
   */
  async getSignalStats(limit = 300, withExamples = false) {
    const out: Record<
      string,
      { total: number; buy: number; sell: number; examples?: any[] }
    > = {};

    for (const sym of this.symbols) {
      const primary = `signal:final:{${sym}}`;
      const fallback = `signal:detected:{${sym}}`;

      const rows =
        (await this.tryReadStream(primary, limit)) ??
        (await this.tryReadStream(fallback, limit)) ??
        [];

      const stats = { total: 0, buy: 0, sell: 0, examples: [] as any[] };

      for (const [id, h] of rows) {
        stats.total++;
        const dir = String(h.dir ?? '');
        if (dir === 'buy') stats.buy++;
        else if (dir === 'sell') stats.sell++;

        if (withExamples && stats.examples.length < 5) {
          const nStrength = this.round(this.num(h.strength), 3);
          const nNotional = h['evidence.notional3s']
            ? this.round(this.num(h['evidence.notional3s']), 2)
            : h.notional3s
              ? this.round(this.num(h.notional3s), 2)
              : null;

          stats.examples.push({
            id,
            ts: this.num(h.ts),
            dir,
            strength: nStrength,
            src: String(h['evidence.src'] ?? h.src ?? ''),
            notional3s: nNotional,
          });
        }
      }

      out[sym] = stats;
      if (!withExamples) delete (out[sym] as any).examples;
    }

    return out;
  }

  /* ----------------------------- 私有: 读取流 ----------------------------- */
  /**
   * 读取最近 limit 条（旧→新），返回 [id, hash]；若 key 不存在或为空返回 null
   */
  private async tryReadStream(
    key: string,
    limit: number,
  ): Promise<Array<[string, Record<string, string>]> | null> {
    try {
      const rows = await this.redis.xrange(key, '-', '+', limit);
      if (!rows || rows.length === 0) return null;
      return rows;
    } catch (e) {
      // XRANGE 若 key 不存在有的实现会抛错，有的返回空；这里做统一兼容
      this.logger.debug(
        `[obs] tryReadStream failed key=${key}: ${(e as Error)?.message}`,
      );
      return null;
    }
  }
}
