// src/pricing/price-resolver.service.ts
/* eslint-disable @typescript-eslint/no-unused-vars */
import { Injectable, Logger } from '@nestjs/common';
import { RedisStreamsService } from 'src/redis-streams/redis-streams.service';

type Hit = { px: number; ts: number; source: string };

@Injectable()
export class PriceResolverService {
  private readonly logger = new Logger(PriceResolverService.name);

  // 在目标时间点左右各搜这么多毫秒
  private readonly WIN_MS = Number(process.env.EVAL_PX_SEARCH_MS ?? '15000');

  /**
   * 价格来源优先级（逗号分隔，不区分大小写）
   * 可选：mid,last,win:1m,ws:kline1m,bf:kline1m
   */
  private readonly PREF: string[] = String(
    process.env.EVAL_PRICE_PREF ?? 'mid,last,win:1m,ws:kline1m,bf:kline1m',
  )
    .split(',')
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);

  // 回填流的 1m k线基键名（out 流）
  private readonly BF_BASE = process.env.BF_KLINE_STREAM_BASE ?? 'bf:kline1m';

  constructor(private readonly streams: RedisStreamsService) {}

  /** 给定时间点 t(ms) 附近找价格；命中返回 px/ts/source，失败返回 null */
  async getPriceAt(t: number, sym: string): Promise<Hit | null> {
    if (!Number.isFinite(t) || !sym) return null;
    const start = t - this.WIN_MS;
    const end = t + this.WIN_MS;

    for (const src of this.PREF) {
      try {
        let hit: Hit | null = null;

        if (src === 'mid') hit = await this.tryMidAround(sym, start, end, t);
        else if (src === 'last')
          hit = await this.tryLastAround(sym, start, end, t);
        else if (src === 'win:1m')
          hit = await this.tryWin1mClose(sym, start, end, t);
        else if (src === 'ws:kline1m')
          hit = await this.tryWsKline1mClose(sym, start, end, t);
        else if (src === 'bf:kline1m')
          hit = await this.tryBfKline1mClose(sym, start, end, t);

        if (hit) {
          this.logger.debug(
            `[resolve] ${sym} @${new Date(t).toISOString()} -> px=${hit.px} ` +
              `source=${hit.source} (ts=${new Date(hit.ts).toISOString()})`,
          );
          return hit;
        }
      } catch (e) {
        this.logger.debug(
          `[getPriceAt] source=${src} sym=${sym} fail: ${(e as Error).message}`,
        );
      }
    }

    this.logger.warn(
      `[resolve] ${sym} @${new Date(t).toISOString()} -> no price found`,
    );
    return null;
  }

  /* ============ 各来源（全部用 readWindowAsObjects） ============ */

  /** mid：ws:{sym}:book 的 bid1/ask1 → (bid+ask)/2 */
  private async tryMidAround(
    sym: string,
    start: number,
    end: number,
    anchor: number,
  ): Promise<Hit | null> {
    const key = this.streams.buildKey(sym, 'book'); // 已前缀
    const rows = await this.streams.readWindowAsObjects(key, start, end, 1000);
    if (!rows?.length) return null;

    let best: Hit | null = null;
    for (const r of rows) {
      const ts = Number(r.ts);
      const bid = Number(r['bid1.px']);
      const ask = Number(r['ask1.px']);
      if (
        !Number.isFinite(ts) ||
        !Number.isFinite(bid) ||
        !Number.isFinite(ask)
      )
        continue;
      if (bid <= 0 || ask <= 0) continue;
      const mid = (bid + ask) / 2;
      const cur: Hit = { px: mid, ts, source: 'mid' };
      if (!best || Math.abs(ts - anchor) < Math.abs(best.ts - anchor))
        best = cur;
    }
    return best;
  }

  /** last：ws:{sym}:trades 的 px */
  private async tryLastAround(
    sym: string,
    start: number,
    end: number,
    anchor: number,
  ): Promise<Hit | null> {
    const key = this.streams.buildKey(sym, 'trades'); // 已前缀
    const rows = await this.streams.readWindowAsObjects(key, start, end, 2000);
    if (!rows?.length) return null;

    let best: Hit | null = null;
    for (const r of rows) {
      const ts = Number(r.ts);
      const px = Number(r.px);
      if (!Number.isFinite(ts) || !Number.isFinite(px) || px <= 0) continue;
      const cur: Hit = { px, ts, source: 'last' };
      if (!best || Math.abs(ts - anchor) < Math.abs(best.ts - anchor))
        best = cur;
    }
    return best;
  }

  /** win:1m：out 流的 1m 聚合 close */
  private async tryWin1mClose(
    sym: string,
    start: number,
    end: number,
    anchor: number,
  ): Promise<Hit | null> {
    const key = this.streams.buildOutStreamKey(sym, 'win:1m'); // 已前缀
    const rows = await this.streams.readWindowAsObjects(key, start, end, 200);
    if (!rows?.length) return null;

    let best: Hit | null = null;
    for (const r of rows) {
      const ts = Number(r.ts);
      const px = Number(r.close ?? r.c);
      if (!Number.isFinite(ts) || !Number.isFinite(px) || px <= 0) continue;
      const cur: Hit = { px, ts, source: 'win:1m' };
      if (!best || Math.abs(ts - anchor) < Math.abs(best.ts - anchor))
        best = cur;
    }
    return best;
  }

  /** ws:kline1m：交易所 WS 的 1m k线 close（原始） */
  private async tryWsKline1mClose(
    sym: string,
    start: number,
    end: number,
    anchor: number,
  ): Promise<Hit | null> {
    const key = this.streams.buildKlineKey(sym, '1m'); // 已前缀
    const rows = await this.streams.readWindowAsObjects(key, start, end, 500);
    if (!rows?.length) return null;

    let best: Hit | null = null;
    for (const r of rows) {
      const ts = Number(r.ts);
      const px = Number(r.c ?? r.close);
      if (!Number.isFinite(ts) || !Number.isFinite(px) || px <= 0) continue;
      const cur: Hit = { px, ts, source: 'ws:kline1m' };
      if (!best || Math.abs(ts - anchor) < Math.abs(best.ts - anchor))
        best = cur;
    }
    return best;
  }

  /** bf:kline1m：离线/回填 1m k线 close（兜底） */
  private async tryBfKline1mClose(
    sym: string,
    start: number,
    end: number,
    anchor: number,
  ): Promise<Hit | null> {
    const key = this.streams.buildOutStreamKey(sym, this.BF_BASE); // 已前缀
    const rows = await this.streams.readWindowAsObjects(key, start, end, 1000);
    if (!rows?.length) return null;

    let best: Hit | null = null;
    for (const r of rows) {
      const ts = Number(r.ts);
      const px = Number(r.c ?? r.close);
      if (!Number.isFinite(ts) || !Number.isFinite(px) || px <= 0) continue;
      const cur: Hit = { px, ts, source: 'bf:kline1m' };
      if (!best || Math.abs(ts - anchor) < Math.abs(best.ts - anchor))
        best = cur;
    }
    return best;
  }
}
