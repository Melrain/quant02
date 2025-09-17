import {
  Injectable,
  Logger,
  OnModuleInit,
  OnModuleDestroy,
} from '@nestjs/common';

import { newWin1m, applyTrade, vwapOf, type Win1m } from './window.state';
import {
  detAggressiveFlow,
  detDeltaStrength,
  detBreakout,
  type IntraSignal,
} from './detectors/intra-detectors';
import { confOf } from './symbol.config';
import { RedisStreamsService } from 'src/redis-streams/redis-streams.service';

type Trade = { ts: number; px: string; qty: string; side?: 'buy' | 'sell' };

// 轻量 EWMA（用于动态阈值参考：近1h |delta3s| 的平滑）
class Ewma {
  private v = 0;
  private inited = false;
  constructor(private readonly alpha = 0.01) {}
  push(x: number) {
    this.v = this.inited ? this.alpha * x + (1 - this.alpha) * this.v : x;
    this.inited = true;
  }
  value() {
    return this.inited ? this.v : 0;
  }
}

@Injectable()
export class WindowWorkerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(WindowWorkerService.name);
  private running = false;

  // 短写 -> instId 映射，兼容已是完整 instId 的情况
  private toInstId(token: string): string {
    const t = token.trim();
    if (!t) return '';
    const u = t.toUpperCase();
    return u.includes('-') ? u : `${u}-USDT-SWAP`;
  }
  // 与 okx-ws.service.ts 一致：优先 OKX_ASSETS（短写），否则 OKX_SYMBOLS（可混用）
  private parseSymbolsFromEnv(): string[] {
    const raw =
      process.env.OKX_ASSETS ??
      process.env.OKX_SYMBOLS ??
      'btc,eth,doge,ltc,shib,pump,wlfi,xpl';
    const list = raw
      .split(',')
      .map((s) => this.toInstId(s))
      .filter(Boolean);
    return Array.from(new Set(list));
  }

  // 订阅标的（来自配置）
  // 订阅标的（来自环境变量；缺少专用配置时用默认 DEFAULT_SYM_CONF）
  private readonly symbols = this.parseSymbolsFromEnv();

  // 参数
  private readonly FLOW_WINDOW_MS = 3_000; // 3s 滑窗
  private readonly PRICE_HIST_N = 50; // 近价缓存长度
  private readonly TF5M_MS = 5 * 60_000;
  private readonly TF15M_MS = 15 * 60_000;

  // 运行态
  private win = new Map<string, Win1m>(); // 当前 1m 窗口
  private win5m = new Map<string, Win1m>();
  private win15m = new Map<string, Win1m>();
  private lastPrices = new Map<string, number[]>(); // 近N价缓存（用于简单趋势/统计）
  private flow3s = new Map<
    string,
    {
      buy: number;
      sell: number;
      buf: Array<{ ts: number; buy: number; sell: number }>;
    }
  >(); // 近3秒滑窗（名义额）
  private ewmaAbsDelta3s = new Map<string, Ewma>(); // 动态阈值参考
  private lastSignalAt = new Map<string, { buy?: number; sell?: number }>(); // 冷却

  constructor(private readonly stream: RedisStreamsService) {}

  async onModuleInit() {
    try {
      // 确保组存在（仅 trades）
      const tradeKeys = this.symbols.map((s) =>
        this.stream.buildKey(s, 'trades'),
      );
      await this.stream.ensureGroups(tradeKeys, 'cg:window', '$');

      // 初始化容器
      for (const s of this.symbols) {
        if (!this.lastPrices.has(s)) this.lastPrices.set(s, []);
        if (!this.flow3s.has(s))
          this.flow3s.set(s, { buy: 0, sell: 0, buf: [] });
        this.ewmaAbsDelta3s.set(s, new Ewma(0.01));
        this.lastSignalAt.set(s, {});
      }

      this.running = true;
      void this.loop();
      this.logger.log(`WindowWorker started for ${this.symbols.join(', ')}`);
    } catch (e) {
      this.logger.error('onModuleInit failed', e);
      this.running = false;
    }
  }

  async onModuleDestroy() {
    this.running = false;
  }

  private getCloseTs(ts: number) {
    return Math.floor(ts / 60_000) * 60_000 + 60_000;
  }

  private async loop() {
    const consumer = `window#${process.pid}`;

    while (this.running) {
      try {
        const keys = this.symbols.map((s) => this.stream.buildKey(s, 'trades'));
        const batch = await this.stream.readGroup({
          group: 'cg:window',
          consumer,
          keys,
          count: 200,
          blockMs: 200,
        });
        if (!batch) continue;

        const msgs = this.stream.normalizeBatch(batch);
        const ackMap = new Map<string, string[]>();

        for (const m of msgs) {
          try {
            if (m.kind !== 'trades') continue;
            const t = this.toTrade(m.payload);
            if (!t) continue;

            // 1) 切桶
            const closeTs = this.getCloseTs(t.ts);
            const cur = this.win.get(m.symbol);
            if (!cur || cur.closeTs !== closeTs) {
              if (cur) {
                const gap = closeTs - cur.closeTs > 60_000;
                await this.seal1m(m.symbol, cur, gap); // 封旧桶（写 Stream + 向上滚）
              }
              this.win.set(m.symbol, newWin1m(closeTs, t.px));
              if (!this.lastPrices.has(m.symbol))
                this.lastPrices.set(m.symbol, []);
              if (!this.flow3s.has(m.symbol))
                this.flow3s.set(m.symbol, { buy: 0, sell: 0, buf: [] });
            }

            // 2) 应用成交
            const w = this.win.get(m.symbol)!;
            applyTrade(w, t.px, t.qty, t.side);

            // 3) 刷新进行中窗口 Hash（关键）
            await this.writeWinState1m(m.symbol, w, t.ts);

            // 4) 维护滑窗/近价
            this.pushFlow3s(m.symbol, t);
            const arr = this.lastPrices.get(m.symbol)!;
            const p = Number(t.px);
            if (Number.isFinite(p)) {
              arr.push(p);
              if (arr.length > this.PRICE_HIST_N) arr.shift();
            }

            // 5) 动态阈值参考（EWMA |delta3s|）
            const flow = this.flow3s.get(m.symbol)!;
            this.ewmaAbsDelta3s
              .get(m.symbol)!
              .push(Math.abs(flow.buy - flow.sell));

            // 6) 检测并写信号（带冷却）
            await this.detectAndEmit(m.symbol, w, t.ts);

            // 7) ACK
            if (!ackMap.has(m.key)) ackMap.set(m.key, []);
            ackMap.get(m.key)!.push(m.id);
          } catch (e) {
            this.logger.warn(
              `process message failed: ${m?.id ?? ''} -> ${(e as Error).message}`,
            );
          }
        }

        for (const [key, ids] of ackMap)
          if (ids.length) await this.stream.ack(key, 'cg:window', ids);
      } catch (e) {
        this.logger.error('loop iteration error', e);
      }
    }
  }

  private toTrade(p: Record<string, string>): Trade | null {
    const ts = Number(p.ts);
    const px = Number(p.px);
    const qty = Number(p.qty);
    if (!Number.isFinite(ts) || !Number.isFinite(px) || !Number.isFinite(qty))
      return null;
    return { ts, px: p.px, qty: p.qty, side: (p.side as any) ?? undefined };
  }

  /** 名义额滑窗（按币种乘数换算） */
  private pushFlow3s(sym: string, t: Trade) {
    const f = this.flow3s.get(sym)!;
    const conf = confOf(sym);
    const px = Number(t.px);
    const qty = Number(t.qty);
    const notional =
      Number.isFinite(px) && Number.isFinite(qty)
        ? px * qty * conf.contractMultiplier
        : 0;
    const buy = t.side === 'buy' ? notional : 0;
    const sell = t.side === 'sell' ? notional : 0;

    f.buf.push({ ts: t.ts, buy, sell });
    f.buy += buy;
    f.sell += sell;

    const cutoff = t.ts - this.FLOW_WINDOW_MS;
    while (f.buf.length && f.buf[0].ts < cutoff) {
      const x = f.buf.shift()!;
      f.buy -= x.buy;
      f.sell -= x.sell;
    }
  }

  private async detectAndEmit(sym: string, w: Win1m, ts: number) {
    const conf = confOf(sym);
    const f = this.flow3s.get(sym)!;
    const arr = this.lastPrices.get(sym)!;
    const dyn = this.ewmaAbsDelta3s.get(sym)!.value();

    const ctx = {
      now: ts,
      sym,
      win: w,
      lastPrices: arr,
      buyNotional3s: f.buy,
      sellNotional3s: f.sell,
      minNotional3s: conf.minNotional3s,
      breakoutBandPct: conf.breakoutBandPct,
      dynAbsDelta: dyn,
      dynDeltaK: conf.dynDeltaK,
    };

    let best: IntraSignal | null = null;
    for (const det of [detAggressiveFlow, detDeltaStrength, detBreakout]) {
      const s = det(ctx as any);
      if (s && (!best || Number(s.strength) > Number(best.strength))) best = s;
    }
    if (!best) return;

    // 冷却（同方向）
    const last = this.lastSignalAt.get(sym)!;
    const lastTs = best.dir === 'buy' ? (last.buy ?? 0) : (last.sell ?? 0);
    if (ts - lastTs < conf.cooldownMs) return;

    const signalKey = this.stream.buildOutKey(sym, 'signal:detected');
    await this.stream.xadd(
      signalKey,
      {
        ts: String(best.ts),
        kind: 'intra',
        instId: sym,
        dir: best.dir,
        strength: best.strength,
        'evidence.delta3s': best.evidence.delta3s ?? '',
        'evidence.buyShare3s': best.evidence.buyShare3s ?? '',
        'evidence.notional3s': best.evidence.notional3s ?? '',
        'evidence.breakout': best.evidence.breakout ?? '',
        'evidence.band': best.evidence.band ?? '',
        'evidence.eps': best.evidence.eps ?? '',
        strategyId: 'intra.v1',
        ttlMs: String(Math.max(3000, conf.cooldownMs)),
      },
      { maxlenApprox: 5_000 },
    );

    if (best.dir === 'buy') last.buy = ts;
    else last.sell = ts;
  }

  /** 进行中 1m 窗口 Hash（win:state:1m:{sym}） */
  private async writeWinState1m(sym: string, w: Win1m, nowTs: number) {
    const key = this.stream.buildOutKey(sym, 'win:state:1m');
    await this.stream.hset(key, {
      startTs: String(w.startTs),
      closeTs: String(w.closeTs),
      updatedTs: String(nowTs),
      open: w.open,
      high: w.high,
      low: w.low,
      last: w.last,
      vol: String(w.vol),
      vbuy: String(w.vbuy),
      vsell: String(w.vsell),
      vwapNum: String(w.vwapNum),
      vwapDen: String(w.vwapDen),
      tickN: String(w.tickN),
    });
    await this.stream.expire(key, 10 * 60); // 10 分钟（秒）
  }

  /** 封 1m 条（写 win:1m）并向上滚 5m/15m */
  private async seal1m(sym: string, s: Win1m, gap: boolean) {
    const vwap = vwapOf(s);
    const key = this.stream.buildOutKey(sym, 'win:1m');
    await this.stream.xadd(
      key,
      {
        ts: String(s.closeTs),
        open: s.open,
        high: s.high,
        low: s.low,
        close: s.last,
        vol: String(s.vol),
        vbuy: String(s.vbuy),
        vsell: String(s.vsell),
        vwap: String(vwap),
        tickN: String(s.tickN),
        gap: gap ? '1' : '0',
      },
      { maxlenApprox: 2000 },
    );

    await this.rollUpFrom1m(sym, s);
  }

  // === 1m → 5m/15m 滚动 ===
  private async rollUpFrom1m(sym: string, m1: Win1m) {
    await this.rollUpGeneric(sym, m1, '5m', this.TF5M_MS, this.win5m);
    await this.rollUpGeneric(sym, m1, '15m', this.TF15M_MS, this.win15m);
  }

  private async rollUpGeneric(
    sym: string,
    m1: Win1m,
    tfName: '5m' | '15m',
    tfMs: number,
    store: Map<string, Win1m>,
  ) {
    const tfClose = this.getTfCloseFrom1mClose(m1.closeTs, tfMs);
    let w = store.get(sym);

    // 新 TF 边界：封旧 TF
    if (w && w.closeTs !== tfClose) {
      const gap = tfClose - w.closeTs > tfMs;
      await this.sealTf(sym, w, tfName, gap);
      w = undefined;
    }

    // 无 TF 窗口：创建
    if (!w) {
      w = this.newWinSpan(tfClose, m1.open, tfMs);
      store.set(sym, w);
    }

    // 累积 1m → TF
    this.applyBarToWin(w, m1);

    // 刷新进行中 TF Hash
    await this.writeWinStateTf(sym, w, tfName, Date.now());
  }

  private async sealTf(
    sym: string,
    s: Win1m,
    tfName: '5m' | '15m',
    gap: boolean,
  ) {
    const vwap = vwapOf(s);
    const key = this.stream.buildOutKey(sym, `win:${tfName}`);
    await this.stream.xadd(
      key,
      {
        ts: String(s.closeTs),
        open: s.open,
        high: s.high,
        low: s.low,
        close: s.last,
        vol: String(s.vol),
        vbuy: String(s.vbuy),
        vsell: String(s.vsell),
        vwap: String(vwap),
        tickN: String(s.tickN),
        gap: gap ? '1' : '0',
      },
      { maxlenApprox: 2000 },
    );
  }

  private async writeWinStateTf(
    sym: string,
    w: Win1m,
    tfName: '5m' | '15m',
    nowTs: number,
  ) {
    const key = this.stream.buildOutKey(sym, `win:state:${tfName}`);
    await this.stream.hset(key, {
      startTs: String(w.startTs),
      closeTs: String(w.closeTs),
      updatedTs: String(nowTs),
      open: w.open,
      high: w.high,
      low: w.low,
      last: w.last,
      vol: String(w.vol),
      vbuy: String(w.vbuy),
      vsell: String(w.vsell),
      vwapNum: String(w.vwapNum),
      vwapDen: String(w.vwapDen),
      tickN: String(w.tickN),
    });
    await this.stream.expire(key, 10 * 60);
  }

  private getTfCloseFrom1mClose(closeTs1m: number, tfMs: number) {
    // 把 1m close 对齐到上位 TF 的 close（上取整到 tf 边界）
    return Math.floor((closeTs1m - 1) / tfMs) * tfMs + tfMs;
  }

  private newWinSpan(closeTs: number, firstPx: string, spanMs: number): Win1m {
    return {
      startTs: closeTs - spanMs,
      closeTs,
      open: firstPx,
      high: firstPx,
      low: firstPx,
      last: firstPx,
      vol: 0,
      vbuy: 0,
      vsell: 0,
      vwapNum: 0,
      vwapDen: 0,
      tickN: 0,
    };
  }

  private applyBarToWin(dst: Win1m, srcBar: Win1m) {
    dst.last = srcBar.last;
    if (Number(srcBar.high) > Number(dst.high)) dst.high = srcBar.high;
    if (Number(srcBar.low) < Number(dst.low)) dst.low = srcBar.low;

    dst.vol += srcBar.vol;
    dst.vbuy += srcBar.vbuy;
    dst.vsell += srcBar.vsell;
    dst.tickN += srcBar.tickN;

    dst.vwapNum += srcBar.vwapNum;
    dst.vwapDen += srcBar.vwapDen;
  }
}
