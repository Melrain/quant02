/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable @typescript-eslint/no-unsafe-return */

// src/window/window-worker.service.ts
// 负责：
//  - 读 trades 流，按 1m 切桶并写 win:1m 流
//  - 滑窗统计近3s 名义额流入（买/卖）
//  - 缓存近价（用于简单趋势/统计）
//  - 检测信号（基于近3s 流入 + 近价行为）
//  - 写信号流 signal:detected:{sym}
//  - 写进行中窗口 Hash（win:state:1m/{sym}，以及 5m/15m）
//  - 向上滚动 5m/15m 窗口（win:5m/15m + win:state:5m/15m/{sym})
//
// 依赖：Redis Streams + Hash；单进程单消费者

import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { RedisStreamsService } from 'src/redis-streams/redis-streams.service';
import { parseSymbolsFromEnv } from 'src/utils/utils';

// 窗口聚合
import { newWin1m, applyTrade, vwapOf, type Win1m } from './window.state';

// 检测 & 聚合
import type { DetectorCtx } from './detectors/intra-detectors';
import { IntraAggregator } from './detectors/IntraAggregator';

// 动态参数（由 symbol.config.ts 汇总基础配置 + dyn gate）
import { confOf, type SymbolConfig } from './symbol.config';

type EffectiveParams = Required<SymbolConfig>;

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

  // 标的列表
  private readonly symbols = parseSymbolsFromEnv();

  // 固定参数
  private readonly FLOW_WINDOW_MS = 3_000; // 3s 滑窗
  private readonly PRICE_HIST_N = 50; // 近价缓存长度
  private readonly TF5M_MS = 5 * 60_000;
  private readonly TF15M_MS = 15 * 60_000;

  // 运行态
  private win = new Map<string, Win1m>(); // 当前 1m
  private win5m = new Map<string, Win1m>();
  private win15m = new Map<string, Win1m>();

  private lastPrices = new Map<string, number[]>(); // 近N价
  private flow3s = new Map<
    string,
    {
      buy: number;
      sell: number;
      buf: Array<{ ts: number; buy: number; sell: number }>;
      maxTs: number; // 已见到的最大事件时间（严格窗口剪裁）
    }
  >();

  private ewmaAbsDelta3s = new Map<string, Ewma>(); // |delta3s| 平滑
  private aggs = new Map<string, IntraAggregator>(); // 每标的一套聚合器（含冷却/去重）

  // 生效参数缓存 + 变化日志去抖
  private effCache = new Map<string, { ts: number; eff: EffectiveParams }>();
  private lastEffSnapshot = new Map<string, string>(); // sym -> JSON

  constructor(
    private readonly streams: RedisStreamsService,
    private readonly eventEmitter: EventEmitter2,
  ) {}

  /* --------------------------------- 生命周期 -------------------------------- */

  async onModuleInit() {
    try {
      // 确保消费组存在（trades）
      const tradeKeys = this.symbols.map((s) =>
        this.streams.buildKey(s, 'trades'),
      );
      await this.streams.ensureGroups(tradeKeys, 'cg:window', '$');

      // 初始化 per-symbol 状态
      const now = Date.now();
      for (const s of this.symbols) {
        this.lastPrices.set(s, []);
        this.flow3s.set(s, { buy: 0, sell: 0, buf: [], maxTs: 0 });
        this.ewmaAbsDelta3s.set(s, new Ewma(0.01));

        const eff = this.getEff(s, now);
        this.logger.log(
          `[param.effective:init] ${s} ` +
            `cm=${eff.contractMultiplier} minNotional3s=${eff.minNotional3s} ` +
            `cooldown=${eff.cooldownMs} dedup=${eff.dedupMs} ` +
            `minStrength=${eff.minStrength} cb=${eff.consensusBoost} ` +
            `band=${eff.breakoutBandPct} K=${eff.dynDeltaK} liqK=${eff.liqK}`,
        );

        this.aggs.set(
          s,
          new IntraAggregator({
            cooldownMs: eff.cooldownMs,
            dedupMs: eff.dedupMs,
            minStrength: eff.minStrength,
          }),
        );

        // 保存初始快照，避免第一条成交就“变化”
        this.lastEffSnapshot.set(s, this.snapshotStr(eff));
      }

      this.running = true;
      void this.loop();
      this.logger.log(`WindowWorker started for ${this.symbols.join(', ')}`);
    } catch (e) {
      this.logger.error('onModuleInit failed', e as Error);
      this.running = false;
    }
  }

  async onModuleDestroy() {
    this.running = false;
  }

  /* ----------------------------------- 主循环 -------------------------------- */

  private async loop() {
    const consumer = `window#${process.pid}`;
    while (this.running) {
      try {
        const keys = this.symbols.map((s) =>
          this.streams.buildKey(s, 'trades'),
        );
        const batch = await this.streams.readGroup({
          group: 'cg:window',
          consumer,
          keys,
          count: 200,
          blockMs: 200,
        });
        if (!batch) continue;

        const msgs = this.streams.normalizeBatch(batch);
        const ackMap = new Map<string, string[]>();

        for (const m of msgs) {
          try {
            if (m.kind !== 'trades') continue;
            const t = this.toTrade(m.payload);
            if (!t) continue;

            // 读取生效参数（带 1s 本地缓存）
            const eff = this.getEff(m.symbol, Date.now());
            this.logEffChangeIfNeeded(m.symbol, eff);

            // 1) 1m 切桶
            const closeTs = this.getCloseTs(t.ts);
            const cur = this.win.get(m.symbol);
            if (!cur || cur.closeTs !== closeTs) {
              if (cur) {
                const gap = closeTs - cur.closeTs > 60_000;
                await this.seal1m(m.symbol, cur, gap); // 封旧桶，并向上滚
              }
              this.win.set(m.symbol, newWin1m(closeTs, t.px));
              // 确保容器存在
              if (!this.lastPrices.has(m.symbol))
                this.lastPrices.set(m.symbol, []);
              if (!this.flow3s.has(m.symbol))
                this.flow3s.set(m.symbol, {
                  buy: 0,
                  sell: 0,
                  buf: [],
                  maxTs: 0,
                });
            }

            // 2) 应用成交
            const w = this.win.get(m.symbol)!;
            applyTrade(w, t.px, t.qty, t.side);

            // 3) 刷“进行中 1m 状态” Hash
            await this.writeWinState1m(m.symbol, w, t.ts);

            // 4) 维护 3s 滑窗与近价
            this.pushFlow3s(m.symbol, t, eff);
            const arr = this.lastPrices.get(m.symbol)!;
            const p = Number(t.px);
            if (Number.isFinite(p)) {
              arr.push(p);
              if (arr.length > this.PRICE_HIST_N) arr.shift();
            }

            // 5) 动态阈值 EWMA(|delta3s|)
            const f = this.flow3s.get(m.symbol)!;
            this.ewmaAbsDelta3s.get(m.symbol)!.push(Math.abs(f.buy - f.sell));

            // 6) 检测 & 输出信号
            await this.detectAndEmit(m.symbol, w, t.ts, eff);

            // 7) ACK
            if (!ackMap.has(m.key)) ackMap.set(m.key, []);
            ackMap.get(m.key)!.push(m.id);
          } catch (e) {
            this.logger.warn(
              `process message failed: ${m?.id ?? ''} -> ${(e as Error).message}`,
            );
          }
        }

        // 统一 ACK
        for (const [key, ids] of ackMap)
          if (ids.length) await this.streams.ack(key, 'cg:window', ids);
      } catch (e) {
        this.logger.error('loop iteration error', e as Error);
      }
    }
  }

  /* --------------------------------- 工具方法 -------------------------------- */

  private getCloseTs(ts: number) {
    return Math.floor(ts / 60_000) * 60_000 + 60_000;
  }

  private toTrade(p: Record<string, string>): Trade | null {
    const ts = Number(p.ts);
    const px = Number(p.px);
    const qty = Number(p.qty);
    if (!Number.isFinite(ts) || !Number.isFinite(px) || !Number.isFinite(qty))
      return null;
    return { ts, px: p.px, qty: p.qty, side: (p.side as any) ?? undefined };
  }

  private snapshotStr(eff: EffectiveParams) {
    return JSON.stringify({
      cm: eff.contractMultiplier,
      minN: eff.minNotional3s,
      cd: eff.cooldownMs,
      dd: eff.dedupMs,
      ms: eff.minStrength,
      cb: eff.consensusBoost,
      band: eff.breakoutBandPct,
      K: eff.dynDeltaK,
      lq: eff.liqK,
    });
  }

  private logEffChangeIfNeeded(sym: string, eff: EffectiveParams) {
    const nowStr = this.snapshotStr(eff);
    const prev = this.lastEffSnapshot.get(sym);
    if (nowStr !== prev) {
      this.lastEffSnapshot.set(sym, nowStr);
      this.logger.log(`[param.effective:change] ${sym} ${nowStr}`);
    }
  }

  /** 1s 本地缓存：合并 symbol.config.ts & dyn gate 的生效参数 */
  private getEff(sym: string, now: number): EffectiveParams {
    const c = this.effCache.get(sym);
    if (c && now - c.ts < 1000) return c.eff;
    const eff = confOf(sym);
    this.effCache.set(sym, { ts: now, eff });
    return eff;
  }

  /** 3s 名义额滑窗（严格窗口：使用 maxTs 剪裁） */
  private pushFlow3s(sym: string, t: Trade, eff: EffectiveParams) {
    const f = this.flow3s.get(sym)!;
    f.maxTs = Math.max(f.maxTs, t.ts);

    const px = Number(t.px);
    const qty = Number(t.qty);
    const notional =
      Number.isFinite(px) && Number.isFinite(qty)
        ? px * qty * eff.contractMultiplier
        : 0;
    const buy = t.side === 'buy' ? notional : 0;
    const sell = t.side === 'sell' ? notional : 0;

    const cutoff = f.maxTs - this.FLOW_WINDOW_MS;

    // 只在事件仍落在窗口内时计入（避免乱序导致窗口膨胀）
    if (t.ts >= cutoff) {
      f.buf.push({ ts: t.ts, buy, sell });
      f.buy += buy;
      f.sell += sell;
    }

    // 严格剪裁窗口左侧
    while (f.buf.length && f.buf[0].ts < cutoff) {
      const x = f.buf.shift()!;
      f.buy -= x.buy;
      f.sell -= x.sell;
    }
  }

  /* ------------------------------ 检测 & 输出信号 ------------------------------ */

  private async detectAndEmit(
    sym: string,
    w: Win1m,
    ts: number,
    eff: EffectiveParams,
  ) {
    // 近3s买/卖名义额
    const f = this.flow3s.get(sym)!;
    // 近价序列
    const arr = this.lastPrices.get(sym)!;
    // 动态阈值参考：EWMA(|delta3s|)
    const dyn = this.ewmaAbsDelta3s.get(sym)!.value();

    // 统一检测上下文
    const ctx: DetectorCtx = {
      now: ts,
      sym,
      win: w,
      lastPrices: arr,
      buyNotional3s: f.buy,
      sellNotional3s: f.sell,
      minNotional3s: eff.minNotional3s,
      breakoutBandPct: eff.breakoutBandPct,
      dynAbsDelta: dyn,
      dynDeltaK: eff.dynDeltaK,
      liqK: eff.liqK,
    };

    // 聚合器（内含：多路 detector + 冷却 + 去重 + 共识）
    const agg = this.aggs.get(sym)!;
    const best = agg.detect(ctx);
    if (!best) return;

    // 日志（结构化）
    const sum3s = f.buy + f.sell;
    const paramStr =
      `cm=${eff.contractMultiplier} minNotional3s=${eff.minNotional3s} ` +
      `band=${eff.breakoutBandPct} dynK=${eff.dynDeltaK} liqK=${eff.liqK} ` +
      `minStrength=${eff.minStrength} cb=${eff.consensusBoost} ` +
      `cooldown=${eff.cooldownMs} dedup=${eff.dedupMs} dynAbsDelta=${dyn.toFixed(0)}`;

    this.logger.log(
      `[signal] inst=${sym} dir=${best.dir} strength=${best.strength} src=${best.evidence?.src ?? ''} ` +
        `notional3s=${sum3s.toFixed(0)} (buy=${f.buy.toFixed(0)} sell=${f.sell.toFixed(0)} min=${eff.minNotional3s}) ` +
        `delta3s=${best.evidence?.delta3s ?? ''} zLike=${best.evidence?.zLike ?? ''} ` +
        `buyShare3s=${best.evidence?.buyShare3s ?? ''} breakout=${best.evidence?.breakout ?? ''} ` +
        `| params: ${paramStr}`,
    );

    // 输出到 Redis Stream（跨进程订阅）
    const signalKey = this.streams.buildOutKey(sym, 'signal:detected');
    await this.streams.xadd(
      signalKey,
      {
        ts: String(best.ts),
        kind: 'intra',
        instId: sym,
        dir: best.dir,
        strength: best.strength,
        'evidence.src': best.evidence?.src ?? '',
        'evidence.delta3s': best.evidence?.delta3s ?? '',
        'evidence.zLike': best.evidence?.zLike ?? '',
        'evidence.buyShare3s': best.evidence?.buyShare3s ?? '',
        'evidence.notional3s': best.evidence?.notional3s ?? '',
        'evidence.breakout': best.evidence?.breakout ?? '',
        'evidence.band': best.evidence?.band ?? '',
        'evidence.eps': best.evidence?.eps ?? '',
        strategyId: 'intra.v1',
        ttlMs: String(Math.max(3000, eff.cooldownMs)),
      },
      { maxlenApprox: 5_000 },
    );

    // 进程内广播
    this.eventEmitter.emit('signal.detected', {
      ts: best.ts,
      kind: 'intra',
      instId: sym,
      dir: best.dir,
      strength: best.strength,
      evidence: best.evidence,
      strategyId: 'intra.v1',
    });
  }

  /* ------------------------------ 写入窗口状态/滚动 ----------------------------- */

  /** 进行中 1m 窗口 Hash（win:state:1m:{sym}） */
  private async writeWinState1m(sym: string, w: Win1m, nowTs: number) {
    const key = this.streams.buildOutKey(sym, 'win:state:1m');
    await this.streams.hset(key, {
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
    await this.streams.expire(key, 10 * 60); // sec
  }

  /** 封 1m 条（写 win:1m）并向上滚 5m/15m */
  private async seal1m(sym: string, s: Win1m, gap: boolean) {
    const vwap = vwapOf(s);
    const key = this.streams.buildOutKey(sym, 'win:1m');
    await this.streams.xadd(
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

  // 1m → 5m/15m
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
    const tfClose = Math.floor((m1.closeTs - 1) / tfMs) * tfMs + tfMs;
    let w = store.get(sym);

    // 新 TF 边界：封旧 TF
    if (w && w.closeTs !== tfClose) {
      const gap = tfClose - w.closeTs > tfMs;
      await this.sealTf(sym, w, tfName, gap);
      w = undefined as any;
    }

    // 无 TF 窗口：创建
    if (!w) {
      w = {
        startTs: tfClose - tfMs,
        closeTs: tfClose,
        open: m1.open,
        high: m1.open,
        low: m1.open,
        last: m1.open,
        vol: 0,
        vbuy: 0,
        vsell: 0,
        vwapNum: 0,
        vwapDen: 0,
        tickN: 0,
      };
      store.set(sym, w);
    }

    // 累积 1m → TF
    w.last = m1.last;
    if (Number(m1.high) > Number(w.high)) w.high = m1.high;
    if (Number(m1.low) < Number(w.low)) w.low = m1.low;
    w.vol += m1.vol;
    w.vbuy += m1.vbuy;
    w.vsell += m1.vsell;
    w.tickN += m1.tickN;
    w.vwapNum += m1.vwapNum;
    w.vwapDen += m1.vwapDen;

    // 刷进行中 TF Hash
    await this.writeWinStateTf(sym, w, tfName, Date.now());
  }

  private async sealTf(
    sym: string,
    s: Win1m,
    tfName: '5m' | '15m',
    gap: boolean,
  ) {
    const vwap = vwapOf(s);
    const key = this.streams.buildOutKey(sym, `win:${tfName}`);
    await this.streams.xadd(
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
    const key = this.streams.buildOutKey(sym, `win:state:${tfName}`);
    await this.streams.hset(key, {
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
    await this.streams.expire(key, 10 * 60); // sec
  }
}
