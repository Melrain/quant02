// window-worker.service.ts
// 负责：
//  - 读 trades 流，按 1m 切桶并写 win:1m 流
//  - 滑窗统计近3s 名义额流入（买/卖）
//  - 缓存近价（用于简单趋势/统计）
//  - 检测信号（基于近3s 流入 + 近价行为）
//  - 写信号流 win:signal:detected
//  - 写进行中窗口 Hash（win:state:1m/{sym}，以及 5m/15m）
//  - 向上滚动 5m/15m 窗口（win:5m/15m + win:state:5m/15m/{sym}）
//
// 基于 Redis Streams + Hash 实现，单进程单消费者
//
// 参数：
//  - 标的列表：环境变量 OKX_ASSETS（短写，优先）或 OKX_SYMBOLS（可混用）
//  - 窗口参数：常量
//  - 信号检测参数：来自 symbol.config.ts（可按标的覆盖）
//
// 注意：
//  - 本模块不直接与交易所交互，依赖 Redis Streams 作为数据总线
//  - 本模块不直接与交易模块交互，信号通过 Redis Stream 输出，供其他模块订阅
//  - 本模块不负责持久化任何运行态，所有中间态均存于 Redis（可重启恢复）
//  - 本模块假设单进程单消费者运行（无分片），否则会有重复信号
//  - 本模块假设系统时钟单调递增，否则可能出现异常
//  - 本模块假设 Redis 可用且性能良好，否则会阻塞

import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { newWin1m, applyTrade, vwapOf, type Win1m } from './window.state';
import type { DetectorCtx } from './detectors/intra-detectors';
import { IntraAggregator } from './detectors/IntraAggregator';
import { RedisStreamsService } from 'src/redis-streams/redis-streams.service';
import { EventEmitter2 } from '@nestjs/event-emitter';
// 新增：从 symbol.config.ts 引入动态参数
// 新增：从 symbol.config.ts 引入动态参数
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
  //临时变量，储存近3秒买卖名义额滑窗
  private flow3s = new Map<
    string,
    {
      buy: number;
      sell: number;
      buf: Array<{ ts: number; buy: number; sell: number }>;
      maxTs: number; // 该标的已见到的最大事件时间，用于严格剪裁
    }
  >(); // 近3秒滑窗（名义额）
  private ewmaAbsDelta3s = new Map<string, Ewma>(); // 动态阈值参考
  private aggs = new Map<string, IntraAggregator>(); // 每个标的一个聚合器（冷却/去重/共识）

  // 参数快照：仅当变化时打印
  private lastEffSnapshot = new Map<string, string>(); // sym -> JSON

  // 新增：1s 本地缓存
  private effCache = new Map<string, { ts: number; eff: EffectiveParams }>();
  private getEff(sym: string, now: number): EffectiveParams {
    const c = this.effCache.get(sym);
    if (c && now - c.ts < 1000) return c.eff;
    const eff = confOf(sym);
    this.effCache.set(sym, { ts: now, eff });
    return eff;
  }

  constructor(
    private readonly stream: RedisStreamsService,
    private readonly eventEmitter: EventEmitter2,
  ) {}

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
          this.flow3s.set(s, { buy: 0, sell: 0, buf: [], maxTs: 0 });
        this.ewmaAbsDelta3s.set(s, new Ewma(0.01));

        // 使用动态参数初始化聚合器
        const eff = this.getEff(s, Date.now());
        this.logger.log(
          `[param.effective:init] ${s} ` +
            `cm=${eff.contractMultiplier} ` +
            `minNotional3s=${eff.minNotional3s} ` +
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

        // 记录首个快照（避免启动后第一条 trade 又打印一次 change）
        this.lastEffSnapshot.set(
          s,
          JSON.stringify({
            cm: eff.contractMultiplier,
            minN: eff.minNotional3s,
            cd: eff.cooldownMs,
            dd: eff.dedupMs,
            ms: eff.minStrength,
            cb: eff.consensusBoost,
            band: eff.breakoutBandPct,
            K: eff.dynDeltaK,
            lq: eff.liqK,
          }),
        );
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

            // 读取该符号的动态参数（带 1s 本地缓存）
            const eff = this.getEff(m.symbol, Date.now());
            // 打印“参数变化”日志（不刷屏）
            const snapNow = JSON.stringify({
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
            const snapPrev = this.lastEffSnapshot.get(m.symbol);
            if (snapPrev !== snapNow) {
              this.lastEffSnapshot.set(m.symbol, snapNow);
              this.logger.log(
                `[param.effective:change] ${m.symbol} ${snapNow}`,
              );
            }

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

            // 3) 刷新进行中窗口 Hash
            await this.writeWinState1m(m.symbol, w, t.ts);

            // 4) 维护滑窗/近价（使用动态参数）
            this.pushFlow3s(m.symbol, t, eff);
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

            // 6) 检测并写信号（聚合器内含冷却/去重），使用动态阈值参数
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

        for (const [key, ids] of ackMap)
          if (ids.length) await this.stream.ack(key, 'cg:window', ids);
      } catch (e) {
        this.logger.error('loop iteration error', e as Error);
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

  /** 名义额滑窗（按币种乘数换算，使用动态参数） */
  private pushFlow3s(sym: string, t: Trade, eff: EffectiveParams) {
    const f = this.flow3s.get(sym)!;
    // 使用已见到的最大事件时间做剪裁，避免乱序/延迟污染窗口
    f.maxTs = Math.max(f.maxTs, t.ts);
    const px = Number(t.px);
    const qty = Number(t.qty);
    const notional =
      Number.isFinite(px) && Number.isFinite(qty)
        ? px * qty * eff.contractMultiplier
        : 0;
    const buy = t.side === 'buy' ? notional : 0;
    const sell = t.side === 'sell' ? notional : 0;

    f.buf.push({ ts: t.ts, buy, sell });
    f.buy += buy;
    f.sell += sell;

    const cutoff = f.maxTs - this.FLOW_WINDOW_MS;
    // 丢弃早于窗口的迟到数据，保持严格 3s 语义
    if (t.ts >= cutoff) {
      f.buf.push({ ts: t.ts, buy, sell });
      f.buy += buy;
      f.sell += sell;
    }
    while (f.buf.length && f.buf[0].ts < cutoff) {
      const x = f.buf.shift()!;
      f.buy -= x.buy;
      f.sell -= x.sell;
    }
  }

  /**
   * 基于当前 1m 窗口与近3秒名义额流入，检测盘中信号并输出。
   *
   * 调用时机：
   *  - 每条成交在已应用到 1m 窗口（applyTrade）、滑窗（pushFlow3s）、近价之后调用。
   *
   * 输入参数：
   *  - sym: 标的 instId
   *  - w:   当前“进行中”的 1m 窗口（包含最新 OHLC、成交量、VWAP 累计等）
   *  - ts:  当前成交时间戳（毫秒）
   *  - eff: 生效的动态参数（来自 symbol.config.ts；包含 minNotional3s、band、dynDeltaK、liqK 等）
   *
   * 依赖运行态：
   *  - this.flow3s.get(sym): 近3秒买/卖名义额滑窗（单位：计价币名义额）
   *  - this.lastPrices.get(sym): 近价序列（用于简单趋势/统计）
   *  - this.ewmaAbsDelta3s.get(sym): |delta3s| 的 EWMA，作为动态阈值参考（越大代表近期波动/流入越活跃）
   *
   * 流程：
   *  1) 汇集上下文（DetectorCtx）：当前时间、标的、进行中 1m、近价、3s 买/卖名义额、动态/静态阈值。
   *  2) 调用聚合器 detect(ctx)：内部做多路检测，并带冷却与去重，输出最优信号 best 或 null。
   *  3) 若产生信号：
   *     - 打印一条可读性较强的日志，便于回放与排障；
   *     - 写入 Redis Stream: win:{sym}:signal:detected（通过 buildOutKey），包含 evidence.* 细节；
   *     - 通过事件总线 eventEmitter.emit('signal.detected', ...) 广播给进程内订阅者。
   *
   * 注意：
   *  - 冷却/去重在 IntraAggregator 内部完成，避免在高频成交下重复触发。
   *  - ttlMs 取 max(3000, cooldownMs)，保证下游在冷却期内能感知该信号。
   *  - evidence.* 字段是探测过程中的解释性指标（如 delta3s、zLike、buyShare3s、breakout、band、eps 等），便于分析。
   */
  private async detectAndEmit(
    sym: string,
    w: Win1m,
    ts: number,
    eff: EffectiveParams,
  ) {
    // 近3秒买/卖名义额统计（pushFlow3s 已更新）
    const f = this.flow3s.get(sym)!;
    // 近价序列（用于简单趋势/统计）
    const arr = this.lastPrices.get(sym)!;
    // 动态阈值参考：|delta3s| 的 EWMA（近似近1h），反映近期活跃度/波动强度
    const dyn = this.ewmaAbsDelta3s.get(sym)!.value();

    // 组装检测上下文，供聚合器使用
    const ctx: DetectorCtx = {
      now: ts, // 当前检测时刻（毫秒）
      sym, // 标的
      win: w, // 当前 1m 窗口（进行中）
      lastPrices: arr, // 近价序列
      buyNotional3s: f.buy, // 近3s 买入名义额
      sellNotional3s: f.sell, // 近3s 卖出名义额
      minNotional3s: eff.minNotional3s, // 最小活跃度门槛
      breakoutBandPct: eff.breakoutBandPct, // 突破带宽参数（相对）
      dynAbsDelta: dyn, // 动态阈值：EWMA(|delta3s|)
      dynDeltaK: eff.dynDeltaK, // 动态阈值缩放系数
      liqK: eff.liqK, // 流动性相关系数（用于放大/缩小阈值）
    };

    // 聚合器内含：多路 detector + 冷却 + 去重 + 共识规则
    const agg = this.aggs.get(sym)!;
    const best = agg.detect(ctx);
    if (!best) return; // 无信号则退出

    // 统计项仅用于日志展示（总名义额与买/卖拆分）
    const sum3s = f.buy + f.sell;
    // 参数快照字符串（便于回放定位阈值）
    const paramStr =
      `cm=${eff.contractMultiplier} minNotional3s=${eff.minNotional3s} ` +
      `band=${eff.breakoutBandPct} dynK=${eff.dynDeltaK} liqK=${eff.liqK} ` +
      `minStrength=${eff.minStrength} cb=${eff.consensusBoost} ` +
      `cooldown=${eff.cooldownMs} dedup=${eff.dedupMs} dynAbsDelta=${dyn.toFixed(0)}`;

    // 结构化日志，便于检索/复盘
    this.logger.log(
      `[signal] inst=${sym} dir=${best.dir} strength=${best.strength} src=${best.evidence.src ?? ''} ` +
        `notional3s=${sum3s.toFixed(0)} (buy=${f.buy.toFixed(0)} sell=${f.sell.toFixed(0)} min=${eff.minNotional3s}) ` +
        `delta3s=${best.evidence.delta3s ?? ''} zLike=${best.evidence.zLike ?? ''} ` +
        `buyShare3s=${best.evidence.buyShare3s ?? ''} breakout=${best.evidence.breakout ?? ''} ` +
        `| params: ${paramStr}`,
    );

    // 输出到 Redis Stream：供跨进程/跨语言模块订阅
    const signalKey = this.stream.buildOutKey(sym, 'signal:detected');
    await this.stream.xadd(
      signalKey,
      {
        ts: String(best.ts), // 信号时间（通常等于 ctx.now）
        kind: 'intra', // 策略大类：盘中
        instId: sym, // 标的
        dir: best.dir, // 方向：buy/sell
        strength: best.strength, // 强度：由聚合器评估
        // 解释性证据（各 detector 的核心指标）
        'evidence.src': best.evidence.src ?? '',
        'evidence.delta3s': best.evidence.delta3s ?? '',
        'evidence.zLike': best.evidence.zLike ?? '',
        'evidence.buyShare3s': best.evidence.buyShare3s ?? '',
        'evidence.notional3s': best.evidence.notional3s ?? '',
        'evidence.breakout': best.evidence.breakout ?? '',
        'evidence.band': best.evidence.band ?? '',
        'evidence.eps': best.evidence.eps ?? '',
        strategyId: 'intra.v1', // 策略/版本标识
        ttlMs: String(Math.max(3000, eff.cooldownMs)), // 建议下游视为“有效期”
      },
      { maxlenApprox: 5_000 }, // 控制流长度，避免无限增长
    );

    // 进程内事件广播（Nest 事件总线），便于本进程内的其他服务响应
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
}
