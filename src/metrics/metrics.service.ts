// src/metrics/metrics.service.ts
/* eslint-disable @typescript-eslint/no-unused-vars */
import { Injectable } from '@nestjs/common';
import {
  Counter,
  Gauge,
  Histogram,
  Registry,
  collectDefaultMetrics,
} from 'prom-client';

@Injectable()
export class MetricsService {
  private readonly registry = new Registry();

  // ---- Histograms ----
  private readonly routerLatency: Histogram<string>;
  private readonly routerPublishMs: Histogram<string>;

  // ✅ 新增：
  private readonly routerPriceLag: Histogram<string>;

  // ---- Counters ----
  private readonly detectedTotal: Counter<string>;
  private readonly finalTotal: Counter<string>;
  private readonly droppedTotal: Counter<string>;

  // ---- Gauges (dyngate) ----
  private readonly dyngateEffMin0: Gauge<string>;
  private readonly dyngateCooldownMs: Gauge<string>;
  private readonly dyngateMinNotional3s: Gauge<string>;
  private readonly dyngateVolPct: Gauge<string>;
  private readonly dyngateLiqPct: Gauge<string>;
  private readonly dyngateOiRegime: Gauge<string>;
  private readonly dyngateBreakoutBandPct: Gauge<string>;
  // ---- Sim gauges ----
  // （首批保留）
  private readonly simPosNotional: Gauge<string>; // signed notional
  private readonly simPosAvgPx: Gauge<string>; // avg price of current pos
  private readonly simUnrealized: Gauge<string>; // unrealized pnl
  private readonly simEquity: Gauge<string>; // equity = realized_today + unrealized
  private readonly simLastTs: Gauge<string>; // last processed timestamp (ms)
  // （日度统计保留）
  private readonly simDailyPnl: Gauge<string>;
  private readonly simDailyTrades: Gauge<string>;
  private readonly simDailyTurnover: Gauge<string>;
  private readonly simDailyFees: Gauge<string>;
  private readonly simDailyRealizedPnL: Gauge<string>; // alias to simDailyPnl? kept for backward compatibility if needed
  // 在类字段里新增
  private readonly evalTotal: Counter<string>;
  private readonly evalWin: Counter<string>;
  private readonly evalLoss: Counter<string>;
  private readonly evalRetBp: Histogram<string>;
  private readonly evalOpenJobs: Gauge<string>;

  // 模拟sim
  private readonly simTrades: Counter<string>;
  private readonly simTurnover: Counter<string>;
  private readonly simFees: Counter<string>;
  private readonly simReverses: Counter<string>;

  private readonly simRealizedPnlNet: Gauge<string>;
  private readonly simRealizedPnlGross: Gauge<string>;

  private readonly simPosSize: Gauge<string>;

  // simDailyPnl 已提前声明

  private readonly routerPriceStaleTotal = new Counter({
    name: 'quant_router_price_stale_total',
    help: 'Count of stale refPx at routing time',
    labelNames: ['sym', 'dir', 'src'] as const,
    registers: [this.registry],
  });

  constructor() {
    // 采集 Node 默认指标，统一前缀 quant_
    collectDefaultMetrics({
      prefix: 'quant_',
      register: this.registry,
    });

    // Counters
    this.detectedTotal = new Counter({
      name: 'quant_signals_detected_total',
      help: 'Number of detected signals (before router decisions)',
      labelNames: ['sym', 'dir', 'src'] as const,
      registers: [this.registry],
    });

    this.finalTotal = new Counter({
      name: 'quant_signals_final_total',
      help: 'Number of final routed signals',
      labelNames: ['sym', 'dir', 'src'] as const,
      registers: [this.registry],
    });

    this.droppedTotal = new Counter({
      name: 'quant_router_dropped_total',
      help: 'Number of signals dropped by router with reason',
      labelNames: ['sym', 'dir', 'src', 'reason'] as const,
      registers: [this.registry],
    });

    // Gauges (dyngate)
    this.dyngateEffMin0 = new Gauge({
      name: 'quant_dyngate_effmin0',
      help: 'Effective minimum strength used by router gate',
      labelNames: ['sym'] as const,
      registers: [this.registry],
    });

    this.dyngateCooldownMs = new Gauge({
      name: 'quant_dyngate_cooldown_ms',
      help: 'Effective cooldown (ms) used by router gate',
      labelNames: ['sym'] as const,
      registers: [this.registry],
    });

    this.dyngateMinNotional3s = new Gauge({
      name: 'quant_dyngate_min_notional3s',
      help: 'Effective minNotional3s used by detectors/router',
      labelNames: ['sym'] as const,
      registers: [this.registry],
    });

    this.dyngateVolPct = new Gauge({
      name: 'quant_dyngate_vol_pct',
      help: 'Volatility percentile (0~1) used to derive gates',
      labelNames: ['sym'] as const,
      registers: [this.registry],
    });

    this.dyngateLiqPct = new Gauge({
      name: 'quant_dyngate_liq_pct',
      help: 'Liquidity percentile (0~1) used to derive gates',
      labelNames: ['sym'] as const,
      registers: [this.registry],
    });

    this.dyngateOiRegime = new Gauge({
      name: 'quant_dyngate_oi_regime',
      help: 'OI regime (-1, 0, 1) after persistence filter',
      labelNames: ['sym'] as const,
      registers: [this.registry],
    });

    this.dyngateBreakoutBandPct = new Gauge({
      name: 'quant_dyngate_breakout_band_pct',
      help: 'Breakout band width (ratio) used by breakout detector',
      labelNames: ['sym'] as const,
      registers: [this.registry],
    });

    // Histograms
    this.routerLatency = new Histogram({
      name: 'quant_router_latency_ms',
      help: 'Latency from detected.ts to router (ms)',
      labelNames: ['sym', 'dir', 'src'] as const,
      buckets: [5, 10, 20, 50, 100, 200, 500, 1000, 3000, 5000],
      registers: [this.registry],
    });

    this.routerPublishMs = new Histogram({
      name: 'quant_router_publish_ms',
      help: 'Time spent publishing to signal:final (ms)',
      labelNames: ['sym', 'stage'] as const, // stage=final
      buckets: [1, 2, 5, 10, 20, 50, 100, 200],
      registers: [this.registry],
    });

    this.evalTotal = new Counter({
      name: 'quant_eval_total',
      help: 'Total evaluated signals',
      labelNames: ['sym', 'hz', 'dir'] as const,
      registers: [this.registry],
    });

    this.evalWin = new Counter({
      name: 'quant_eval_win_total',
      help: 'Wins (retBp >= 0)',
      labelNames: ['sym', 'hz', 'dir'] as const,
      registers: [this.registry],
    });

    this.evalLoss = new Counter({
      name: 'quant_eval_loss_total',
      help: 'Losses (retBp < 0)',
      labelNames: ['sym', 'hz', 'dir'] as const,
      registers: [this.registry],
    });

    this.evalRetBp = new Histogram({
      name: 'quant_eval_ret_bp',
      help: 'Return in basis points per evaluated signal',
      labelNames: ['sym', 'hz', 'dir'] as const,
      buckets: [
        -100, -50, -20, -10, -5, -2, -1, -0.5, 0, 0.5, 1, 2, 5, 10, 20, 50, 100,
      ],
      registers: [this.registry],
    });

    this.evalOpenJobs = new Gauge({
      name: 'quant_eval_open_jobs',
      help: 'Open evaluation jobs in memory',
      labelNames: [] as const,
      registers: [this.registry],
    });

    // ✅ 新增：
    this.routerPriceLag = new Histogram({
      name: 'quant_router_price_lag_ms',
      help: 'Price lag between signal ts and refPx ts (ms)',
      labelNames: ['sym', 'dir', 'src'] as const,
      // 典型分布分桶（毫秒），可按你线上分位再调
      buckets: [
        50, 100, 150, 200, 300, 500, 800, 1200, 2000, 3000, 5000, 8000, 12000,
      ],
      registers: [this.registry],
    });

    // sim 模拟
    this.simTrades = new Counter({
      name: 'quant_sim_trades_total',
      help: 'Sim trade events',
      labelNames: ['sym', 'side', 'reason'] as const,
      registers: [this.registry],
    });

    this.simTurnover = new Counter({
      name: 'quant_sim_turnover_total',
      help: 'Sim total turnover (USDT)',
      labelNames: ['sym'] as const,
      registers: [this.registry],
    });

    this.simFees = new Counter({
      name: 'quant_sim_fees_total',
      help: 'Sim total fees (USDT)',
      labelNames: ['sym'] as const,
      registers: [this.registry],
    });

    this.simReverses = new Counter({
      name: 'quant_sim_reverses_total',
      help: 'Sim reverse count',
      labelNames: ['sym'] as const,
      registers: [this.registry],
    });

    this.simRealizedPnlNet = new Gauge({
      name: 'quant_sim_realized_pnl_net',
      help: 'Sim realized PnL (net, USDT)',
      labelNames: ['sym'] as const,
      registers: [this.registry],
    });

    this.simRealizedPnlGross = new Gauge({
      name: 'quant_sim_realized_pnl_gross',
      help: 'Sim realized PnL (gross, USDT)',
      labelNames: ['sym'] as const,
      registers: [this.registry],
    });

    this.simPosSize = new Gauge({
      name: 'quant_sim_position_size',
      help: 'Current simulated position size (signed)',
      labelNames: ['sym'] as const,
      registers: [this.registry],
    });

    this.simPosAvgPx = new Gauge({
      name: 'quant_sim_position_avg_px',
      help: 'Current simulated position average price',
      labelNames: ['sym'] as const,
      registers: [this.registry],
    });

    this.simLastTs = new Gauge({
      name: 'quant_sim_last_ts_ms',
      help: 'Last processed timestamp (ms)',
      labelNames: ['sym'] as const,
      registers: [this.registry],
    });

    this.simDailyPnl = new Gauge({
      name: 'quant_sim_day_realized_pnl',
      help: 'Daily realized PnL (USDT)',
      labelNames: ['sym', 'day'] as const,
      registers: [this.registry],
    });
    // 日度统计（已保留 daily_* 指标，不再重复定义）
    this.simDailyTrades = new Gauge({
      name: 'quant_sim_daily_trades',
      help: 'Daily trades',
      labelNames: ['sym', 'day'] as const,
      registers: [this.registry],
    });
    this.simDailyTurnover = new Gauge({
      name: 'quant_sim_daily_turnover',
      help: 'Daily turnover (USDT)',
      labelNames: ['sym', 'day'] as const,
      registers: [this.registry],
    });
    this.simDailyFees = new Gauge({
      name: 'quant_sim_daily_fees',
      help: 'Daily fees (USDT)',
      labelNames: ['sym', 'day'] as const,
      registers: [this.registry],
    });
    this.simDailyRealizedPnL = new Gauge({
      name: 'quant_sim_daily_realized_pnl',
      help: 'Realized PnL of today (quote)',
      labelNames: ['sym', 'day'] as const,
      registers: [this.registry],
    });
    // 快照指标
    this.simPosNotional = new Gauge({
      name: 'quant_sim_pos_notional',
      help: 'Signed notional of current position (quote). Long>0, Short<0',
      labelNames: ['sym'] as const,
      registers: [this.registry],
    });
    this.simUnrealized = new Gauge({
      name: 'quant_sim_unrealized_pnl',
      help: 'Unrealized PnL of current position (quote)',
      labelNames: ['sym'] as const,
      registers: [this.registry],
    });
    this.simEquity = new Gauge({
      name: 'quant_sim_equity',
      help: 'Equity = realized_today + unrealized',
      labelNames: ['sym'] as const,
      registers: [this.registry],
    });
  }

  // 给 /metrics 控制器用
  getRegistry(): Registry {
    return this.registry;
  }

  /* ========== 路由/环境模块会用到的方法（和你现有调用保持一致） ========== */

  /** 入口时延观测 */
  observeRouterLatency(sym: string, latMs: number, dir = 'na', src = 'na') {
    if (Number.isFinite(latMs) && latMs >= 0) {
      this.routerLatency.labels(sym, dir, src).observe(latMs);
    }
  }

  /** 记录被丢弃（reason: strength/cooldown/dedup/disabled/bad_row/exception 等） */
  incDrop(sym: string, dir: 'buy' | 'sell', reason: string, src = 'unknown') {
    this.droppedTotal
      .labels(sym || 'na', dir || 'na', src || 'unknown', reason)
      .inc();
  }

  /** 设置动态门槛与冷却（简版） */
  setDynGate(sym: string, effMin0: number, cooldownMs: number) {
    if (Number.isFinite(effMin0)) this.dyngateEffMin0.labels(sym).set(effMin0);
    if (Number.isFinite(cooldownMs))
      this.dyngateCooldownMs.labels(sym).set(cooldownMs);
  }

  // ✅ 新增：
  observePriceLag(sym: string, lagMs: number, dir = 'na', src = 'na') {
    if (!Number.isFinite(lagMs) || lagMs < 0 || lagMs > 24 * 3600_000) return;
    this.routerPriceLag.labels(sym, dir, src).observe(lagMs);
  }

  incPriceStale(sym: string, dir: string, src: string) {
    this.routerPriceStaleTotal.labels(sym, dir || 'na', src || 'na').inc();
  }

  /** 设置动态门槛（扩展版，一次性把快照相关指标都打点） */
  setDynGateExtended(
    sym: string,
    payload: {
      effMin0: number;
      minNotional3s: number;
      volPct: number;
      liqPct: number;
      oiRegime: number;
      cooldownMs: number;
      breakoutBandPct: number;
    },
  ) {
    const p = payload || ({} as any);
    if (Number.isFinite(p.effMin0))
      this.dyngateEffMin0.labels(sym).set(p.effMin0);
    if (Number.isFinite(p.cooldownMs))
      this.dyngateCooldownMs.labels(sym).set(p.cooldownMs);
    if (Number.isFinite(p.minNotional3s))
      this.dyngateMinNotional3s.labels(sym).set(p.minNotional3s);
    if (Number.isFinite(p.volPct)) this.dyngateVolPct.labels(sym).set(p.volPct);
    if (Number.isFinite(p.liqPct)) this.dyngateLiqPct.labels(sym).set(p.liqPct);
    if (Number.isFinite(p.oiRegime))
      this.dyngateOiRegime.labels(sym).set(p.oiRegime);
    if (Number.isFinite(p.breakoutBandPct))
      this.dyngateBreakoutBandPct.labels(sym).set(p.breakoutBandPct);
  }

  /** 最终通过计数 */
  incFinal(sym: string, dir: 'buy' | 'sell', src: string) {
    this.finalTotal.labels(sym, dir, src || 'unknown').inc();
  }

  /** 可选：入口计数（如果你在别处需要） */
  incDetected(sym: string, dir: 'buy' | 'sell', src: string) {
    this.detectedTotal.labels(sym, dir, src || 'unknown').inc();
  }

  /** 可选：发布耗时直方图 */
  observePublish(sym: string, stage: string, ms: number) {
    if (Number.isFinite(ms) && ms >= 0) {
      this.routerPublishMs.labels(sym, stage).observe(ms);
    }
  }

  // 在类方法里新增
  incEvalTotal(sym: string, hz: string, dir: 'buy' | 'sell') {
    this.evalTotal.labels(sym, hz, dir).inc();
  }
  incEvalWin(sym: string, hz: string, dir: 'buy' | 'sell') {
    this.evalWin.labels(sym, hz, dir).inc();
  }
  incEvalLoss(sym: string, hz: string, dir: 'buy' | 'sell') {
    this.evalLoss.labels(sym, hz, dir).inc();
  }
  observeEvalReturn(
    sym: string,
    hz: string,
    dir: 'buy' | 'sell',
    retBp: number,
  ) {
    this.evalRetBp.labels(sym, hz, dir).observe(retBp);
  }
  setEvalOpenJobs(n: number) {
    this.evalOpenJobs.set(n);
  }

  // —— 对外方法（sim-trade.service.ts 调用）——
  simIncTrade(
    sym: string,
    side: 'buy' | 'sell',
    reason: 'open' | 'add' | 'close' | 'reverse' | 'fee',
  ) {
    this.simTrades.labels(sym, side, reason).inc();
  }
  simAddTurnover(sym: string, notional: number) {
    this.simTurnover.labels(sym).inc(Math.max(0, notional));
  }
  simAddFees(sym: string, fee: number) {
    this.simFees.labels(sym).inc(Math.max(0, fee));
    this.simIncTrade(sym, 'buy', 'fee'); // side 任意占位，或不打 trade 也行
  }
  simIncReverse(sym: string) {
    this.simReverses.labels(sym).inc();
  }
  simSetRealized(sym: string, gross: number, net: number) {
    this.simRealizedPnlGross.labels(sym).set(gross);
    this.simRealizedPnlNet.labels(sym).set(net);
  }
  simSetPos(sym: string, size: number, avgPx: number) {
    this.simPosSize.labels(sym).set(size);
    if (Number.isFinite(avgPx)) this.simPosAvgPx.labels(sym).set(avgPx);
  }
  simSetLastTs(sym: string, tsMs: number) {
    this.simLastTs.labels(sym).set(tsMs);
  }
  simSetDaily(
    sym: string,
    day: string,
    pnl: number,
    trades: number,
    turnover: number,
    fees: number,
  ) {
    this.simDailyPnl.labels(sym, day).set(pnl);
    this.simDailyTrades.labels(sym, day).set(trades);
    this.simDailyTurnover.labels(sym, day).set(turnover);
    this.simDailyFees.labels(sym, day).set(fees);
  }

  simSetUnrealized(sym: string, v: number) {
    if (Number.isFinite(v)) this.simUnrealized.labels(sym).set(v);
  }
  simSetEquity(sym: string, v: number) {
    if (Number.isFinite(v)) this.simEquity.labels(sym).set(v);
  }
}
