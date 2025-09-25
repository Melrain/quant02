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

  // ---- Histograms ----
  private readonly routerLatency: Histogram<string>;
  private readonly routerPublishMs: Histogram<string>;

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
}
