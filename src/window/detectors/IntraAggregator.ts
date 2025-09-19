/* eslint-disable @typescript-eslint/no-unused-vars */
// ---------------- intra-aggregator.ts ----------------
import type { DetectorCtx, IntraSignal } from './intra-detectors';
import {
  detAggressiveFlow,
  detDeltaStrength,
  detBreakout,
} from './intra-detectors';

// 量化工具
const clamp01 = (x: number) => Math.max(0, Math.min(1, x));
const roundTo = (x: number, step: number) => Math.round(x / step) * step;
const safeNum = (s?: string) => (s ? Number(s) : NaN);
const hashStr = (s: string) => {
  // 轻量 hash，避免引第三方库
  let h = 2166136261 >>> 0;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return (h >>> 0).toString(16);
};

type Source = 'breakout' | 'delta' | 'flow' | 'unknown';
const srcOf = (s: IntraSignal): Source => {
  const src = s.evidence.src;
  if (src === 'breakout' || src === 'delta' || src === 'flow') return src;
  if (s.evidence.breakout) return 'breakout';
  if (s.evidence.zLike) return 'delta';
  if (s.evidence.buyShare3s) return 'flow';
  return 'unknown';
};

export type AggregatorConfig = {
  // 门槛与共识
  minStrength: number; // 基线门槛，建议 0.65
  minStrengthFloor: number; // 地板，建议 0.60
  consensusK: number; // 降门槛系数（每多一个同向候选，门槛降低 k）
  consensusKHiVolDiscount: number; // 高波动折扣（如 0.5）

  // 控频
  cooldownMs: number; // 同方向冷却
  dedupMs: number; // 去重窗口
  // 证据分桶粒度（更鲁棒的 dedup）
  bucket: {
    zLike: number; // 0.05
    buyShare: number; // 0.02
  };

  // 最小位移（同方向再次发信的硬条件）
  minMoveBp: number; // 基点，如 3
  minMoveAtrRatio: number; // ATR 比例，如 0.2

  // 方向对称性闸门
  symmetryStrengthEps: number; // 强度差阈值，如 0.03
};

const DEFAULT_CFG: AggregatorConfig = {
  minStrength: 0.65,
  minStrengthFloor: 0.6,
  consensusK: 0.08,
  consensusKHiVolDiscount: 0.5,

  cooldownMs: 7000,
  dedupMs: 3000,
  bucket: { zLike: 0.05, buyShare: 0.02 },

  minMoveBp: 3,
  minMoveAtrRatio: 0.2,

  symmetryStrengthEps: 0.03,
};

export class IntraAggregator {
  private cfg: AggregatorConfig;
  private lastEmitTs = new Map<string, number>(); // key: sym|dir
  private lastEmitPx = new Map<string, number>(); // key: sym|dir -> midPx
  private lastSigKey = new Map<string, string>(); // key: sym|dir -> approxKey

  constructor(cfg?: Partial<AggregatorConfig>) {
    this.cfg = { ...DEFAULT_CFG, ...(cfg || {}) };
    // 夹紧
    this.cfg.minStrength = clamp01(this.cfg.minStrength);
    this.cfg.minStrengthFloor = clamp01(this.cfg.minStrengthFloor);
    this.cfg.consensusK = Math.max(0, Math.min(0.2, this.cfg.consensusK));
    this.cfg.consensusKHiVolDiscount = clamp01(
      this.cfg.consensusKHiVolDiscount,
    );
  }

  reset(): void {
    this.lastEmitTs.clear();
    this.lastEmitPx.clear();
    this.lastSigKey.clear();
  }

  setConfig(patch: Partial<AggregatorConfig>): void {
    const m = { ...this.cfg, ...patch };
    this.cfg = { ...DEFAULT_CFG, ...m };
    this.cfg.minStrength = clamp01(this.cfg.minStrength);
    this.cfg.minStrengthFloor = clamp01(this.cfg.minStrengthFloor);
    this.cfg.consensusK = Math.max(0, Math.min(0.2, this.cfg.consensusK));
  }

  getConfig(): AggregatorConfig {
    return { ...this.cfg };
  }

  /** 统一计算 ATR 值（若无则用带宽近似） */
  private getAtr(ctx: DetectorCtx): number {
    const atrRaw = (ctx.win as any).atr;
    if (Number.isFinite(Number(atrRaw))) return Math.max(1e-12, Number(atrRaw));
    const high = Number((ctx.win as any).high);
    const low = Number((ctx.win as any).low);
    const band =
      Number.isFinite(high) && Number.isFinite(low)
        ? Math.max(1e-12, high - low)
        : 0;
    // 近似：把 1m 带宽的 2/3 当作 ATR 粗估
    return band > 0 ? Math.max(1e-12, band * (2 / 3)) : 1e-12;
  }

  /** 同方向再次发信的最小位移是否满足 */
  private passMinMove(ctx: DetectorCtx, dir: 'buy' | 'sell'): boolean {
    const lastPx = Number((ctx.win as any).last);
    if (!Number.isFinite(lastPx)) return true; // 无法判断则放行
    const key = `${ctx.sym}|${dir}`;
    const prev = this.lastEmitPx.get(key);
    if (!Number.isFinite(prev ?? NaN)) return true;
    const atr = this.getAtr(ctx);
    const bpMove = Math.abs((lastPx - (prev as number)) / lastPx) * 1e4; // 基点
    const atrMove = Math.abs(lastPx - (prev as number)) / Math.max(1e-12, atr);
    const need =
      Math.max(this.cfg.minMoveBp, 0) <= bpMove &&
      atrMove >= this.cfg.minMoveAtrRatio;
    return need;
  }

  /** 构造量化后的 approxKey（更鲁棒的去重） */
  private buildApproxKey(
    sym: string,
    dir: 'buy' | 'sell',
    s: IntraSignal,
  ): string {
    const src = srcOf(s);
    const z = roundTo(safeNum(s.evidence.zLike), this.cfg.bucket.zLike);
    const sh = roundTo(
      safeNum(s.evidence.buyShare3s),
      this.cfg.bucket.buyShare,
    );
    const st = Math.round(Number(s.strength) * 100); // 百分刻度
    return `${sym}|${dir}|${src}|${st}|z:${Number.isFinite(z) ? z.toFixed(2) : 'na'}|sh:${Number.isFinite(sh) ? sh.toFixed(2) : 'na'}`;
  }

  /** 方向对称性闸门：当两边数量接近且强度差很小时不发 */
  private symmetryGate(buyArr: IntraSignal[], sellArr: IntraSignal[]): boolean {
    if (buyArr.length === 0 || sellArr.length === 0) return true;
    const nDiff = Math.abs(buyArr.length - sellArr.length);
    if (nDiff >= 1) return true;
    const maxBuy = Math.max(...buyArr.map((s) => Number(s.strength)));
    const maxSell = Math.max(...sellArr.map((s) => Number(s.strength)));
    return Math.abs(maxBuy - maxSell) >= this.cfg.symmetryStrengthEps;
  }

  detect(ctx: DetectorCtx): IntraSignal | null {
    // 1) 产候选（稳定排序，保证可复现）
    const candsRaw: (IntraSignal | null)[] = [
      detAggressiveFlow(ctx),
      detDeltaStrength(ctx),
      detBreakout(ctx),
    ];
    const cands = candsRaw.filter(Boolean) as IntraSignal[];
    if (cands.length === 0) return null;

    const ordered = [...cands].sort((a, b) => {
      // end_ts 相同，再按来源、方向、强度排序
      const sa = srcOf(a),
        sb = srcOf(b);
      const srcOrder: Record<Source, number> = {
        breakout: 3,
        delta: 2,
        flow: 1,
        unknown: 0,
      };
      if (sa !== sb) return srcOrder[sb] - srcOrder[sa];
      if (a.dir !== b.dir) return a.dir === 'buy' ? -1 : 1; // buy 优先仅为稳定序
      const da = Number(a.strength),
        db = Number(b.strength);
      if (db !== da) return db - da;
      return 0;
    });

    // 2) 按方向分组，应用“单一共识机制：降门槛 + 地板”
    const byDir = new Map<'buy' | 'sell', IntraSignal[]>();
    for (const s of ordered) {
      const arr = byDir.get(s.dir) || [];
      arr.push(s);
      byDir.set(s.dir, arr);
    }
    const buyArr = byDir.get('buy') || [];
    const sellArr = byDir.get('sell') || [];

    // 高波动折扣：当 ATR 分位高时（此处以 dynAbsDelta 作为替代强度），降低 k
    const hiVol =
      ctx.dynAbsDelta > 0 && ctx.dynAbsDelta > 1.5 * ctx.minNotional3s;
    const kEff = hiVol
      ? this.cfg.consensusK * this.cfg.consensusKHiVolDiscount
      : this.cfg.consensusK;

    const strong: IntraSignal[] = [];
    for (const [dir, arr] of [
      ['buy', buyArr],
      ['sell', sellArr],
    ] as const) {
      const effMin = Math.max(
        this.cfg.minStrengthFloor,
        this.cfg.minStrength - kEff * (arr.length - 1),
      );
      strong.push(...arr.filter((s) => Number(s.strength) >= effMin));
    }
    if (strong.length === 0) return null;

    // 3) 方向对称性闸门
    const strongBuy = strong.filter((s) => s.dir === 'buy');
    const strongSell = strong.filter((s) => s.dir === 'sell');
    if (!this.symmetryGate(strongBuy, strongSell)) return null;

    // 4) 选择最终候选（强度优先，来源作为稳定次序）
    const orderMap: Record<Source, number> = {
      breakout: 3,
      delta: 2,
      flow: 1,
      unknown: 0,
    };
    const chosen = [...strong].sort((a, b) => {
      const sa = Number(a.strength),
        sb = Number(b.strength);
      if (sb !== sa) return sb - sa;
      return orderMap[srcOf(b)] - orderMap[srcOf(a)];
    })[0];

    // 5) 冷却
    const key = `${ctx.sym}|${chosen.dir}`;
    const now = ctx.now;
    const lastTs = this.lastEmitTs.get(key) || 0;
    if (now - lastTs < this.cfg.cooldownMs) return null;

    // 6) 最小位移（需在冷却通过后再判）
    if (!this.passMinMove(ctx, chosen.dir)) return null;

    // 7) 去重（量化后的 approxKey）
    const approxKey = this.buildApproxKey(ctx.sym, chosen.dir, chosen);
    const lastApprox = this.lastSigKey.get(key);
    if (
      lastApprox &&
      lastApprox === approxKey &&
      now - lastTs < this.cfg.dedupMs
    ) {
      return null;
    }

    // 8) 记录状态
    this.lastEmitTs.set(key, now);
    const lastPx = Number((ctx.win as any).last);
    if (Number.isFinite(lastPx)) this.lastEmitPx.set(key, lastPx);
    this.lastSigKey.set(key, approxKey);

    // 9) 证据聚合 + 可复现元信息
    const dirArr = (byDir.get(chosen.dir) || []).map((s) => s);
    const zAgg = Math.max(
      ...dirArr
        .map((s) => safeNum(s.evidence.zLike))
        .filter((n) => Number.isFinite(n)),
      Number.NEGATIVE_INFINITY,
    );
    const shAgg = Math.max(
      ...dirArr
        .map((s) => safeNum(s.evidence.buyShare3s))
        .filter((n) => Number.isFinite(n)),
      Number.NEGATIVE_INFINITY,
    );
    const candidatesHash = hashStr(
      JSON.stringify(
        ordered.map((s) => ({
          dir: s.dir,
          src: srcOf(s),
          st: s.strength,
        })),
      ),
    );

    return {
      ...chosen,
      evidence: {
        ...chosen.evidence,
        dir: chosen.dir,
        candidates_hash: candidatesHash,
        approx_key: approxKey,
        zLike_max: Number.isFinite(zAgg) ? zAgg.toFixed(3) : '',
        buyShare3s_max: Number.isFinite(shAgg) ? shAgg.toFixed(3) : '',
      },
      kind: 'intra',
    };
  }
}
