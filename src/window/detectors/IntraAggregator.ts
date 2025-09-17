import type { DetectorCtx, IntraSignal } from './intra-detectors';
import {
  detAggressiveFlow,
  detDeltaStrength,
  detBreakout,
} from './intra-detectors';

export type AggregatorConfig = {
  minStrength: number; // 最小强度门槛
  cooldownMs: number; // 同方向冷却
  dedupMs: number; // 去重窗口（同 sym/dir/证据近似）
  consensusBoost: number; // 共识加权
};

const DEFAULT_CFG: AggregatorConfig = {
  minStrength: 0.65,
  cooldownMs: 6000,
  dedupMs: 3000,
  consensusBoost: 0.1,
};

// 参数夹紧与来源识别
const clamp01 = (x: number) => Math.max(0, Math.min(1, x));
const clampNonNegInt = (x: number) => Math.max(0, Math.floor(x));
const srcOf = (s: IntraSignal): 'breakout' | 'delta' | 'flow' | 'unknown' => {
  if (s.evidence.breakout) return 'breakout';
  if (s.evidence.zLike) return 'delta'; // 用 zLike 区分 delta
  if (s.evidence.buyShare3s) return 'flow';
  return 'unknown';
};

export class IntraAggregator {
  private cfg: AggregatorConfig;
  private lastEmitTs = new Map<string, number>(); // key: sym|dir
  private lastSigKey = new Map<string, string>(); // 近似去重：sym|dir -> hash

  constructor(cfg?: Partial<AggregatorConfig>) {
    const m = { ...DEFAULT_CFG, ...(cfg || {}) };
    this.cfg = {
      minStrength: clamp01(m.minStrength),
      cooldownMs: clampNonNegInt(m.cooldownMs),
      dedupMs: clampNonNegInt(m.dedupMs),
      consensusBoost: Math.max(0, Math.min(0.3, m.consensusBoost)),
    };
  }

  reset(): void {
    this.lastEmitTs.clear();
    this.lastSigKey.clear();
  }

  setConfig(patch: Partial<AggregatorConfig>): void {
    const m = { ...this.cfg, ...patch };
    this.cfg = {
      minStrength: clamp01(m.minStrength),
      cooldownMs: clampNonNegInt(m.cooldownMs),
      dedupMs: clampNonNegInt(m.dedupMs),
      consensusBoost: Math.max(0, Math.min(0.3, m.consensusBoost)),
    };
  }

  getConfig(): AggregatorConfig {
    return { ...this.cfg };
  }

  detect(ctx: DetectorCtx): IntraSignal | null {
    const cands = [
      detAggressiveFlow(ctx),
      detDeltaStrength(ctx),
      detBreakout(ctx),
    ].filter(Boolean) as IntraSignal[];
    if (cands.length === 0) return null;

    // 过滤低强度
    const strong = cands.filter(
      (s) => Number(s.strength) >= this.cfg.minStrength,
    );
    if (strong.length === 0) return null;

    // 共识：按方向聚类
    const byDir = new Map<'buy' | 'sell', IntraSignal[]>();
    for (const s of strong) {
      const arr = byDir.get(s.dir) || [];
      arr.push(s);
      byDir.set(s.dir, arr);
    }

    // 稳定选择器：强度优先；平手按来源优先级（breakout > delta > flow）
    const order = { breakout: 3, delta: 2, flow: 1, unknown: 0 } as const;
    const cmp = (a: IntraSignal, b: IntraSignal) => {
      const sa = Number(a.strength);
      const sb = Number(b.strength);
      if (sb !== sa) return sb - sa;
      return order[srcOf(b)] - order[srcOf(a)];
    };

    let chosen: IntraSignal;
    const buyArr = byDir.get('buy') || [];
    const sellArr = byDir.get('sell') || [];
    if (buyArr.length !== sellArr.length) {
      const arr = buyArr.length > sellArr.length ? buyArr : sellArr;
      const top = [...arr].sort(cmp)[0];
      const boost = Math.min(this.cfg.consensusBoost, 0.05 * (arr.length - 1));
      chosen = {
        ...top,
        strength: Math.min(1, Number(top.strength) + boost).toFixed(3),
      };
    } else {
      chosen = [...strong].sort(cmp)[0];
    }

    // 冷却与去重（同方向）
    const key = `${ctx.sym}|${chosen.dir}`;
    const now = ctx.now;
    const lastTs = this.lastEmitTs.get(key) || 0;
    if (now - lastTs < this.cfg.cooldownMs) return null;

    const src = srcOf(chosen);
    const keyField =
      src === 'breakout'
        ? chosen.evidence.breakout
        : src === 'flow'
          ? chosen.evidence.buyShare3s
          : src === 'delta'
            ? chosen.evidence.zLike
            : 'na';
    const approxKey = `${key}|${Math.round(Number(chosen.strength) * 100)}|${src}|${keyField ?? 'na'}`;
    const lastApprox = this.lastSigKey.get(key);
    if (
      lastApprox &&
      lastApprox === approxKey &&
      now - lastTs < this.cfg.dedupMs
    )
      return null;

    this.lastEmitTs.set(key, now);
    this.lastSigKey.set(key, approxKey);

    // 仅统计与“最终方向”一致的来源，避免日志混淆
    const dirArr = byDir.get(chosen.dir) || [];
    const srcList = dirArr.length
      ? dirArr.map((s) => srcOf(s)).join('+')
      : srcOf(chosen);

    // 从同向候选里聚合关键证据（用于日志/监控更直观）
    const deltaArr = (byDir.get(chosen.dir) || []).filter(
      (s) => srcOf(s) === 'delta',
    );
    const zLikeAgg = deltaArr.length
      ? Math.max(...deltaArr.map((s) => Number(s.evidence.zLike) || 0))
      : NaN;

    const flowArr = (byDir.get(chosen.dir) || []).filter(
      (s) => srcOf(s) === 'flow',
    );
    const shareAgg = flowArr.length
      ? Math.max(...flowArr.map((s) => Number(s.evidence.buyShare3s) || 0))
      : NaN;

    return {
      ...chosen,
      evidence: {
        dir: chosen.dir,
        strength: chosen.strength,
        src: srcList,
        delta3s: chosen.evidence.delta3s ?? '',
        // 若 chosen 不是 delta/flow，也补充聚合值，便于阅读
        zLike:
          chosen.evidence.zLike ??
          (Number.isFinite(zLikeAgg) ? zLikeAgg.toFixed(3) : ''),
        buyShare3s:
          chosen.evidence.buyShare3s ??
          (Number.isFinite(shareAgg) ? shareAgg.toFixed(3) : ''),
        breakout: chosen.evidence.breakout ?? '',
      },
      kind: 'intra',
    };
  }
}
