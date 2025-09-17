import type { Win1m } from '../window.state';

export type DetectorCtx = {
  now: number; // ms
  sym: string;
  win: Win1m;
  lastPrices: number[]; // 近 N 价（可用于微趋势/波动归一）

  // 3s 滑窗累计（名义额，已按品种换算：qty * px * contractMultiplier）
  buyNotional3s: number;
  sellNotional3s: number;

  // 自适应参数
  minNotional3s: number;
  breakoutBandPct: number;

  // 动态阈值参考
  dynAbsDelta: number; // 近 1h |delta3s| 的 EWMA（或中位数）
  dynDeltaK: number; // 动态阈值系数
};

export type IntraSignal = {
  ts: number;
  dir: 'buy' | 'sell';
  strength: string; // "0.000"~"1.000"
  evidence: Record<string, string>; // 证据，如 delta3s / buyShare3s / notional3s / band
  kind?: 'intra';
};

/** 1) 成交方向极端（近3s） */
export function detAggressiveFlow(ctx: DetectorCtx): IntraSignal | null {
  const buy = ctx.buyNotional3s;
  const sell = ctx.sellNotional3s;
  const sum = buy + sell;

  // 更稳健的最小流动性门槛：结合历史动态值
  const liqTh = Math.max(ctx.minNotional3s, 0.5 * ctx.dynAbsDelta);
  if (!(sum > liqTh)) return null;

  const buyShare = sum > 0 ? buy / sum : 0.5;
  const delta = buy - sell;
  const absDelta = Math.abs(delta);
  const dynTh = Math.max(ctx.minNotional3s, ctx.dynAbsDelta); // 作为显著性参考

  // share 强度（阈值 + 线性放大）
  let shareStrength = 0;
  let dir: 'buy' | 'sell' | null = null;
  if (buyShare >= 0.8) {
    shareStrength = Math.min(1, (buyShare - 0.75) / 0.25);
    dir = 'buy';
  } else if (buyShare <= 0.2) {
    shareStrength = Math.min(1, (0.25 - buyShare) / 0.25);
    dir = 'sell';
  } else {
    return null;
  }

  // 叠加“显著性”分量：|delta| 相对动态阈值
  const signif = Math.min(1, absDelta / (3 * dynTh));
  const strength = Math.min(1, 0.6 * shareStrength + 0.4 * signif);

  return {
    ts: ctx.now,
    dir: dir, // 已在分支保证
    strength: strength.toFixed(3),
    kind: 'intra',
    evidence: {
      buyShare3s: buyShare.toFixed(3),
      notional3s: sum.toFixed(2),
      delta3s: delta.toFixed(2),
      liqTh: String(Math.round(liqTh)),
      signif: signif.toFixed(3),
    },
  };
}

/** 2) Delta 累积强度（近3s，动态阈值） */
export function detDeltaStrength(ctx: DetectorCtx): IntraSignal | null {
  const delta = ctx.buyNotional3s - ctx.sellNotional3s;
  const base = Math.abs(delta);

  // 最低流动性过滤，避免小额噪声触发
  const sum = ctx.buyNotional3s + ctx.sellNotional3s;
  const liqTh = Math.max(ctx.minNotional3s * 0.5, 0.25 * ctx.dynAbsDelta);
  if (sum < liqTh) return null;

  // 动态阈值：max(最小值, dynAbsDelta * K)
  const dynTh = Math.max(ctx.minNotional3s, ctx.dynAbsDelta * ctx.dynDeltaK);
  if (!(base > dynTh)) return null;

  // 数值稳定：避免 dynAbsDelta 为 0
  const denom = Math.max(1e-9, ctx.dynAbsDelta);
  const zLike = base / denom;
  const strength = Math.min(1, base / (4 * dynTh));

  return {
    ts: ctx.now,
    dir: delta > 0 ? 'buy' : 'sell',
    strength: strength.toFixed(3),
    kind: 'intra',
    evidence: {
      delta3s: delta.toFixed(2),
      dynTh: String(Math.round(dynTh)),
      zLike: zLike.toFixed(3),
    },
  };
}

/** 3) 突破预警（窗口内高低点 + 最小确认幅度 + 轻量动量/迟滞） */
export function detBreakout(ctx: DetectorCtx): IntraSignal | null {
  const last = Number(ctx.win.last);
  const hh = Number(ctx.win.high);
  const ll = Number(ctx.win.low);
  if (!Number.isFinite(last) || !Number.isFinite(hh) || !Number.isFinite(ll))
    return null;

  const band = hh - ll;
  if (!(band > 0)) return null;

  // clamp，避免极端配置
  const pct = Math.min(0.2, Math.max(0, ctx.breakoutBandPct));
  const eps = band * pct;

  // 轻量动量：使用最近价的斜率作为一致性确认
  const lp = ctx.lastPrices ?? [];
  const slope =
    lp.length >= 3
      ? (lp[lp.length - 1] - lp[0]) / Math.max(1, lp.length - 1)
      : 0;

  if (last >= hh + eps) {
    const dist = (last - (hh + eps)) / band; // 相对带宽
    const strength = Math.min(
      1,
      Math.max(
        0.4,
        0.55 + Math.min(0.35, dist * 2) + (slope > 0 ? 0.1 : -0.05),
      ),
    );
    return {
      ts: ctx.now,
      dir: 'buy',
      strength: strength.toFixed(3),
      kind: 'intra',
      evidence: {
        breakout: 'high',
        band: band.toFixed(2),
        eps: eps.toFixed(6),
        dist: dist.toFixed(3),
        slope: slope.toFixed(6),
      },
    };
  }
  if (last <= ll - eps) {
    const dist = (ll - eps - last) / band;
    const strength = Math.min(
      1,
      Math.max(
        0.4,
        0.55 + Math.min(0.35, dist * 2) + (slope < 0 ? 0.1 : -0.05),
      ),
    );
    return {
      ts: ctx.now,
      dir: 'sell',
      strength: strength.toFixed(3),
      kind: 'intra',
      evidence: {
        breakout: 'low',
        band: band.toFixed(2),
        eps: eps.toFixed(6),
        dist: dist.toFixed(3),
        slope: slope.toFixed(6),
      },
    };
  }
  return null;
}
