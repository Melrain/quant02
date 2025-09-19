// ---------------- intra-detectors.ts ----------------
//主要判断是否有大单流入
//1. 成交方向极端（近3s）
//2. Delta 累积强度（近3s，动态阈值）
//3. 突破预警（带宽 + 轻量动量 + 成交确认）
export type Win1m = {
  open: string | number;
  high: string | number;
  low: string | number;
  last: string | number;
  // 可选：若你已有 1m ATR，填入更佳；没有就传 NaN
  atr?: string | number | undefined;
};

export type DetectorCtx = {
  now: number; // ms (aligned end_ts)
  sym: string;
  win: Win1m;
  lastPrices: number[]; // 近 N 价（用于轻量动量/一致性）

  // 3s 滑窗累计（名义额，qty*px*multiplier 已换算到 USD）
  buyNotional3s: number;
  sellNotional3s: number;

  // 自适应参数
  minNotional3s: number;
  breakoutBandPct: number; // 0.02~0.08 推荐

  // 动态阈值参考
  dynAbsDelta: number; // 近 1h |delta3s| 的 EWMA 或 p50
  dynDeltaK: number; // 动态阈值系数（delta 用）

  liqK: number; // 流动性系数（低流动性熔断）
};

export type IntraSignal = {
  ts: number;
  dir: 'buy' | 'sell';
  strength: string; // "0.000"~"1.000" (原始强度，未做跨源归一)
  evidence: Record<string, string>;
  kind?: 'intra';
};

const toNum = (x: any) => (typeof x === 'number' ? x : Number(x));
const clamp01 = (x: number) => Math.max(0, Math.min(1, x));

/** 1) 成交方向极端（近3s） */
export function detAggressiveFlow(ctx: DetectorCtx): IntraSignal | null {
  const buy = ctx.buyNotional3s;
  const sell = ctx.sellNotional3s;
  const sum = buy + sell;

  // 低流动性熔断：sum 必须大于门槛（按历史动态值放大）
  const liqTh = Math.max(ctx.minNotional3s, ctx.liqK * ctx.dynAbsDelta);
  if (!(sum > liqTh)) return null;

  const buyShare = sum > 0 ? buy / sum : 0.5;
  const delta = buy - sell;
  const absDelta = Math.abs(delta);
  const dynTh = Math.max(ctx.minNotional3s, ctx.dynAbsDelta); // 仅作显著性参考

  // 强偏的一侧才触发
  let dir: 'buy' | 'sell' | null = null;
  let shareStrength = 0;
  if (buyShare >= 0.8) {
    dir = 'buy';
    shareStrength = (buyShare - 0.75) / 0.25; // 0.75->0, 1->1
  } else if (buyShare <= 0.2) {
    dir = 'sell';
    shareStrength = (0.25 - buyShare) / 0.25; // 0->1, 0.25->0
  } else {
    return null;
  }
  shareStrength = clamp01(shareStrength);

  // 显著性：|delta| 相对动态阈值
  const signif = clamp01(absDelta / (3 * dynTh));
  const strength = clamp01(0.6 * shareStrength + 0.4 * signif);

  return {
    ts: ctx.now,
    dir: dir,
    strength: strength.toFixed(3),
    kind: 'intra',
    evidence: {
      src: 'flow',
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
  const sum = ctx.buyNotional3s + ctx.sellNotional3s;

  // 低流动性过滤（略低于 flow 的阈值）
  const liqTh = Math.max(
    0.5 * ctx.minNotional3s,
    0.5 * ctx.liqK * ctx.dynAbsDelta,
  );
  if (sum < liqTh) return null;

  // 动态阈值
  const dynTh = Math.max(ctx.minNotional3s, ctx.dynAbsDelta * ctx.dynDeltaK);
  const base = Math.abs(delta);
  if (!(base > dynTh)) return null;

  // 数值稳定
  const denom = Math.max(1e-9, ctx.dynAbsDelta);
  const zLike = base / denom;
  const strength = clamp01(base / (4 * dynTh)); // 保守上限

  return {
    ts: ctx.now,
    dir: delta > 0 ? 'buy' : 'sell',
    strength: strength.toFixed(3),
    kind: 'intra',
    evidence: {
      src: 'delta',
      delta3s: delta.toFixed(2),
      dynTh: String(Math.round(dynTh)),
      zLike: zLike.toFixed(3),
    },
  };
}

/** 3) 突破预警（带宽 + 轻量动量 + 成交确认） */
export function detBreakout(ctx: DetectorCtx): IntraSignal | null {
  const last = toNum(ctx.win.last);
  const hh = toNum(ctx.win.high);
  const ll = toNum(ctx.win.low);
  if (!Number.isFinite(last) || !Number.isFinite(hh) || !Number.isFinite(ll))
    return null;
  const band = hh - ll;
  if (!(band > 0)) return null;

  const pct = Math.min(0.2, Math.max(0, ctx.breakoutBandPct));
  const eps = band * pct;

  const lp = ctx.lastPrices ?? [];
  const slope =
    lp.length >= 3
      ? (lp[lp.length - 1] - lp[0]) / Math.max(1, lp.length - 1)
      : 0;

  const sum3s = ctx.buyNotional3s + ctx.sellNotional3s;
  // 成交确认：越界后的 3s 内成交额需达到 dynAbsDelta 的一定比例
  const volConfirm = sum3s >= 0.5 * ctx.dynAbsDelta; // 系数 0.5 可调

  if (last >= hh + eps && (slope > 0 || volConfirm)) {
    const dist = (last - (hh + eps)) / band;
    const strength = clamp01(
      0.55 + Math.min(0.35, dist * 2) + (slope > 0 ? 0.1 : 0),
    );
    return {
      ts: ctx.now,
      dir: 'buy',
      strength: strength.toFixed(3),
      kind: 'intra',
      evidence: {
        src: 'breakout',
        breakout: 'high',
        band: band.toFixed(2),
        eps: eps.toFixed(6),
        dist: dist.toFixed(3),
        slope: slope.toFixed(6),
        volConfirm: volConfirm ? '1' : '0',
      },
    };
  }
  if (last <= ll - eps && (slope < 0 || volConfirm)) {
    const dist = (ll - eps - last) / band;
    const strength = clamp01(
      0.55 + Math.min(0.35, dist * 2) + (slope < 0 ? 0.1 : 0),
    );
    return {
      ts: ctx.now,
      dir: 'sell',
      strength: strength.toFixed(3),
      kind: 'intra',
      evidence: {
        src: 'breakout',
        breakout: 'low',
        band: band.toFixed(2),
        eps: eps.toFixed(6),
        dist: dist.toFixed(3),
        slope: slope.toFixed(6),
        volConfirm: volConfirm ? '1' : '0',
      },
    };
  }
  return null;
}
