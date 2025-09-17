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
  strength: string; // "0.0"~"1.0"
  evidence: Record<string, string>; // 证据，如 delta3s / buyShare3s / notional3s / band
  kind?: 'intra';
};

/** 1) 成交方向极端（近3s） */
export function detAggressiveFlow(ctx: DetectorCtx): IntraSignal | null {
  const buy = ctx.buyNotional3s;
  const sell = ctx.sellNotional3s;
  const sum = buy + sell;
  if (!(sum > ctx.minNotional3s)) return null;

  const buyShare = sum > 0 ? buy / sum : 0.5;

  if (buyShare >= 0.8) {
    const strength = Math.min(1, (buyShare - 0.75) / 0.25);
    return {
      ts: ctx.now,
      dir: 'buy',
      strength: strength.toFixed(3),
      kind: 'intra',
      evidence: {
        buyShare3s: buyShare.toFixed(3),
        notional3s: sum.toFixed(2),
        delta3s: (buy - sell).toFixed(2),
      },
    };
  }
  if (buyShare <= 0.2) {
    const strength = Math.min(1, (0.25 - buyShare) / 0.25);
    return {
      ts: ctx.now,
      dir: 'sell',
      strength: strength.toFixed(3),
      kind: 'intra',
      evidence: {
        buyShare3s: buyShare.toFixed(3),
        notional3s: sum.toFixed(2),
        delta3s: (buy - sell).toFixed(2),
      },
    };
  }
  return null;
}

/** 2) Delta 累积强度（近3s，动态阈值） */
export function detDeltaStrength(ctx: DetectorCtx): IntraSignal | null {
  const delta = ctx.buyNotional3s - ctx.sellNotional3s;
  const base = Math.abs(delta);

  // 动态阈值：max(最小值, dynAbsDelta * K)
  const dynTh = Math.max(ctx.minNotional3s, ctx.dynAbsDelta * ctx.dynDeltaK);
  if (!(base > dynTh)) return null;

  const strength = Math.min(1, base / (4 * dynTh));
  return {
    ts: ctx.now,
    dir: delta > 0 ? 'buy' : 'sell',
    strength: strength.toFixed(3),
    kind: 'intra',
    evidence: {
      delta3s: delta.toFixed(2),
      dynTh: String(Math.round(dynTh)),
    },
  };
}

/** 3) 突破预警（窗口内高低点 + 最小确认幅度） */
export function detBreakout(ctx: DetectorCtx): IntraSignal | null {
  const last = Number(ctx.win.last);
  const hh = Number(ctx.win.high);
  const ll = Number(ctx.win.low);
  if (!Number.isFinite(last) || !Number.isFinite(hh) || !Number.isFinite(ll))
    return null;

  const band = hh - ll;
  if (!(band > 0)) return null;

  const eps = band * ctx.breakoutBandPct;

  if (last >= hh + eps) {
    return {
      ts: ctx.now,
      dir: 'buy',
      strength: '0.6',
      kind: 'intra',
      evidence: {
        breakout: 'high',
        band: band.toFixed(2),
        eps: eps.toFixed(6),
      },
    };
  }
  if (last <= ll - eps) {
    return {
      ts: ctx.now,
      dir: 'sell',
      strength: '0.6',
      kind: 'intra',
      evidence: { breakout: 'low', band: band.toFixed(2), eps: eps.toFixed(6) },
    };
  }
  return null;
}
