import { Bar } from './win.repo';

export function atr(bars: Bar[], period = 14): number | null {
  if (bars.length < period + 1) return null;
  const trs: number[] = [];
  for (let i = 1; i < bars.length; i++) {
    const a = bars[i - 1],
      b = bars[i];
    trs.push(
      Math.max(
        b.high - b.low,
        Math.abs(b.high - a.close),
        Math.abs(b.low - a.close),
      ),
    );
  }
  const s = trs.slice(-period);
  return s.length < period ? null : s.reduce((x, y) => x + y, 0) / period;
}

export function detectSwing(bars: Bar[], minSwingPct = 0.8) {
  if (bars.length < 5) return null;
  const hi = Math.max(...bars.map((b) => b.high));
  const lo = Math.min(...bars.map((b) => b.low));
  const pct = ((hi - lo) / lo) * 100;
  return pct < minSwingPct ? null : { low: lo, high: hi };
}

export type FibLevels = {
  '0.382': number;
  '0.5': number;
  '0.618': number;
  '0.786': number;
  '1.272': number;
  '1.618': number;
};

export function fibLevels(s: { low: number; high: number }): FibLevels {
  const r = s.high - s.low;
  return {
    '0.382': s.high - 0.382 * r,
    '0.5': s.high - 0.5 * r,
    '0.618': s.high - 0.618 * r,
    '0.786': s.high - 0.786 * r,
    '1.272': s.low + 1.272 * r,
    '1.618': s.low + 1.618 * r,
  };
}

export const near = (p: number, lvl: number, atrVal: number, k: number) =>
  Math.abs(p - lvl) <= k * atrVal;
