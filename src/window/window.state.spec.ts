import { newWin1m, applyTrade, vwapOf } from './window.state';

describe('window.state', () => {
  it('newWin1m initializes fields correctly', () => {
    const w = newWin1m(120_000, '100');
    expect(w.startTs).toBe(60_000);
    expect(w.closeTs).toBe(120_000);
    expect(w.open).toBe('100');
    expect(w.high).toBe('100');
    expect(w.low).toBe('100');
    expect(w.last).toBe('100');
    expect(w.vol).toBe(0);
    expect(w.vbuy).toBe(0);
    expect(w.vsell).toBe(0);
    expect(w.vwapNum).toBe(0);
    expect(w.vwapDen).toBe(0);
    expect(w.tickN).toBe(0);
  });

  it('applyTrade updates OHLC, volume, vwap and tickN', () => {
    const w = newWin1m(120_000, '100');

    applyTrade(w, '100', '1', 'buy');
    expect(w.last).toBe('100');
    expect(w.high).toBe('100');
    expect(w.low).toBe('100');
    expect(w.vol).toBeCloseTo(1);
    expect(w.vbuy).toBeCloseTo(1);
    expect(w.vsell).toBeCloseTo(0);
    expect(w.vwapNum).toBeCloseTo(100 * 1);
    expect(w.vwapDen).toBeCloseTo(1);
    expect(w.tickN).toBe(1);

    applyTrade(w, '105', '2', 'sell');
    expect(w.last).toBe('105');
    expect(w.high).toBe('105');
    expect(w.low).toBe('100');
    expect(w.vol).toBeCloseTo(3);
    expect(w.vbuy).toBeCloseTo(1);
    expect(w.vsell).toBeCloseTo(2);
    expect(w.vwapNum).toBeCloseTo(100 * 1 + 105 * 2);
    expect(w.vwapDen).toBeCloseTo(3);
    expect(w.tickN).toBe(2);

    const vwap = vwapOf(w); // (100*1 + 105*2) / 3 = 310/3
    expect(vwap).toBeCloseTo(103.3333333, 6);
  });

  it('vwapOf falls back to last when no volume', () => {
    const w = newWin1m(60_000, '123.45');
    expect(vwapOf(w)).toBeCloseTo(123.45, 5);
  });
});
