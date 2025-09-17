export type Win1m = {
  startTs: number; // bar 起始（closeTs - 60_000）
  closeTs: number; // bar 关口（毫秒）
  open: string;
  high: string;
  low: string;
  last: string;
  vol: number;
  vbuy: number;
  vsell: number;
  vwapNum: number;
  vwapDen: number;
  tickN: number;
};

export function newWin1m(closeTs: number, firstPx: string): Win1m {
  return {
    startTs: closeTs - 60_000,
    closeTs,
    open: firstPx,
    high: firstPx,
    low: firstPx,
    last: firstPx,
    vol: 0,
    vbuy: 0,
    vsell: 0,
    vwapNum: 0,
    vwapDen: 0,
    tickN: 0,
  };
}

export function applyTrade(
  w: Win1m,
  px: string,
  qty: string,
  side?: 'buy' | 'sell',
) {
  w.last = px;
  if (Number(px) > Number(w.high)) w.high = px;
  if (Number(px) < Number(w.low)) w.low = px;

  const q = Number(qty);
  if (Number.isFinite(q)) {
    w.vol += q;
    if (side === 'buy') w.vbuy += q;
    if (side === 'sell') w.vsell += q;
    w.vwapNum += Number(px) * q;
    w.vwapDen += q;
  }
  w.tickN += 1;
}

export function vwapOf(w: Win1m): number {
  return w.vwapDen > 0 ? w.vwapNum / w.vwapDen : Number(w.last);
}
