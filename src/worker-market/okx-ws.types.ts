export type TradeEvent = {
  type: 'market.trade';
  src: 'okx';
  instId: string;
  ts: number; // 毫秒
  px: string;
  qty: string; //合约张数或币数量（按instType决定，但先不混，保持raw含义）
  side: 'buy' | 'sell'; // taker方向
  taker: 0 | 1; // 0-非吃单，1-吃单
  tradeId?: string; // OKX trades 提供的成交ID
  seq?: number; // okx独有，逐仓合约的成交序号
  ingestId?: string; // 数据源标识，便于追溯
  recvTs?: number; // 数据接收时间，便于追溯
};

// 盘口快照/增量
export type OrderBookEvent = {
  type: 'market.book';
  src: 'okx';
  instId: string;
  ts: number;
  // 仅保最小必要：一级价与汇总，避免大体量搬运；完整深度另设专线/专存（可选）
  bid1?: { px: string; sz: string };
  ask1?: { px: string; sz: string };
  bidSz10?: string; // 买10档总量
  askSz10?: string; // 卖10档总量
  spread?: string; // 买一卖一价差 ask1-bid1
  snapshot: boolean; // 首帧 true; 后续增量 false
  checksum?: number; // 如使用 L2-TBT 效验
  // 便于对接 books/books-l2-tbt 的可选元数据（调试/追溯）
  action?: 'partial' | 'update';
  u?: number; // last update id（l2-tbt）
  pu?: number; // prev update id（l2-tbt）
  ingestId?: string; // 数据源标识，便于追溯
  recvTs?: number; // 数据接收时间，便于追溯
};

// K线封装
export type KlineEvent = {
  type: 'market.kline';
  src: 'okx';
  instId: string;
  tf: string; // '1m' | '5m' | '15m' | '1h' 等
  ts: number; // 毫秒 bar close 的时间（统一对齐）
  open: string;
  high: string;
  low: string;
  close: string;
  vol: string; // 成交量（按 OKX 定义）
  confirm?: 0 | 1; // OKX candle 的第9列，1=已收盘，0=进行中
  ingestId?: string; // 数据源标识，便于追溯
  recvTs?: number; // 数据接收时间，便于追溯
  quoteVol?: string; // 计价币种成交量（可选）
};

// 持仓兴趣(OpenInterestEvent)
export type OpenInterestEvent = {
  type: 'market.oi';
  src: 'okx';
  instId: string;
  ts: number; // 毫秒
  oi: string; //合约张数或币量
  oiCcy?: string;
  ingestId?: string; // 数据源标识，便于追溯
  recvTs?: number; // 数据接收时间，便于追溯
};

//资金费/基差
export type FundingRateEvent = {
  type: 'market.funding';
  src: 'okx';
  instId: string;
  ts: number;
  rate: string;
  nextFundingTime?: number;
  ingestId?: string; // 数据源标识，便于追溯
  recvTs?: number; // 数据接收时间，便于追溯
};

export type BasisEvent = {
  type: 'market.basis';
  src: 'okx';
  instId: string;
  ts: number;
  basis: string; // 可由 mark/index/last 推导，若在client测就计算，明确单位
  zscore?: string;
};

/**私有回执 供Portfolio用 **/
export type OrderEvent = {
  type: 'exec.order';
  src: 'okx';
  instId: string;
  ts: number;
  ordId: string;
  clOrdId?: string;
  state: 'live' | 'partially_filled' | 'filled' | 'canceled' | 'rejected';
  side: 'buy' | 'sell';
  posSide?: 'long' | 'short';
  px?: string;
  sz?: string;
  fillPx?: string;
  fillSz?: string;
  fee?: string;
  feeCcy?: string;
  tag?: string; // strategyId#planId...
  reason?: string; //失败或取消原因
};

export type PositionEvent = {
  type: 'exec.position';
  src: 'okx';
  instId: string;
  ts: number;
  posSide?: 'long' | 'short';
  qty: string; //当前张数/币量
  avgPx?: string;
  mgnMode?: 'cross' | 'isolated';
  upl?: string; // 未实现盈亏(可选)
};

// tickers 频道
export type TickerEvent = {
  type: 'market.ticker';
  src: 'okx';
  instId: string;
  ts: number;
  last: string;
  bid1?: { px: string; sz?: string };
  ask1?: { px: string; sz?: string };
  open24h?: string;
  high24h?: string;
  low24h?: string;
  vol24h?: string; // base 成交量
  volCcy24h?: string; // quote 成交量
  ingestId?: string; // 数据源标识，便于追溯
  recvTs?: number; // 数据接收时间，便于追溯
};
