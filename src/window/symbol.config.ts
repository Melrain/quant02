// 每个 instId（如 BTC-USDT-SWAP）的一些口径与阈值
export type SymbolConfig = {
  // 名义额换算：如果 qty 是“张”，需要 contractMultiplier * px → USD 名义额
  // 如果 qty 已经是币，则 notional = px * qty
  contractMultiplier?: number; // e.g. 合约乘数（张→币 or 直接张→USD）
  minNotional3s?: number; // flow/delta 检测的最小近3秒总名义额
  cooldownMs?: number; // 同方向信号冷却
  breakoutBandPct?: number; // 突破最小确认阈值（band 的百分比）
  dynDeltaK?: number; // 动态阈值放大系数
};

// 缺省值（未配置的 instId 用这套）
export const DEFAULT_SYM_CONF: Required<SymbolConfig> = {
  contractMultiplier: 1, // 默认 qty * px 即 USD 名义额
  minNotional3s: 2_000, // 低于此不触发 flow/delta 类信号
  cooldownMs: 3000, // 3s 冷却
  breakoutBandPct: 0.02, // 2% band 确认
  dynDeltaK: 1.0, // 动态阈值系数
};

// 这里填你的关注列表与特化参数
export const SYMBOLS: Record<string, SymbolConfig> = {
  'BTC-USDT-SWAP': {
    contractMultiplier: 1,
    minNotional3s: 10_000,
    cooldownMs: 3000,
    breakoutBandPct: 0.01,
    dynDeltaK: 1.0,
  },
  'ETH-USDT-SWAP': {
    contractMultiplier: 1,
    minNotional3s: 5_000,
    cooldownMs: 3000,
    breakoutBandPct: 0.015,
    dynDeltaK: 1.0,
  },
  // 新增：按流动性与噪声设置起步值（后续回放再调）
  'DOGE-USDT-SWAP': {
    contractMultiplier: 1, // 若交易所 qty=张，请改成每张对应的币量乘数
    minNotional3s: 1_500,
    cooldownMs: 3000,
    breakoutBandPct: 0.025, // 噪声高，放大确认
    dynDeltaK: 1.1,
  },
  'LTC-USDT-SWAP': {
    contractMultiplier: 1,
    minNotional3s: 3_000,
    cooldownMs: 3000,
    breakoutBandPct: 0.018,
    dynDeltaK: 1.0,
  },
  'SHIB-USDT-SWAP': {
    contractMultiplier: 1,
    minNotional3s: 1_000,
    cooldownMs: 3000,
    breakoutBandPct: 0.03,
    dynDeltaK: 1.15,
  },
  // 下列为小盘/新币，阈值更宽松且确认更严格（建议先回放调参）
  'PUMP-USDT-SWAP': {
    contractMultiplier: 1,
    minNotional3s: 500,
    cooldownMs: 3000,
    breakoutBandPct: 0.05,
    dynDeltaK: 1.2,
  },
  'WLFI-USDT-SWAP': {
    contractMultiplier: 1,
    minNotional3s: 500,
    cooldownMs: 3000,
    breakoutBandPct: 0.05,
    dynDeltaK: 1.2,
  },
  'XPL-USDT-SWAP': {
    contractMultiplier: 1,
    minNotional3s: 500,
    cooldownMs: 3000,
    breakoutBandPct: 0.05,
    dynDeltaK: 1.2,
  },
};

export function confOf(sym: string): Required<SymbolConfig> {
  return { ...DEFAULT_SYM_CONF, ...(SYMBOLS[sym] || {}) };
}
