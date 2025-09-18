// 每个 instId（如 BTC-USDT-SWAP）的一些口径与阈值
export type SymbolConfig = {
  contractMultiplier?: number; // e.g. 合约乘数（张→币 or 直接张→USD）
  minNotional3s?: number; // flow/delta 检测的最小近3秒总名义额
  cooldownMs?: number; // 同方向信号冷却
  breakoutBandPct?: number; // 突破最小确认阈值（band 的百分比）
  dynDeltaK?: number; // 动态阈值放大系数

  // 聚合器可按品种覆盖（可选）
  dedupMs?: number; // 去重窗口
  minStrength?: number; // 最小强度门槛
  consensusBoost?: number; // 共识加权

  liqK?: number; // 流动性系数：liqTh = max(minNotional3s, liqK * dynAbsDelta)
};

// 缺省值（未配置的 instId 用这套）
export const DEFAULT_SYM_CONF: Required<SymbolConfig> = {
  contractMultiplier: 1,
  minNotional3s: 2_000, // 最低流动性门槛，避免小额噪声触发
  cooldownMs: 6000, // 默认为 6s（更稳健）
  breakoutBandPct: 0.02,
  dynDeltaK: 1.2, // 略高，提升显著性门槛
  liqK: 0.8, // 流动性门槛系数
  dedupMs: 3000,
  minStrength: 0.65,
  consensusBoost: 0.1,
};

// 分层示例：请按历史数据再微调
export const SYMBOLS: Record<string, SymbolConfig> = {
  // 大盘
  'BTC-USDT-SWAP': {
    contractMultiplier: 1,
    minNotional3s: 3_000_000,
    cooldownMs: 9000,
    dedupMs: 4000,
    minStrength: 0.7,
    breakoutBandPct: 0.012,
    dynDeltaK: 1.8,
    liqK: 1.2,
  },
  'ETH-USDT-SWAP': {
    contractMultiplier: 1,
    minNotional3s: 1_200_000,
    cooldownMs: 8000,
    dedupMs: 4000,
    minStrength: 0.68,
    breakoutBandPct: 0.015,
    dynDeltaK: 1.6,
    liqK: 1.0,
  },

  // 中盘
  'LTC-USDT-SWAP': {
    contractMultiplier: 1,
    minNotional3s: 100_000,
    cooldownMs: 7000,
    dedupMs: 3500,
    minStrength: 0.65,
    breakoutBandPct: 0.018,
    dynDeltaK: 1.3,
    liqK: 0.9,
  },
  'DOGE-USDT-SWAP': {
    contractMultiplier: 1,
    minNotional3s: 50_000,
    cooldownMs: 7000,
    dedupMs: 3500,
    minStrength: 0.65,
    breakoutBandPct: 0.025,
    dynDeltaK: 1.3,
    liqK: 0.8,
  },

  // 噪声高/小盘
  'SHIB-USDT-SWAP': {
    contractMultiplier: 1,
    minNotional3s: 30_000,
    cooldownMs: 9000,
    dedupMs: 4000,
    minStrength: 0.65,
    breakoutBandPct: 0.025,
    dynDeltaK: 1.3,
    liqK: 0.7,
  },
  'PUMP-USDT-SWAP': {
    contractMultiplier: 1,
    minNotional3s: 8_000,
    cooldownMs: 12_000,
    dedupMs: 5000,
    minStrength: 0.65,
    breakoutBandPct: 0.03,
    dynDeltaK: 1.2,
    liqK: 0.7,
  },
  'WLFI-USDT-SWAP': {
    contractMultiplier: 1,
    minNotional3s: 8_000,
    cooldownMs: 12_000,
    dedupMs: 5000,
    minStrength: 0.65,
    breakoutBandPct: 0.03,
    dynDeltaK: 1.2,
    liqK: 0.7,
  },
  'XPL-USDT-SWAP': {
    contractMultiplier: 1,
    minNotional3s: 8_000,
    cooldownMs: 12_000,
    dedupMs: 5000,
    minStrength: 0.65,
    breakoutBandPct: 0.03,
    dynDeltaK: 1.2,
    liqK: 0.7,
  },
};

export function confOf(sym: string): Required<SymbolConfig> {
  return { ...DEFAULT_SYM_CONF, ...(SYMBOLS[sym] || {}) };
}
