// src/params/param.types.ts
export type DynamicSug = {
  ts: number;
  stale: 0 | 1;
  minNotional3s_sug?: number;
  cooldownMs_sug?: number;
  dedupMs_sug?: number;
  breakoutBandPct_sug?: number;
  dynDeltaK_sug?: number;
  liqK_sug?: number;
  // 可加：src_age_trade/book/oi/funding/basis
};

export type EffectiveWrite = {
  contractMultiplier?: number;
  minNotional3s?: number;
  cooldownMs?: number;
  dedupMs?: number;
  minStrength?: number;
  consensusBoost?: number;
  breakoutBandPct?: number;
  dynDeltaK?: number;
  liqK?: number;
  source: 'static' | 'dynamic' | 'override' | 'fallback';
  ts: number;
  clamped?: '1' | '0';
  useDynamic?: '1' | '0';
};
