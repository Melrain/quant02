/* eslint-disable @typescript-eslint/no-unused-vars */
// src/params/param.repository.ts
import { Injectable, Logger } from '@nestjs/common';
import { RedisClient } from '../redis/redis.client';
import { confOf, SymbolConfig } from '../window/symbol.config';

type EffectiveHash = Partial<
  Record<
    | 'contractMultiplier'
    | 'minNotional3s'
    | 'cooldownMs'
    | 'dedupMs'
    | 'minStrength'
    | 'consensusBoost'
    | 'breakoutBandPct'
    | 'dynDeltaK'
    | 'liqK'
    | 'source'
    | 'ts',
    string
  >
>;

export type EffectiveParams = Required<
  Pick<
    SymbolConfig,
    | 'contractMultiplier'
    | 'minNotional3s'
    | 'cooldownMs'
    | 'dedupMs'
    | 'minStrength'
    | 'consensusBoost'
    | 'breakoutBandPct'
    | 'dynDeltaK'
    | 'liqK'
  >
> & {
  source: 'static' | 'dynamic' | 'override' | 'fallback';
  ts: number;
};

function toNum(x: any, fallback: number): number {
  const n = Number(x);
  return Number.isFinite(n) ? n : fallback;
}

@Injectable()
export class ParamRepository {
  private readonly logger = new Logger(ParamRepository.name);
  // 轻量本地缓存（毫秒级 TTL，避免每笔都 hit Redis）
  private cache = new Map<string, { exp: number; val: EffectiveParams }>();
  private readonly ttlMs = 1000; // 1s 缓存即可

  constructor(private readonly redis: RedisClient) {}

  private key(sym: string) {
    return `qt:param:effective:${sym}`;
  }

  async getEffective(sym: string): Promise<EffectiveParams> {
    const now = Date.now();
    const hit = this.cache.get(sym);
    if (hit && hit.exp > now) return hit.val;

    const base = confOf(sym); // 静态基线（回退用）
    let source: EffectiveParams['source'] = 'static';
    let ts = now;

    try {
      const raw = await this.redis.hgetall(this.key(sym));
      if (raw && Object.keys(raw).length > 0) {
        // 合并：Redis 优先，缺失回退 base
        const val: EffectiveParams = {
          contractMultiplier: toNum(
            raw.contractMultiplier,
            base.contractMultiplier ?? 1,
          ),
          minNotional3s: toNum(raw.minNotional3s, base.minNotional3s ?? 0),
          cooldownMs: toNum(raw.cooldownMs, base.cooldownMs ?? 3000),
          dedupMs: toNum(raw.dedupMs, base.dedupMs ?? 1000),
          minStrength: toNum(raw.minStrength, base.minStrength ?? 0.55),
          consensusBoost: toNum(raw.consensusBoost, base.consensusBoost ?? 0.1),
          breakoutBandPct: toNum(
            raw.breakoutBandPct,
            base.breakoutBandPct ?? 0.001,
          ),
          dynDeltaK: toNum(raw.dynDeltaK, base.dynDeltaK ?? 1.0),
          liqK: toNum(raw.liqK, base.liqK ?? 1.0),
          source: (raw.source as any) || 'static',
          ts: toNum(raw.ts, now),
        };

        source = val.source;
        ts = val.ts;

        // 写入缓存
        this.cache.set(sym, { exp: now + this.ttlMs, val });
        return val;
      }
    } catch (e) {
      this.logger.warn(`read effective params failed for ${sym}: ${String(e)}`);
    }

    // 回退静态
    const fallbackVal: EffectiveParams = {
      contractMultiplier: base.contractMultiplier ?? 1,
      minNotional3s: base.minNotional3s ?? 0,
      cooldownMs: base.cooldownMs ?? 3000,
      dedupMs: base.dedupMs ?? 1000,
      minStrength: base.minStrength ?? 0.55,
      consensusBoost: base.consensusBoost ?? 0.1,
      breakoutBandPct: base.breakoutBandPct ?? 0.001,
      dynDeltaK: base.dynDeltaK ?? 1.0,
      liqK: base.liqK ?? 1.0,
      source: 'fallback',
      ts,
    };
    this.cache.set(sym, { exp: now + this.ttlMs, val: fallbackVal });
    return fallbackVal;
  }
}
