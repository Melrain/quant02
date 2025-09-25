// src/dyn-gate/dyn-gate-reader.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { RedisStreamsService } from 'src/redis-streams/redis-streams.service';

export type DynGateSnapshot = {
  effMin0: number; // 动态强度门槛（映射到 aggregator.minStrength）
  minNotional3s: number; // （给 ctx 用，不直接进 aggregator）
  minMoveBp: number; // ⬇️ 聚合器硬条件
  minMoveAtrRatio: number;
  cooldownMs: number;
  dedupMs: number;
  breakoutBandPct: number; // （给 breakout detector 用，不直接进 aggregator）

  // 证据（可选）
  volPct?: number;
  liqPct?: number;
  rateExc?: number;
  eventFlag?: number;
  oiRegime?: number;

  updated_at?: number;
  version?: string;
};

const DEFAULT_GATE: DynGateSnapshot = {
  effMin0: 0.65,
  minNotional3s: 2_000,
  minMoveBp: 3,
  minMoveAtrRatio: 0.2,
  cooldownMs: 6_000,
  dedupMs: 3_000,
  breakoutBandPct: 0.02,

  volPct: 0.5,
  liqPct: 0.5,
  rateExc: 0,
  eventFlag: 0,
  oiRegime: 0,
  updated_at: 0,
  version: 'fallback',
};

@Injectable()
export class DynGateReaderService {
  private readonly logger = new Logger(DynGateReaderService.name);

  constructor(private readonly redis: RedisStreamsService) {}

  private toNum(v: any, dflt: number): number {
    const n = Number(v);
    return Number.isFinite(n) ? n : dflt;
  }

  async get(sym: string): Promise<DynGateSnapshot> {
    const key = `dyn:gate:{${sym}}`;
    try {
      const h = await this.redis.hgetall(key);
      if (!h || Object.keys(h).length === 0) {
        // 首次启动/还没产出时回退
        return { ...DEFAULT_GATE };
      }
      return {
        effMin0: this.toNum(h.effMin0, DEFAULT_GATE.effMin0),
        minNotional3s: this.toNum(h.minNotional3s, DEFAULT_GATE.minNotional3s),
        minMoveBp: this.toNum(h.minMoveBp, DEFAULT_GATE.minMoveBp),
        minMoveAtrRatio: this.toNum(
          h.minMoveAtrRatio,
          DEFAULT_GATE.minMoveAtrRatio,
        ),
        cooldownMs: this.toNum(h.cooldownMs, DEFAULT_GATE.cooldownMs),
        dedupMs: this.toNum(h.dedupMs, DEFAULT_GATE.dedupMs),
        breakoutBandPct: this.toNum(
          h.breakoutBandPct,
          DEFAULT_GATE.breakoutBandPct,
        ),

        volPct: this.toNum(h.volPct, DEFAULT_GATE.volPct!),
        liqPct: this.toNum(h.liqPct, DEFAULT_GATE.liqPct!),
        rateExc: this.toNum(h.rateExc, DEFAULT_GATE.rateExc!),
        eventFlag: this.toNum(h.eventFlag, DEFAULT_GATE.eventFlag!),
        oiRegime: this.toNum(h.oiRegime, DEFAULT_GATE.oiRegime!),
        updated_at: this.toNum(h.updated_at, DEFAULT_GATE.updated_at!),
        version: h.version ?? DEFAULT_GATE.version,
      };
    } catch (e) {
      this.logger.warn(
        `read dyn gate failed for ${sym}: ${(e as Error).message}`,
      );
      return { ...DEFAULT_GATE };
    }
  }
}
