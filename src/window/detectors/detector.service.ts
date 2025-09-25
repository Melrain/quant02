// src/detector/detector.service.ts
import { Injectable, Logger } from '@nestjs/common';

import type { DetectorCtx, IntraSignal } from './intra-detectors';
import { DynGateReaderService } from 'src/dyn-gate-reader/dyn-gate-reader.service';
import { AggregatorConfig, IntraAggregator } from './IntraAggregator';

const BASE_CFG: Partial<AggregatorConfig> = {
  // 不会被 DynGate 覆盖的静态项（如对称闸门、分桶粒度）
  bucket: { zLike: 0.05, buyShare: 0.02 },
  symmetryStrengthEps: 0.03,
  // 其余项都由 DynGate 覆盖
};

@Injectable()
export class DetectorService {
  private readonly logger = new Logger(DetectorService.name);
  private readonly aggs = new Map<string, IntraAggregator>(); // sym -> agg

  constructor(private readonly gateReader: DynGateReaderService) {}

  private getOrCreateAgg(sym: string): IntraAggregator {
    let agg = this.aggs.get(sym);
    if (!agg) {
      agg = new IntraAggregator(BASE_CFG);
      this.aggs.set(sym, agg);
    }
    return agg;
  }

  /** 外部调用：给定 ctx 做一次检测，返回信号或 null */
  async detect(sym: string, ctx: DetectorCtx): Promise<IntraSignal | null> {
    // 1) 取动态门槛
    const gate = await this.gateReader.get(sym);

    // 2) 注入进 Aggregator（方案 A 的核心）
    const agg = this.getOrCreateAgg(sym);
    agg.setConfig({
      minStrength: gate.effMin0, // 核心门槛
      cooldownMs: gate.cooldownMs, // 同方向冷却
      dedupMs: gate.dedupMs, // 去重窗口
      minMoveBp: gate.minMoveBp, // 再次发信的最小位移（bp）
      minMoveAtrRatio: gate.minMoveAtrRatio,
      // 其余维持 BASE_CFG
    });

    // 3) 补充 ctx 里会用到的市场环境（如果你的 detectors 会参考）
    (ctx as any).breakoutBandPct = gate.breakoutBandPct;
    (ctx as any).minNotional3s = Math.max(1, gate.minNotional3s); // 兜底
    (ctx as any).env = {
      volPct: gate.volPct,
      liqPct: gate.liqPct,
      rateExc: gate.rateExc,
      eventFlag: gate.eventFlag,
      oiRegime: gate.oiRegime,
      updated_at: gate.updated_at,
      version: gate.version,
    };

    // 4) 走你的聚合逻辑
    try {
      return agg.detect(ctx);
    } catch (e) {
      this.logger.warn(`detect failed for ${sym}: ${(e as Error).message}`);
      return null;
    }
  }
}
