/* eslint-disable @typescript-eslint/no-unused-vars */
import { Injectable, Logger } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { clear } from 'console';
import { RedisStreamsService } from 'src/redis-streams/redis-streams.service';
import { parseSymbolsFromEnv } from 'src/utils/utils';

// ========== 类型定义（内部用 type，非 DTO） ==========
type EnvFactors = {
  volPct: number; // 波动率分位 0~1
  liqPct: number; // 活跃度分位 0~1
  rateExc: number; // 信号速率异常度 >=0
  eventFlag: 0 | 1; // 资金费/交割事件窗口
  oiRegime: -1 | 0 | 1; // OI regime: 减仓/正常/加仓
  updatedAt: number; // 时间戳
};

type DynGateParams = {
  effMin0: number;
  minNotional3s: number;
  minMoveBp: number;
  minMoveAtrRatio: number;
  cooldownMs: number;
  dedupMs: number;
  breakoutBandPct: number;

  // for debugging / evidence
  volPct: number;
  liqPct: number;
  rateExc: number;
  eventFlag: number;
  oiRegime: number;

  updated_at: number;
  version: string;
};

// ========== Service ==========
@Injectable()
export class MarketEnvUpdaterService {
  private readonly logger = new Logger(MarketEnvUpdaterService.name);

  // 可以从配置或数据库里取；先写死几个
  private readonly symbols = parseSymbolsFromEnv();

  constructor(private readonly streams: RedisStreamsService) {}

  /**
   * 每 60s 跑一次（示例用 10s）
   */
  @Cron('*/5 * * * * *')
  async updateMarketEnv() {
    const now = Date.now();
    this.logger.log(
      `MarketEnvUpdater triggered @${new Date(now).toISOString()}`,
    );

    for (const sym of this.symbols) {
      try {
        const factors = await this.computeEnvFactors(sym, now);
        const params = this.mapFactorsToParams(sym, factors, now);
        await this.writeSnapshot(sym, params);
        this.logger.log(
          `[dyn.gate] ${sym} effMin0=${params.effMin0.toFixed(2)} ` +
            `minNotional3s=${params.minNotional3s} cooldown=${params.cooldownMs}`,
        );
      } catch (e) {
        this.logger.error(
          `[dyn.gate] ${sym} update failed: ${(e as Error).message}`,
        );
      }
    }
  }

  // ========== 核心逻辑 ==========

  /** 读取 Redis Streams，计算环境因子 */
  private async computeEnvFactors(
    sym: string,
    now: number,
  ): Promise<EnvFactors> {
    const bars5m = await this.streams.xrange(`win:5m:${sym}`, '-', '+', 50);
    const bars15m = await this.streams.xrange(`win:15m:${sym}`, '-', '+', 50);

    // TODO: 实际计算函数，比如 calcVolPct(bars5m, bars15m)
    const volPct = 0.5; // placeholder
    const liqPct = 0.5; // placeholder

    // 2) 读取 OI
    const oiRows = await this.streams.xrange(`qt:raw:oi:${sym}`, '-', '+', 20);
    // TODO: 对比近/远期 OI 算 regime
    const oiRegime: -1 | 0 | 1 = 0;

    // 3) 读取 funding
    const fundingRows = await this.streams.xrevrange(
      `qt:raw:funding:${sym}`,
      '+',
      '-',
      1,
    );
    // TODO: 判断离下次 funding 是否 <5min
    const eventFlag: 0 | 1 = 0;

    // 4) 读取信号流，算速率异常
    const signals = await this.streams.xrange(
      `win:${sym}:signal:detected`,
      '-',
      '+',
      100,
    );
    // TODO: 最近 60s 信号数 / 期望速率
    const rateExc = 0;

    return {
      volPct,
      liqPct,
      rateExc,
      eventFlag,
      oiRegime,
      updatedAt: now,
    };
  }

  /** 将环境因子映射成动态门槛参数 */
  private mapFactorsToParams(
    sym: string,
    f: EnvFactors,
    now: number,
  ): DynGateParams {
    // TODO: base 参数应从 symbol.config.ts 取，这里先写死
    const baseMinStrength = 0.65;
    const baseMinNotional3s = 2000;
    const baseCooldownMs = 6000;
    const baseBandPct = 0.02;
    const baseDedupMs = 3000;

    const effMin0 = Math.min(
      0.75,
      Math.max(
        0.6,
        baseMinStrength +
          0.05 * (f.volPct > 0.8 ? 1 : 0) +
          0.05 * Math.min(1, f.rateExc) +
          0.1 * f.eventFlag +
          0.04 * (f.oiRegime ? 1 : 0),
      ),
    );

    const minNotional3s = Math.max(
      baseMinNotional3s,
      baseMinNotional3s * (0.9 + 0.3 * f.liqPct),
    );

    const minMoveBp = 2 + 4 * f.volPct; // 2~6 bp
    const minMoveAtrRatio = 0.15 + 0.2 * f.volPct; // 0.15~0.35

    const cooldownMs =
      baseCooldownMs * (1 + 0.5 * Math.min(1, f.rateExc) + 0.5 * f.eventFlag);

    const breakoutBandPct = Math.min(0.05, baseBandPct * (1 + 0.5 * f.volPct));

    return {
      effMin0,
      minNotional3s,
      minMoveBp,
      minMoveAtrRatio,
      cooldownMs,
      dedupMs: baseDedupMs,
      breakoutBandPct,

      volPct: f.volPct,
      liqPct: f.liqPct,
      rateExc: f.rateExc,
      eventFlag: f.eventFlag,
      oiRegime: f.oiRegime,

      updated_at: now,
      version: 'v1.0',
    };
  }

  /** 写入 Redis：Hash 快照 + Stream 审计 */
  private async writeSnapshot(sym: string, p: DynGateParams) {
    const hashKey = `dyn:gate:${sym}`;
    const logKey = `dyn:gate:log:${sym}`;

    // 1) Hash 快照
    await this.streams.hset(hashKey, {
      ...Object.fromEntries(Object.entries(p).map(([k, v]) => [k, String(v)])),
    });

    // 2) Stream 审计
    await this.streams.xadd(
      logKey,
      {
        ...Object.fromEntries(
          Object.entries(p).map(([k, v]) => [k, String(v)]),
        ),
      },
      { maxlenApprox: 2000 },
    );
  }
}
