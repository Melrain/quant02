// okx-bootstrap.service.ts（或你放引导逻辑的文件）
import { Injectable, Logger } from '@nestjs/common';
import { RedisStreamsService } from 'src/redis-streams/redis-streams.service';

type OkxFundingRow = {
  instId: string;
  fundingRate: string; // e.g. "-0.000102"
  fundingTime: string; // ms string
  nextFundingTime?: string; // 不一定带
};

@Injectable()
export class FundingBootstrapService {
  private readonly logger = new Logger(FundingBootstrapService.name);
  // 可调：冷启动最多拉多少条
  private readonly FUNDING_BOOTSTRAP_N = 50;

  constructor(private readonly streams: RedisStreamsService) {}

  /** 冷启动：补齐 funding 历史 */
  async bootstrapFundingHistory(instId: string) {
    try {
      // 1) 先看 Redis 里 funding stream 最新一条的 ts（方便去重）
      const key = this.streams.buildKey(instId, 'funding');
      const latest = await this.streams.readLatestAsObjects(key, 1);
      const lastTsInStream = Number(latest?.[0]?.ts) || 0;

      // 2) 拉 OKX REST（注意：OKX 可能会返回空）
      const url = `https://www.okx.com/api/v5/public/funding-rate-history?instId=${encodeURIComponent(instId)}&limit=${this.FUNDING_BOOTSTRAP_N}`;
      const res = await fetch(url);
      if (!res.ok) {
        this.logger.warn(`[funding.bootstrap] ${instId} http=${res.status}`);
        return;
      }
      const json = await res.json();
      const rows = (json?.data ?? []) as OkxFundingRow[];

      // 3) 统计 REST 原始范围
      const tsList = rows
        .map((r) => Number(r.fundingTime))
        .filter((n) => Number.isFinite(n));
      const restMinTs = tsList.length ? Math.min(...tsList) : NaN;
      const restMaxTs = tsList.length ? Math.max(...tsList) : NaN;

      this.logger.debug(
        `[funding.bootstrap] ${instId} rest_count=${rows.length} rest_range=[${isNaN(restMinTs) ? '-' : new Date(restMinTs).toISOString()}, ${isNaN(restMaxTs) ? '-' : new Date(restMaxTs).toISOString()}] last_in_stream=${lastTsInStream ? new Date(lastTsInStream).toISOString() : '-'}`,
      );

      if (!rows.length) {
        this.logger.log(
          `[funding.bootstrap] ${instId} added=0 reason=rest_empty`,
        );
        return;
      }

      // 4) 过滤掉 <= lastTsInStream 的旧数据；OKX data 通常是新→旧，先统一正序方便思考
      const filtered = rows
        .slice()
        .sort((a, b) => Number(a.fundingTime) - Number(b.fundingTime)) // 旧→新
        .filter((r) => Number(r.fundingTime) > lastTsInStream);

      const filtTsList = filtered.map((r) => Number(r.fundingTime));
      const filtMin = filtTsList.length
        ? new Date(Math.min(...filtTsList)).toISOString()
        : '-';
      const filtMax = filtTsList.length
        ? new Date(Math.max(...filtTsList)).toISOString()
        : '-';

      this.logger.debug(
        `[funding.bootstrap] ${instId} filtered_count=${filtered.length} filtered_range=[${filtMin}, ${filtMax}]`,
      );

      if (!filtered.length) {
        this.logger.log(
          `[funding.bootstrap] ${instId} added=0 reason=no_newer_data`,
        );
        return;
      }

      // 5) 逐条写入 Redis Stream（按旧→新）
      let added = 0;
      for (const r of filtered) {
        const ts = Number(r.fundingTime);
        const rate = r.fundingRate;
        const nextFundingTime = r.nextFundingTime
          ? String(r.nextFundingTime)
          : undefined;

        await this.streams.xadd(
          key,
          {
            type: 'market.funding',
            src: 'okx',
            instId,
            ts: String(ts),
            rate,
            nextFundingTime,
          },
          { maxlenApprox: 1_000 },
        );
        added++;
      }

      // 6) 顺带刷新 state:funding（哈希）为最新一条，便于下游读取
      const newest = filtered[filtered.length - 1];
      const stateKey = this.streams.buildOutKey(instId, 'state:funding');
      await this.streams.hset(stateKey, {
        ts: String(newest.fundingTime),
        rate: newest.fundingRate,
        nextFundingTime: newest.nextFundingTime ?? '',
        source: 'bootstrap/rest',
        updatedTs: String(Date.now()),
      });
      await this.streams.expire(stateKey, 4 * 60 * 60); // 4h TTL

      this.logger.log(
        `[funding.bootstrap] ${instId} added=${added} range=[${filtMin}, ${filtMax}]`,
      );
    } catch (e) {
      this.logger.warn(
        `[funding.bootstrap] ${instId} error: ${(e as Error).message}`,
      );
    }
  }
}
