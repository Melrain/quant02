// src/window/signal-evaluator.service.ts

import {
  Injectable,
  Logger,
  OnModuleInit,
  OnModuleDestroy,
} from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { RedisStreamsService } from 'src/redis-streams/redis-streams.service';
import { MetricsService } from 'src/metrics/metrics.service';
import { PriceResolverService } from 'src/price-resolver/price-resolver.service';
import { parseSymbolsFromEnv } from 'src/utils/utils';

type FinalRow = Record<string, string>;

type Job = {
  id: string; // msgId|hzName
  finalId: string; // 原始 final 的 stream id（便于审计）
  sym: string;
  dir: 'buy' | 'sell';
  ts0: number; // signal ts
  p0: number; // entry px
  p0_src?: string; // entry px source
  hzMs: number; // horizon (ms)
  hzName: string; // 5m/15m/1h/...
  dueAt: number; // ceil(ts0 + hzMs) 到下一根1m
  retry: number; // p1 获取重试次数
};

@Injectable()
export class SignalEvaluatorService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(SignalEvaluatorService.name);
  private running = false;
  private readonly symbols = parseSymbolsFromEnv();

  // 内存任务表：key=job.id
  private pending = new Map<string, Job>();

  // ===== ENV 配置 =====
  private readonly horizons = this.parseHorizons(
    process.env.EVAL_HORIZONS ?? '5m,15m',
  );
  private readonly successBp = Number(process.env.EVAL_SUCCESS_BP ?? '5'); // 成功阈
  private readonly neutralBandBp = Number(
    process.env.EVAL_NEUTRAL_BAND_BP ?? '2',
  ); // 中性带
  private readonly feeBp = Number(process.env.EVAL_FEE_BP ?? '0'); // 手续费/滑点（bp）
  private readonly maxRetry = Math.max(
    0,
    Number(process.env.EVAL_MAX_RETRY ?? '6'),
  );
  private readonly pxSearchMs = Math.max(
    0,
    Number(process.env.EVAL_PX_SEARCH_MS ?? '15000'),
  );

  constructor(
    private readonly redis: RedisStreamsService,
    private readonly metrics: MetricsService,
    private readonly pricing: PriceResolverService,
  ) {}

  async onModuleInit() {
    const keys = this.symbols.map((s) =>
      this.redis.buildOutKey(s, 'signal:final'),
    );
    await this.redis.ensureGroups(keys, 'cg:signal-eval', '$');

    this.running = true;
    void this.loop();

    this.logger.log(
      `SignalEvaluator started for ${this.symbols.join(', ')} | horizons=${this.horizons
        .map((h) => h.name)
        .join(
          '/',
        )} | successBp=${this.successBp} | neutralBandBp=${this.neutralBandBp} | feeBp=${this.feeBp} | maxRetry=${this.maxRetry} | pxSearchMs=${this.pxSearchMs}`,
    );
  }

  async onModuleDestroy() {
    this.running = false;
  }

  // ========= 主消费循环：读取 signal:final -> 生成评估任务 =========
  private async loop() {
    const consumer = `eval#${process.pid}`;
    while (this.running) {
      try {
        const keys = this.symbols.map((s) =>
          this.redis.buildOutKey(s, 'signal:final'),
        );
        const batch = await this.redis.readGroup({
          group: 'cg:signal-eval',
          consumer,
          keys,
          count: 200,
          blockMs: 500,
        });
        if (!batch) continue;

        const msgs = this.redis.normalizeBatch(batch);
        const ackMap = new Map<string, string[]>();

        for (const m of msgs) {
          try {
            const sym = m.symbol;
            const h = m.payload as FinalRow;

            const ts0 = Number(h.ts);
            const dir = (h.dir as 'buy' | 'sell') ?? 'buy';
            if (
              !sym ||
              !Number.isFinite(ts0) ||
              (dir !== 'buy' && dir !== 'sell')
            ) {
              this.safeAck(ackMap, m.key, m.id);
              continue;
            }

            // —— p0 选取策略：优先 final.refPx（若不过期），否则交给 PriceResolver —— //
            const refPx = Number(h.refPx);
            const refTs = Number(h.refPx_ts);
            const refStaleFlag =
              String(h.refPx_stale ?? 'false').toLowerCase() === 'true';
            const staleByTs =
              Number.isFinite(refTs) && this.pxSearchMs > 0
                ? Math.abs(refTs - ts0) > this.pxSearchMs
                : false;

            let p0 = NaN;
            let p0_src = '';

            if (
              Number.isFinite(refPx) &&
              refPx > 0 &&
              !refStaleFlag &&
              !staleByTs
            ) {
              p0 = refPx;
              p0_src = h.refPx_source || 'refPx';
            } else {
              const hit = await this.pricing.getPriceAt(ts0, sym);
              if (hit && Number.isFinite(hit.px) && hit.px > 0) {
                p0 = hit.px;
                p0_src = hit.source ?? 'resolver';
              }
            }

            if (!Number.isFinite(p0) || p0 <= 0) {
              this.logger.debug(
                `[eval.skip] ${sym} missing entry px @ ts=${ts0} (refPx=${refPx}, refPx_ts=${refTs}, refStale=${refStaleFlag}, staleByTs=${staleByTs})`,
              );
              this.safeAck(ackMap, m.key, m.id);
              continue;
            }

            // 为每个 horizon 建任务；到期锚点用 ceil 到下一根 1m
            for (const hz of this.horizons) {
              const dueAt = ceilToNextMinute(ts0 + hz.ms);
              const job: Job = {
                id: `${m.id}|${hz.name}`,
                finalId: m.id,
                sym,
                dir,
                ts0,
                p0,
                p0_src,
                hzMs: hz.ms,
                hzName: hz.name,
                dueAt,
                retry: 0,
              };
              this.pending.set(job.id, job);
            }

            this.metrics.setEvalOpenJobs(this.pending.size);
            this.safeAck(ackMap, m.key, m.id);
          } catch (e) {
            this.logger.warn(
              `eval build job failed id=${m.id}: ${(e as Error).message}`,
            );
          }
        }

        for (const [key, ids] of ackMap) {
          if (ids.length) await this.redis.ack(key, 'cg:signal-eval', ids);
        }
      } catch (e) {
        this.logger.error(`eval loop error: ${(e as Error).message}`);
      }
    }
  }

  // ========= 每秒解算到期任务：取 p1 -> 计算 -> 落审计 =========
  @Cron('*/1 * * * * *')
  async tickResolve() {
    if (!this.running) return;
    const now = Date.now();

    const due: Job[] = [];
    for (const j of this.pending.values()) {
      if (j.dueAt <= now) due.push(j);
    }
    if (!due.length) return;

    for (const j of due) {
      try {
        // 用 PriceResolver 在 dueAt 附近找价
        const hit = await this.pricing.getPriceAt(j.dueAt, j.sym);
        const hasP1 = !!hit && Number.isFinite(hit.px) && hit.px > 0;

        if (!hasP1) {
          if (j.retry < this.maxRetry) {
            j.retry++;
            this.logger.debug(
              `[eval.retry] ${j.sym} ${j.dir} hz=${j.hzName} retry=${j.retry}/${this.maxRetry}`,
            );
            continue; // 等下次 tick
          }

          // 超过最大重试：落 miss 样本（不计算收益）
          await this.redis.xadd(
            this.redis.buildOutKey(j.sym, 'eval:done'),
            {
              ts0: String(j.ts0),
              dueAt: String(j.dueAt),
              horizon: j.hzName,
              dir: j.dir,
              p0: String(j.p0),

              usedPx: '',
              usedPx_ts: '0',
              usedPx_source: 'na',
              priceLagMs: '0',

              retRawBp: '',
              retNetBp: '',
              thresholdBp: String(this.successBp),
              neutralBandBp: String(this.neutralBandBp),
              neutral: 'false',
              success: 'false',
              miss_px: 'true',
              finalId: j.finalId,
              retry: String(j.retry),
            },
            { maxlenApprox: 5000 },
          );

          this.pending.delete(j.id);
          this.metrics.setEvalOpenJobs(this.pending.size);
          continue;
        }

        // 有 p1：计算收益
        const p1 = hit.px;
        const p1ts = hit.ts ?? j.dueAt;
        const priceLagMs = Math.max(0, p1ts - j.dueAt);

        const rawBp =
          j.dir === 'buy' ? (p1 / j.p0 - 1) * 10_000 : (j.p0 / p1 - 1) * 10_000;

        // 扣手续费/滑点（bp）
        const netBp = rawBp - this.feeBp;

        const isNeutral = Math.abs(netBp) < this.neutralBandBp;
        const isSuccess = !isNeutral && netBp >= this.successBp;

        // 指标
        this.metrics.observeEvalReturn(j.sym, j.hzName, j.dir, netBp);
        this.metrics.incEvalTotal(j.sym, j.hzName, j.dir);
        if (isSuccess) this.metrics.incEvalWin(j.sym, j.hzName, j.dir);
        else this.metrics.incEvalLoss(j.sym, j.hzName, j.dir);

        // 审计
        await this.redis.xadd(
          this.redis.buildOutKey(j.sym, 'eval:done'),
          {
            ts0: String(j.ts0),
            dueAt: String(j.dueAt),
            horizon: j.hzName,
            dir: j.dir,
            p0: String(j.p0),

            usedPx: String(p1),
            usedPx_ts: String(p1ts),
            usedPx_source: String(hit.source ?? 'na'),
            priceLagMs: String(priceLagMs),

            retRawBp: rawBp.toFixed(4),
            retNetBp: netBp.toFixed(4),
            thresholdBp: String(this.successBp),
            neutralBandBp: String(this.neutralBandBp),
            neutral: String(isNeutral),
            success: String(isSuccess),
            miss_px: 'false',
            finalId: j.finalId,
            retry: String(j.retry),
          },
          { maxlenApprox: 5000 },
        );

        this.pending.delete(j.id);
      } catch (e) {
        this.logger.warn(
          `[eval.resolve] ${j.sym} ${j.dir} hz=${j.hzName} failed: ${(e as Error).message}`,
        );
      }
    }

    this.metrics.setEvalOpenJobs(this.pending.size);
  }

  // ========= 工具 =========
  private safeAck(ackMap: Map<string, string[]>, key: string, id: string) {
    if (!ackMap.has(key)) ackMap.set(key, []);
    ackMap.get(key)!.push(id);
  }

  private parseHorizons(env: string): Array<{ name: string; ms: number }> {
    const out: Array<{ name: string; ms: number }> = [];
    for (const raw of env
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean)) {
      let ms = NaN;
      if (/^\d+ms$/i.test(raw)) ms = Number(raw.replace(/ms$/i, ''));
      else if (/^\d+s$/i.test(raw)) ms = Number(raw.replace(/s$/i, '')) * 1000;
      else if (/^\d+m$/i.test(raw))
        ms = Number(raw.replace(/m$/i, '')) * 60_000;
      else if (/^\d+h$/i.test(raw))
        ms = Number(raw.replace(/h$/i, '')) * 3_600_000;
      else if (/^\d+$/.test(raw)) ms = Number(raw); // 数字按 ms
      if (Number.isFinite(ms) && ms > 0) out.push({ name: raw, ms });
    }
    return out.length
      ? out
      : [
          { name: '5m', ms: 300_000 },
          { name: '15m', ms: 900_000 },
        ];
  }
}

function ceilToNextMinute(ms: number) {
  return Math.floor((ms - 1) / 60_000) * 60_000 + 60_000;
}
