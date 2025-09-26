/* eslint-disable @typescript-eslint/no-unused-vars */
import {
  Injectable,
  Logger,
  OnModuleInit,
  OnModuleDestroy,
} from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { RedisStreamsService } from 'src/redis-streams/redis-streams.service';
import { parseSymbolsFromEnv } from 'src/utils/utils';
import { MetricsService } from 'src/metrics/metrics.service';

type FinalRow = Record<string, string>;
type Job = {
  id: string; // job id
  sym: string;
  dir: 'buy' | 'sell';
  ts0: number; // signal ts
  p0: number; // entry price
  hzMs: number; // horizon ms
  dueAt: number; // resolve time
};

@Injectable()
export class SignalEvaluatorService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(SignalEvaluatorService.name);
  private running = false;
  private readonly symbols = parseSymbolsFromEnv();

  // 简单内存任务表：key = jobId
  private pending = new Map<string, Job>();

  // 评估用的两个 horizon
  private readonly horizons = [
    { name: '3m', ms: 3 * 60_000 },
    { name: '15m', ms: 15 * 60_000 },
  ];

  constructor(
    private readonly redis: RedisStreamsService,
    private readonly metrics: MetricsService,
  ) {}

  async onModuleInit() {
    // 为每个 symbol 的 final 流建组
    const keys = this.symbols.map((s) =>
      this.redis.buildOutKey(s, 'signal:final'),
    );
    await this.redis.ensureGroups(keys, 'cg:signal-eval', '$');

    this.running = true;
    void this.loop();
    this.logger.log(
      `SignalEvaluator started for ${this.symbols.join(', ')}, horizons=${this.horizons
        .map((h) => h.name)
        .join('/')}`,
    );
  }

  async onModuleDestroy() {
    this.running = false;
  }

  // 主循环：消费 signal:final 并创建评估任务
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

            // 取进场价：优先 win:state:1m 的 last（更稳健）
            const p0 = await this.getEntryPrice(sym, ts0);
            if (!Number.isFinite(p0) || p0 <= 0) {
              this.logger.debug(`[eval.skip] ${sym} bad p0 for ts=${ts0}`);
              this.safeAck(ackMap, m.key, m.id);
              continue;
            }

            // 为每个 horizon 建两个任务
            for (const hz of this.horizons) {
              const job: Job = {
                id: `${m.id}|${hz.name}`,
                sym,
                dir,
                ts0,
                p0,
                hzMs: hz.ms,
                dueAt: ts0 + hz.ms,
              };
              this.pending.set(job.id, job);
            }

            // 观测 open 任务数
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

  // 每秒检查一次是否有到期任务
  @Cron('*/1 * * * * *')
  async tickResolve() {
    if (!this.running) return;
    const now = Date.now();

    // 快速扫描（任务量不大时足够；后续量大可换最小堆）
    const due: Job[] = [];
    for (const j of this.pending.values()) {
      if (j.dueAt <= now) due.push(j);
    }
    if (!due.length) return;

    for (const j of due) {
      try {
        // 取对价：用 win:1m 的 close（选取 >= dueAt 的下一根bar）
        const p1 = await this.getPriceAtCloseOf(symMinuteCeil(j.dueAt), j.sym);
        if (!Number.isFinite(p1) || p1 <= 0) {
          // 价格还没出，等下一轮
          continue;
        }

        // 方向化收益（bp）
        // buy:  (p1/p0 - 1) * 10_000
        // sell: (p0/p1 - 1) * 10_000  等价  -(p1/p0 - 1)*10_000
        const retBp =
          j.dir === 'buy' ? (p1 / j.p0 - 1) * 10_000 : (j.p0 / p1 - 1) * 10_000;

        const hzName =
          this.horizons.find((h) => h.ms === j.hzMs)?.name ?? `${j.hzMs}ms`;

        // 指标上报
        this.metrics.observeEvalReturn(j.sym, hzName, j.dir, retBp);
        this.metrics.incEvalTotal(j.sym, hzName, j.dir);
        if (retBp >= 0) this.metrics.incEvalWin(j.sym, hzName, j.dir);
        else this.metrics.incEvalLoss(j.sym, hzName, j.dir);

        // 审计流
        const key = this.redis.buildOutKey(j.sym, 'eval:done');
        await this.redis.xadd(
          key,
          {
            ts0: String(j.ts0),
            dueAt: String(j.dueAt),
            horizon: hzName,
            dir: j.dir,
            p0: String(j.p0),
            p1: String(p1),
            retBp: String(retBp.toFixed(2)),
          },
          { maxlenApprox: 5000 },
        );

        // 移除任务
        this.pending.delete(j.id);
      } catch (e) {
        this.logger.warn(
          `[eval.resolve] ${j.sym} ${j.dir} hz=${j.hzMs} failed: ${(e as Error).message}`,
        );
      }
    }

    this.metrics.setEvalOpenJobs(this.pending.size);
  }

  private safeAck(ackMap: Map<string, string[]>, key: string, id: string) {
    if (!ackMap.has(key)) ackMap.set(key, []);
    ackMap.get(key)!.push(id);
  }

  /** 进场价：优先 win:state:1m 的 last；兜底用最近 win:1m close */
  private async getEntryPrice(sym: string, ts0: number): Promise<number> {
    const stKey = this.redis.buildOutKey(sym, 'win:state:1m');
    const st = await this.redis.hgetall(stKey);
    const last = Number(st?.last);
    if (Number.isFinite(last) && last > 0) return last;

    // 兜底：找 ts0 前后 2 分钟范围内最近的一根 1m close
    const k1Key = this.redis.buildOutKey(sym, 'win:1m');
    const rows = await this.redis.readWindowAsObjects(
      k1Key,
      ts0 - 2 * 60_000,
      ts0 + 2 * 60_000,
    );
    const closes = rows
      .map((r) => ({ ts: Number(r.ts), close: Number(r.close) }))
      .filter((x) => Number.isFinite(x.ts) && Number.isFinite(x.close));
    if (!closes.length) return NaN;
    // 取最接近 ts0 的那根
    closes.sort((a, b) => Math.abs(a.ts - ts0) - Math.abs(b.ts - ts0));
    return closes[0].close;
  }

  /** 取 >= t 的下一根 1m bar 的 close */
  private async getPriceAtCloseOf(t: number, sym: string): Promise<number> {
    const key = this.redis.buildOutKey(sym, 'win:1m');
    const rows = await this.redis.readWindowAsObjects(
      key,
      t - 60_000,
      t + 4 * 60_000,
    );
    const bars = rows
      .map((r) => ({ ts: Number(r.ts), close: Number(r.close) }))
      .filter((x) => Number.isFinite(x.ts) && Number.isFinite(x.close))
      .sort((a, b) => a.ts - b.ts);

    for (const b of bars) {
      if (b.ts >= t) return b.close;
    }
    return NaN;
  }
}

function symMinuteCeil(ms: number) {
  return Math.floor((ms - 1) / 60_000) * 60_000 + 60_000;
}
