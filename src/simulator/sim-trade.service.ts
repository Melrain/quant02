/* eslint-disable @typescript-eslint/no-unused-vars */
import {
  Injectable,
  Logger,
  OnModuleInit,
  OnModuleDestroy,
} from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { RedisStreamsService } from 'src/redis-streams/redis-streams.service';
import { MetricsService } from 'src/metrics/metrics.service';
import { parseSymbolsFromEnv } from 'src/utils/utils';
import { PriceResolverService } from 'src/price-resolver/price-resolver.service';

type FinalRow = Record<string, string>;

type Pos = {
  dir: 'buy' | 'sell' | 'flat';
  avgPx: number; // 均价（仅在非 flat 有意义）
  notional: number; // 累计名义（报价币），open/add 叠加
  entryTs: number; // 最近进入该方向的时间
  lastSigId?: string; // 幂等
};

function toNum(v: any): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : NaN;
}
function yyyymmddUTC(ts: number): string {
  const d = new Date(ts);
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(d.getUTCDate()).padStart(2, '0');
  return `${yyyy}${mm}${dd}`;
}

@Injectable()
export class SimTradeService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(SimTradeService.name);
  private running = false;

  // === 配置（可 .env） ===
  private readonly ENABLED =
    (process.env.SIM_ENABLED ?? 'true').toLowerCase() !== 'false';
  private readonly GROUP = process.env.SIM_STREAM_GROUP ?? 'cg:simulator';
  private readonly NOTIONAL_Q = Number(
    process.env.SIM_NOTIONAL_QUOTE ?? '1000',
  ); // 每条信号的名义（USDT）
  private readonly FEE_BP = Number(process.env.SIM_FEE_BP ?? '5'); // 单边费率（bp）
  private readonly USE_REF_FIRST =
    (process.env.SIM_USE_REF_PX_FIRST ?? 'true').toLowerCase() !== 'false';
  private readonly IDEM_TTL_SEC = Number(
    process.env.SIM_IDEM_TTL_SEC ?? '86400',
  );

  // —— MTM 配置 ——
  private readonly MTM_INTERVAL_MS = Number(
    process.env.SIM_MTM_INTERVAL_MS ?? '5000',
  );
  private readonly MTM_WRITE_STREAM =
    (process.env.SIM_MTM_WRITE_STREAM ?? 'false').toLowerCase() === 'true';

  private lastMtmTick = 0;

  private readonly symbols = parseSymbolsFromEnv();

  constructor(
    private readonly redis: RedisStreamsService,
    private readonly pricing: PriceResolverService,
    private readonly metrics: MetricsService,
  ) {}

  async onModuleInit() {
    if (!this.ENABLED) {
      this.logger.warn('SimTradeService disabled by SIM_ENABLED=false');
      return;
    }

    // 为每个 symbol 的 final 流建组
    const keys = this.symbols.map((s) =>
      this.redis.buildOutKey(s, 'signal:final'),
    );
    await this.redis.ensureGroups(keys, this.GROUP, '$');

    this.running = true;
    void this.loop();

    this.logger.log(
      `SimTradeService started for ${this.symbols.join(', ')} | Q=${this.NOTIONAL_Q} FEE=${this.FEE_BP}bp | MTM=${this.MTM_INTERVAL_MS}ms stream=${this.MTM_WRITE_STREAM}`,
    );
  }

  async onModuleDestroy() {
    this.running = false;
  }

  /* =================== 主循环：消费 signal:final 做撮合 =================== */
  private async loop() {
    const consumer = process.env.SIM_CONSUMER_ID || `sim#${process.pid}`;
    while (this.running) {
      try {
        const keys = this.symbols.map((s) =>
          this.redis.buildOutKey(s, 'signal:final'),
        );
        const batch = await this.redis.readGroup({
          group: this.GROUP,
          consumer,
          keys,
          count: 200,
          blockMs: 800,
        });
        if (!batch) continue;

        const msgs = this.redis.normalizeBatch(batch);
        const ackMap = new Map<string, string[]>();

        for (const m of msgs) {
          try {
            const sym = m.symbol;
            const h = m.payload as FinalRow;

            const ts = Number(h.ts);
            const dir = h.dir === 'buy' || h.dir === 'sell' ? h.dir : null;
            if (!sym || !Number.isFinite(ts) || !dir) {
              this.safeAck(ackMap, m.key, m.id);
              continue;
            }

            // 幂等：同一 signal.id 只处理一次
            const idemKey = `sim:idem:{${sym}}:${m.id}`;
            const ok = await (this.redis as any).redis?.setNxEx?.(
              idemKey,
              this.IDEM_TTL_SEC,
            );
            if (ok === false) {
              this.safeAck(ackMap, m.key, m.id);
              continue;
            }

            // 1) 决定成交价
            let px: number | null = null;
            let pxSrc: string | undefined;
            const refPx = toNum(h.refPx);
            const refTs = toNum(h.refPx_ts);
            const refStale =
              String(h.refPx_stale ?? '').toLowerCase() === 'true';

            if (
              this.USE_REF_FIRST &&
              Number.isFinite(refPx) &&
              refPx > 0 &&
              !refStale
            ) {
              px = refPx;
              pxSrc = 'refPx';
            } else {
              const hit = await this.pricing.getPriceAt(
                Number.isFinite(refTs) ? refTs : ts,
                sym,
              );
              if (hit) {
                px = hit.px;
                pxSrc = hit.source;
              }
            }

            if (!Number.isFinite(px) || px! <= 0) {
              // 缺价：记录事件并打 dropped 指标
              await this.writeEvent(sym, {
                ts: String(ts),
                type: 'miss_px',
                sigId: m.id,
              });
              this.metrics?.incDrop?.(
                sym,
                dir,
                'sim.miss_px',
                h['evidence.src'] ?? 'unknown',
              );
              this.metrics?.simSetLastTs?.(sym, ts);
              this.safeAck(ackMap, m.key, m.id);
              continue;
            }

            // 2) 读取当前仓位
            const pos = await this.readPos(sym);

            // 3) 交易记账
            const Q = this.NOTIONAL_Q;
            const feeOneSide = (Q * this.FEE_BP) / 10000; // 单边费（开/加）
            let realized = 0;

            if (pos.dir === 'flat') {
              // —— 直接开仓 —— //
              await this.appendTrade(sym, {
                ts,
                instId: sym,
                side: dir,
                px: px!,
                notional: Q,
                fee: feeOneSide,
                kind: 'open',
                sigId: m.id,
                priceSource: pxSrc ?? 'na',
              });
              await this.writePos(sym, {
                dir,
                avgPx: px!,
                notional: Q,
                entryTs: ts,
                lastSigId: m.id,
              });

              // 指标
              this.metrics.simIncTrade(sym, dir, 'open');
              this.metrics.simAddTurnover(sym, Q);
              this.metrics.simAddFees(sym, feeOneSide);
              this.metrics.simSetPos(sym, dir === 'buy' ? +Q : -Q, px!);
              this.metrics.simSetLastTs(sym, ts);

              // 日度累计 + 日度指标
              await this.bumpDaily(sym, ts, {
                realizedPnL: 0 - feeOneSide,
                turnover: Q,
                fees: feeOneSide,
                trades: 1,
              });
              await this.syncDailyMetrics(sym, ts);
            } else if (pos.dir === dir) {
              // —— 同向加仓 —— //
              const newNotional = pos.notional + Q;
              const newAvg = (pos.avgPx * pos.notional + px! * Q) / newNotional;

              await this.appendTrade(sym, {
                ts,
                instId: sym,
                side: dir,
                px: px!,
                notional: Q,
                fee: feeOneSide,
                kind: 'add',
                sigId: m.id,
                priceSource: pxSrc ?? 'na',
              });
              await this.writePos(sym, {
                dir,
                avgPx: newAvg,
                notional: newNotional,
                entryTs: pos.entryTs, // 不变
                lastSigId: m.id,
              });

              // 指标
              this.metrics.simIncTrade(sym, dir, 'add');
              this.metrics.simAddTurnover(sym, Q);
              this.metrics.simAddFees(sym, feeOneSide);
              this.metrics.simSetPos(
                sym,
                dir === 'buy' ? +newNotional : -newNotional,
                newAvg,
              );
              this.metrics.simSetLastTs(sym, ts);

              // 日度累计 + 日度指标
              await this.bumpDaily(sym, ts, {
                realizedPnL: 0 - feeOneSide,
                turnover: Q,
                fees: feeOneSide,
                trades: 1,
              });
              await this.syncDailyMetrics(sym, ts);
            } else {
              // —— 反向：先全平旧仓再开新仓 —— //
              realized = this.realizePnL(pos, px!, dir);
              const feeClose = (pos.notional * this.FEE_BP) / 10000;
              const feeOpen = feeOneSide;
              const feesTotal = feeClose + feeOpen;

              await this.appendTrade(sym, {
                ts,
                instId: sym,
                side: dir,
                px: px!,
                notional: Q,
                fee: feesTotal,
                kind: 'reverse',
                sigId: m.id,
                priceSource: pxSrc ?? 'na',
                realizedPnL: realized,
              });
              // 开反向新仓
              await this.writePos(sym, {
                dir,
                avgPx: px!,
                notional: Q,
                entryTs: ts,
                lastSigId: m.id,
              });

              // 指标
              this.metrics.simIncReverse(sym);
              this.metrics.simIncTrade(sym, dir, 'reverse');
              this.metrics.simAddTurnover(sym, pos.notional + Q);
              this.metrics.simAddFees(sym, feesTotal);
              this.metrics.simSetPos(sym, dir === 'buy' ? +Q : -Q, px!);
              this.metrics.simSetLastTs(sym, ts);

              // 日度累计（净值=realized - feesTotal） + 日度指标
              await this.bumpDaily(sym, ts, {
                realizedPnL: realized - feesTotal,
                turnover: pos.notional + Q,
                fees: feesTotal,
                reverseCount: 1,
                trades: 1,
              });
              await this.syncDailyMetrics(sym, ts);
            }

            // 路由成功计数（按你的口径，可保留）
            this.metrics?.incFinal?.(sym, dir, h['evidence.src'] ?? 'sim');

            this.safeAck(ackMap, m.key, m.id);
          } catch (e) {
            this.logger.warn(
              `sim process failed id=${m.id}: ${(e as Error).message}`,
            );
          }
        }

        for (const [key, ids] of ackMap) {
          if (ids.length) await this.redis.ack(key, this.GROUP, ids);
        }
      } catch (e) {
        this.logger.error(`sim loop error: ${(e as Error).message}`);
      }
    }
  }

  /* =================== 定时 MTM：未实现盈亏 / 权益 =================== */

  // 每秒 tick 一次，按 SIM_MTM_INTERVAL_MS 节流
  @Cron('* * * * * *')
  async tickMtm() {
    if (!this.running) return;
    const now = Date.now();
    if (now - this.lastMtmTick < Math.max(1000, this.MTM_INTERVAL_MS)) return;
    this.lastMtmTick = now;

    for (const sym of this.symbols) {
      try {
        const pos = await this.readPos(sym);
        if (
          pos.dir === 'flat' ||
          pos.notional <= 0 ||
          !Number.isFinite(pos.avgPx)
        ) {
          // 清空/置零 MTM 快照
          await this.writeMtm(sym, {
            ts: now,
            px: NaN,
            unrealized: 0,
            equity: await this.readTodayNet(sym, now), // 只有已实现
            dir: 'flat',
            avgPx: NaN,
            notional: 0,
          });
          this.metrics?.simSetUnrealized?.(sym, 0);
          this.metrics?.simSetEquity?.(sym, await this.readTodayNet(sym, now));
          continue;
        }

        // 取最近价格（用同一 PriceResolver，传当前 now 即可）
        const hit = await this.pricing.getPriceAt(now, sym);
        if (!hit || !Number.isFinite(hit.px) || hit.px <= 0) {
          // 拿不到价格就跳过这次（不覆盖上一次 mtm）
          continue;
        }

        const px = hit.px;
        const r =
          pos.dir === 'buy'
            ? (px - pos.avgPx) / pos.avgPx
            : (pos.avgPx - px) / pos.avgPx;

        const unrealized = pos.notional * r; // 以报价币计
        const realizedToday = await this.readTodayNet(sym, now); // 已实现净额
        const equity = realizedToday + unrealized;

        // 写 MTM 快照 + 指标
        await this.writeMtm(sym, {
          ts: now,
          px,
          unrealized,
          equity,
          dir: pos.dir,
          avgPx: pos.avgPx,
          notional: pos.notional,
          pxSource: hit.source,
        });
        this.metrics?.simSetUnrealized?.(sym, unrealized);
        this.metrics?.simSetEquity?.(sym, equity);

        if (this.MTM_WRITE_STREAM) {
          await this.redis.xadd(
            `sim:mtm:stream:{${sym}}`,
            {
              ts: String(now),
              px: String(px),
              unrealized: String(unrealized),
              equity: String(equity),
              dir: pos.dir,
              avgPx: String(pos.avgPx),
              notional: String(pos.notional),
              priceSource: hit.source ?? 'na',
            },
            { maxlenApprox: 5000 },
          );
        }
      } catch (e) {
        this.logger.debug(`[mtm] ${sym} failed: ${(e as Error).message}`);
      }
    }
  }

  /* =================== 小工具 / 持久化 =================== */

  private safeAck(ackMap: Map<string, string[]>, key: string, id: string) {
    if (!ackMap.has(key)) ackMap.set(key, []);
    ackMap.get(key)!.push(id);
  }

  private async readPos(sym: string): Promise<Pos> {
    const key = `sim:pos:{${sym}}`;
    const h = await this.redis.hgetall(key);
    const dir = (h?.dir as any) || 'flat';
    const avgPx = toNum(h?.avgPx);
    const notional = toNum(h?.notional);
    const entryTs = toNum(h?.entryTs);
    const lastSigId = h?.lastSigId;

    if (dir !== 'buy' && dir !== 'sell') {
      return { dir: 'flat', avgPx: NaN, notional: 0, entryTs: 0 };
    }
    return {
      dir,
      avgPx: Number.isFinite(avgPx) ? avgPx : NaN,
      notional: Number.isFinite(notional) ? notional : 0,
      entryTs: Number.isFinite(entryTs) ? entryTs : 0,
      lastSigId,
    };
  }

  private async writePos(sym: string, p: Pos) {
    const key = `sim:pos:{${sym}}`;
    await this.redis.hset(key, {
      dir: p.dir,
      avgPx: String(p.avgPx),
      notional: String(p.notional),
      entryTs: String(p.entryTs),
      ...(p.lastSigId ? { lastSigId: p.lastSigId } : {}),
    });
  }

  private async appendTrade(
    sym: string,
    row: {
      ts: number;
      instId: string;
      side: 'buy' | 'sell';
      px: number;
      notional: number;
      fee: number;
      kind: 'open' | 'add' | 'close' | 'reverse';
      sigId: string;
      priceSource: string;
      realizedPnL?: number;
    },
  ) {
    const key = `sim:trades:{${sym}}`;
    await this.redis.xadd(
      key,
      {
        ts: String(row.ts),
        instId: row.instId,
        side: row.side,
        px: String(row.px),
        notional: String(row.notional),
        fee: String(row.fee),
        kind: row.kind,
        sigId: row.sigId,
        priceSource: row.priceSource,
        ...(Number.isFinite(row.realizedPnL!)
          ? { realizedPnL: String(row.realizedPnL) }
          : {}),
      },
      { maxlenApprox: 5000 },
    );
  }

  private async writeEvent(sym: string, payload: Record<string, string>) {
    const key = `sim:events:{${sym}}`;
    await this.redis.xadd(key, payload, { maxlenApprox: 2000 });
  }

  private async bumpDaily(
    sym: string,
    ts: number,
    inc: {
      realizedPnL?: number;
      turnover?: number;
      fees?: number;
      trades?: number;
      reverseCount?: number;
    },
  ) {
    const dayKey = `sim:daily:{${sym}}:${yyyymmddUTC(ts)}`;
    const cur = await this.redis.hgetall(dayKey);
    const to = {
      realizedPnL: (toNum(cur?.realizedPnL) || 0) + (inc.realizedPnL || 0),
      turnover: (toNum(cur?.turnover) || 0) + (inc.turnover || 0),
      fees: (toNum(cur?.fees) || 0) + (inc.fees || 0),
      trades: (toNum(cur?.trades) || 0) + (inc.trades || 0),
      reverseCount: (toNum(cur?.reverseCount) || 0) + (inc.reverseCount || 0),
    };
    await this.redis.hset(dayKey, {
      realizedPnL: String(to.realizedPnL),
      turnover: String(to.turnover),
      fees: String(to.fees),
      trades: String(to.trades),
      reverseCount: String(to.reverseCount),
      lastTs: String(ts),
    });
  }

  /** 同步“当天”快照到 Prometheus（Gauge 语义） */
  private async syncDailyMetrics(sym: string, ts: number) {
    const day = yyyymmddUTC(ts);
    const dayKey = `sim:daily:{${sym}}:${day}`;
    const h = await this.redis.hgetall(dayKey);
    const pnl = toNum(h?.realizedPnL) || 0;
    const trades = toNum(h?.trades) || 0;
    const turnover = toNum(h?.turnover) || 0;
    const fees = toNum(h?.fees) || 0;

    this.metrics.simSetDaily(sym, day, pnl, trades, turnover, fees);

    // “已实现 PnL”也用 Gauge 暂存当日累计：方便在面板直接展示
    this.metrics.simSetRealized(sym, pnl + fees /*gross*/, pnl /*net*/);
  }

  /** 写入/刷新 MTM 快照（hash） */
  private async writeMtm(
    sym: string,
    row: {
      ts: number;
      px: number; // 标记价
      unrealized: number; // 未实现盈亏（报价币）
      equity: number; // 当日净值(已实现净额 + 未实现)
      dir: 'buy' | 'sell' | 'flat';
      avgPx: number;
      notional: number;
      pxSource?: string;
    },
  ) {
    const key = `sim:mtm:{${sym}}`;
    await this.redis.hset(key, {
      ts: String(row.ts),
      px: String(row.px),
      unrealized: String(row.unrealized),
      equity: String(row.equity),
      dir: row.dir,
      avgPx: String(row.avgPx),
      notional: String(row.notional),
      ...(row.pxSource ? { priceSource: row.pxSource } : {}),
    });
  }

  /** 读取“当日已实现净额”（sim:daily 的 realizedPnL 字段） */
  private async readTodayNet(sym: string, ts: number): Promise<number> {
    const dayKey = `sim:daily:{${sym}}:${yyyymmddUTC(ts)}`;
    const h = await this.redis.hgetall(dayKey);
    return toNum(h?.realizedPnL) || 0;
  }

  /* =================== 计算口径 =================== */

  /** 平仓盈亏：按名义近似；多= (px-avg)/avg * notional；空= (avg-px)/avg * notional */
  private realizePnL(pos: Pos, px: number, newDir: 'buy' | 'sell'): number {
    if (pos.dir === 'flat' || pos.notional <= 0 || !Number.isFinite(pos.avgPx))
      return 0;
    const r =
      pos.dir === 'buy'
        ? (px - pos.avgPx) / pos.avgPx
        : (pos.avgPx - px) / pos.avgPx;
    return pos.notional * r;
  }
}
