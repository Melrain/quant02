import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { RedisStreamsService } from 'src/redis-streams/redis-streams.service';
import { parseSymbolsFromEnv } from 'src/utils/utils';

type IntentRow = Record<string, string>;
type Dir = 'buy' | 'sell';
type Pos = {
  dir: Dir | 'flat';
  avgPx: number;
  notional: number;
  entryTs: number;
  lastSigId?: string;
};

function toNum(v: any): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : NaN;
}
function yyyymmddUTC(ts: number): string {
  const d = new Date(ts);
  return `${d.getUTCFullYear()}${String(d.getUTCMonth() + 1).padStart(2, '0')}${String(d.getUTCDate()).padStart(2, '0')}`;
}

@Injectable()
export class SimTradeIntentService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(SimTradeIntentService.name);
  private running = false;

  // 配置
  private readonly ENABLED =
    (process.env.SIM_INTENT_ENABLED ?? 'true').toLowerCase() !== 'false';
  private readonly GROUP =
    process.env.SIM_INTENT_GROUP ?? 'cg:simulator-intent';
  private readonly NOTIONAL_Q = Number(
    process.env.SIM_INTENT_NOTIONAL_QUOTE ?? '1000',
  ); // 兜底名义
  private readonly USE_SIZE_AS_NOTIONAL =
    (process.env.SIM_INTENT_USE_SIZE_AS_NOTIONAL ?? 'true').toLowerCase() ===
    'true';
  private readonly IDEM_TTL_SEC = Number(
    process.env.SIM_INTENT_IDEM_TTL_SEC ?? '86400',
  );
  private readonly NS = process.env.SIM_INTENT_NS ?? 'sim2'; // 键前缀：sim2:*

  private readonly symbols = parseSymbolsFromEnv();

  constructor(private readonly redis: RedisStreamsService) {
    this.logger.log(
      `SimTradeIntentService constructed: symbols=${this.symbols.join(
        ', ',
      )} | NS=${this.NS} GROUP=${this.GROUP}`,
    );
  }

  async onModuleInit() {
    this.logger.log(
      `SimTradeIntentService init: symbols=${this.symbols.join(
        ', ',
      )} | NS=${this.NS} GROUP=${this.GROUP}`,
    );
    if (!this.ENABLED) {
      this.logger.warn(
        'SimTradeIntentService disabled by SIM_INTENT_ENABLED=false',
      );
      return;
    }
    const keys = this.symbols.map((s) =>
      this.redis.buildOutStreamKey(s, 'order:intent'),
    );
    await this.redis.ensureGroups(keys, this.GROUP, '$');
    this.running = true;
    void this.loop();
    this.logger.log(
      `SimTradeIntentService started for ${this.symbols.join(', ')} | NS=${this.NS} GROUP=${this.GROUP}`,
    );
  }
  async onModuleDestroy() {
    this.running = false;
  }

  private async loop() {
    const consumer =
      process.env.SIM_INTENT_CONSUMER_ID || `simIntent#${process.pid}`;
    while (this.running) {
      const keys = this.symbols.map((s) =>
        this.redis.buildOutStreamKey(s, 'order:intent'),
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
          const h = m.payload as IntentRow;
          const side =
            h.side === 'buy' || h.side === 'sell' ? (h.side as Dir) : null;
          const entry = toNum(h.entry);
          const ts = Date.now(); // intent 没携带 ts 就用当前；若有可改成 Number(h.ts)
          if (!sym || !side || !Number.isFinite(entry) || entry <= 0) {
            this.safeAck(ackMap, m.key, m.id);
            continue;
          }

          // 幂等
          const idemKey = `${this.NS}:idem:{${sym}}:${m.id}`;
          const ok = await (this.redis as any).redis?.setNxEx?.(
            idemKey,
            this.IDEM_TTL_SEC,
          );
          if (ok === false) {
            this.safeAck(ackMap, m.key, m.id);
            continue;
          }

          // 读仓位
          const pos = await this.readPos(sym);

          // 名义：用 size 或兜底
          const size = toNum(h.size);
          const Q =
            this.USE_SIZE_AS_NOTIONAL && Number.isFinite(size) && size > 0
              ? size
              : this.NOTIONAL_Q;

          // 费用（可选）：这里简单置 0，如需费率可加个 FEE_BP 环境变量
          const fee = 0;

          // 成交逻辑：直接以 entry 成交
          if (pos.dir === 'flat') {
            await this.appendTrade(sym, {
              ts,
              instId: sym,
              side,
              px: entry,
              notional: Q,
              fee,
              kind: 'open',
              sigId: m.id,
              priceSource: 'intent.entry',
            });
            await this.writePos(sym, {
              dir: side,
              avgPx: entry,
              notional: Q,
              entryTs: ts,
              lastSigId: m.id,
            });
          } else if (pos.dir === side) {
            const newNotional = pos.notional + Q;
            const newAvg = (pos.avgPx * pos.notional + entry * Q) / newNotional;
            await this.appendTrade(sym, {
              ts,
              instId: sym,
              side,
              px: entry,
              notional: Q,
              fee,
              kind: 'add',
              sigId: m.id,
              priceSource: 'intent.entry',
            });
            await this.writePos(sym, {
              dir: side,
              avgPx: newAvg,
              notional: newNotional,
              entryTs: pos.entryTs,
              lastSigId: m.id,
            });
          } else {
            // 反向：先平旧仓（按名义近似）
            const realized = this.realizePnL(pos, entry);
            await this.appendTrade(sym, {
              ts,
              instId: sym,
              side,
              px: entry,
              notional: Q,
              fee,
              kind: 'reverse',
              sigId: m.id,
              priceSource: 'intent.entry',
              realizedPnL: realized,
            });
            await this.writePos(sym, {
              dir: side,
              avgPx: entry,
              notional: Q,
              entryTs: ts,
              lastSigId: m.id,
            });
            await this.bumpDaily(sym, ts, {
              realizedPnL: realized,
              turnover: pos.notional + Q,
              fees: fee,
              reverseCount: 1,
              trades: 1,
            });
          }

          // 日度统计：open/add 情况
          if (pos.dir === 'flat' || pos.dir === side) {
            await this.bumpDaily(sym, ts, {
              realizedPnL: 0,
              turnover: Q,
              fees: fee,
              trades: 1,
            });
          }

          this.safeAck(ackMap, m.key, m.id);
        } catch (e) {
          this.logger.warn(
            `sim-intent process failed id=${m.id}: ${(e as Error).message}`,
          );
        }
      }
      for (const [key, ids] of ackMap)
        if (ids.length) await this.redis.ack(key, this.GROUP, ids);
    }
  }

  private safeAck(ackMap: Map<string, string[]>, key: string, id: string) {
    if (!ackMap.has(key)) ackMap.set(key, []);
    ackMap.get(key)!.push(id);
  }

  // 读写与统计（用独立命名空间）
  private async readPos(sym: string): Promise<Pos> {
    const key = `${this.NS}:pos:{${sym}}`;
    const h = await this.redis.hgetall(key);
    const dir = (h?.dir as any) || 'flat',
      avgPx = toNum(h?.avgPx),
      notional = toNum(h?.notional),
      entryTs = toNum(h?.entryTs);
    if (dir !== 'buy' && dir !== 'sell')
      return { dir: 'flat', avgPx: NaN, notional: 0, entryTs: 0 };
    return {
      dir,
      avgPx: Number.isFinite(avgPx) ? avgPx : NaN,
      notional: Number.isFinite(notional) ? notional : 0,
      entryTs: Number.isFinite(entryTs) ? entryTs : 0,
      lastSigId: h?.lastSigId,
    };
  }
  private async writePos(sym: string, p: Pos) {
    const key = `${this.NS}:pos:{${sym}}`;
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
      side: Dir;
      px: number;
      notional: number;
      fee: number;
      kind: 'open' | 'add' | 'reverse';
      sigId: string;
      priceSource: string;
      realizedPnL?: number;
    },
  ) {
    const key = `${this.NS}:trades:{${sym}}`;
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
    const dayKey = `${this.NS}:daily:{${sym}}:${yyyymmddUTC(ts)}`;
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

  // 平仓盈亏：按名义近似
  private realizePnL(pos: Pos, px: number): number {
    if (pos.dir === 'flat' || pos.notional <= 0 || !Number.isFinite(pos.avgPx))
      return 0;
    const r =
      pos.dir === 'buy'
        ? (px - pos.avgPx) / pos.avgPx
        : (pos.avgPx - px) / pos.avgPx;
    return pos.notional * r;
  }
}
