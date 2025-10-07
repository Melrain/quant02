// src/order/fibonacci-order-filter.service.ts
import {
  Injectable,
  Logger,
  OnModuleInit,
  OnModuleDestroy,
} from '@nestjs/common';
import { RedisStreamsService } from 'src/redis-streams/redis-streams.service';
import { parseSymbolsFromEnv } from 'src/utils/utils';
import {
  FibonacciFilterService,
  FinalSignal,
  Side,
} from './fibonacci-filter.service';

type FinalRow = Record<string, string>;

@Injectable()
export class FibonacciOrderFilterService
  implements OnModuleInit, OnModuleDestroy
{
  private readonly logger = new Logger(FibonacciOrderFilterService.name);
  private running = false;
  private readonly symbols = parseSymbolsFromEnv();

  constructor(
    private readonly redis: RedisStreamsService,
    private readonly fibFilter: FibonacciFilterService,
  ) {}

  async onModuleInit() {
    const keys = this.symbols.map((s) =>
      this.redis.buildOutKey(s, 'signal:final'),
    );
    await this.redis.ensureGroups(keys, 'cg:order-fib', '$');
    this.running = true;
    void this.loop();
    this.logger.log(
      `FibonacciOrderFilter started for ${this.symbols.join(', ')}`,
    );
  }
  async onModuleDestroy() {
    this.running = false;
  }

  private async loop() {
    const consumer = `fib#${process.pid}`;
    while (this.running) {
      const keys = this.symbols.map((s) =>
        this.redis.buildOutKey(s, 'signal:final'),
      );
      const batch = await this.redis.readGroup({
        group: 'cg:order-fib',
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
          const dir = (h.dir as Side) ?? 'buy';
          const ts0 = Number(h.ts);
          const refPx = Number(h.refPx || h.p0); // 入口价来源：final 的 refPx；没有就用 close
          if (
            !sym ||
            !Number.isFinite(ts0) ||
            (dir !== 'buy' && dir !== 'sell')
          ) {
            this.safeAck(ackMap, m.key, m.id);
            continue;
          }

          // 只做消息到 FinalSignal 的映射，计算与写流交给 fibFilter
          const maybeEntry =
            Number.isFinite(refPx) && refPx > 0 ? refPx : undefined;
          const size = Number(h.size ?? process.env.FIB_DEFAULT_SIZE ?? '0');

          const finalMsg: FinalSignal = {
            sym,
            side: dir,
            size,
            ts: ts0, // 传入 ts，过滤器会写进 intent
            ...(maybeEntry ? { entryHint: maybeEntry } : {}),
          };

          await this.fibFilter.handleFinalSignal(finalMsg);
          this.safeAck(ackMap, m.key, m.id);
        } catch (e) {
          this.logger.warn(
            `[order-fib] process failed id=${m.id}: ${(e as Error).message}`,
          );
        }
      }

      for (const [key, ids] of ackMap) {
        if (ids.length) await this.redis.ack(key, 'cg:order-fib', ids);
      }
    }
  }

  private safeAck(ackMap: Map<string, string[]>, key: string, id: string) {
    if (!ackMap.has(key)) ackMap.set(key, []);
    ackMap.get(key)!.push(id);
  }
}
