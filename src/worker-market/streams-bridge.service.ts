import {
  Injectable,
  Logger,
  OnModuleInit,
  OnModuleDestroy,
} from '@nestjs/common';
import { EventEmitter2, OnEvent } from '@nestjs/event-emitter';
import { RedisStreamsService } from 'src/redis-streams/redis-streams.service';

type AnyMarketEvent = {
  type: string; // e.g. 'market.trade'
  src: 'okx';
  instId: string; // e.g. 'BTC-USDT-SWAP'
  ts: number; // exchange ms
  ingestId?: string;
  recvTs?: number;
  // plus channel-specific fields...
};

@Injectable()
export class StreamsBridgeService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(StreamsBridgeService.name);
  // 简单节流缓存（盘口帧聚合用）
  private lastBookByInst = new Map<
    string,
    { ts: number; payload: Record<string, string> }
  >();
  private bookFlushTimer?: NodeJS.Timeout;

  constructor(
    private readonly events: EventEmitter2,
    private readonly stream: RedisStreamsService,
  ) {}

  async onModuleInit() {
    // 一次性创建消费组（可选，首次部署时跑一次即可）
    // 这里演示创建 trades 组，实际你可以在部署脚本里统一创建
    // const k = this.stream.buildKey('BTC-USDT-SWAP', 'trades');
    // await this.stream.ensureGroup(k, 'cg:window', '$');

    // 开启盘口聚合定时器（如果需要）
    this.bookFlushTimer = setInterval(() => {
      void this.flushBooks();
    }, 80); // 80~100ms
    this.logger.log('StreamsBridgeService started');
  }

  async onModuleDestroy() {
    if (this.bookFlushTimer) clearInterval(this.bookFlushTimer);
  }

  /* ---------- 交易 ---------- */
  @OnEvent('market.trade', { async: true })
  async handleTrade(
    ev: AnyMarketEvent & {
      px: string;
      qty: string;
      side: 'buy' | 'sell';
      tradeId?: string;
      taker?: number;
    },
  ) {
    const key = this.stream.buildKey(ev.instId, 'trades');
    await this.stream.xadd(
      key,
      {
        type: ev.type,
        src: ev.src,
        instId: ev.instId,
        ts: String(ev.ts),
        recvTs: ev.recvTs ? String(ev.recvTs) : undefined,
        ingestId: ev.ingestId,
        px: ev.px,
        qty: ev.qty,
        side: ev.side,
        taker: ev.taker != null ? String(ev.taker) : undefined,
        tradeId: ev.tradeId,
      },
      { maxlenApprox: 10_000 },
    );
  }

  /* ---------- 盘口（聚合写入，降频） ---------- */
  @OnEvent('market.book', { async: true })
  async handleBook(
    ev: AnyMarketEvent & {
      bid1?: { px: string; sz: string };
      ask1?: { px: string; sz: string };
      bidSz10?: string;
      askSz10?: string;
      spread?: string;
      snapshot?: boolean;
      u?: number;
      pu?: number;
      checksum?: number;
      action?: 'snapshot' | 'update';
    },
  ) {
    // 组一个轻量 payload
    const payload: Record<string, string> = {
      type: ev.type,
      src: ev.src,
      instId: ev.instId,
      ts: String(ev.ts),
      recvTs: ev.recvTs ? String(ev.recvTs) : '',
      ingestId: ev.ingestId ?? '',
      'bid1.px': ev.bid1?.px ?? '',
      'bid1.sz': ev.bid1?.sz ?? '',
      'ask1.px': ev.ask1?.px ?? '',
      'ask1.sz': ev.ask1?.sz ?? '',
      bidSz10: ev.bidSz10 ?? '',
      askSz10: ev.askSz10 ?? '',
      spread: ev.spread ?? '',
      snapshot: ev.snapshot ? '1' : '0',
      u: ev.u != null ? String(ev.u) : '',
      pu: ev.pu != null ? String(ev.pu) : '',
      checksum: ev.checksum != null ? String(ev.checksum) : '',
      action: ev.action ?? '',
    };
    this.lastBookByInst.set(ev.instId, { ts: ev.ts, payload });
    // 不立刻 xadd，定时器批量 flush 降频
  }

  private async flushBooks() {
    if (!this.lastBookByInst.size) return;
    const entries = Array.from(this.lastBookByInst.entries());
    this.lastBookByInst.clear();
    // 批量写（可选：用 pipeline；这里逐条也可）
    await Promise.all(
      entries.map(async ([instId, { payload }]) => {
        const key = this.stream.buildKey(instId, 'book');
        await this.stream.xadd(key, payload, { maxlenApprox: 2_000 });
      }),
    ).catch((err) =>
      this.logger.warn(`flushBooks error: ${String(err?.message || err)}`),
    );
  }

  /* ---------- K线 ---------- */
  @OnEvent('market.kline', { async: true })
  async handleKline(
    ev: AnyMarketEvent & {
      tf: string;
      open: string;
      high: string;
      low: string;
      close: string;
      vol: string;
      confirm?: 0 | 1;
    },
  ) {
    const key = this.stream.buildKlineKey(ev.instId, ev.tf); // ws:{sym}:kline1m/5m...
    await this.stream.xadd(
      key,
      {
        type: ev.type,
        src: ev.src,
        instId: ev.instId,
        ts: String(ev.ts),
        tf: ev.tf,
        confirm: ev.confirm != null ? String(ev.confirm) : undefined,
        o: ev.open,
        h: ev.high,
        l: ev.low,
        c: ev.close,
        vol: ev.vol,
        recvTs: ev.recvTs ? String(ev.recvTs) : undefined,
        ingestId: ev.ingestId,
      },
      { maxlenApprox: 2_000 },
    );
  }

  /* ---------- OI ---------- */
  @OnEvent('market.oi', { async: true })
  async handleOi(ev: AnyMarketEvent & { oi: string; oiCcy?: string }) {
    const key = this.stream.buildKey(ev.instId, 'oi');
    await this.stream.xadd(
      key,
      {
        type: ev.type,
        src: ev.src,
        instId: ev.instId,
        ts: String(ev.ts),
        oi: ev.oi,
        oiCcy: ev.oiCcy ?? '',
        recvTs: ev.recvTs ? String(ev.recvTs) : undefined,
        ingestId: ev.ingestId,
      },
      { maxlenApprox: 1_000 },
    );
  }

  /* ---------- Funding ---------- */
  @OnEvent('market.funding', { async: true })
  async handleFunding(
    ev: AnyMarketEvent & { rate: string; nextFundingTime?: number },
  ) {
    const key = this.stream.buildKey(ev.instId, 'funding');
    await this.stream.xadd(
      key,
      {
        type: ev.type,
        src: ev.src,
        instId: ev.instId,
        ts: String(ev.ts),
        rate: ev.rate,
        nextFundingTime:
          ev.nextFundingTime != null ? String(ev.nextFundingTime) : undefined,
        recvTs: ev.recvTs ? String(ev.recvTs) : undefined,
        ingestId: ev.ingestId,
      },
      { maxlenApprox: 1_000 },
    );
  }

  /* ---------- Ticker（可选） ---------- */
  @OnEvent('market.ticker', { async: true })
  async handleTicker(
    ev: AnyMarketEvent & {
      last: string;
      bid1?: { px: string; sz: string };
      ask1?: { px: string; sz: string };
      open24h?: string;
      high24h?: string;
      low24h?: string;
      vol24h?: string;
      volCcy24h?: string;
    },
  ) {
    const key = this.stream.buildKey(ev.instId, 'ticker');
    await this.stream.xadd(
      key,
      {
        type: ev.type,
        src: ev.src,
        instId: ev.instId,
        ts: String(ev.ts),
        last: ev.last,
        'bid1.px': ev.bid1?.px ?? '',
        'bid1.sz': ev.bid1?.sz ?? '',
        'ask1.px': ev.ask1?.px ?? '',
        'ask1.sz': ev.ask1?.sz ?? '',
        open24h: ev.open24h ?? '',
        high24h: ev.high24h ?? '',
        low24h: ev.low24h ?? '',
        vol24h: ev.vol24h ?? '',
        volCcy24h: ev.volCcy24h ?? '',
        recvTs: ev.recvTs ? String(ev.recvTs) : undefined,
        ingestId: ev.ingestId,
      },
      { maxlenApprox: 2_000 },
    );
  }
}
