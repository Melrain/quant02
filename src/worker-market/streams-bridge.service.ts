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
  ts: number; // exchange ms (注意：K线里是“开盘时间”)
  ingestId?: string;
  recvTs?: number;
};

@Injectable()
export class StreamsBridgeService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(StreamsBridgeService.name);

  // 盘口聚合缓存（降频）
  private lastBookByInst = new Map<
    string,
    { ts: number; payload: Record<string, string> }
  >();
  private bookFlushTimer?: NodeJS.Timeout;

  // 已确认K线去重：key = instId|tf -> lastConfirmedStartTs
  private lastConfirmedBar = new Map<string, number>();

  constructor(
    private readonly events: EventEmitter2,
    private readonly stream: RedisStreamsService,
  ) {}

  async onModuleInit() {
    // 可选：首次部署时统一 ensure group
    // const k = this.stream.buildKey('BTC-USDT-SWAP', 'trades');
    // await this.stream.ensureGroup(k, 'cg:window', '$');

    // 开启盘口聚合降频
    this.bookFlushTimer = setInterval(() => {
      void this.flushBooks();
    }, 80);
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
  }

  private async flushBooks() {
    if (!this.lastBookByInst.size) return;
    const entries = Array.from(this.lastBookByInst.entries());
    this.lastBookByInst.clear();
    await Promise.all(
      entries.map(async ([instId, { payload }]) => {
        const key = this.stream.buildKey(instId, 'book');
        await this.stream.xadd(key, payload, { maxlenApprox: 2_000 });
      }),
    ).catch((err) =>
      this.logger.warn(`flushBooks error: ${String(err?.message || err)}`),
    );
  }

  /* ---------- K线（5m/15m 收线落地 + 实时快照） ---------- */
  @OnEvent('market.kline', { async: true })
  async handleKline(
    ev: AnyMarketEvent & {
      tf: string; // '5m' | '15m' | '1h' ...
      open: string;
      high: string;
      low: string;
      close: string;
      vol: string; // OKX第6字段
      confirm?: 0 | 1; // OKX第9字段(0/1)，1=已收线
      quoteVol?: string; // 可选：如在 ws-client 里带出 c[7]/c[6]
    },
  ) {
    try {
      // 0) 写原始 WS kline 流（可选，保留用于排障/回溯）
      const wsKey = this.stream.buildKlineKey(ev.instId, ev.tf); // ws:{sym}:kline5m 等
      await this.stream.xadd(
        wsKey,
        {
          type: ev.type,
          src: ev.src,
          instId: ev.instId,
          ts: String(ev.ts), // ★ OKX 这里是“开盘时间”
          tf: ev.tf,
          confirm: ev.confirm != null ? String(ev.confirm) : '0',
          o: ev.open,
          h: ev.high,
          l: ev.low,
          c: ev.close,
          vol: ev.vol ?? '',
          volCcyQuote: ev.quoteVol ?? '',
          recvTs: ev.recvTs ? String(ev.recvTs) : undefined,
          ingestId: ev.ingestId,
        },
        { maxlenApprox: 2_000 },
      );

      // 1) 计算 closeTs（开盘 + 周期）
      const tfMs =
        ev.tf === '5m' ? 5 * 60_000 : ev.tf === '15m' ? 15 * 60_000 : undefined; // 其他周期需要时再补
      const closeTs = tfMs ? ev.ts + tfMs : ev.ts;

      // 2) 快照：每 tick 覆盖，方便实时读取
      if (ev.tf === '5m' || ev.tf === '15m') {
        const stateKey = this.stream.buildOutKey(
          ev.instId,
          `win:state:${ev.tf}`,
        );
        await this.stream.hset(stateKey, {
          startTs: String(ev.ts), // 开盘
          closeTs: String(closeTs), // 收盘
          updatedTs: String(ev.recvTs ?? Date.now()),
          open: ev.open,
          high: ev.high,
          low: ev.low,
          last: ev.close,
          vol: ev.vol ?? '',
          volCcyQuote: ev.quoteVol ?? '',
          confirm: ev.confirm ? '1' : '0',
          source: 'exchange',
        });
        await this.stream.expire(stateKey, 60 * 60); // 1h TTL
      }

      // 3) 历史序列：仅在 confirm=1 时追加，且做去重
      if ((ev.tf === '5m' || ev.tf === '15m') && ev.confirm === 1) {
        const k = `${ev.instId}|${ev.tf}`;
        const last = this.lastConfirmedBar.get(k) ?? 0;
        if (ev.ts > last) {
          const winKey = this.stream.buildOutKey(ev.instId, `win:${ev.tf}`);
          await this.stream.xadd(
            winKey,
            {
              ts: String(closeTs), // ★ 用收盘时刻对齐时间轴
              open: ev.open,
              high: ev.high,
              low: ev.low,
              close: ev.close,
              vol: ev.vol ?? '',
              volCcyQuote: ev.quoteVol ?? '',
              confirm: '1',
              src: ev.src ?? 'okx',
            },
            { maxlenApprox: 5_000 },
          );
          this.lastConfirmedBar.set(k, ev.ts);
        }
      }
    } catch (err) {
      this.logger.warn(
        `handleKline error: ${String((err as Error)?.message || err)}`,
      );
    }
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
