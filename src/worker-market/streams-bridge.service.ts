/* eslint-disable @typescript-eslint/no-unsafe-return */
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

  // ② 在类里加一个 tf→毫秒 的工具（放在 methods 里，任何位置都行）
  private tfToMs(tf: string): number | undefined {
    const m = {
      '1m': 60_000,
      '3m': 3 * 60_000,
      '5m': 5 * 60_000,
      '15m': 15 * 60_000,
      '30m': 30 * 60_000,
      '1h': 60 * 60_000,
      '2h': 2 * 60 * 60_000,
      '4h': 4 * 60 * 60_000,
      '6h': 6 * 60 * 60_000,
      '12h': 12 * 60 * 60_000,
      '1d': 24 * 60 * 60_000,
      '1w': 7 * 24 * 60 * 60_000,
      '1mth': 30 * 24 * 60 * 60_000, // 若用到月线，OKX月线是 1M，normalize 后你若转成 '1mth' 就能匹配
    } as const;
    return (m as any)[tf];
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
  // ③ 用下面这段替换你现在的 handleKline（整段覆盖）
  @OnEvent('market.kline', { async: true })
  async handleKline(
    ev: AnyMarketEvent & {
      tf: string; // '5m' | '15m' | '30m' | '1h' ...
      open: string;
      high: string;
      low: string;
      close: string;
      vol: string; // OKX 第6字段（成交量，单位视合约）
      confirm?: 0 | 1; // OKX 第9字段，1=已收线
      quoteVol?: string; // 若 ws-client 已带 c[7]（报价币成交额）
    },
  ) {
    try {
      // --- A) 原始 WS kline 流（可选保留用于排障回放） ---
      const wsKey = this.stream.buildKlineKey(ev.instId, ev.tf); // ws:{sym}:kline5m
      await this.stream.xadd(
        wsKey,
        {
          type: ev.type,
          src: ev.src,
          instId: ev.instId,
          ts: String(ev.ts), // ★ OKX c[0] 是“bar 开始时间（毫秒）”
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

      // --- B) 计算本根 K 的收盘时间（用于对齐你内部时间轴） ---
      const tfMs = this.tfToMs(ev.tf);
      const startTs = ev.ts; // bar 开始
      const closeTs = tfMs ? startTs + tfMs : startTs; // bar 收盘（若未知 tf 就用 startTs 兜底）

      // --- C) 实时快照（Hash）：每 tick 覆盖，供其他模块秒级读取 ---
      // 你主要关心 5m / 15m，就只对这两个写；要扩到 30m/1h，直接把判断里加上
      if (ev.tf === '5m' || ev.tf === '15m') {
        const stateKey = this.stream.buildOutKey(
          ev.instId,
          `win:state:${ev.tf}`,
        );
        await this.stream.hset(stateKey, {
          startTs: String(startTs),
          closeTs: String(closeTs),
          updatedTs: String(ev.recvTs ?? Date.now()),
          open: ev.open,
          high: ev.high,
          low: ev.low,
          last: ev.close, // 用 close 当作当前“last”
          vol: ev.vol ?? '',
          volCcyQuote: ev.quoteVol ?? '',
          confirm: ev.confirm ? '1' : '0',
          source: 'exchange',
        });
        await this.stream.expire(stateKey, 60 * 60); // 1h TTL
      }

      // --- D) 历史序列（Stream）：仅在 confirm=1 时追加，且按 bar 开始时间去重 ---
      // 注意：xadd 里我们用 closeTs 作为 ts 字段，让时间线按“收盘点”对齐
      if ((ev.tf === '5m' || ev.tf === '15m') && ev.confirm === 1) {
        const dedupKey = `${ev.instId}|${ev.tf}`;
        const lastStart = this.lastConfirmedBar.get(dedupKey) ?? 0;
        if (startTs > lastStart) {
          const winKey = this.stream.buildOutKey(ev.instId, `win:${ev.tf}`);
          await this.stream.xadd(
            winKey,
            {
              ts: String(closeTs), // ★ 用“收盘时刻”写入，避免和你的 1m 聚合时间轴不一致
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
          this.lastConfirmedBar.set(dedupKey, startTs);
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

    // 多写一份hash，方便快速读取最新值
    const stateKey = this.stream.buildOutKey(ev.instId, 'state:oi');
    await this.stream.hset(stateKey, {
      ts: String(ev.ts),
      oi: ev.oi,
      oiCcy: ev.oiCcy ?? '',
      updatedTs: String(ev.recvTs ?? Date.now()),
      source: 'exchange',
    });
    await this.stream.expire(stateKey, 3600); // 1h TTL
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
    // 多写一份hash，方便快速读取最新值
    const stateKey = this.stream.buildOutKey(ev.instId, 'state:funding');
    await this.stream.hset(stateKey, {
      ts: String(ev.ts),
      rate: ev.rate,
      nextFundingTime: ev.nextFundingTime ? String(ev.nextFundingTime) : '',
      updatedTs: String(ev.recvTs ?? Date.now()),
      source: 'exchange',
    });
    await this.stream.expire(stateKey, 4 * 3600); // 4h
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
