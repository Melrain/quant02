import {
  Injectable,
  Logger,
  OnModuleInit,
  OnModuleDestroy,
} from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { OkxWsClient } from './okx-ws-client';

type CandleBar = Parameters<OkxWsClient['subscribeCandles']>[1];
type BookDepth = Parameters<OkxWsClient['subscribeOrderBook']>[1];

@Injectable()
export class OkxWsService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(OkxWsService.name);
  private subscribedArgs: { channel: string; instId: string }[] = []; // 可选：记录退订

  private toInstId(token: string): string {
    const t = token.trim();
    if (!t) return '';
    const u = t.toUpperCase();
    if (u.includes('-')) return u;
    return `${u}-USDT-SWAP`;
  }

  private parseSymbolsFromEnv(): string[] {
    const raw =
      process.env.OKX_ASSETS ??
      process.env.OKX_SYMBOLS ??
      'btc,eth,doge,ltc,shib,pump,wlfi,xpl';
    const list = raw
      .split(',')
      .map((s) => this.toInstId(s))
      .filter(Boolean);
    return Array.from(new Set(list));
  }

  constructor(
    private readonly client: OkxWsClient,
    private readonly bus: EventEmitter2,
  ) {}

  async onModuleInit() {
    this.bus.on('okx.open', () => this.logger.log('OKX WS open'));
    this.bus.on('okx.close', () => this.logger.warn('OKX WS close'));
    this.bus.on('okx.reconnected', () => this.logger.log('OKX WS reconnected'));
    this.bus.on('okx.error', (e: any) =>
      this.logger.warn(`OKX WS error: ${e?.message ?? e}`),
    );
    this.bus.on('okx.subscribed', (arg: any) => {
      this.logger.log(`Subscribed ${JSON.stringify(arg)}`);
      if (arg?.channel && arg?.instId) {
        this.subscribedArgs.push({ channel: arg.channel, instId: arg.instId });
      }
    });
    this.bus.on('okx.unsubscribed', (arg: any) =>
      this.logger.log(`Unsubscribed ${JSON.stringify(arg)}`),
    );

    const instIds = this.parseSymbolsFromEnv();
    this.logger.log(`Bootstrapping OKX symbols: ${instIds.join(', ')}`);

    await this.client.connect();
    await this.bootstrapSymbols(instIds, {
      trades: true,
      tickers: true,
      bookDepth: 'books5',
      candles: ['5m', '15m'], // ★ 默认订阅 5m/15m
    });
  }

  async onModuleDestroy() {
    // 可选：优雅退订（不是必须）
    try {
      if (this.subscribedArgs.length) {
        await this.client.unsubscribe(this.subscribedArgs);
      }
    } catch (e) {
      this.logger.warn(
        `unsubscribe on destroy failed: ${(e as Error).message}`,
      );
    }
    await this.client.close();
  }

  connect() {
    return this.client.connect();
  }
  close() {
    return this.client.close();
  }
  subscribe(arg: Parameters<OkxWsClient['subscribe']>[0]) {
    return this.client.subscribe(arg as any);
  }
  unsubscribe(arg: Parameters<OkxWsClient['unsubscribe']>[0]) {
    return this.client.unsubscribe(arg as any);
  }

  subscribeTickers(instId: string) {
    return this.client.subscribeTickers(instId);
  }
  subscribeTrades(instId: string) {
    return this.client.subscribeTrades(instId);
  }
  subscribeCandles(instId: string, bar?: CandleBar) {
    return this.client.subscribeCandles(instId, bar);
  }
  subscribeOrderBook(instId: string, depth?: BookDepth) {
    return this.client.subscribeOrderBook(instId, depth);
  }
  subscribeOpenInterest(instId: string) {
    return this.client.subscribeOpenInterest(instId);
  }
  subscribeFundingRate(instId: string) {
    return this.client.subscribeFundingRate(instId);
  }

  async bootstrapSymbols(
    instIds: string[],
    opts?: {
      trades?: boolean;
      tickers?: boolean;
      bookDepth?: BookDepth;
      candles?: CandleBar[];
      oi?: boolean;
      funding?: boolean;
    },
  ) {
    const want = {
      trades: true,
      tickers: true,
      bookDepth: 'books5' as BookDepth,
      candles: ['5m', '15m'] as CandleBar[],
      oi: true,
      funding: true,
      ...(opts ?? {}),
    };
    for (const s of instIds) {
      if (want.trades) await this.client.subscribeTrades(s);
      if (want.tickers) await this.client.subscribeTickers(s);
      if (want.oi) await this.client.subscribeOpenInterest(s);
      if (want.funding) await this.client.subscribeFundingRate(s);
      if (want.bookDepth)
        await this.client.subscribeOrderBook(s, want.bookDepth);
      for (const b of want.candles) await this.client.subscribeCandles(s, b);
    }
  }
}
