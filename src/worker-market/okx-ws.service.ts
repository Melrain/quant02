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

  // 将短写 token 映射为 OKX instId；若已是完整 instId 则原样（转大写）
  private toInstId(token: string): string {
    const t = token.trim();
    if (!t) return '';
    const u = t.toUpperCase();
    // 已经是完整 instId（包含连字符），例如 BTC-USDT-SWAP
    if (u.includes('-')) return u;
    // 短写 -> 统一映射到 USDT 永续
    return `${u}-USDT-SWAP`;
  }

  // 解析环境变量：支持短写（btc,eth,...）或完整（BTC-USDT-SWAP,ETH-USDT-SWAP,...）
  private parseSymbolsFromEnv(): string[] {
    // 优先 OKX_ASSETS（短写），否则用 OKX_SYMBOLS（可短写或全称）
    const raw =
      process.env.OKX_ASSETS ??
      process.env.OKX_SYMBOLS ??
      'btc,eth,doge,ltc,shib,pump,wlfi,xpl'; // 默认短写
    const list = raw
      .split(',')
      .map((s) => this.toInstId(s))
      .filter(Boolean);
    return Array.from(new Set(list)); // 去重
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
    this.bus.on('okx.subscribed', (arg: any) =>
      this.logger.log(`Subscribed ${JSON.stringify(arg)}`),
    );
    this.bus.on('okx.unsubscribed', (arg: any) =>
      this.logger.log(`Unsubscribed ${JSON.stringify(arg)}`),
    );

    // 使用短写/混合环境变量
    const instIds = this.parseSymbolsFromEnv();
    this.logger.log(`Bootstrapping OKX symbols: ${instIds.join(', ')}`);

    await this.client.connect();
    await this.bootstrapSymbols(instIds);
  }

  async onModuleDestroy() {
    await this.client.close();
  }

  // 对外便捷 API（转调 client）
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

  // 可选：一键批量订阅
  async bootstrapSymbols(
    instIds: string[],
    opts?: {
      trades?: boolean;
      tickers?: boolean;
      bookDepth?: BookDepth;
      candles?: CandleBar[];
    },
  ) {
    const want = {
      trades: true,
      tickers: true,
      bookDepth: 'books5' as BookDepth,
      candles: [] as CandleBar[],
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
