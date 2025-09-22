/* eslint-disable @typescript-eslint/no-base-to-string */
/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable @typescript-eslint/no-unsafe-call */
import { Injectable, Logger } from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';
import WebSocket from 'ws';
import { randomUUID } from 'crypto';
import {
  TickerEvent,
  TradeEvent,
  KlineEvent,
  OrderBookEvent,
  OpenInterestEvent,
  FundingRateEvent,
} from './okx-ws.types';
import { z } from 'zod';

/* ---------------- Zod schemas ---------------- */
const ZArg = z.object({
  channel: z.string(),
  instId: z.string().optional(),
  instType: z.string().optional(), // 仅非 candle 可用；我们不会给 candle 带
  uly: z.string().optional(),
  ccy: z.string().optional(),
});
const ZOkxDataMsg = z.object({
  arg: ZArg,
  data: z.array(z.any()),
  action: z.enum(['snapshot', 'update']).optional(),
});
const ZEventMsg = z.object({
  event: z.enum(['subscribe', 'unsubscribe', 'error']),
  arg: ZArg.optional(),
  code: z.string().optional(),
  msg: z.string().optional(),
});
type OkxChannelArg = z.infer<typeof ZArg>;
type OkxDataMsg = z.infer<typeof ZOkxDataMsg>;

/* ---------------- Helpers ---------------- */
const now = () => Date.now();
const subKey = (a: OkxChannelArg) =>
  JSON.stringify({ channel: a.channel, instId: a.instId });

/** 规范化 candle 频道名：5m/15m 保持 m 小写；1h→1H，1d→1D，1w→1W，1m→1m */
function toOkxCandleChannel(bar: string): string {
  const s = String(bar).trim().toLowerCase();
  const canon = s
    .replace(/h$/, 'H')
    .replace(/d$/, 'D')
    .replace(/w$/, 'W')
    .replace(/m$/, 'm');
  return `candle${canon}`;
}
/** 将 OKX candle channel 转回统一 tf：candle5m→5m, candle1H→1h */
function normalizeTf(ch: string): string {
  return ch.replace('candle', '').toLowerCase();
}

/* 为了兼容你的 OkxWsService 的参数类型提取： */
export type CandleBar =
  | '1m'
  | '3m'
  | '5m'
  | '15m'
  | '30m'
  | '1H'
  | '2H'
  | '4H'
  | '6H'
  | '12H'
  | '1D'
  | '1W'
  | '1M';

export type BookDepth = 'books5' | 'books' | 'books-l2-tbt';

/* ---------------- Client ---------------- */
@Injectable()
export class OkxWsClient {
  private readonly logger = new Logger(OkxWsClient.name);

  private publicUrls = ['wss://ws.okx.com:8443/ws/v5/public'];
  private businessUrls = ['wss://ws.okx.com:8443/ws/v5/business'];

  private wsPublic: WebSocket | null = null;
  private wsBusiness: WebSocket | null = null;

  /** 仅缓存 {channel, instId} 作为幂等键；附带原始 arg 备份（非 candle 可含 instType） */
  private subs = new Map<
    string,
    OkxChannelArg & { _target: 'public' | 'business' }
  >();

  constructor(private readonly eventBus: EventEmitter2) {}

  /* ---------- Public APIs ---------- */
  async connect() {
    if (!this.wsPublic)
      this.wsPublic = this.buildWs(this.publicUrls[0], 'public');
    if (!this.wsBusiness)
      this.wsBusiness = this.buildWs(this.businessUrls[0], 'business');
  }

  async close() {
    try {
      this.wsPublic?.terminate();
      this.wsBusiness?.terminate();
    } finally {
      this.wsPublic = this.wsBusiness = null;
    }
  }

  /** 支持单个或数组订阅；会自动路由到 public/business */
  async subscribe(
    arg:
      | (OkxChannelArg & { priority?: 1 | 2 | 3 })
      | Array<OkxChannelArg & { priority?: 1 | 2 | 3 }>,
  ) {
    const list = (Array.isArray(arg) ? arg : [arg]).filter((a) => !!a?.channel);
    if (!list.length) return;

    // 清洗参数：candle 只允许 channel+instId；其他保留常见字段
    const cleaned = list.map((a) => {
      const isCandle = a.channel.startsWith('candle');
      const target: 'public' | 'business' = isCandle ? 'business' : 'public';
      const b: OkxChannelArg = isCandle
        ? { channel: a.channel, instId: a.instId } // candle 禁止 instType/uly/ccy
        : {
            channel: a.channel,
            instId: a.instId,
            instType: a.instType,
            uly: a.uly,
            ccy: a.ccy,
          };
      const key = subKey(b);
      this.subs.set(key, { ...b, _target: target });
      return { cleaned: b, target };
    });

    for (const { cleaned: a, target } of cleaned) {
      const ws = target === 'business' ? this.wsBusiness : this.wsPublic;
      if (ws?.readyState === WebSocket.OPEN) {
        this.logger.debug(`OKX SUB via ${target} :: ${JSON.stringify([a])}`);
        ws.send(JSON.stringify({ op: 'subscribe', args: [a] }));
      }
    }
  }

  /** 支持单个或数组退订 */
  async unsubscribe(arg: OkxChannelArg | OkxChannelArg[]) {
    const list = (Array.isArray(arg) ? arg : [arg]).filter((a) => !!a?.channel);
    if (!list.length) return;

    for (const a of list) {
      const isCandle = a.channel.startsWith('candle');
      const target: 'public' | 'business' = isCandle ? 'business' : 'public';
      const key = subKey(a);
      this.subs.delete(key);
      const ws = target === 'business' ? this.wsBusiness : this.wsPublic;
      ws?.send(JSON.stringify({ op: 'unsubscribe', args: [a] }));
    }
  }

  /* 便捷 API —— 完全保留给 OkxWsService 使用 */
  subscribeTickers(instId: string) {
    return this.subscribe({ channel: 'tickers', instId, priority: 3 });
  }
  subscribeTrades(instId: string) {
    return this.subscribe({ channel: 'trades', instId, priority: 1 });
  }
  subscribeCandles(instId: string, bar: CandleBar = '1m') {
    // 允许传小写/大写，调用方传 '5m'|'15m' 即可
    const channel = toOkxCandleChannel(String(bar));
    return this.subscribe({ channel, instId, priority: 2 });
  }
  subscribeOrderBook(instId: string, depth: BookDepth = 'books5') {
    return this.subscribe({ channel: depth, instId, priority: 1 });
  }
  subscribeOpenInterest(instId: string) {
    return this.subscribe({ channel: 'open-interest', instId, priority: 2 });
  }
  subscribeFundingRate(instId: string) {
    return this.subscribe({ channel: 'funding-rate', instId, priority: 2 });
  }

  /* ---------- Internals ---------- */
  private buildWs(url: string, label: 'public' | 'business'): WebSocket {
    const ws = new WebSocket(url);

    ws.on('open', () => {
      this.logger.log(`${label} WS open`);
      // 重放对应目标的订阅
      const args = Array.from(this.subs.values()).filter(
        (a) => a._target === label,
      );
      if (args.length) {
        const cleaned = args.map((a) => {
          // 发送前去掉 _target
          const { _target, ...x } = a;
          return x;
        });
        this.logger.debug(
          `OKX RESUB via ${label} :: ${JSON.stringify(cleaned)}`,
        );
        // 分批发送
        const size = 20;
        for (let i = 0; i < cleaned.length; i += size) {
          ws.send(
            JSON.stringify({
              op: 'subscribe',
              args: cleaned.slice(i, i + size),
            }),
          );
        }
      }
      this.eventBus.emit('okx.open'); // 给外部 service 留的 hook
    });

    ws.on('message', (raw) => this.handleMessage(raw));
    ws.on('error', (e) => {
      this.logger.warn(`${label} WS error: ${e?.message ?? e}`);
      this.eventBus.emit('okx.error', e);
    });
    ws.on('close', () => {
      this.logger.warn(`${label} WS close, will reconnect`);
      this.eventBus.emit('okx.close');
      setTimeout(() => {
        if (label === 'public') this.wsPublic = this.buildWs(url, label);
        else this.wsBusiness = this.buildWs(url, label);
        this.eventBus.emit('okx.reconnected');
      }, 2000);
    });

    return ws;
  }

  private handleMessage(raw: WebSocket.RawData) {
    const text = typeof raw === 'string' ? raw : raw.toString('utf8');
    if (text === 'ping') return;
    if (text === 'pong') return;

    let json: any;
    try {
      json = JSON.parse(text);
    } catch {
      return;
    }

    // event frames (sub/unsub/error)
    const evt = ZEventMsg.safeParse(json);
    if (evt.success) {
      const m = evt.data;
      if (m.event === 'subscribe') this.eventBus.emit('okx.subscribed', m.arg);
      else if (m.event === 'unsubscribe')
        this.eventBus.emit('okx.unsubscribed', m.arg);
      else if (m.event === 'error') {
        const err = new Error(`${m.code ?? ''} ${m.msg ?? ''}`.trim());
        this.eventBus.emit('okx.error', err);
      }
      return;
    }

    // data frames
    const dm = ZOkxDataMsg.safeParse(json);
    if (!dm.success) return;
    const m = dm.data;
    const ch = m.arg.channel;

    if (ch.startsWith('candle')) this.handleCandles(m);
    else if (ch === 'tickers') this.handleTickers(m);
    else if (ch === 'trades') this.handleTrades(m);
    else if (ch.startsWith('books')) this.handleBooks(m);
    else if (ch === 'open-interest') this.handleOpenInterest(m);
    else if (ch === 'funding-rate') this.handleFundingRate(m);
  }

  /* ---------- Handlers → Domain events ---------- */
  private handleCandles(m: OkxDataMsg) {
    const { instId, channel } = m.arg;
    const tf = normalizeTf(channel); // 解析出 timeframe
    const recvTs = now();
    for (const c of m.data) {
      const ev: KlineEvent = {
        type: 'market.kline',
        src: 'okx',
        instId: instId!,
        tf,
        ts: Number(c[0]),
        open: c[1],
        high: c[2],
        low: c[3],
        close: c[4],
        vol: c[5],
        // ★ 新增：把报价币计成交额带出来（OKX 第 8 个字段）
        quoteVol: c[7], // <= 关键
        confirm: c.length >= 9 ? (Number(c[8]) as 0 | 1) : undefined,
        ingestId: randomUUID(),
        recvTs,
      };
      this.eventBus.emit(ev.type, ev);
    }
  }

  private handleTrades(m: OkxDataMsg) {
    const { instId } = m.arg;
    const recvTs = now();
    for (const t of m.data) {
      const ev: TradeEvent = {
        type: 'market.trade',
        src: 'okx',
        instId: instId!,
        ts: Number(t.ts),
        px: t.px,
        qty: t.sz,
        side: t.side,
        taker: 1,
        tradeId: t.tradeId,
        ingestId: randomUUID(),
        recvTs,
      };
      this.eventBus.emit(ev.type, ev);
    }
  }

  private handleTickers(m: OkxDataMsg) {
    const { instId } = m.arg;
    const recvTs = now();
    for (const d of m.data) {
      const ev: TickerEvent = {
        type: 'market.ticker',
        src: 'okx',
        instId: instId!,
        ts: Number(d.ts),
        last: d.last,
        bid1: d.bidPx ? { px: d.bidPx, sz: d.bidSz } : undefined,
        ask1: d.askPx ? { px: d.askPx, sz: d.askSz } : undefined,
        open24h: d.open24h,
        high24h: d.high24h,
        low24h: d.low24h,
        vol24h: d.vol24h,
        volCcy24h: d.volCcy24h,
        ingestId: randomUUID(),
        recvTs,
      };
      this.eventBus.emit(ev.type, ev);
    }
  }

  private handleBooks(m: OkxDataMsg) {
    const { instId } = m.arg;
    const d = m.data[0];
    if (!d) return;
    const recvTs = now();

    const bids: [string, string][] = (d.bids ?? []).map((x: any[]) => [
      String(x[0]),
      String(x[1]),
    ]);
    const asks: [string, string][] = (d.asks ?? []).map((x: any[]) => [
      String(x[0]),
      String(x[1]),
    ]);
    const bid1 = bids[0] ? { px: bids[0][0], sz: bids[0][1] } : undefined;
    const ask1 = asks[0] ? { px: asks[0][0], sz: asks[0][1] } : undefined;

    const sumTopN = (levels: [string, string][], n: number) =>
      levels.slice(0, n).reduce((acc, [, sz]) => acc + Number(sz), 0);

    const ev: OrderBookEvent = {
      type: 'market.book',
      src: 'okx',
      instId: instId!,
      ts: Number(d.ts),
      bid1,
      ask1,
      bidSz10: String(sumTopN(bids, 10)),
      askSz10: String(sumTopN(asks, 10)),
      spread:
        bid1 && ask1 ? String(Number(ask1.px) - Number(bid1.px)) : undefined,
      snapshot:
        m.action === 'snapshot' ||
        (!m.action && bids.length > 0 && asks.length > 0),
      action: m.action as any,
      ingestId: randomUUID(),
      recvTs,
    };
    this.eventBus.emit(ev.type, ev);
  }

  private handleOpenInterest(m: OkxDataMsg) {
    const recvTs = now();
    for (const d of m.data) {
      const ev: OpenInterestEvent = {
        type: 'market.oi',
        src: 'okx',
        instId: d.instId ?? m.arg.instId!,
        ts: Number(d.ts),
        oi: d.oi,
        oiCcy: d.oiCcy,
        ingestId: randomUUID(),
        recvTs,
      };
      this.eventBus.emit(ev.type, ev);
    }
  }

  private handleFundingRate(m: OkxDataMsg) {
    const recvTs = now();
    for (const d of m.data) {
      const ev: FundingRateEvent = {
        type: 'market.funding',
        src: 'okx',
        instId: d.instId ?? m.arg.instId!,
        ts: Number(d.ts),
        rate: d.fundingRate ?? d.fundRate ?? d.rate,
        nextFundingTime: d.nextFundingTime
          ? Number(d.nextFundingTime)
          : undefined,
        ingestId: randomUUID(),
        recvTs,
      };
      this.eventBus.emit(ev.type, ev);
    }
  }
}
