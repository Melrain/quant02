/* eslint-disable @typescript-eslint/require-await */
/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import { Injectable, Logger } from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';
import WebSocket from 'ws';

type OkxChannelArg = {
  channel: string;
  instId?: string;
  instType?: string;
  uly?: string;
  ccy?: string;
};

type OkxDataMsg = {
  arg: OkxChannelArg;
  data: any[];
  action?: 'snapshot' | 'update';
};

@Injectable()
export class OkxWsClient {
  private url = 'wss://ws.okx.com:8443/ws/v5/public';
  private ws: WebSocket | null = null;
  private manualClose = false;
  private pingTimer: NodeJS.Timeout | null = null;
  private reConnectDelay = 1000;
  private readonly maxReconnectDelay = 10_000;

  private readonly logger = new Logger(OkxWsClient.name);
  // 建立一个池子，存储所有的订阅
  private readonly subs = new Map<string, OkxChannelArg>();
  // 用于标记 是否已收到首帧快照
  private readonly seenSnapShot = new Set<string>();

  // 注入全局事件总线
  constructor(private readonly eventBus: EventEmitter2) {}

  //连接与关闭
  async connect() {
    // 避免重复连接
    if (
      this.ws &&
      (this.ws.readyState === WebSocket.OPEN ||
        this.ws.readyState === WebSocket.CONNECTING)
    )
      return;

    // 重置标记
    this.manualClose = false;
    // 建立连接
    this.ws = new WebSocket(this.url);

    this.ws.on('open', () => {
      this.eventBus.emit('okx.ws.open');
      this.reConnectDelay = 1000; // 重置重连延时

      // 重新订阅
      const args = Array.from(this.subs.values());
      if (args.length) this.send({ op: 'subscribe', args });

      // 启动心跳
      this.startPing();
    });

    this.ws.on('message', (raw) => {
      // eslint-disable-next-line @typescript-eslint/no-base-to-string
      const text = typeof raw === 'string' ? raw : raw.toString('utf8');
      if (text === 'ping') {
        this.safeSend('pong');
        return;
      }
      if (text === 'pong') return;

      let msg: any;
      try {
        msg = JSON.parse(text);
      } catch {
        return;
      }

      // 基础的事件比如连接断开错误
      if (msg?.event === 'subscribe') {
        this.eventBus.emit('okx.subscribed', msg.arg);
      }
      if (msg?.event === 'unsubscribe') {
        this.eventBus.emit('okx.unsubscribed', msg.arg);
      }
      if (msg?.event === 'error') {
        this.eventBus.emit(
          'okx.ws.error',
          new Error(`${msg.code ?? ''} ${msg.msg ?? ''}`.trim()),
        );
        return;
      }

      // 数据事件
      if (msg?.arg && Array.isArray(msg?.data)) {
        // 原始数据广播，业务层可以自行处理或在此做映射
        this.eventBus.emit('okx.ws.data', msg as OkxDataMsg);
      }
    });
  }

  // 工具类
  private send(obj: any) {
    this.safeSend(JSON.stringify(obj));
  }
  private safeSend(payload: string) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    try {
      this.ws.send(payload);
    } catch {
      this.logger.debug('ws send error, ignored');
    }
  }

  private startPing() {
    this.stopPing();
    this.pingTimer = setInterval(() => this.safeSend('ping'), 20_2000);
  }

  private stopPing() {
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
  }
}
