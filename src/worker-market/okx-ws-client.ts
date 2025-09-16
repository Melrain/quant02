/* eslint-disable @typescript-eslint/no-unsafe-call */ // 关闭对“可能不安全调用”的 ESLint 警告（ws 原始数据解析场景常见）
// apps/worker-market/src/okx/okx-ws-client.ts // 标注来源，便于溯源
import { Injectable, Logger } from '@nestjs/common'; // Nest 依赖注入与日志
import { EventEmitter2 } from '@nestjs/event-emitter'; // 事件总线，用于对外发布标准化事件
import WebSocket from 'ws'; // Node WebSocket 客户端
import { randomUUID } from 'crypto'; // 生成事件追踪用的唯一 ID
import {
  TickerEvent,
  TradeEvent,
  KlineEvent,
  OrderBookEvent,
  OpenInterestEvent,
  FundingRateEvent,
} from './okx-ws.types'; // 统一定义的领域事件类型
import { z } from 'zod'; // 轻量运行时校验库，用于解析 OKX 消息

// ------------------------------
// Minimal protocol schemas (Zod)
// ------------------------------
// OKX 消息中的 arg 字段结构
const ZArg = z.object({
  channel: z.string(), // 频道名，例如 tickers、trades、books5、candle1m 等
  instId: z.string().optional(), // 交易对，如 BTC-USDT-SWAP
  instType: z.string().optional(), // 合约类型，如 SWAP/FUTURES/OPTION
  uly: z.string().optional(), // 标的，如 BTC-USD
  ccy: z.string().optional(), // 币种，如 USDT
});

// OKX 数据帧（包含实际行情数据）
const ZOkxDataMsg = z.object({
  arg: ZArg, // 通道参数
  data: z.array(z.any()), // 数据数组（不同频道结构不同）
  action: z.enum(['snapshot', 'update']).optional(), // 对 order book 有意义
});

// OKX 订阅状态与错误事件帧
const ZEventMsg = z.object({
  event: z.enum(['subscribe', 'unsubscribe', 'error']), // 事件类型
  arg: ZArg.optional(), // 对应的通道参数
  code: z.string().optional(), // 错误码
  msg: z.string().optional(), // 错误信息
});

// TS 类型推断：订阅参数
type OkxChannelArg = z.infer<typeof ZArg>;
// TS 类型推断：数据消息
type OkxDataMsg = z.infer<typeof ZOkxDataMsg>;

// ------------------------------
// Helpers
// ------------------------------
const now = () => Date.now(); // 毫秒时间戳，简化可测试性

/**
 * 精确相加两个正小数字符串（避免浮点误差）。
 * 示例："1.2" + "3.45" = "4.65"。
 */
function addDecStr(a?: string, b?: string): string | undefined {
  if (!a && !b) return undefined; // 两者都为空 -> undefined
  if (!a) return b!; // 仅 b 有值 -> b
  if (!b) return a; // 仅 a 有值 -> a
  // 小数对齐（补零），变为大整数相加
  const [ai, af = ''] = a.split('.'); // a 的整数与小数部分
  const [bi, bf = ''] = b.split('.'); // b 的整数与小数部分
  const n = Math.max(af.length, bf.length); // 小数位最大长度
  const A = ai + af.padEnd(n, '0'); // a 按 n 位补零
  const B = bi + bf.padEnd(n, '0'); // b 按 n 位补零

  // 大整数逐位相加
  let carry = 0; // 进位
  let res = ''; // 结果字符串
  for (
    let i = A.length - 1, j = B.length - 1; // 从低位到高位
    i >= 0 || j >= 0 || carry; // 任一存在则继续
    i--, j--
  ) {
    const da = i >= 0 ? A.charCodeAt(i) - 48 : 0; // A 当前位数字
    const db = j >= 0 ? B.charCodeAt(j) - 48 : 0; // B 当前位数字
    const s = da + db + carry; // 位和 + 进位
    res = String.fromCharCode(48 + (s % 10)) + res; // 取个位拼接
    carry = Math.floor(s / 10); // 更新进位
  }
  if (n === 0) return res.replace(/^0+(\d)/, '$1'); // 无小数 -> 去除多余前导 0
  const head = res.slice(0, -n) || '0'; // 整数部分
  const tail = res.slice(-n).replace(/0+$/, ''); // 小数部分去尾随 0
  return tail.length ? `${head}.${tail}` : head; // 组装结果
}

/** 将 OKX candle 频道转为统一 tf 表达（如 "candle1H" -> "1h"） */
function normalizeTf(ch: string): string {
  const raw = ch.replace('candle', ''); // 去掉前缀 candle
  return raw.toLowerCase(); // 统一小写
}

/** 指数退避 + 抖动，避免重连风暴 */
function nextDelay(prev: number, max: number): number {
  const base = Math.min(prev * 2, max); // 指数退避
  const jitter = base * (Math.random() * 0.2 - 0.1); // 加入 ±10% 抖动
  return Math.max(500, Math.floor(base + jitter)); // 不低于 500ms
}

/** 构造稳定的订阅 key，用于 Map/Set 去重与跟踪 */
function subKey(a: OkxChannelArg): string {
  const { channel, instId, instType, uly, ccy } = a; // 关键字段
  return JSON.stringify({ channel, instId, instType, uly, ccy }); // 作为唯一键
}

// ------------------------------
// Client
// ------------------------------
// 允许注入到 Nest 容器
@Injectable()
export class OkxWsClient {
  // 组件专属日志器
  private readonly logger = new Logger(OkxWsClient.name);

  // public endpoints (可扩容为多域名容灾池)
  // 公共行情 WS 域名列表（可扩容容灾）
  private urls = ['wss://ws.okx.com:8443/ws/v5/public'];
  // 轮询下标
  private urlIdx = 0;

  // 当前 WebSocket 连接
  private ws: WebSocket | null = null;
  // 是否用户主动关闭（避免自动重连）
  private manualClose = false;

  // heartbeats / liveness
  // ping 定时器
  private pingTimer: NodeJS.Timeout | null = null;
  // 看门狗定时器
  private watchdogTimer: NodeJS.Timeout | null = null;
  // 最近收到数据/心跳时间
  private lastActivityTs = 0;
  // ping 周期
  private readonly pingIntervalMs = 20_000;
  // 超时未活动则重建连接
  private readonly idleTimeoutMs = 40_000;

  // reconnect/backoff
  // 初始重连延迟
  private reConnectDelay = 1000;
  // 最大重连延迟
  private readonly maxReconnectDelay = 10_000;

  // subscriptions & first-snapshot tracking
  private readonly subs = new Map<
    string,
    OkxChannelArg & { priority?: 1 | 2 | 3 }
  >();
  // 已持久化的订阅（用于重连回放）
  private readonly seenSnapShot = new Set<string>();
  // 记录 order book 首个快照是否已到

  // failed sub retry queue
  private readonly retryQueue: Array<
    OkxChannelArg & { priority?: 1 | 2 | 3; __retry?: number }
  > = [];
  // 订阅失败后待重试队列

  // 注入事件总线
  constructor(private readonly eventBus: EventEmitter2) {}

  // --------------------------
  // Public APIs
  // --------------------------
  // 建立连接（含重放订阅）
  async connect(): Promise<void> {
    if (
      this.ws &&
      (this.ws.readyState === WebSocket.OPEN ||
        this.ws.readyState === WebSocket.CONNECTING)
    )
      return;

    this.manualClose = false; // 标记为自动控制的连接
    const url = this.urls[this.urlIdx % this.urls.length]; // 取当前域名
    this.logger.log(`Connecting to ${url}`); // 打印连接中
    this.ws = new WebSocket(url); // 建立 WS

    // 连接成功回调
    this.ws.on('open', () => {
      this.eventBus.emit('okx.open'); // 发出打开事件
      this.logger.log('WS open'); // 日志
      this.reConnectDelay = 1000; // 重置退避
      this.seenSnapShot.clear(); // 重要：重连后需等待新快照
      this.startPing(); // 启动心跳
      this.touchActivity(); // 更新活跃时间

      // 重放订阅（按优先级 1 高 -> 3 低）
      // 取所有已登记订阅
      const args = Array.from(this.subs.values());
      if (args.length) {
        const sorted = args.sort(
          (a, b) => (a.priority ?? 2) - (b.priority ?? 2), // 排序
        );
        this.batchSubscribe(sorted); // 批量订阅
      }

      // 顺带尝试发送之前失败的订阅
      this.drainRetryQueue();
    });

    // 统一处理消息
    this.ws.on('message', (raw) => this.handleMessage(raw));
    // 错误事件
    this.ws.on('error', (e) => {
      this.eventBus.emit('okx.error', e); // 对外抛错
      this.logger.warn(`WS error: ${e?.message}`); // 打印错误信息
    });
    // 连接关闭事件
    this.ws.on('close', () => {
      this.logger.warn('WS close'); // 提示关闭
      this.stopPing(); // 停止心跳
      this.stopWatchdog(); // 停止看门狗
      this.ws = null; // 释放引用
      this.eventBus.emit('okx.close'); // 发出关闭事件
      if (this.manualClose) return; // 手动关闭则不重连

      // 轮询下一个域名（简单容灾）
      this.urlIdx = (this.urlIdx + 1) % this.urls.length;

      // 延迟重连（退避）
      setTimeout(() => {
        this.connect()
          .then(() => this.eventBus.emit('okx.reconnected')) // 通知已重连
          .catch(() => {}); // 忽略异常，交给下轮
      }, this.reConnectDelay);
      this.reConnectDelay = nextDelay(
        this.reConnectDelay,
        this.maxReconnectDelay,
      ); // 更新退避
    });
  }

  // 主动关闭连接
  async close(): Promise<void> {
    this.manualClose = true; // 标记手动关闭
    this.stopPing(); // 停止心跳
    this.stopWatchdog(); // 停止看门狗
    try {
      this.ws?.terminate(); // 立刻终止连接
    } catch {
      this.logger.warn('WS terminate failed'); // 终止失败日志
    }
    this.ws = null; // 释放引用
  }

  async subscribe(
    arg:
      | (OkxChannelArg & { priority?: 1 | 2 | 3 })
      | Array<OkxChannelArg & { priority?: 1 | 2 | 3 }>,
  ) {
    const list = (Array.isArray(arg) ? arg : [arg]).filter((a) => !!a?.channel); // 归一为数组并过滤无效项
    if (!list.length) return; // 空则退出
    for (const a of list) this.subs.set(subKey(a), a); // 持久化存储以便重连回放
    this.batchSubscribe(list); // 若已连接则立即发送订阅
  }

  // 退订
  async unsubscribe(arg: OkxChannelArg | OkxChannelArg[]) {
    const list = (Array.isArray(arg) ? arg : [arg]).filter((a) => !!a?.channel); // 归一 & 过滤
    if (!list.length) return; // 无内容
    for (const a of list) this.subs.delete(subKey(a)); // 从持久化订阅中移除
    this.send({ op: 'unsubscribe', args: list }); // 发退订
  }

  // Convenience subs with priority (1 high, 2 mid, 3 low)
  // 行情快照（ticker）- 低优先级
  subscribeTickers(instId: string) {
    return this.subscribe({ channel: 'tickers', instId, priority: 3 });
  }
  // 成交明细 - 高优先级
  subscribeTrades(instId: string) {
    return this.subscribe({ channel: 'trades', instId, priority: 1 });
  }
  subscribeCandles(
    instId: string,
    bar:
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
      | '1M' = '1m',
  ) {
    return this.subscribe({ channel: `candle${bar}`, instId, priority: 2 }); // K 线 - 中优先级
  }
  subscribeOrderBook(
    instId: string,
    depth: 'books5' | 'books' | 'books-l2-tbt' = 'books5',
  ) {
    return this.subscribe({ channel: depth, instId, priority: 1 }); // 订单簿 - 高优先级
  }
  subscribeOpenInterest(instType: 'SWAP' | 'FUTURES' | 'OPTION', uly: string) {
    return this.subscribe({
      channel: 'open-interest',
      instType,
      uly,
      priority: 2,
    });
  }
  subscribeFundingRate(instId: string) {
    return this.subscribe({ channel: 'funding-rate', instId, priority: 2 }); // 资金费率 - 中优先级
  }

  // --------------------------
  // Internals
  // --------------------------
  // 启动 ping 心跳与看门狗
  private startPing() {
    this.stopPing(); // 先清理已有心跳
    this.pingTimer = setInterval(
      () => this.safeSend('ping'), // 定期发送 ping 文本帧
      this.pingIntervalMs,
    );
    this.startWatchdog(); // 联动看门狗
  }
  private stopPing() {
    // 存在则清理
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
  }
  // 启动看门狗，监控空闲
  private startWatchdog() {
    this.stopWatchdog(); // 清理旧的
    this.watchdogTimer = setInterval(
      () => {
        const idle = now() - this.lastActivityTs; // 空闲时间
        if (!this.ws) return; // 无连接则忽略
        // 超出阈值则回收重建
        if (idle > this.idleTimeoutMs) {
          this.logger.warn(
            `Watchdog idle ${idle}ms > ${this.idleTimeoutMs}ms, recycling socket`,
          );
          try {
            this.ws.terminate(); // 强制关闭，close 回调会触发重连
          } catch {
            this.logger.warn('WS terminate failed'); // 终止失败也仅记录
          }
        }
      },
      Math.min(this.idleTimeoutMs, 10_000), // 定期检查，最长 10s 一次
    );
  }
  private stopWatchdog() {
    // 清理定时器
    if (this.watchdogTimer) {
      clearInterval(this.watchdogTimer);
      this.watchdogTimer = null;
    }
  }
  // 更新活跃时间戳
  private touchActivity() {
    this.lastActivityTs = now();
  }

  // 发送 JSON 对象
  private send(obj: any) {
    this.safeSend(JSON.stringify(obj)); // 序列化后发送
  }
  // 安全发送，检查连接状态
  private safeSend(payload: string) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return; // 未连接
    try {
      this.ws.send(payload); // 发送文本帧
    } catch {
      this.logger.warn('WS send failed'); // 发送失败仅告警
    }
  }

  // 批量订阅，按 20 条一包发送
  private batchSubscribe(
    list: Array<OkxChannelArg & { priority?: 1 | 2 | 3 }>,
  ) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return; // 未连接则跳过
    // 可选：分块以降低突发速率
    const chunks: (typeof list)[] = [];
    const size = 20; // 每块大小
    for (let i = 0; i < list.length; i += size)
      chunks.push(list.slice(i, i + size)); // 切块
    for (const chunk of chunks) this.send({ op: 'subscribe', args: chunk }); // 发送
  }

  // 入重试队列
  private queueRetry(arg: OkxChannelArg & { priority?: 1 | 2 | 3 }) {
    const exist = this.retryQueue.find((x) => subKey(x) === subKey(arg)); // 去重
    if (exist) return; // 已存在则忽略
    this.retryQueue.push({ ...arg, __retry: 0 }); // 记录并标注重试计数
  }
  // 尝试发送重试队列
  private drainRetryQueue() {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return; // 未连接则跳过
    const items = this.retryQueue.splice(0, this.retryQueue.length); // 取尽
    if (items.length) this.batchSubscribe(items); // 批量发送
  }

  // --------------------------
  // Message handling
  // --------------------------
  // 收到任意消息
  private handleMessage(raw: WebSocket.RawData) {
    this.touchActivity(); // 更新活跃时间
    // eslint-disable-next-line @typescript-eslint/no-base-to-string
    const text = typeof raw === 'string' ? raw : raw.toString('utf8'); // 二进制转字符串
    // 服务器 ping
    if (text === 'ping') {
      // 立即 pong
      this.safeSend('pong');
      return;
    }
    // 心跳 pong，忽略
    if (text === 'pong') return;

    let json: unknown; // 准备解析 JSON
    try {
      json = JSON.parse(text); // 尝试解析
    } catch {
      // 无效 JSON，忽略
      return;
    }

    // 处理事件帧（订阅/退订/错误）
    const evt = ZEventMsg.safeParse(json);
    if (evt.success) {
      const m = evt.data; // 提取数据
      // 订阅成功
      if (m.event === 'subscribe') {
        this.eventBus.emit('okx.subscribed', m.arg); // 对外广播
        return;
      }
      // 退订成功
      if (m.event === 'unsubscribe') {
        this.eventBus.emit('okx.unsubscribed', m.arg);
        return;
      }
      // 错误事件
      if (m.event === 'error') {
        // 广播错误，并对订阅失败进行重试计划
        this.eventBus.emit(
          'okx.error',
          new Error(`${m.code ?? ''} ${m.msg ?? ''}`.trim()),
        );
        // 订阅相关错误
        if (m.arg && m.msg?.toLowerCase().includes('sub')) {
          // 入重试队列 + 按当前退避延迟后重试
          this.queueRetry(m.arg);
          setTimeout(() => this.drainRetryQueue(), this.reConnectDelay);
          this.reConnectDelay = nextDelay(
            this.reConnectDelay,
            this.maxReconnectDelay,
          ); // 更新退避时间
        }
        return;
      }
    }

    // 处理数据帧
    const dm = ZOkxDataMsg.safeParse(json);
    if (!dm.success) return; // 非数据帧则忽略

    const m = dm.data; // 数据体
    const ch = m.arg.channel; // 频道

    // 根据频道分发到不同处理器
    if (ch === 'tickers') this.handleTickers(m);
    else if (ch === 'trades') this.handleTrades(m);
    else if (ch === 'books5' || ch === 'books' || ch === 'books-l2-tbt')
      this.handleBooks(m);
    else if (ch?.startsWith('candle')) this.handleCandles(m);
    else if (ch === 'open-interest') this.handleOpenInterest(m);
    else if (ch === 'funding-rate') this.handleFundingRate(m);
  }

  // --------------------------
  // Channel handlers → Domain
  // --------------------------
  // 处理 ticker 数据帧
  private handleTickers(m: OkxDataMsg) {
    const recvTs = now(); // 记录接收时间
    const { instId } = m.arg; // 交易对
    // 遍历数据项
    for (const d of m.data) {
      // 组装领域事件
      const ev: TickerEvent = {
        type: 'market.ticker', // 事件类型
        src: 'okx', // 来源交易所
        instId: instId!, // 交易对（非空）
        ts: Number(d.ts), // 交易所时间戳
        last: d.last, // 最新价
        bid1: d.bidPx ? { px: d.bidPx, sz: d.bidSz } : undefined, // 买一
        ask1: d.askPx ? { px: d.askPx, sz: d.askSz } : undefined, // 卖一
        open24h: d.open24h, // 24h 开盘价
        high24h: d.high24h, // 24h 最高
        low24h: d.low24h, // 24h 最低
        vol24h: d.vol24h, // 24h 量（张/币，视合约）
        volCcy24h: d.volCcy24h, // 24h 币本位量
        // tracing
        ingestId: randomUUID(), // 唯一追踪 ID
        recvTs, // 网关接收时间
      };
      this.eventBus.emit(ev.type, ev); // 对外发布
    }
  }

  // 处理成交数据帧
  private handleTrades(m: OkxDataMsg) {
    const recvTs = now(); // 接收时间
    const { instId } = m.arg; // 交易对
    // 遍历成交
    for (const t of m.data) {
      // 组装成交事件
      const ev: TradeEvent = {
        type: 'market.trade', // 事件类型
        src: 'okx', // 来源
        instId: instId!, // 交易对
        ts: Number(t.ts), // 交易所时间戳
        px: t.px, // 成交价
        qty: t.sz, // 成交量
        side: t.side, // 买/卖方向
        taker: 1, // OKX 推送为吃单侧
        tradeId: t.tradeId, // 成交 ID
        // tracing
        ingestId: randomUUID(), // 唯一追踪 ID
        recvTs, // 接收时间
      };
      this.eventBus.emit(ev.type, ev); // 发布
    }
  }

  // 处理 K 线数据帧
  private handleCandles(m: OkxDataMsg) {
    const recvTs = now(); // 接收时间
    const { instId, channel } = m.arg; // 交易对与频道
    const tf = normalizeTf(channel); // 解析出 timeframe
    // 遍历每根 K
    for (const c of m.data) {
      // 组装 K 线事件
      const ev: KlineEvent = {
        type: 'market.kline', // 类型
        src: 'okx', // 来源
        instId: instId!, // 交易对
        tf, // 时间框
        ts: Number(c[0]), // K 线结束时间（ms）
        open: c[1], // 开
        high: c[2], // 高
        low: c[3], // 低
        close: c[4], // 收
        vol: c[5], // 量
        confirm: c.length >= 9 ? (Number(c[8]) as 0 | 1) : undefined, // 是否确认收线
        // tracing
        ingestId: randomUUID(), // 唯一追踪 ID
        recvTs, // 接收时间
      };
      this.eventBus.emit(ev.type, ev); // 发布
    }
  }

  // 处理订单簿（含快照与增量）
  private handleBooks(m: OkxDataMsg) {
    const recvTs = now(); // 接收时间
    const { instId, channel } = m.arg; // 交易对与频道
    const d = m.data[0]; // OKX books 数据通常为单元素数组
    if (!d) return; // 保护：无数据直接返回

    // 规整 bids/asks 每个档位为 [价格, 数量] 的字符串二元组
    const bids: [string, string][] = (d.bids ?? []).map((x: any[]) => [
      String(x[0]),
      String(x[1]),
    ]);
    const asks: [string, string][] = (d.asks ?? []).map((x: any[]) => [
      String(x[0]),
      String(x[1]),
    ]);

    const bid1 = bids[0] ? { px: bids[0][0], sz: bids[0][1] } : undefined; // 买一
    const ask1 = asks[0] ? { px: asks[0][0], sz: asks[0][1] } : undefined; // 卖一

    const sumTopN = (levels: [string, string][], n: number) =>
      levels
        .slice(0, n)
        .reduce<
          string | undefined
        >((acc, [, sz]) => addDecStr(acc, sz), undefined); // 前 N 档累加数量

    const key = `${channel}:${instId}`; // 用于快照标记
    const snapshot =
      m.action === 'snapshot' || (!m.action && !this.seenSnapShot.has(key)); // 识别首包为快照
    if (snapshot) this.seenSnapShot.add(key); // 记录已收到快照

    const spread =
      bid1 && ask1
        ? (() => {
            // 价差 = 卖一 - 买一（仅在两者存在时计算）
            // 更严格的精度可在上层使用 Decimal 处理
            try {
              const a = Number(ask1.px);
              const b = Number(bid1.px);
              if (Number.isFinite(a) && Number.isFinite(b))
                return String(a - b);
            } catch {
              this.logger.warn('spread calc failed'); // 计算异常仅记录
            }
            return undefined; // 无法计算则为空
          })()
        : undefined;
    // 组装订单簿事件
    const ev: OrderBookEvent = {
      type: 'market.book', // 类型
      src: 'okx', // 来源
      instId: instId!, // 交易对
      ts: Number(d.ts), // 交易所时间戳
      bid1, // 买一
      ask1, // 卖一
      bidSz10: bids.length ? sumTopN(bids, 10) : undefined, // 买方前 10 档合计
      askSz10: asks.length ? sumTopN(asks, 10) : undefined, // 卖方前 10 档合计
      spread, // 价差
      snapshot, // 是否快照
      checksum: d.checksum ? Number(d.checksum) : undefined, // 校验和（仅 books*）
      action: m.action as any, // 快照/增量
      u: d.u ? Number(d.u) : undefined, // 当前数据的深度序号
      pu: d.pu ? Number(d.pu) : undefined, // 前一个序号
      // tracing
      ingestId: randomUUID(), // 追踪 ID
      recvTs, // 接收时间
    };

    this.eventBus.emit(ev.type, ev); // 发布事件
  }

  // 处理持仓量 OI
  private handleOpenInterest(m: OkxDataMsg) {
    const recvTs = now(); // 接收时间
    const { instId } = m.arg; // 可能为空，OKX 会在数据项里给出 instId
    // 遍历
    for (const d of m.data) {
      // 组装事件
      const ev: OpenInterestEvent = {
        type: 'market.oi', // 类型
        src: 'okx', // 来源
        instId: d.instId ?? instId!, // 优先使用数据内 instId
        ts: Number(d.ts), // 时间
        oi: d.oi, // 持仓量
        oiCcy: d.oiCcy, // 币本位持仓量
        // tracing
        ingestId: randomUUID(), // 追踪 ID
        recvTs, // 接收时间
      };
      this.eventBus.emit(ev.type, ev); // 发布
    }
  }

  // 处理资金费率
  private handleFundingRate(m: OkxDataMsg) {
    const recvTs = now(); // 接收时间
    const { instId } = m.arg; // 频道参数中的合约 ID
    // 遍历项
    for (const d of m.data) {
      // 组装事件
      const ev: FundingRateEvent = {
        type: 'market.funding', // 类型
        src: 'okx', // 来源
        instId: d.instId ?? instId!, // instId 兜底
        ts: Number(d.ts), // 时间
        rate: d.fundingRate ?? d.fundRate ?? d.rate, // 兼容字段名
        nextFundingTime: d.nextFundingTime
          ? Number(d.nextFundingTime)
          : undefined, // 下次结算时间（可选）
        // tracing
        ingestId: randomUUID(), // 追踪 ID
        recvTs, // 接收时间
      };
      this.eventBus.emit(ev.type, ev); // 发布
    }
  }
}
