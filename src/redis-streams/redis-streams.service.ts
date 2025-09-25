/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable @typescript-eslint/no-unsafe-return */
// src/infra/redis/redis-stream.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { RedisClient } from '../redis/redis.client';
import {
  IRedisStreamAdmin,
  IRedisStreamConsumer,
  IRedisStreamWriter,
  ReadBatch,
  StreamMessage,
  StreamReadOptions,
  StreamWriteOptions,
  StreamKind,
} from './stream.types';

/**
 * RedisStreamsService
 *
 * 统一封装 Redis Stream 常用命令 + 若干 Hash/Key 便捷方法：
 *  - XADD / XTRIM / XREADGROUP / XACK / XAUTOCLAIM / XRANGE / XREVRANGE(模拟)
 *  - HSET / HGETALL / HGET / EXPIRE / GET
 *
 * Key 约定（Cluster 友好哈希标签）：
 *  - 行情原始：ws:{<symbol>}:<kind> / ws:{<symbol>}:kline<tf>
 *    例：ws:{BTC-USDT-SWAP}:trades、ws:{BTC-USDT-SWAP}:kline1m
 *  - 输出类（派生/快照/信号等）：<base>:{<symbol>}
 *    例：win:state:5m:{BTC-USDT-SWAP}、signal:detected:{BTC-USDT-SWAP}
 */
@Injectable()
export class RedisStreamsService
  implements IRedisStreamWriter, IRedisStreamAdmin, IRedisStreamConsumer
{
  private readonly logger = new Logger(RedisStreamsService.name);

  constructor(private readonly redis: RedisClient) {}

  /* ------------------------------ Key Helpers ------------------------------ */

  /** 业务 Stream Key（带哈希标签 + 统一前缀），适合 raw stream 命令直接使用 */
  buildKey(symbol: string, kind: StreamKind) {
    // 返回已前缀 key
    return `ws:{${symbol}}:${kind}`;
  }

  /**
   * 输出类业务 Key：如 win:1m:{sym} / signal:detected:{sym} / win:state:1m:{sym}
   * 注意：该返回值用于 HSET/HGETALL/EXPIRE 这类“封装方法”，由 RedisClient 统一加前缀；
   * 若要配合 raw stream 命令（xadd/xrange），请使用 buildOutStreamKey。
   */
  buildOutKey(symbol: string, base: string) {
    // 返回业务 key（不显式加环境前缀）
    return `${base}:{${symbol}}`;
  }

  /** 输出类 Stream Key（已前缀版），可直接用于 raw stream 命令 */
  buildOutStreamKey(symbol: string, base: string) {
    // 用 RedisClient.key(...) 加环境前缀
    return this.redis.key(`${base}:{${symbol}}`);
  }

  /** kline key：ws:{symbol}:kline1m / kline5m ...（已前缀） */
  buildKlineKey(symbol: string, timeframe: string) {
    return this.redis.key(`ws:{${symbol}}:kline${timeframe}`);
  }

  /** 如果你想把合约从现货名转成永续： */
  toSwapSymbol(symbol: string) {
    return `${symbol}-SWAP`;
  }

  /* ---------------------------------- XADD --------------------------------- */

  /**
   * 写入一条消息；payload 内部全部转 string，并过滤 undefined/null。
   * 适用于“已前缀”的 stream key（如 buildKey/buildKlineKey/buildOutStreamKey 的返回值）。
   */
  async xadd(
    key: string,
    payload: Record<string, string | number | null | undefined>,
    opts?: StreamWriteOptions,
  ): Promise<string> {
    const r = this.redis.raw();
    const k = key; // 要求传入“已前缀”的真实 key

    const fields: string[] = [];
    for (const [f, v] of Object.entries(payload)) {
      if (v === undefined || v === null) continue;
      fields.push(f, String(v));
    }

    const args: any[] = [k];
    if (opts?.maxlenApprox && opts.maxlenApprox > 0)
      args.push('MAXLEN', '~', String(opts.maxlenApprox));
    if (opts?.minIdMsApprox && opts.minIdMsApprox > 0)
      args.push('MINID', '~', `${opts.minIdMsApprox}-0`);
    args.push('*', ...fields);

    return (await (r as any).xadd(...args)) as string;
  }

  /** 裁剪：根据近似 maxlen/minid 做 XTRIM（key 需已前缀） */
  async trim(key: string, opts: StreamWriteOptions): Promise<void> {
    const r = this.redis.raw();
    const k = key;
    if (!opts.maxlenApprox && !opts.minIdMsApprox) return;
    const args: any[] = [k];
    if (opts.maxlenApprox) args.push('MAXLEN', '~', String(opts.maxlenApprox));
    if (opts.minIdMsApprox) args.push('MINID', '~', `${opts.minIdMsApprox}-0`);
    await (r as any).xtrim(...args);
  }

  /* ------------------------------ Group Admin ------------------------------ */

  /** 确保消费组存在；不存在则创建（忽略 BUSYGROUP 错误） */
  async ensureGroup(key: string, group: string, start: '$' | '0' = '$') {
    const r = this.redis.raw();
    try {
      await (r as any).xgroup('CREATE', key, group, start, 'MKSTREAM');
    } catch (e: any) {
      if (!String(e?.message).includes('BUSYGROUP')) throw e;
    }
  }

  /** 批量确保组存在（Pipeline） */
  async ensureGroups(keys: string[], group: string, start: '$' | '0' = '$') {
    const r = this.redis.raw();
    const pipe = (r as any).pipeline();
    for (const k of keys) pipe.xgroup('CREATE', k, group, start, 'MKSTREAM');
    const res = await pipe.exec();
    if (!res) return;
    for (const item of res) {
      const err = Array.isArray(item) ? item[0] : null;
      if (err && !String(err.message || err).includes('BUSYGROUP')) {
        throw err;
      }
    }
  }

  /* ------------------------------ Read (Group) ----------------------------- */

  /** XREADGROUP 读取多个 key（keys 均需已前缀） */
  async readGroup(opts: StreamReadOptions): Promise<ReadBatch | null> {
    const r = this.redis.raw();
    const { group, consumer, blockMs = 2000, count = 500 } = opts;

    const ks = opts.keys;
    const cursors =
      opts.cursors && opts.cursors.length === ks.length
        ? opts.cursors
        : new Array(ks.length).fill('>');

    const args: any[] = [
      'GROUP',
      group,
      consumer,
      'COUNT',
      String(count),
      'BLOCK',
      String(blockMs),
      'STREAMS',
      ...ks,
      ...cursors,
    ];

    try {
      return (await (r as any).xreadgroup(...args)) as ReadBatch | null;
    } catch (e) {
      this.logger.warn(
        `xreadgroup failed: group=${group} consumer=${consumer} keys=${ks.join(', ')} err=${(e as Error).message}`,
      );
      return null;
    }
  }

  /* --------------------------- Read (XREVRANGE) ---------------------------- */

  /** 读取某个 stream 的最近 count 条（倒序）。key 需已前缀。 */
  async xrevrangeLatest(
    key: string,
    count = 1,
  ): Promise<Array<[id: string, fields: string[]]>> {
    const r = this.redis.raw();
    try {
      const rows = (await (r as any).xrevrange(
        key,
        '+',
        '-',
        'COUNT',
        String(count),
      )) as Array<[string, string[]]> | null;
      return rows ?? [];
    } catch (e) {
      this.logger.debug(
        `xrevrangeLatest 失败: ${key} -> ${(e as Error).message}`,
      );
      return [];
    }
  }

  /** 将 XREVRANGE 的 fields 数组转换为对象列表（倒序→对象） */
  async readLatestAsObjects(
    key: string,
    count = 1,
  ): Promise<Record<string, string>[]> {
    const rows = await this.xrevrangeLatest(key, count);
    return rows.map(([, arr]) => {
      const obj: Record<string, string> = {};
      for (let i = 0; i < arr.length; i += 2) obj[arr[i]] = arr[i + 1];
      return obj;
    });
  }

  /* --------------------------- Read (XRANGE, by time) ---------------------- */

  /**
   * 按毫秒时间窗口读取（XRANGE），startMs/endMs 都是业务毫秒，内部转换为 "<ms>-<seq>"。
   * 返回 entries（旧→新）。key 需已前缀。
   */
  async xrangeByTime(
    key: string,
    startMs: number,
    endMs: number,
    count?: number,
  ): Promise<Array<[id: string, fields: string[]]>> {
    const r = this.redis.raw();
    const startId = `${Math.max(0, Math.floor(startMs))}-0`;
    const endId = `${Math.max(0, Math.floor(endMs))}-999999`;
    try {
      const args: any[] = [key, startId, endId];
      if (count && count > 0) args.push('COUNT', String(count));
      const rows = (await (r as any).xrange(...args)) as Array<
        [string, string[]]
      > | null;
      return rows ?? [];
    } catch (e) {
      this.logger.debug(
        `xrangeByTime 失败: ${key} [${startId}, ${endId}] -> ${(e as Error).message}`,
      );
      return [];
    }
  }

  /** 将 XRANGE 窗口读取转换为对象数组（旧→新） */
  async readWindowAsObjects(
    key: string,
    startMs: number,
    endMs: number,
    count?: number,
  ): Promise<Record<string, string>[]> {
    const rows = await this.xrangeByTime(key, startMs, endMs, count);
    return rows.map(([, arr]) => {
      const obj: Record<string, string> = {};
      for (let i = 0; i < arr.length; i += 2) {
        obj[arr[i]] = arr[i + 1];
      }
      return obj;
    });
  }

  /**
   * 读取最近 count 条（按旧→新排序），从候选字段中取首个数值，返回 number[]。
   * 例：fieldCandidates = ['c','close']。
   */
  async readLatestNumericSeries(
    key: string,
    fieldCandidates: string[],
    count = 50,
  ): Promise<number[]> {
    const latest = await this.readLatestAsObjects(key, count);
    if (!latest.length) return [];
    latest.reverse();
    const toNum = (v: any) => {
      const n = Number(v);
      return Number.isFinite(n) ? n : undefined;
    };
    return latest
      .map((row) => {
        for (const f of fieldCandidates) {
          if (row[f] !== undefined) {
            const n = toNum(row[f]);
            if (n !== undefined) return n;
          }
        }
        return undefined;
      })
      .filter((x): x is number => x !== undefined);
  }

  /* ------------------------------ Normalize ------------------------------- */

  /**
   * 将 XREADGROUP raw entries 转换为统一 StreamMessage[]。
   * 解析：
   *  - ts：优先 payload.ts（数字），否则用 ID 高位（服务器时间），否则 Date.now()
   *  - key：ws:{symbol}:{kind} 或 ws:{symbol}:kline{tf}
   *  - kind：若是 kline{tf}，则 kind='kline' 且 payload._tf = tf
   */
  normalizeBatch(batch: ReadBatch | null): StreamMessage[] {
    if (!batch) return [];
    const messages: StreamMessage[] = [];

    for (const [fullKey, entries] of batch) {
      const key = fullKey;
      const parts = key.split(':');

      // 解析 {symbol}
      const braceIndex = parts.findIndex(
        (p) => p.startsWith('{') && p.endsWith('}'),
      );
      const symbol =
        braceIndex >= 0
          ? parts[braceIndex].slice(1, -1)
          : parts[1]?.startsWith('{')
            ? parts[1].slice(1, -1)
            : 'UNKNOWN';

      // 解析 kind 与 timeframe
      const rawKind = parts[parts.length - 1] ?? 'trades';
      let kind: StreamKind = 'trades';
      let tf: string | undefined;
      const m = /^kline(\w+)$/i.exec(rawKind);
      if (m) {
        kind = 'kline';
        tf = m[1]?.toLowerCase();
      } else {
        kind = rawKind as StreamKind;
      }

      for (const [id, fields] of entries) {
        const payload: Record<string, string> = {};
        for (let i = 0; i < fields.length; i += 2)
          payload[fields[i]] = fields[i + 1];

        // ts 优先：payload.ts -> id 毫秒 -> now
        let ts: number;
        const tsFromPayload = Number(payload.ts);
        if (!Number.isNaN(tsFromPayload) && payload.ts !== undefined) {
          ts = tsFromPayload;
        } else {
          const idMs = Number(id.split('-')[0]);
          ts = Number.isFinite(idMs) ? idMs : Date.now();
        }

        const enriched = tf ? { ...payload, _tf: tf } : payload;
        messages.push({ key, id, ts, symbol, kind, payload: enriched });
      }
    }
    return messages;
  }

  // 解析业务 key -> { symbol, kind, tf }
  private parseKey(fullKey: string): {
    symbol: string;
    kind: StreamKind;
    tf?: string;
  } {
    const parts = fullKey.split(':');
    const braceIndex = parts.findIndex(
      (p) => p.startsWith('{') && p.endsWith('}'),
    );
    const symbol =
      braceIndex >= 0
        ? parts[braceIndex].slice(1, -1)
        : parts[1]?.startsWith('{')
          ? parts[1].slice(1, -1)
          : 'UNKNOWN';

    const rawKind = parts[parts.length - 1] ?? 'trades';
    let kind: StreamKind = 'trades';
    let tf: string | undefined;
    const m = /^kline(\w+)$/i.exec(rawKind);
    if (m) {
      kind = 'kline';
      tf = m[1]?.toLowerCase();
    } else {
      kind = rawKind as StreamKind;
    }
    return { symbol, kind, tf };
  }

  /* ----------------------------------- ACK --------------------------------- */

  async ack(key: string, group: string, ids: string[]): Promise<number> {
    if (!ids.length) return 0;
    const r = this.redis.raw();
    try {
      return (await (r as any).xack(key, group, ...ids)) as number;
    } catch (e) {
      this.logger.warn(
        `xack failed: key=${key} group=${group} ids=${ids.length} err=${(e as Error).message}`,
      );
      return 0;
    }
  }

  /* ------------------------------ XAUTOCLAIM ------------------------------- */

  /**
   * 认领超时 pending（兼容 Redis 7 返回三元组格式）
   * key 需已前缀
   */
  async xautoclaim(
    key: string,
    group: string,
    consumer: string,
    minIdleMs: number,
    startId = '0-0',
    count = 100,
  ): Promise<{
    entries: Array<[id: string, fields: string[]]>;
    nextStartId: string;
  }> {
    const r = this.redis.raw();
    const args: any[] = [key, group, consumer, String(minIdleMs), startId];
    if (count) args.push('COUNT', String(count));
    try {
      const raw = await (r as any).xautoclaim(...args);
      // Redis 7: [nextStartId, entries, deleted]; Redis 6.2: [nextStartId, entries]
      const nextStartId = raw?.[0] ?? '0-0';
      const entries = (raw?.[1] ?? []) as Array<[string, string[]]>;
      return { entries, nextStartId };
    } catch (e) {
      this.logger.warn(
        `xautoclaim failed: key=${key} group=${group} consumer=${consumer} err=${(e as Error).message}`,
      );
      return { entries: [], nextStartId: startId };
    }
  }

  // 批量 XAUTOCLAIM（便捷）
  async xautoclaimAll(
    keys: string[],
    group: string,
    consumer: string,
    minIdleMs: number,
    countPerKey = 100,
  ): Promise<Array<{ key: string; entries: Array<[string, string[]]> }>> {
    const out: Array<{ key: string; entries: Array<[string, string[]]> }> = [];
    for (const k of keys) {
      let startId = '0-0';
      const acc: Array<[string, string[]]> = [];
      for (let i = 0; i < 3; i++) {
        const { entries, nextStartId } = await this.xautoclaim(
          k,
          group,
          consumer,
          minIdleMs,
          startId,
          countPerKey,
        );
        if (!entries.length) break;
        acc.push(...entries);
        startId = nextStartId;
      }
      if (acc.length) out.push({ key: k, entries: acc });
    }
    return out;
  }

  /* ------------------------------ XRANGE (compat) -------------------------- */

  // 提供一个“伪 XREVRANGE”接口（若你需要旧 -> 新 -> 取尾部 -> 倒序）
  async xrevrange(
    key: string,
    end: string = '+',
    start: string = '-',
    count: number = 600,
  ): Promise<Array<[string, Record<string, string>]>> {
    const rows = await this.xrange(key, '-', '+', count);
    const tail = rows.slice(-Math.min(count, rows.length)).reverse();
    return tail;
  }

  /**
   * XRANGE 封装（key 需已前缀）
   */
  // 统一 XRANGE 调用，兼容不同 ioredis 版本
  async xrange(
    key: string,
    start: string = '-',
    end: string = '+',
    count?: number,
  ): Promise<Array<[string, Record<string, string>]>> {
    const r: any = this.redis.raw(); // ★ 正确获取底层 ioredis 实例

    const args: (string | number)[] = [key, start, end];
    if (count && count > 0) {
      args.push('COUNT', count);
    }

    let res: any;
    try {
      if (typeof r.xrange === 'function') {
        // ioredis 直接有 xrange 方法
        res = await r.xrange(...args);
      } else if (typeof r.call === 'function') {
        // 通用 call
        res = await r.call('XRANGE', ...args);
      } else if (typeof r.send_command === 'function') {
        // 老版本 send_command
        res = await r.send_command('XRANGE', args);
      } else if (typeof r.sendCommand === 'function') {
        // 新版 sendCommand(Command)
        // 避免直接依赖 ioredis.Command 类型，这里改用 call 语义
        res = await r.call('XRANGE', ...args);
      } else {
        throw new Error('XRANGE not available on redis client');
      }
    } catch (e) {
      this.logger.debug(
        `xrange failed: ${key} [${start}, ${end}] -> ${(e as Error).message}`,
      );
      throw e;
    }

    // Redis 返回: [id, [k1,v1,k2,v2,...]][]
    return (res ?? []).map(([id, arr]: [string, string[]]) => [
      id,
      this.arrayToHash(arr),
    ]);
  }

  /** 小工具：把 [k1,v1,k2,v2,...] 转成 {k1:v1,...} */
  private arrayToHash(arr: string[]): Record<string, string> {
    const h: Record<string, string> = {};
    for (let i = 0; i < arr.length; i += 2) {
      h[arr[i]] = arr[i + 1];
    }
    return h;
  }

  /* ------------------------------ Hash / Key ------------------------------- */

  /** HSET（走封装，自动前缀） */
  async hset(key: string, obj: Record<string, string | number>) {
    return this.redis.hset(key, obj);
  }

  /** HGETALL（优先走封装；否则 raw/call 兜底），不存在返回 null */
  async hgetall(key: string): Promise<Record<string, string> | null> {
    // 1) 优先走封装（通常带环境前缀）
    if (typeof (this.redis as any).hgetall === 'function') {
      const obj = await (this.redis as any).hgetall(key);
      if (!obj || Object.keys(obj).length === 0) return null;
      return obj as Record<string, string>;
    }

    // 2) 兜底 raw/call
    const cli: any =
      this.redis.raw?.() ?? (this as any).redis ?? (this as any).client;
    const k =
      typeof (this.redis as any).key === 'function'
        ? (this.redis as any).key(key)
        : key;

    let res: any;
    if (typeof cli?.hgetall === 'function') {
      res = await cli.hgetall(k);
    } else if (typeof cli?.call === 'function') {
      res = await cli.call('HGETALL', k);
    } else if (typeof cli?.send_command === 'function') {
      res = await cli.send_command('HGETALL', [k]);
    } else {
      throw new Error('Redis client does not support HGETALL');
    }

    if (Array.isArray(res)) {
      const h: Record<string, string> = {};
      for (let i = 0; i < res.length; i += 2) h[res[i]] = res[i + 1];
      return Object.keys(h).length ? h : null;
    }
    return res && Object.keys(res).length
      ? (res as Record<string, string>)
      : null;
  }

  /** HGET 单字段（不存在返回 null） */
  async hget(key: string, field: string): Promise<string | null> {
    // 1) 优先走封装
    if (typeof (this.redis as any).hget === 'function') {
      const v = await (this.redis as any).hget(key, field);
      return v ?? null;
    }
    // 2) 兜底 raw/call
    const cli: any =
      this.redis.raw?.() ?? (this as any).redis ?? (this as any).client;
    const k =
      typeof (this.redis as any).key === 'function'
        ? (this.redis as any).key(key)
        : key;

    if (typeof cli?.hget === 'function')
      return (await cli.hget(k, field)) ?? null;
    if (typeof cli?.call === 'function')
      return await cli.call('HGET', k, field);
    if (typeof cli?.send_command === 'function')
      return await cli.send_command('HGET', [k, field]);
    throw new Error('Redis client does not support HGET');
  }

  /** EXPIRE（秒） */
  async expire(key: string, seconds: number) {
    return this.redis.expire(key, seconds);
  }

  /** GET（字符串键，计数器/速率用） */
  async get(key: string): Promise<string | null> {
    // 1) 优先走封装
    if (typeof (this.redis as any).get === 'function') {
      const v = await (this.redis as any).get(key);
      return v ?? null;
    }
    // 2) 兜底 raw/call
    const cli: any =
      this.redis.raw?.() ?? (this as any).redis ?? (this as any).client;
    const k =
      typeof (this.redis as any).key === 'function'
        ? (this.redis as any).key(key)
        : key;

    if (typeof cli?.get === 'function') return (await cli.get(k)) ?? null;
    if (typeof cli?.call === 'function') return await cli.call('GET', k);
    if (typeof cli?.send_command === 'function')
      return await cli.send_command('GET', [k]);
    throw new Error('Redis client does not support GET');
  }
}
