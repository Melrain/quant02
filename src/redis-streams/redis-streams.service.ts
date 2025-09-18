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
 * RedisStreamService
 *
 * 统一封装 Redis Stream 常用命令：
 *  - XADD (可选 MAXLEN~/MINID~ 控制内存)
 *  - XGROUP CREATE (ensureGroup)
 *  - XREADGROUP (批量拉取)
 *  - XACK (确认消费)
 *  - XAUTOCLAIM (认领超时 pending)
 *
 * 并提供统一消息结构：{ key, id, ts, symbol, kind, payload }
 * key 约定（Cluster 友好哈希标签）：ws:{<symbol>}:<kind> / ws:{<symbol>}:kline<tf>
 * 例：ws:{BTC-USDT-SWAP}:trades、ws:{BTC-USDT-SWAP}:kline1m
 */
@Injectable()
export class RedisStreamsService
  implements IRedisStreamWriter, IRedisStreamAdmin, IRedisStreamConsumer
{
  private readonly logger = new Logger(RedisStreamsService.name);

  constructor(private readonly redis: RedisClient) {}

  /* ------------------------------ Key Helpers ------------------------------ */

  /** 业务 Stream Key（带哈希标签 + 统一前缀） */
  buildKey(symbol: string, kind: StreamKind) {
    // 使用业务 key，底层 RedisClient 会统一加环境前缀
    return `ws:{${symbol}}:${kind}`;
  }
  /** 输出类业务 Key：如 win:1m:{sym} / signal:detected:{sym} / win:state:1m:{sym} */
  buildOutKey(symbol: string, base: string) {
    return `${base}:{${symbol}}`;
  }

  /** kline key：ws:{symbol}:kline1m / kline5m ... */
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
   * 控制策略：可选 maxlenApprox / minIdMsApprox 用于近似裁剪，防止无限增长。
   * @param key 这里既可以传 `buildKey()` 等返回的“已前缀”key，也可以传业务 key（不带前缀），
   *            若传业务 key，请使用 this.redis.key(key) 包装后再传入本方法。
   */
  async xadd(
    key: string,
    payload: Record<string, string | number | null | undefined>,
    opts?: StreamWriteOptions,
  ): Promise<string> {
    const r = this.redis.raw();

    // 若调用方传的是“业务 key”（不带前缀），请手动包一层：
    // const k = this.redis.key(key);
    // 这里默认 key 已是“已前缀的真实 key”（推荐直接用 buildKey/buildKlineKey 的返回值）
    const k = key;

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

  /** 裁剪：根据近似 maxlen/minid 做 XTRIM */
  async trim(key: string, opts: StreamWriteOptions): Promise<void> {
    const r = this.redis.raw();
    const k = key; // 同上，传入时即已前缀
    if (!opts.maxlenApprox && !opts.minIdMsApprox) return;
    const args: any[] = [k];
    if (opts.maxlenApprox) args.push('MAXLEN', '~', String(opts.maxlenApprox));
    if (opts.minIdMsApprox) args.push('MINID', '~', `${opts.minIdMsApprox}-0`);
    await (r as any).xtrim(...args);
  }

  /* ------------------------------ Group Admin ------------------------------ */

  /**
   * 确保消费组存在；不存在则创建（忽略 BUSYGROUP 错误）。
   * @param start '$' 表示从组创建后新消息开始；'0' 表示消费历史。
   * @param key 传入“已前缀的真实 key”（推荐使用 buildKey 的返回值）
   */
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

  /**
   * 调用 XREADGROUP 读取多个 key；
   * 传入的 keys 应该是“已前缀 key”（直接用 buildKey/buildKlineKey）。
   */
  async readGroup(opts: StreamReadOptions): Promise<ReadBatch | null> {
    const r = this.redis.raw();
    const { group, consumer, blockMs = 2000, count = 500 } = opts;

    const ks = opts.keys; // 假设传入即为已前缀 key
    const cursors =
      opts.cursors && opts.cursors.length === ks.length
        ? opts.cursors
        : new Array(ks.length).fill('>');

    // XREADGROUP GROUP <group> <consumer> COUNT <n> BLOCK <ms> STREAMS k1 k2 ... id1 id2 ...
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

  /** 读取某个 stream 的最近 count 条（倒序）。key 传“已前缀 key”。 */
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

  /** 将 XREVRANGE 的 fields 数组转换为对象列表（倒序→对象）。 */
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
   * 返回 entries（旧→新）。key 传“已前缀 key”。
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

  /** 将 XRANGE 窗口读取转换为对象数组（旧→新）。 */
  async readWindowAsObjects(
    key: string,
    startMs: number,
    endMs: number,
    count?: number,
  ): Promise<Record<string, string>[]> {
    const rows = await this.xrangeByTime(key, startMs, endMs, count);
    return rows.map(([, arr]) => {
      const obj: Record<string, string> = {};
      for (let i = 0; i < arr.length; i += 2) obj[arr[i]] = arr[i + 1];
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
      // fullKey 如 "dev:ws:{BTC-USDT-SWAP}:kline1m" 或 "ws:{BTC-USDT-SWAP}:trades"
      // 先去掉环境前缀（如果有）
      const key = fullKey; // 这里保留完整 key，不做截断，下面解析时仅作 split 参考
      const parts = key.split(':');

      // 解析 {symbol}
      // 形式：prefix? "ws" "{SYMBOL}" "<kind>"
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

  // 新增：解析业务 key -> { symbol, kind, tf }
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
   * key 传“已前缀 key”
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

  // 新增：对多个键批量 XAUTOCLAIM（便捷）
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
      // 迭代认领，直到拿不到更多
      // 注意：避免死循环；每个 key 做 3 次页翻最大 ~ 3 * countPerKey
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

  // src/redis-streams/redis-streams.service.ts
  async xrevrange(
    key: string,
    end: string = '+', // 保留参数签名，实际不使用
    start: string = '-', // 保留参数签名，实际不使用
    count: number = 600,
  ): Promise<Array<[string, Record<string, string>]>> {
    // 直接调用你已有的 XRANGE：从头到尾取最多 count 条
    // 如果你的 xrange 签名不同（比如没有 count 参数），按你的实现调整
    const rows = await this.xrange(key, '-', '+', count);
    // 取尾部 count 条并倒序，模拟 XREVRANGE 语义（新→旧）
    const tail = rows.slice(-Math.min(count, rows.length)).reverse();
    return tail;
  }

  // src/redis-streams/redis-streams.service.ts

  /**
   * XRANGE 封装
   * @param key   stream 键名
   * @param start 起始 ID（通常用 '-' 表示最旧）
   * @param end   结束 ID（通常用 '+' 表示最新）
   * @param count 限制条数（可选）
   * @returns     Array<[id, Record<string,string>]>
   */
  async xrange(
    key: string,
    start: string = '-',
    end: string = '+',
    count?: number,
  ): Promise<Array<[string, Record<string, string>]>> {
    const cli: any = (this as any).redis || (this as any).client;

    const args: (string | number)[] = [key, start, end];
    if (count) {
      args.push('COUNT', count);
    }

    // ioredis 有 call / send_command，不同版本名字不同
    let res: any;
    if (typeof cli?.xrange === 'function') {
      res = await cli.xrange(...args);
    } else if (typeof cli?.call === 'function') {
      res = await cli.call('XRANGE', ...args);
    } else if (typeof cli?.send_command === 'function') {
      res = await cli.send_command('XRANGE', args);
    } else {
      throw new Error('Redis client does not support XRANGE');
    }

    // 把 Redis 的 [id, [k1,v1,k2,v2,...]] 转成 [id, {k1:v1,...}]
    return (res as any[]).map(([id, arr]) => [id, this.arrayToHash(arr)]);
  }

  /** 小工具：把 [k1,v1,k2,v2,...] 转成 {k1:v1,...} */
  private arrayToHash(arr: string[]): Record<string, string> {
    const h: Record<string, string> = {};
    for (let i = 0; i < arr.length; i += 2) {
      h[arr[i]] = arr[i + 1];
    }
    return h;
  }

  /** Hash:  代理（自动前缀由 RedisClient 处理） */
  async hset(key: string, obj: Record<string, string | number>) {
    return this.redis.hset(key, obj);
  }

  /** Key 过期（秒）代理 */
  async expire(key: string, seconds: number) {
    return this.redis.expire(key, seconds);
  }
}
