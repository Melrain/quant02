/* eslint-disable @typescript-eslint/no-unsafe-return */
/* eslint-disable @typescript-eslint/no-unsafe-call */
// src/infra/redis/redis.client.ts
import Redis, { Redis as RedisType } from 'ioredis';
import { randomUUID } from 'crypto';

export type RedisClientOptions = {
  url?: string; // e.g. 'redis://localhost:6379'
  keyPrefix?: string; // 自动给所有 key 加前缀，便于环境隔离
  defaultTTL?: number; // 默认过期（秒），setJSON 时可用
  enableAutoPipelining?: boolean;
};

/** 最小可用 Redis 工具类：CRUD + JSON 便捷 + 分布式锁 */
export class RedisClient {
  private readonly redis: RedisType;
  private readonly prefix: string;
  private readonly defaultTTL?: number;
  private shaCache = new Map<string, string>();

  constructor(opts: RedisClientOptions = {}) {
    const {
      url = process.env.REDIS_URL || 'redis://127.0.0.1:6379',
      keyPrefix = '',
      defaultTTL,
      enableAutoPipelining = true,
    } = opts;

    this.redis = new Redis(url, { enableAutoPipelining });
    this.prefix = keyPrefix
      ? keyPrefix.endsWith(':')
        ? keyPrefix
        : keyPrefix + ':'
      : '';
    this.defaultTTL = defaultTTL;
  }

  /* --------------------------- Key helpers --------------------------- */
  private k(key: string) {
    return this.prefix + key;
  }

  /**
   * 返回带环境前缀的真实 Redis Key（供外部直接调用底层 ioredis 时使用）
   * 示例：prefix = "dev:", key("bucket:BTC:trades") => "dev:bucket:BTC:trades"
   */
  public key(key: string) {
    return this.k(key);
  }

  /** 批量版本：keys(["a","b"]) => 带前缀后的数组 */
  public keys(keys: string[]) {
    return keys.map((k) => this.k(k));
  }

  /* ------------------------------- CRUD ------------------------------ */
  /** String:set（可选 ttl 秒） */
  async set(key: string, value: string, ttlSeconds?: number) {
    if (ttlSeconds && ttlSeconds > 0) {
      return this.redis.set(this.k(key), value, 'EX', ttlSeconds);
    }
    return this.redis.set(this.k(key), value);
  }

  /** String:get */
  async get(key: string) {
    return this.redis.get(this.k(key));
  }

  /** 删除 Key */
  async del(key: string | string[]) {
    const keys = Array.isArray(key) ? key.map((k) => this.k(k)) : [this.k(key)];
    if (keys.length === 0) return 0;
    return this.redis.del(...keys);
  }

  /** 是否存在 */
  async exists(key: string) {
    return this.redis.exists(this.k(key));
  }

  /** 设定过期（秒） */
  async expire(key: string, ttlSeconds: number) {
    return this.redis.expire(this.k(key), ttlSeconds);
  }

  /* ------------------------------ HASH ------------------------------- */
  async hset(key: string, obj: Record<string, string | number>) {
    const flat: Array<string> = [];
    for (const [k, v] of Object.entries(obj)) flat.push(k, String(v));
    return this.redis.hset(this.k(key), ...flat);
  }

  async hgetall<T extends Record<string, string>>(key: string): Promise<T> {
    return this.redis.hgetall(this.k(key)) as any;
  }

  async hget(key: string, field: string) {
    return this.redis.hget(this.k(key), field);
  }

  /* ------------------------------- LIST ------------------------------ */
  async lpush(key: string, ...values: string[]) {
    return this.redis.lpush(this.k(key), ...values);
  }
  async rpush(key: string, ...values: string[]) {
    return this.redis.rpush(this.k(key), ...values);
  }
  async blpop(key: string, timeoutSec = 0) {
    // 返回 [key, value] | null
    return this.redis.blpop(this.k(key), timeoutSec);
  }
  async brpop(key: string, timeoutSec = 0) {
    return this.redis.brpop(this.k(key), timeoutSec);
  }
  async lrange(key: string, start = 0, stop = -1) {
    return this.redis.lrange(this.k(key), start, stop);
  }

  /* ------------------------------- ZSE.T ------------------------------ */
  async zadd(key: string, score: number, member: string) {
    return this.redis.zadd(this.k(key), score, member);
  }
  async zrangeByScore(
    key: string,
    min: number | string,
    max: number | string,
    withScores = false,
  ) {
    const args: [string, string, string] = [
      this.k(key),
      String(min),
      String(max),
    ];
    return withScores
      ? this.redis.zrangebyscore(args[0], args[1], args[2], 'WITHSCORES')
      : this.redis.zrangebyscore(args[0], args[1], args[2]);
  }
  async zrem(key: string, member: string) {
    return this.redis.zrem(this.k(key), member);
  }

  /* ------------------------------- JSON ------------------------------ */

  /** 简易 JSON set（带 TTL 可选） */
  async setJSON(key: string, obj: any, ttlSec?: number) {
    const k = this.k(key);
    if (ttlSec || this.defaultTTL) {
      await this.redis.set(
        k,
        JSON.stringify(obj),
        'EX',
        ttlSec ?? this.defaultTTL!,
      );
    } else {
      await this.redis.set(k, JSON.stringify(obj));
    }
  }

  /** XADD 原始版本 */
  async xadd(
    streamKey: string,
    fields: Record<string, string>,
    maxlenApprox?: number,
  ) {
    const args: (string | number)[] = [this.k(streamKey)];
    if (maxlenApprox) args.push('MAXLEN', '~', String(maxlenApprox));
    args.push('*');
    for (const [k, v] of Object.entries(fields)) {
      args.push(k, String(v));
    }
    return this.redis.xadd(...(args as [string, ...any[]]));
  }

  /** XADD 序列化 JSON（字段名统一为 event） */
  async xaddJson(streamKey: string, obj: unknown) {
    return this.redis.xadd(
      this.k(streamKey),
      '*',
      'event',
      JSON.stringify(obj),
    );
  }

  /** 取 JSON（自动反序列化） */
  async getJSON<T>(key: string): Promise<T | null> {
    const v = await this.get(key);
    if (v == null) return null;
    try {
      return JSON.parse(v) as T;
    } catch {
      return null;
    }
  }

  // 1) 非阻塞扫描
  async scanKeys(pattern: string, count = 500): Promise<string[]> {
    const keys: string[] = [];
    let cursor = '0';
    do {
      const [next, chunk] = await this.redis.scan(
        cursor,
        'MATCH',
        pattern,
        'COUNT',
        count,
      );
      cursor = next;
      keys.push(...chunk);
    } while (cursor !== '0');
    return keys;
  }

  // 2) 批量 hgetall（pipeline）
  async hgetallMany(keys: string[]): Promise<Record<string, string>[]> {
    if (!keys.length) return [];
    const pipe = this.redis.pipeline();
    keys.forEach((k) => pipe.hgetall(k));
    const res = await pipe.exec();
    // res: Array<[err, data]> | null (type may include null), guard it
    if (!res) return [];
    return res
      .map((item) => (Array.isArray(item) ? item[1] : null))
      .filter(Boolean) as Record<string, string>[];
  }

  // 3) 简单排序工具（如果你常用 timestamp 作为 key 最尾段）
  static extractTsFromKey(key: string): number {
    const tail = key.split(':').pop() ?? '';
    const ts = Number(tail);
    return Number.isFinite(ts) ? ts : 0;
  }
  /* -------------------------- 分布式锁（简单） ------------------------ */
  /**
   * 尝试获取锁：SET key value NX PX ttlMs
   * 成功返回 token（用于释放），失败返回 null
   */
  async tryLock(lockKey: string, ttlMs = 5000): Promise<string | null> {
    const token = randomUUID();
    // Order must be value, 'PX', ttl, 'NX' to satisfy ioredis typings
    const ok = await this.redis.set(this.k(lockKey), token, 'PX', ttlMs, 'NX');
    return ok === 'OK' ? token : null;
  }
  /** 仅加载 Lua，返回 sha */
  async loadScript(lua: string): Promise<string> {
    return (await this.redis.call('script', 'load', lua)) as string;
  }
  /**
   * 释放锁（Lua 脚本，确保只删除自己的锁）
   * 成功返回 true，失败/不是你的锁返回 false
   */
  async unlock(lockKey: string, token: string): Promise<boolean> {
    const script = `
      if redis.call("GET", KEYS[1]) == ARGV[1] then
        return redis.call("DEL", KEYS[1])
      else
        return 0
      end
    `;
    const res = await this.redis.eval(script, 1, this.k(lockKey), token);
    return res === 1;
  }

  /**
   * 带自旋的加锁：在 waitMs 内循环尝试，每次 sleep backoffMs
   * 返回 token；拿不到返回 null
   */
  async lockWithRetry(
    lockKey: string,
    ttlMs = 5000,
    waitMs = 3000,
    backoffMs = 100,
  ): Promise<string | null> {
    const deadline = Date.now() + waitMs;
    while (Date.now() < deadline) {
      const token = await this.tryLock(lockKey, ttlMs);
      if (token) return token;
      await new Promise((r) => setTimeout(r, backoffMs));
    }
    return null;
  }

  /** 去重：SETNX + EX */
  async setNxEx(key: string, ttlSec: number): Promise<boolean> {
    const k = this.k(key);
    const ok = await this.redis.set(k, '1', 'EX', ttlSec, 'NX');
    return ok === 'OK';
  }

  /** 统一 evalsha 缓存 */
  async evalshaCached(
    name: string,
    lua: string,
    keys: string[],
    ...argv: string[]
  ) {
    let sha = this.shaCache.get(name);
    try {
      if (!sha) {
        sha = (await this.redis.call('script', 'load', lua)) as string;
        this.shaCache.set(name, sha);
      }
      return await this.redis.evalsha(
        sha,
        keys.length,
        ...keys.map((k) => this.k(k)),
        ...argv,
      );
    } catch (e: any) {
      const msg = String(e?.message || e);
      if (msg.includes('NOSCRIPT')) {
        const newSha = (await this.redis.call('script', 'load', lua)) as string;
        this.shaCache.set(name, newSha);
        return await this.redis.evalsha(
          newSha,
          keys.length,
          ...keys.map((k) => this.k(k)),
          ...argv,
        );
      }
      throw e;
    }
  }

  /* ----------------------------- 连接控制 ---------------------------- */
  on(event: 'connect' | 'error' | 'close', cb: (...args: any[]) => void) {
    // 透传底层连接事件
    this.redis.on(event, cb);
    return this;
  }

  async quit() {
    await this.redis.quit();
  }

  /** 暴露底层 ioredis 实例（必要时用） */
  /** 暴露底层 ioredis，用于 XREADGROUP / XAUTOCLAIM 等高级命令 */
  raw(): RedisType {
    return this.redis;
  }
}
