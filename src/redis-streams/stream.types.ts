/** 业务流类型 */
export type StreamKind =
  | 'trades'
  | 'book'
  | 'kline' // kline{tf} 会被规范化为 kind=kline，并在 payload._tf 填充具体 tf
  | 'oi'
  | 'funding'
  | 'ticker'
  | 'custom';

/** 写入选项 */
export type StreamWriteOptions = {
  /** 近似长度裁剪：MAXLEN ~ N */
  maxlenApprox?: number;
  /** 近似按时间裁剪：MINID ~ <ms>-0 */
  minIdMsApprox?: number;
};

/** 读选项（消费组） */
export type StreamReadOptions = {
  group: string;
  consumer: string;
  /** 业务 key（不带前缀、不带哈希标签，内部统一处理） */
  keys: string[];
  /** 与 keys 等长的游标；不传或长度不等则自动用 '>' */
  cursors?: string[];
  /** 每批条数（COUNT） */
  count?: number;
  /** 阻塞毫秒（BLOCK） */
  blockMs?: number;
};

/** XREADGROUP 返回的 raw batch 形态 */
export type ReadBatch = Array<
  [key: string, entries: Array<[id: string, fields: string[]]>]
>;

/** 统一消息结构（已规范化） */
export type StreamMessage = {
  key: string; // 实际（已加前缀）key
  id: string; // Redis Stream ID
  ts: number; // 业务时间戳（payload.ts 优先, 否则用 ID 高位, 再否则 now）
  symbol: string; // 由 key 解析出的 {symbol}
  kind: StreamKind; // trades|book|kline|...
  payload: Record<string, string>; // 扁平字段（string）
};

/** Writer 能力 */
export interface IRedisStreamWriter {
  xadd(
    key: string,
    payload: Record<string, string | number | null | undefined>,
    opts?: StreamWriteOptions,
  ): Promise<string>;
  trim(key: string, opts: StreamWriteOptions): Promise<void>;
}

/** Admin 能力（组管理等） */
export interface IRedisStreamAdmin {
  ensureGroup(key: string, group: string, start?: '$' | '0'): Promise<void>;
  ensureGroups(keys: string[], group: string, start?: '$' | '0'): Promise<void>;
}

/** Consumer 能力（读/ack/认领） */
export interface IRedisStreamConsumer {
  readGroup(opts: StreamReadOptions): Promise<ReadBatch | null>;
  ack(key: string, group: string, ids: string[]): Promise<number>;
  xautoclaim(
    key: string,
    group: string,
    consumer: string,
    minIdleMs: number,
    startId?: string,
    count?: number,
  ): Promise<{
    entries: Array<[id: string, fields: string[]]>;
    nextStartId: string;
  }>;
}
