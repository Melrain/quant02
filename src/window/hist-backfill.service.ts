/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable @typescript-eslint/no-unsafe-return */
// src/window/hist-backfill.service.ts
/* eslint-disable no-console */
import { Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import { createClient, RedisClientType } from 'redis';

// ===== ENV =====
const ENV_ASSETS = (process.env.OKX_ASSETS || 'btc,eth')
  .split(',')
  .map((s) => s.trim().toUpperCase())
  .filter(Boolean);

const OKX_REST_BASE = process.env.OKX_REST_BASE || 'https://www.okx.com';
const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379/0';

// 回填范围左右留白（分钟）
const PAD_MIN = +(process.env.HIST_PAD_MIN || process.env.BF_PAD_MIN || '45');

// 是否同时把历史写一份到实时流（可选）
const WRITE_REALTIME_COPY = (process.env.BF_WRITE_REALTIME || '0') === '1';

// 速率限制（OKX REST）
const PAGE_SLEEP_MS = +(process.env.OKX_PAGE_SLEEP_MS || '60');

// 每次 XADD 后的微小 sleep，避免长时间阻塞
const WRITE_SLEEP_EVERY = +(process.env.HIST_WRITE_SLEEP_EVERY || '2000');
const WRITE_SLEEP_MS = +(process.env.HIST_WRITE_SLEEP_MS || '5');

// ========= Utils =========
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function nowISO() {
  return new Date().toISOString();
}

function instIdOf(asset: string) {
  // 和你现有命名保持一致：BTC -> BTC-USDT-SWAP
  return `${asset}-USDT-SWAP`;
}

function histKeyOf(sym: string) {
  return `hist:win:1m:{${sym}}`;
}

function winKeyOf(sym: string) {
  return `win:1m:{${sym}}`;
}

// Stream field 正规化兼容（如果以后读出来时发现有 0/1/2 这样的错误字段，可以复用）
function normalizeFields(fields: string[]) {
  if (fields.length >= 4 && fields[0] === '0' && fields[1] === 'ts') {
    const obj: Record<string, string> = {};
    for (let i = 0; i + 3 < fields.length; i += 4)
      obj[fields[i + 1]] = fields[i + 3];
    return obj;
  }
  const obj: Record<string, string> = {};
  for (let i = 0; i + 1 < fields.length; i += 2) obj[fields[i]] = fields[i + 1];
  return obj;
}

// ========= OKX fetch =========
async function okxPage(url: string) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`OKX http ${r.status}`);
  const j = await r.json();
  if (j.code !== '0') throw new Error(`OKX err ${j.code}:${j.msg}`);
  return (j.data || []) as any[];
}

/**
 * 覆盖闭区间 [needStart, needEnd] 的 1m K 线（升序返回）
 * 主通道：/market/history-candles?before=ts  不断向右（更新方向）推进
 * 兜底：   /market/history-candles?after=ts   从右往左补齐缺口
 */
// 替换 HistBackfillService 里的 fetchCandlesCovering
async function fetchCandlesCovering(
  instId: string,
  needStart: number,
  needEnd: number,
): Promise<
  Array<{ ts: number; open: number; high: number; low: number; close: number }>
> {
  const out = new Map<
    number,
    { ts: number; open: number; high: number; low: number; close: number }
  >();

  const add = (rows: any[]) => {
    for (const r of rows) {
      const ts = +r[0];
      out.set(ts, { ts, open: +r[1], high: +r[2], low: +r[3], close: +r[4] });
    }
  };

  // ✅ 正确方式：用 after 从 needStart 向右翻页，直到覆盖 needEnd
  let after = needStart - 60_000; // 向左预留 1 根避免边界缺失
  let pageCount = 0;
  for (let i = 0; i < 5000; i++) {
    const url =
      `${OKX_REST_BASE}/api/v5/market/history-candles` +
      `?instId=${encodeURIComponent(instId)}&bar=1m&limit=500&after=${after}`;
    const rows = await okxPage(url);
    if (!rows.length) break;

    add(rows);

    // 本页 ts 范围（注意 OKX 返回通常是“最新在前”，这里统一计算）
    const tsList = rows.map((x) => +x[0]);
    const pageMin = Math.min(...tsList);
    const pageMax = Math.max(...tsList);
    pageCount++;
    // 小日志（可保留或注释）
    // console.log(`[hist][${instId}] page#${pageCount} after=${after} -> [${new Date(pageMin).toISOString()} ~ ${new Date(pageMax).toISOString()}], n=${rows.length}`);

    after = pageMax; // 用“本页最右端”的 ts 继续向右推进

    if (pageMax >= needEnd) break;
    await sleep(PAGE_SLEEP_MS);
  }

  // 裁剪并升序
  const arr = Array.from(out.values())
    .filter((k) => k.ts >= needStart && k.ts <= needEnd)
    .sort((a, b) => a.ts - b.ts);

  return arr;
}

// ========= Redis helpers =========
async function readSignalRange(r: RedisClientType, sym: string) {
  const key = `signal:final:{${sym}}`;

  const first = await r.sendCommand<[string, string[]][]>([
    'XRANGE',
    key,
    '-',
    '+',
    'COUNT',
    '1',
  ]);
  const last = await r.sendCommand<[string, string[]][]>([
    'XREVRANGE',
    key,
    '+',
    '-',
    'COUNT',
    '1',
  ]);
  if (!first?.length || !last?.length) return null;

  const fMap = normalizeFields(first[0][1]);
  const lMap = normalizeFields(last[0][1]);

  const minTs = +fMap.ts;
  const maxTs = +lMap.ts;

  if (!Number.isFinite(minTs) || !Number.isFinite(maxTs)) return null;
  return { minTs, maxTs };
}

async function writeHistStream(
  r: RedisClientType,
  sym: string,
  rows: Array<{
    ts: number;
    open: number;
    high: number;
    low: number;
    close: number;
  }>,
) {
  if (!rows.length) return 0;

  const histKey = histKeyOf(sym);
  const rtKey = winKeyOf(sym);

  let written = 0;
  for (const k of rows) {
    const { ts, open, high, low, close } = k;

    // hist 专用流（不裁剪）
    await r.xAdd(histKey, '*', {
      ts: String(ts),
      open: String(open),
      high: String(high),
      low: String(low),
      close: String(close),
      vol: '0',
      vbuy: '0',
      vsell: '0',
      vwap: String(close),
      tickN: '0',
      gap: '0',
    });

    // 可选：同步一份到实时 win:1m（带 MAXLEN~5000 裁剪，避免撑爆）
    if (WRITE_REALTIME_COPY) {
      await r.xAdd(
        rtKey,
        '*',
        {
          ts: String(ts),
          open: String(open),
          high: String(high),
          low: String(low),
          close: String(close),
          vol: '0',
          vbuy: '0',
          vsell: '0',
          vwap: String(close),
          tickN: '0',
          gap: '0',
        },
        {
          TRIM: { strategy: 'MAXLEN', strategyModifier: '~', threshold: 5000 },
        },
      );
    }

    written++;
    if (WRITE_SLEEP_EVERY > 0 && written % WRITE_SLEEP_EVERY === 0) {
      await sleep(WRITE_SLEEP_MS);
    }
  }
  return written;
}

// ========= Service =========
@Injectable()
export class HistBackfillService implements OnApplicationBootstrap {
  private readonly logger = new Logger(HistBackfillService.name);
  private redis!: RedisClientType;

  async onApplicationBootstrap() {
    this.logger.log(`[hist] bootstrap at ${nowISO()}`);
    this.redis = createClient({ url: REDIS_URL });
    await this.redis.connect();

    // 为每个资产做一次 backfill
    for (const asset of ENV_ASSETS) {
      const sym = instIdOf(asset);
      try {
        await this.backfillOne(sym);
      } catch (e) {
        this.logger.error(
          `[hist] backfill failed for ${sym}: ${(e as Error).message}`,
          (e as Error).stack,
        );
      }
    }

    // 如果你希望常驻复用 redis 连接供别的服务使用，就不要 quit；
    // 这里仅 backfill 用，且服务不复用，可按需选择注释下一行：
    // await this.redis.quit();
  }

  private async backfillOne(sym: string) {
    this.logger.log(`[hist] backfill start for ${sym}`);

    // 1) 读取信号时间范围
    const range = await readSignalRange(this.redis, sym);
    if (!range) {
      this.logger.warn(`[hist] no signals found for ${sym}, skip`);
      return;
    }

    const needStart = range.minTs - PAD_MIN * 60_000;
    const needEnd = range.maxTs + PAD_MIN * 60_000;

    this.logger.log(
      `[hist] signals range: ${new Date(range.minTs).toISOString()} ~ ${new Date(range.maxTs).toISOString()} (pad=${PAD_MIN}m)`,
    );
    this.logger.log(
      `[hist] need cover   : ${new Date(needStart).toISOString()} ~ ${new Date(needEnd).toISOString()}`,
    );

    // 2) 去重/已有覆盖：简单计数（可选）
    const histKey = histKeyOf(sym);
    const existing = await this.redis.sendCommand<any>([
      'XRANGE',
      histKey,
      String(needStart),
      String(needEnd),
      'COUNT',
      '1',
    ]);
    const existN = existing?.length || 0;
    if (existN > 0) {
      this.logger.log(
        `[hist] existing rows in range: ${existN} (will still fetch & append; stream is append-only)`,
      );
    }

    // 3) 拉取 K 线覆盖区间
    const rows = await fetchCandlesCovering(sym, needStart, needEnd);
    this.logger.log(`[hist] fetched candles: ${rows.length}`);
    if (!rows.length) {
      this.logger.warn('[hist] nothing fetched, skip writing');
      return;
    }

    // 4) 写入 Redis
    const written = await writeHistStream(this.redis, sym, rows);
    this.logger.log(`[hist] written rows: ${written}`);

    this.logger.log(`[hist] backfill done for ${sym}`);
  }
}
