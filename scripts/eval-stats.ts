/* eslint-disable @typescript-eslint/no-unused-vars */
/* scripts/eval-stats.ts */
import 'dotenv/config';
import { RedisStreamsService } from 'src/redis-streams/redis-streams.service';
import { RedisClient } from 'src/redis/redis.client';

type Row = Record<string, string>;
type Stat = {
  n: number;
  nWin: number;
  nNeutral: number;
  mean: number;
  p25: number;
  p50: number;
  p75: number;
};

function parseMs(v?: string): number | undefined {
  if (!v) return undefined;
  if (/^\d{10,}$/.test(v)) return Number(v);
  const t = Date.parse(v);
  return Number.isFinite(t) ? t : undefined;
}
function arg(name: string): string | undefined {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 && i + 1 < process.argv.length
    ? process.argv[i + 1]
    : undefined;
}
function toBool(s: string | undefined) {
  return s === 'true' || s === '1' || s === 'yes';
}
function quantiles(xs: number[], qs: number[]) {
  if (xs.length === 0) return qs.map(() => NaN);
  const a = xs.slice().sort((x, y) => x - y);
  return qs.map((q) => {
    const idx = (a.length - 1) * q;
    const lo = Math.floor(idx);
    const hi = Math.ceil(idx);
    if (lo === hi) return a[lo];
    const w = idx - lo;
    return a[lo] * (1 - w) + a[hi] * w;
  });
}
function pad(s: string, w: number) {
  return (s + ' '.repeat(w)).slice(0, w);
}

async function main() {
  const sym = arg('sym');
  if (!sym) {
    console.error(
      'Usage: ts-node scripts/eval-stats.ts --sym <INSTID> [--start <ISO|ms>] [--end <ISO|ms>] [--dir buy|sell] [--hz 5m,15m,30m,1h,12h] [--groupByDir]',
    );
    process.exit(1);
  }
  const start = parseMs(arg('start')) ?? 0;
  const end = parseMs(arg('end')) ?? Date.now();
  const filterDir = arg('dir') as 'buy' | 'sell' | undefined;
  const hzFilter = (arg('hz') ?? '')
    .split(',')
    .map((x) => x.trim())
    .filter(Boolean);
  const groupByDir = toBool(arg('groupByDir'));

  const redis = new RedisClient({
    url: process.env.REDIS_URL,
    keyPrefix: process.env.REDIS_KEY_PREFIX || '',
    enableAutoPipelining: true,
  });
  const streams = new RedisStreamsService(redis);

  const key = streams.buildOutStreamKey(sym, 'eval:done');

  console.log(
    `[stats] sym=${sym} window=${new Date(start).toISOString()} ~ ${new Date(end).toISOString()}` +
      (filterDir ? ` dir=${filterDir}` : '') +
      (hzFilter.length ? ` hz=${hzFilter.join('/')}` : '') +
      (groupByDir ? ' (groupBy=dir)' : ''),
  );

  // 以 dueAt 做时间过滤（没有全局 ts 字段）
  const CHUNK_MS = 24 * 60 * 60 * 1000; // 1d
  let from = start;
  const buckets = new Map<string, number[]>(); // key = horizon 或 horizon|dir
  let total = 0;

  while (from < end) {
    const to = Math.min(end, from + CHUNK_MS);
    const rows = await streams.xrangeByTime(key, from, to);
    for (const [, arr] of rows) {
      const h: Row = {};
      for (let i = 0; i < arr.length; i += 2) h[arr[i]] = arr[i + 1];

      const horizon = h.horizon;
      const dir = h.dir as 'buy' | 'sell' | undefined;
      const dueAt = Number(h.dueAt);
      const retNetBp = Number(h.retNetBp);
      if (!horizon || !Number.isFinite(dueAt) || !Number.isFinite(retNetBp))
        continue;
      if (filterDir && dir !== filterDir) continue;
      if (hzFilter.length && !hzFilter.includes(horizon)) continue;
      if (dueAt < from || dueAt > to) continue;

      const key = groupByDir ? `${horizon}|${dir ?? 'na'}` : horizon;
      if (!buckets.has(key)) buckets.set(key, []);
      buckets.get(key)!.push(retNetBp);
      total++;
    }
    from = to;
  }

  // 输出
  if (buckets.size === 0) {
    console.log('no samples');
    await redis.quit();
    return;
  }

  console.log(
    '\n' +
      pad('bucket', 16) +
      pad('n', 8) +
      pad('win%', 8) +
      pad('neutral%', 10) +
      pad('mean', 10) +
      pad('p25', 10) +
      pad('p50', 10) +
      pad('p75', 10),
  );
  console.log('-'.repeat(82));

  for (const [b, arr] of Array.from(buckets.entries()).sort()) {
    const n = arr.length;
    const nWin = arr.filter(
      (x) => x >= Number(process.env.EVAL_SUCCESS_THRESHOLD_BP ?? '5'),
    ).length;
    const nNeutral = arr.filter(
      (x) => Math.abs(x) < Number(process.env.EVAL_NEUTRAL_BAND_BP ?? '2'),
    ).length;
    const mean = arr.reduce((s, x) => s + x, 0) / Math.max(1, n);
    const [p25, p50, p75] = quantiles(arr, [0.25, 0.5, 0.75]);

    const winPct = (100 * nWin) / Math.max(1, n);
    const neutralPct = (100 * nNeutral) / Math.max(1, n);

    const num = (x: number) => (Number.isFinite(x) ? x.toFixed(2) : 'NaN');
    console.log(
      pad(b, 16) +
        pad(String(n), 8) +
        pad(winPct.toFixed(1), 8) +
        pad(neutralPct.toFixed(1), 10) +
        pad(num(mean), 10) +
        pad(num(p25), 10) +
        pad(num(p50), 10) +
        pad(num(p75), 10),
    );
  }

  console.log(`\nTotal samples: ${total}`);
  await redis.quit();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
