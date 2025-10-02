"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
require("dotenv/config");
const redis_streams_service_1 = require("../src/redis-streams/redis-streams.service");
const redis_client_1 = require("../src/redis/redis.client");
function parseMs(v) {
    if (!v)
        return undefined;
    if (/^\d{10,}$/.test(v))
        return Number(v);
    const t = Date.parse(v);
    return Number.isFinite(t) ? t : undefined;
}
function arg(name) {
    const i = process.argv.indexOf(`--${name}`);
    return i >= 0 && i + 1 < process.argv.length
        ? process.argv[i + 1]
        : undefined;
}
function toBool(s) {
    return s === 'true' || s === '1' || s === 'yes';
}
function quantiles(xs, qs) {
    if (xs.length === 0)
        return qs.map(() => NaN);
    const a = xs.slice().sort((x, y) => x - y);
    return qs.map((q) => {
        const idx = (a.length - 1) * q;
        const lo = Math.floor(idx);
        const hi = Math.ceil(idx);
        if (lo === hi)
            return a[lo];
        const w = idx - lo;
        return a[lo] * (1 - w) + a[hi] * w;
    });
}
function pad(s, w) {
    return (s + ' '.repeat(w)).slice(0, w);
}
async function main() {
    const sym = arg('sym');
    if (!sym) {
        console.error('Usage: ts-node scripts/eval-stats.ts --sym <INSTID> [--start <ISO|ms>] [--end <ISO|ms>] [--dir buy|sell] [--hz 5m,15m,30m,1h,12h] [--groupByDir]');
        process.exit(1);
    }
    const start = parseMs(arg('start')) ?? 0;
    const end = parseMs(arg('end')) ?? Date.now();
    const filterDir = arg('dir');
    const hzFilter = (arg('hz') ?? '')
        .split(',')
        .map((x) => x.trim())
        .filter(Boolean);
    const groupByDir = toBool(arg('groupByDir'));
    const redis = new redis_client_1.RedisClient({
        url: process.env.REDIS_URL,
        keyPrefix: process.env.REDIS_KEY_PREFIX || '',
        enableAutoPipelining: true,
    });
    const streams = new redis_streams_service_1.RedisStreamsService(redis);
    const key = streams.buildOutStreamKey(sym, 'eval:done');
    console.log(`[stats] sym=${sym} window=${new Date(start).toISOString()} ~ ${new Date(end).toISOString()}` +
        (filterDir ? ` dir=${filterDir}` : '') +
        (hzFilter.length ? ` hz=${hzFilter.join('/')}` : '') +
        (groupByDir ? ' (groupBy=dir)' : ''));
    const CHUNK_MS = 24 * 60 * 60 * 1000;
    let from = start;
    const buckets = new Map();
    let total = 0;
    while (from < end) {
        const to = Math.min(end, from + CHUNK_MS);
        const rows = await streams.xrangeByTime(key, from, to);
        for (const [, arr] of rows) {
            const h = {};
            for (let i = 0; i < arr.length; i += 2)
                h[arr[i]] = arr[i + 1];
            const horizon = h.horizon;
            const dir = h.dir;
            const dueAt = Number(h.dueAt);
            const retNetBp = Number(h.retNetBp);
            if (!horizon || !Number.isFinite(dueAt) || !Number.isFinite(retNetBp))
                continue;
            if (filterDir && dir !== filterDir)
                continue;
            if (hzFilter.length && !hzFilter.includes(horizon))
                continue;
            if (dueAt < from || dueAt > to)
                continue;
            const key = groupByDir ? `${horizon}|${dir ?? 'na'}` : horizon;
            if (!buckets.has(key))
                buckets.set(key, []);
            buckets.get(key).push(retNetBp);
            total++;
        }
        from = to;
    }
    if (buckets.size === 0) {
        console.log('no samples');
        await redis.quit();
        return;
    }
    console.log('\n' +
        pad('bucket', 16) +
        pad('n', 8) +
        pad('win%', 8) +
        pad('neutral%', 10) +
        pad('mean', 10) +
        pad('p25', 10) +
        pad('p50', 10) +
        pad('p75', 10));
    console.log('-'.repeat(82));
    for (const [b, arr] of Array.from(buckets.entries()).sort()) {
        const n = arr.length;
        const nWin = arr.filter((x) => x >= Number(process.env.EVAL_SUCCESS_THRESHOLD_BP ?? '5')).length;
        const nNeutral = arr.filter((x) => Math.abs(x) < Number(process.env.EVAL_NEUTRAL_BAND_BP ?? '2')).length;
        const mean = arr.reduce((s, x) => s + x, 0) / Math.max(1, n);
        const [p25, p50, p75] = quantiles(arr, [0.25, 0.5, 0.75]);
        const winPct = (100 * nWin) / Math.max(1, n);
        const neutralPct = (100 * nNeutral) / Math.max(1, n);
        const num = (x) => (Number.isFinite(x) ? x.toFixed(2) : 'NaN');
        console.log(pad(b, 16) +
            pad(String(n), 8) +
            pad(winPct.toFixed(1), 8) +
            pad(neutralPct.toFixed(1), 10) +
            pad(num(mean), 10) +
            pad(num(p25), 10) +
            pad(num(p50), 10) +
            pad(num(p75), 10));
    }
    console.log(`\nTotal samples: ${total}`);
    await redis.quit();
}
main().catch((e) => {
    console.error(e);
    process.exit(1);
});
//# sourceMappingURL=eval-stats.js.map