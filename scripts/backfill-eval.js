"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
require("dotenv/config");
const redis_streams_service_1 = require("../src/redis-streams/redis-streams.service");
const redis_client_1 = require("../src/redis/redis.client");
const force = process.argv.includes('--force');
function parseMs(v) {
    if (/^\d{10,}$/.test(v))
        return Number(v);
    const t = Date.parse(v);
    if (Number.isFinite(t))
        return t;
    throw new Error(`Bad time: ${v}`);
}
function parseArg(name, alias) {
    const idx = process.argv.findIndex((x) => x === `--${name}` || (alias && x === alias));
    if (idx >= 0 && idx + 1 < process.argv.length)
        return process.argv[idx + 1];
    return undefined;
}
function parseHorizonsSec(raw, fallback = [300, 900]) {
    const secs = (raw ?? '')
        .split(',')
        .map((s) => Number(s.trim()))
        .filter((x) => Number.isFinite(x) && x > 0);
    const arr = secs.length ? secs : fallback;
    return arr.map((sec) => ({
        name: sec % 3600 === 0 ? `${sec / 3600}h` : `${Math.round(sec / 60)}m`,
        ms: sec * 1000,
    }));
}
async function main() {
    const sym = parseArg('sym');
    const startArg = parseArg('start');
    const endArg = parseArg('end');
    if (!sym || !startArg || !endArg) {
        console.error('Usage: ts-node scripts/backfill-eval.ts --sym <INSTID> --start <ISO|ms> --end <ISO|ms>');
        process.exit(1);
    }
    const startMs = parseMs(startArg);
    const endMs = parseMs(endArg);
    if (endMs <= startMs)
        throw new Error('end must be greater than start');
    const redis = new redis_client_1.RedisClient({
        url: process.env.REDIS_URL,
        keyPrefix: process.env.REDIS_KEY_PREFIX || '',
        enableAutoPipelining: true,
    });
    const streams = new redis_streams_service_1.RedisStreamsService(redis);
    const horizons = parseHorizonsSec(process.env.EVAL_HORIZONS_SEC, [300, 900]);
    const PX_SEARCH_MS = Number(process.env.EVAL_PX_SEARCH_MS ?? '3000');
    const FEE_BP_PER_SIDE = Number(process.env.EVAL_FEE_BP_PER_SIDE ?? '1.0');
    const SLIP_BP = Number(process.env.EVAL_SLIP_BP ?? '0.5');
    const SUCCESS_THRESHOLD_BP = Number(process.env.EVAL_SUCCESS_THRESHOLD_BP ?? '5');
    const NEUTRAL_BAND_BP = Number(process.env.EVAL_NEUTRAL_BAND_BP ?? '2');
    const finalKey = streams.buildOutStreamKey(sym, 'signal:final');
    const doneKey = streams.buildOutStreamKey(sym, 'eval:done');
    console.log(`[backfill] sym=${sym} window=${new Date(startMs).toISOString()} ~ ${new Date(endMs).toISOString()} | horizons=${horizons
        .map((h) => h.name)
        .join('/')} | pxSearch=${PX_SEARCH_MS}ms`);
    const CHUNK_MS = 6 * 60 * 60 * 1000;
    let from = startMs;
    let total = 0, wrote = 0, skipped = 0, misses = 0;
    while (from < endMs) {
        const to = Math.min(endMs, from + CHUNK_MS);
        const rows = await streams.xrangeByTime(finalKey, from, to);
        for (const [id, arr] of rows) {
            total++;
            const h = {};
            for (let i = 0; i < arr.length; i += 2)
                h[arr[i]] = arr[i + 1];
            const ts0 = Number(h.refPx_ts ?? h.ts);
            const p0 = Number(h.refPx);
            const dir = h.dir === 'buy' || h.dir === 'sell' ? h.dir : null;
            if (!Number.isFinite(ts0) || !Number.isFinite(p0) || !dir) {
                skipped++;
                continue;
            }
            for (const hz of horizons) {
                const dueAt = ts0 + hz.ms;
                const idemKey = `qt:eval:done:{${sym}}:${id}:${hz.name}`;
                const ok = force ? true : await redis.setNxEx(idemKey, 3 * 24 * 3600);
                if (!ok) {
                    console.log('[idem-hit]', idemKey);
                    continue;
                }
                const px = await getTickAfter(streams, sym, dueAt, PX_SEARCH_MS);
                let usedPx = null;
                let usedTs = 0;
                let usedSrc = 'na';
                if (px) {
                    usedPx = px.usedPx;
                    usedTs = px.usedTs;
                    usedSrc = px.usedSrc;
                }
                else {
                    const tCeil = Math.floor((dueAt - 1) / 60_000) * 60_000 + 60_000;
                    let usedPxTmp = null;
                    let usedTsTmp = 0;
                    const winKey = streams.buildOutStreamKey(sym, 'win:1m');
                    const winBars = await streams.readWindowAsObjects(winKey, tCeil - 60_000, tCeil + 4 * 60_000);
                    const winList = winBars
                        .map((r) => ({
                        ts: Number(r.ts),
                        close: Number(r.close ?? r.c),
                    }))
                        .filter((x) => Number.isFinite(x.ts) && Number.isFinite(x.close))
                        .sort((a, b) => a.ts - b.ts);
                    const winBar = winList.find((b) => b.ts >= tCeil);
                    if (winBar) {
                        usedPxTmp = winBar.close;
                        usedTsTmp = winBar.ts;
                    }
                    else {
                        const k1Key = streams.buildKlineKey(sym, '1m');
                        const kBars = await streams.readWindowAsObjects(k1Key, tCeil - 60_000, tCeil + 4 * 60_000);
                        const kList = kBars
                            .map((r) => ({
                            ts: Number(r.ts),
                            close: Number(r.close ?? r.c),
                        }))
                            .filter((x) => Number.isFinite(x.ts) && Number.isFinite(x.close))
                            .sort((a, b) => a.ts - b.ts);
                        const kBar = kList.find((b) => b.ts >= tCeil);
                        if (kBar) {
                            usedPxTmp = kBar.close;
                            usedTsTmp = kBar.ts;
                        }
                    }
                    if (usedPxTmp != null) {
                        usedPx = usedPxTmp;
                        usedTs = usedTsTmp;
                        usedSrc = 'kline';
                    }
                }
                let retRawBp = (((usedPx ? usedPx : 0) - p0) / p0) * 10_000;
                if (dir === 'sell')
                    retRawBp = -retRawBp;
                const retNetBp = retRawBp - FEE_BP_PER_SIDE - SLIP_BP;
                await streams.xadd(doneKey, {
                    ts0: String(ts0),
                    dueAt: String(dueAt),
                    horizon: hz.name,
                    dir,
                    p0: String(p0),
                    usedPx: String(usedPx),
                    usedPx_ts: String(usedTs),
                    usedPx_source: usedSrc,
                    priceLagMs: String(Math.max(0, usedTs - ts0)),
                    retRawBp: retRawBp.toFixed(4),
                    retNetBp: retNetBp.toFixed(4),
                    thresholdBp: String(SUCCESS_THRESHOLD_BP),
                    neutralBandBp: String(NEUTRAL_BAND_BP),
                    neutral: String(Math.abs(retNetBp) < NEUTRAL_BAND_BP),
                    success: String(retNetBp >= SUCCESS_THRESHOLD_BP),
                    finalId: id,
                }, { maxlenApprox: 50_000 });
                wrote++;
            }
        }
        console.log(`[chunk] ${new Date(from).toISOString()} ~ ${new Date(to).toISOString()} | finals=${rows.length}`);
        from = to;
    }
    console.log(`[done] sym=${sym} totals: finals=${total}, wrote=${wrote}, skipped_bad=${skipped}, miss_px=${misses}`);
    await redis.quit();
}
async function getTickAfter(streams, sym, targetTs, searchMs) {
    const to = targetTs + searchMs;
    try {
        const bookKey = streams.buildKey(sym, 'book');
        const rows = await streams.readWindowAsObjects(bookKey, targetTs, to, 200);
        if (rows?.length) {
            const r = rows.find((x) => Number(x.ts) >= targetTs);
            if (r) {
                const bid = Number(r['bid1.px']);
                const ask = Number(r['ask1.px']);
                if (Number.isFinite(bid) &&
                    Number.isFinite(ask) &&
                    bid > 0 &&
                    ask > 0) {
                    return {
                        usedPx: (bid + ask) / 2,
                        usedTs: Number(r.ts),
                        usedSrc: 'mid',
                    };
                }
            }
        }
    }
    catch {
    }
    try {
        const tradeKey = streams.buildKey(sym, 'trades');
        const rows = await streams.readWindowAsObjects(tradeKey, targetTs, to, 200);
        if (rows?.length) {
            const r = rows.find((x) => Number(x.ts) >= targetTs);
            if (r) {
                const px = Number(r.px);
                if (Number.isFinite(px) && px > 0) {
                    return { usedPx: px, usedTs: Number(r.ts), usedSrc: 'last' };
                }
            }
        }
    }
    catch {
    }
    return null;
}
main().catch((e) => {
    console.error(e);
    process.exit(1);
});
//# sourceMappingURL=backfill-eval.js.map