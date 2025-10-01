/* scripts/backfill-eval.ts
 * 回放历史 signal:final，生成 eval:done 审计（与在线 evaluator 口径一致）
 * 用法：
 *  npx ts-node -r tsconfig-paths/register --files ./scripts/backfill-eval.ts --sym BTC-USDT-SWAP --start "2025-09-25T00:00:00Z" --end "2025-09-30T00:00:00Z"
 */

import 'dotenv/config';
import { RedisStreamsService } from 'src/redis-streams/redis-streams.service';
import { RedisClient } from 'src/redis/redis.client';

type FinalRow = Record<string, string>;

const force = process.argv.includes('--force');

function parseMs(v: string): number {
  if (/^\d{10,}$/.test(v)) return Number(v); // 毫秒
  const t = Date.parse(v);
  if (Number.isFinite(t)) return t;
  throw new Error(`Bad time: ${v}`);
}

function parseArg(name: string, alias?: string): string | undefined {
  const idx = process.argv.findIndex(
    (x) => x === `--${name}` || (alias && x === alias),
  );
  if (idx >= 0 && idx + 1 < process.argv.length) return process.argv[idx + 1];
  return undefined;
}

/** 将秒列表解析为 {name,ms} 列表（如 "300,900" -> 5m/15m/…） */
function parseHorizonsSec(raw?: string, fallback: number[] = [300, 900]) {
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
    console.error(
      'Usage: ts-node scripts/backfill-eval.ts --sym <INSTID> --start <ISO|ms> --end <ISO|ms>',
    );
    process.exit(1);
  }

  const startMs = parseMs(startArg);
  const endMs = parseMs(endArg);
  if (endMs <= startMs) throw new Error('end must be greater than start');

  const redis = new RedisClient({
    url: process.env.REDIS_URL,
    keyPrefix: process.env.REDIS_KEY_PREFIX || '',
    enableAutoPipelining: true,
  });
  const streams = new RedisStreamsService(redis);

  const horizons = parseHorizonsSec(process.env.EVAL_HORIZONS_SEC, [300, 900]); // 默认 5m/15m
  const PX_SEARCH_MS = Number(process.env.EVAL_PX_SEARCH_MS ?? '3000');
  const FEE_BP_PER_SIDE = Number(process.env.EVAL_FEE_BP_PER_SIDE ?? '1.0');
  const SLIP_BP = Number(process.env.EVAL_SLIP_BP ?? '0.5');
  const SUCCESS_THRESHOLD_BP = Number(
    process.env.EVAL_SUCCESS_THRESHOLD_BP ?? '5',
  );
  const NEUTRAL_BAND_BP = Number(process.env.EVAL_NEUTRAL_BAND_BP ?? '2');

  // 读取 signal:final（用已前缀 stream key）
  const finalKey = streams.buildOutStreamKey(sym, 'signal:final');
  const doneKey = streams.buildOutStreamKey(sym, 'eval:done');

  console.log(
    `[backfill] sym=${sym} window=${new Date(startMs).toISOString()} ~ ${new Date(endMs).toISOString()} | horizons=${horizons
      .map((h) => h.name)
      .join('/')} | pxSearch=${PX_SEARCH_MS}ms`,
  );

  // XRANGE 按时间窗口读（分段以防返回太大）
  const CHUNK_MS = 6 * 60 * 60 * 1000; // 6h 一段
  let from = startMs;
  let total = 0,
    wrote = 0,
    skipped = 0,
    // eslint-disable-next-line prefer-const
    misses = 0;

  while (from < endMs) {
    const to = Math.min(endMs, from + CHUNK_MS);
    const rows = await streams.xrangeByTime(finalKey, from, to);
    for (const [id, arr] of rows) {
      total++;
      const h: FinalRow = {};
      for (let i = 0; i < arr.length; i += 2) h[arr[i]] = arr[i + 1];

      // 取锚点与进场价
      const ts0 = Number(h.refPx_ts ?? h.ts);
      const p0 = Number(h.refPx);
      const dir = h.dir === 'buy' || h.dir === 'sell' ? h.dir : null;

      if (!Number.isFinite(ts0) || !Number.isFinite(p0) || !dir) {
        skipped++;
        continue;
      }

      for (const hz of horizons) {
        const dueAt = ts0 + hz.ms;

        // 幂等：同一 finalId/hz 评过就跳过（3天过期）
        const idemKey = `qt:eval:done:{${sym}}:${id}:${hz.name}`;
        const ok = force ? true : await redis.setNxEx(idemKey, 3 * 24 * 3600);
        if (!ok) {
          console.log('[idem-hit]', idemKey);
          continue;
        }

        // t* 后第一笔 tick（mid 优先 / last 兜底），再兜底 1m kline close
        const px = await getTickAfter(streams, sym, dueAt, PX_SEARCH_MS);
        let usedPx: number | null = null;
        let usedTs = 0;
        let usedSrc: 'mid' | 'last' | 'kline' | 'na' = 'na';

        if (px) {
          usedPx = px.usedPx;
          usedTs = px.usedTs;
          usedSrc = px.usedSrc;
        } else {
          // 兜底：下一根 1m close
          // --- fallback: 下一根 1m close，优先 win:1m，缺失则用 ws:kline1m ---
          const tCeil = Math.floor((dueAt - 1) / 60_000) * 60_000 + 60_000;

          let usedPxTmp: number | null = null;
          let usedTsTmp = 0;

          // 1) win:1m
          const winKey = streams.buildOutStreamKey(sym, 'win:1m');
          const winBars = await streams.readWindowAsObjects(
            winKey,
            tCeil - 60_000,
            tCeil + 4 * 60_000,
          );
          const winList = winBars
            .map((r) => ({
              ts: Number(r.ts),
              close: Number((r as any).close ?? (r as any).c),
            }))
            .filter((x) => Number.isFinite(x.ts) && Number.isFinite(x.close))
            .sort((a, b) => a.ts - b.ts);
          const winBar = winList.find((b) => b.ts >= tCeil);
          if (winBar) {
            usedPxTmp = winBar.close;
            usedTsTmp = winBar.ts;
          } else {
            // 2) ws:kline1m
            const k1Key = streams.buildKlineKey(sym, '1m'); // dev:ws:{sym}:kline1m
            const kBars = await streams.readWindowAsObjects(
              k1Key,
              tCeil - 60_000,
              tCeil + 4 * 60_000,
            );
            const kList = kBars
              .map((r) => ({
                ts: Number(r.ts),
                close: Number((r as any).close ?? (r as any).c),
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

        // 方向化收益（bp）→ 扣成本得到净收益
        let retRawBp = (((usedPx ? usedPx : 0) - p0) / p0) * 10_000;
        if (dir === 'sell') retRawBp = -retRawBp;
        const retNetBp = retRawBp - FEE_BP_PER_SIDE - SLIP_BP;

        // 审计流（与在线 evaluator 对齐）
        await streams.xadd(
          doneKey,
          {
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

            // 记录 final 的原始 id 以便追溯
            finalId: id,
          },
          { maxlenApprox: 50_000 },
        );
        wrote++;
      }
    }
    console.log(
      `[chunk] ${new Date(from).toISOString()} ~ ${new Date(to).toISOString()} | finals=${rows.length}`,
    );
    from = to;
  }

  console.log(
    `[done] sym=${sym} totals: finals=${total}, wrote=${wrote}, skipped_bad=${skipped}, miss_px=${misses}`,
  );
  await redis.quit();
}

/** t* 后第一笔 tick：优先 ws:{sym}:book 的 mid，兜底 ws:{sym}:trades 的 last */
async function getTickAfter(
  streams: RedisStreamsService,
  sym: string,
  targetTs: number,
  searchMs: number,
): Promise<{ usedPx: number; usedTs: number; usedSrc: 'mid' | 'last' } | null> {
  const to = targetTs + searchMs;

  // A) 盘口 mid
  try {
    const bookKey = streams.buildKey(sym, 'book');
    const rows = await streams.readWindowAsObjects(bookKey, targetTs, to, 200);
    if (rows?.length) {
      const r = rows.find((x: any) => Number(x.ts) >= targetTs);
      if (r) {
        const bid = Number((r as any)['bid1.px']);
        const ask = Number((r as any)['ask1.px']);
        if (
          Number.isFinite(bid) &&
          Number.isFinite(ask) &&
          bid > 0 &&
          ask > 0
        ) {
          return {
            usedPx: (bid + ask) / 2,
            usedTs: Number(r.ts),
            usedSrc: 'mid',
          };
        }
      }
    }
  } catch {
    /* empty */
  }

  // B) 成交 last
  try {
    const tradeKey = streams.buildKey(sym, 'trades');
    const rows = await streams.readWindowAsObjects(tradeKey, targetTs, to, 200);
    if (rows?.length) {
      const r = rows.find((x: any) => Number(x.ts) >= targetTs);
      if (r) {
        const px = Number((r as any).px);
        if (Number.isFinite(px) && px > 0) {
          return { usedPx: px, usedTs: Number(r.ts), usedSrc: 'last' };
        }
      }
    }
  } catch {
    /* empty */
  }

  return null;
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
