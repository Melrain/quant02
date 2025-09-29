/* eslint-disable @typescript-eslint/no-unsafe-return */
/* eslint-disable no-console */
import 'dotenv/config';
import { createClient } from 'redis';

const ENV_FILE = process.env.ENV_FILE || '.env.qa';
if (ENV_FILE && ENV_FILE !== '.env.qa') {
  const dotenv = await import('dotenv');
  dotenv.config({ path: ENV_FILE });
}

const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379/0';
const SYM = process.env.SYM || 'BTC-USDT-SWAP';
const OKX_REST_BASE = process.env.OKX_REST_BASE || 'https://www.okx.com';

// 回填范围：以最终信号最早/最晚时间为基准，左右各加 pad 分钟
const PAD_MIN = +(process.env.BF_PAD_MIN || '45');
const WRITE_REALTIME_COPY = (process.env.BF_WRITE_REALTIME || '0') === '1'; // 是否同步写 win:1m:{SYM}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchSignalsRange(r, sym) {
  const key = `signal:final:{${sym}}`;
  const res = await r.sendCommand(['XRANGE', key, '-', '+', 'COUNT', '1']);
  const res2 = await r.sendCommand(['XREVRANGE', key, '+', '-', 'COUNT', '1']);
  if (!res?.length || !res2?.length) return null;

  const f2map = (f) => {
    const m = {};
    for (let i = 0; i + 1 < f.length; i += 2) m[f[i]] = f[i + 1];
    return m;
  };
  const minTs = +f2map(res[0][1]).ts;
  const maxTs = +f2map(res2[0][1]).ts;
  if (!Number.isFinite(minTs) || !Number.isFinite(maxTs)) return null;
  return { minTs, maxTs };
}

// 从最早需要的时间（minTs - pad）开始，用 history-candles 的 after= 游标往前翻，直到 >= maxTsPlusPad
async function fetchOKX1mCovering(instId, minTs, maxTsPlusPad) {
  const needStart = minTs - 45 * 60_000; // 左侧缓冲
  const needEnd = maxTsPlusPad; // 右侧边界
  const out = new Map(); // ts -> kline

  async function page(url) {
    const r = await fetch(url);
    if (!r.ok) throw new Error(`OKX http ${r.status}`);
    const j = await r.json();
    if (j.code !== '0') throw new Error(`OKX err ${j.code}:${j.msg}`);
    return j.data || [];
  }
  function add(rows) {
    for (const row of rows) {
      const ts = +row[0];
      out.set(ts, {
        ts,
        open: +row[1],
        high: +row[2],
        low: +row[3],
        close: +row[4],
      });
    }
  }

  // 以 “右边界+1分钟” 为起点，从右往左用 history-candles 翻页
  let cursor = needEnd + 60_000;
  for (let i = 0; i < 10000; i++) {
    const url = `${OKX_REST_BASE}/api/v5/market/history-candles?instId=${encodeURIComponent(instId)}&bar=1m&limit=500&before=${cursor}`;
    const rows = await page(url);
    if (!rows.length) break;

    add(rows);

    // history-candles 返回按时间倒序；最老的一条在最后
    const oldest = +rows[rows.length - 1][0];
    if (oldest <= needStart) break;

    // 下一页继续往更早翻
    cursor = oldest;
    await sleep(60); // 轻微限速
  }

  return Array.from(out.values()).sort((a, b) => a.ts - b.ts);
}

async function main() {
  const r = createClient({ url: REDIS_URL });
  await r.connect();

  console.log(`\n== Backfill ${SYM} ==`);
  const range = await fetchSignalsRange(r, SYM);
  if (!range) {
    console.log('no signals found, abort.');
    await r.quit();
    return;
  }

  const minTs = range.minTs - PAD_MIN * 60_000;
  const maxTs = range.maxTs + PAD_MIN * 60_000;
  console.log(
    `[range] from signals: ${new Date(range.minTs).toISOString()} ~ ${new Date(range.maxTs).toISOString()} (pad=${PAD_MIN}m)`,
  );

  // 去重：统计 hist:win:1m 里已有覆盖
  const histKey = `hist:win:1m:{${SYM}}`;
  const exist = await r.sendCommand([
    'XRANGE',
    histKey,
    String(minTs),
    String(maxTs),
    'COUNT',
    '1',
  ]);
  console.log(
    `[dedup] existing hist:win:1m rows in range: ${exist?.length || 0}`,
  );

  const kl = await fetchOKX1mCovering(SYM, minTs, maxTs);
  console.log(`[okx] fetched candles: ${kl.length}`);

  const toWrite = kl; // 这里简单处理，全部写入（XADD 自然是追加）
  console.log(`[plan] to write new rows: ${toWrite.length}`);

  // ✅ 正确的 XADD：字段名必须是 ts/open/high/low/close… 不能有 0/1/2 这种编号
  let written = 0;
  for (const k of toWrite) {
    const { ts, open: o, high: h, low: l, close: c } = k;
    await r.xAdd(histKey, '*', {
      ts: String(ts),
      open: String(o),
      high: String(h),
      low: String(l),
      close: String(c),
      vol: '0',
      vbuy: '0',
      vsell: '0',
      vwap: String(c),
      tickN: '0',
      gap: '0',
    });
    if (WRITE_REALTIME_COPY) {
      await r.xAdd(
        `win:1m:{${SYM}}`,
        '*',
        {
          ts: String(ts),
          open: String(o),
          high: String(h),
          low: String(l),
          close: String(c),
          vol: '0',
          vbuy: '0',
          vsell: '0',
          vwap: String(c),
          tickN: '0',
          gap: '0',
        },
        {
          TRIM: { strategy: 'MAXLEN', strategyModifier: '~', threshold: 5000 },
        },
      );
    }
    written++;
    if (written % 2000 === 0) await sleep(5);
  }

  console.log(`[done] XADD -> ${histKey}, written=${written}\n`);
  console.log('Backfill completed.\n');

  await r.quit();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
