/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable @typescript-eslint/no-unsafe-return */
/* eslint-disable no-console */
/* eslint-disable no-console */
import 'dotenv/config';
import { createClient } from 'redis';

// ---------------- env ----------------
const ENV_FILE = process.env.ENV_FILE || '.env.qa';
if (ENV_FILE && ENV_FILE !== '.env.qa') {
  const dotenv = await import('dotenv');
  dotenv.config({ path: ENV_FILE });
}
const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379/0';
const SYM = process.env.SYM || 'BTC-USDT-SWAP';

const HORIZONS_MIN = (process.env.QA_HORIZONS_MIN || '5,10,30')
  .split(',')
  .map((s) => +s.trim())
  .filter((x) => x > 0);
const MAX_SIGNALS = +(process.env.QA_MAX_SIGNALS || '2000');
const FEE_BPS = +(process.env.QA_FEE_BPS || '0');
const USE_CLOSE_PRICE = (process.env.QA_PRICE_MODE || 'close') === 'close';

// ---------------- utils ----------------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const pct = (x) => (Number.isFinite(x) ? (x * 100).toFixed(2) + '%' : 'NaN');
const median = (a) => {
  if (!a.length) return NaN;
  const b = [...a].sort((x, y) => x - y);
  const m = Math.floor(b.length / 2);
  return b.length % 2 ? b[m] : (b[m - 1] + b[m]) / 2;
};
const mean = (a) => (a.length ? a.reduce((p, c) => p + c, 0) / a.length : NaN);

function sessionOfUTC(h) {
  if (h < 8) return 'Asia';
  if (h < 16) return 'EU';
  return 'US';
}

function binarySearchTs(arr, target) {
  let lo = 0,
    hi = arr.length - 1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1,
      v = arr[mid].ts;
    if (v === target) return mid;
    if (v < target) lo = mid + 1;
    else hi = mid - 1;
  }
  return ~lo;
}
function priceAtOrNa(k1m, ts) {
  let i = binarySearchTs(k1m, ts);
  if (i < 0) i = ~i;
  if (i >= k1m.length) i = k1m.length - 1;
  if (i < 0) i = 0;
  const k = k1m[i];
  return USE_CLOSE_PRICE ? k.close : (k.high + k.low) / 2;
}

// ---- TR/ATR ----
function trueRange(prevClose, h, l) {
  return Math.max(h - l, Math.abs(h - prevClose), Math.abs(l - prevClose));
}
function rollingATR(k1m, n = 30) {
  const atr = new Array(k1m.length).fill(NaN);
  for (let i = 1; i < k1m.length; i++) {
    k1m[i].__tr = trueRange(k1m[i - 1].close, k1m[i].high, k1m[i].low);
  }
  for (let i = n; i < k1m.length; i++) {
    let s = 0;
    for (let j = i - n + 1; j <= i; j++) s += k1m[j].__tr || 0;
    atr[i] = s / n;
  }
  return atr;
}
function classifyATRRegime(atr) {
  const vals = atr.filter((x) => Number.isFinite(x) && x > 0);
  if (!vals.length) return { regime: () => 'NA', q1: NaN, q2: NaN };
  const s = [...vals].sort((a, b) => a - b);
  const q = (p) => s[Math.floor(p * (s.length - 1))];
  const q1 = q(1 / 3),
    q2 = q(2 / 3);
  const regime = (x) =>
    !Number.isFinite(x) ? 'NA' : x <= q1 ? 'Low' : x >= q2 ? 'High' : 'Mid';
  return { regime, q1, q2 };
}

// 兼容：若流里是错误的“0/1/2/3…”编号格式，做一次正规化
function normalizeFields(f) {
  // 错误格式通常形如：['0','ts','1','175...','2','open','3','109495', ...]
  if (f.length >= 4 && f[0] === '0' && f[1] === 'ts') {
    const obj = {};
    for (let i = 0; i + 3 < f.length; i += 4) obj[f[i + 1]] = f[i + 3];
    return obj;
  }
  // 正常格式：['ts','175...','open','109495', ...]
  const obj = {};
  for (let i = 0; i + 1 < f.length; i += 2) obj[f[i]] = f[i + 1];
  return obj;
}

// ---------------- data ----------------
async function readSignals(redis, sym, maxN) {
  const key = `signal:final:{${sym}}`;
  const res = await redis.sendCommand([
    'XREVRANGE',
    key,
    '+',
    '-',
    'COUNT',
    String(maxN),
  ]);
  const rows = [];
  for (const [id, f] of res) {
    const map = normalizeFields(f);
    rows.push({
      id,
      ts: +(map.ts || 0),
      instId: map.instId,
      dir: map.dir === 'sell' ? 'sell' : 'buy',
      strength: +(map.strength || '0'),
      src: String(map['evidence.src'] || 'unknown'),
    });
  }
  rows.sort((a, b) => a.ts - b.ts);
  return rows;
}

async function readKlinesFromHist(redis, sym) {
  const key = `hist:win:1m:{${sym}}`;
  const res = await redis.sendCommand(['XRANGE', key, '-', '+']);
  const out = [];
  for (const [id, f] of res) {
    const m = normalizeFields(f);
    const ts = +(m.ts || 0);
    const open = +m.open,
      high = +m.high,
      low = +m.low,
      close = +m.close;
    if (
      Number.isFinite(ts) &&
      Number.isFinite(open) &&
      Number.isFinite(high) &&
      Number.isFinite(low) &&
      Number.isFinite(close)
    ) {
      out.push({ ts, open, high, low, close });
    }
  }
  out.sort((a, b) => a.ts - b.ts);
  return out;
}

// ---------------- eval ----------------
function horizonPnL(sig, k1m, m) {
  const px0 = priceAtOrNa(k1m, sig.ts);
  const px1 = priceAtOrNa(k1m, sig.ts + m * 60_000);
  if (!Number.isFinite(px0) || !Number.isFinite(px1)) return NaN;
  const gross = sig.dir === 'buy' ? px1 / px0 - 1 : px0 / px1 - 1;
  const fee = FEE_BPS / 10000;
  return gross - 2 * fee;
}

function evalA(signals, k1m) {
  const out = {};
  for (const h of HORIZONS_MIN) {
    const arr = signals
      .map((s) => horizonPnL(s, k1m, h))
      .filter(Number.isFinite);
    out[h] = {
      n: arr.length,
      winRate: arr.length ? arr.filter((x) => x > 0).length / arr.length : NaN,
      median: median(arr),
      mean: mean(arr),
    };
  }
  return out;
}

function evalB(signals, k1m) {
  const trades = [];
  let dir = null,
    pxIn = 0,
    lastTs = 0;
  const fee = FEE_BPS / 10000;
  const closeTrade = (exitPx, exitTs, reason) => {
    if (!dir) return;
    const pnl = dir === 'buy' ? exitPx / pxIn - 1 : pxIn / exitPx - 1;
    trades.push({ dir, entryTs: lastTs, exitTs, pnl: pnl - 2 * fee, reason });
    dir = null;
    pxIn = 0;
    lastTs = 0;
  };
  for (const s of signals) {
    const p = priceAtOrNa(k1m, s.ts);
    if (!Number.isFinite(p)) continue;
    if (!dir) {
      dir = s.dir;
      pxIn = p;
      lastTs = s.ts;
    } else if (dir === s.dir) {
      pxIn = (pxIn + p) / 2;
      lastTs = s.ts;
    } else {
      closeTrade(p, s.ts, 'flip');
      dir = s.dir;
      pxIn = p;
      lastTs = s.ts;
    }
  }
  if (dir) {
    const exit = priceAtOrNa(k1m, lastTs + 10 * 60_000);
    if (Number.isFinite(exit)) closeTrade(exit, lastTs + 10 * 60_000, 'tail');
  }
  const arr = trades.map((t) => t.pnl).filter(Number.isFinite);
  return {
    trades: trades.length,
    winRate: trades.length
      ? arr.filter((x) => x > 0).length / trades.length
      : NaN,
    median: median(arr),
    mean: mean(arr),
  };
}

function groupStats(signals, k1m, atr, atrReg) {
  const rows = signals.map((s) => {
    const d = new Date(s.ts);
    const h = d.getUTCHours();
    let idx = binarySearchTs(k1m, s.ts);
    if (idx < 0) idx = ~idx;
    if (idx < 0) idx = 0;
    if (idx >= k1m.length) idx = k1m.length - 1;
    const a = atr[idx];
    return {
      ...s,
      hour: h,
      session: sessionOfUTC(h),
      atrRegime: atrReg.regime(a),
    };
  });

  const compute = (flt) => {
    const sub = rows.filter(flt);
    const out = {};
    for (const h of HORIZONS_MIN) {
      const arr = sub.map((s) => horizonPnL(s, k1m, h)).filter(Number.isFinite);
      out[h] = {
        n: arr.length,
        winRate: arr.length
          ? arr.filter((x) => x > 0).length / arr.length
          : NaN,
        median: median(arr),
        mean: mean(arr),
      };
    }
    return out;
  };

  const bySrc = {},
    srcs = [...new Set(rows.map((r) => r.src))];
  for (const s of srcs) bySrc[s] = compute((r) => r.src === s);

  const bands = [
    { name: 's<0.70', f: (s) => s < 0.7 },
    { name: '0.70-0.80', f: (s) => s >= 0.7 && s < 0.8 },
    { name: '0.80-0.90', f: (s) => s >= 0.8 && s < 0.9 },
    { name: 's>=0.90', f: (s) => s >= 0.9 },
  ];
  const byBand = {};
  for (const b of bands) byBand[b.name] = compute((r) => b.f(r.strength));

  const bySession = {};
  for (const s of ['Asia', 'EU', 'US'])
    bySession[s] = compute((r) => r.session === s);

  const byHour = {};
  for (let h = 0; h < 24; h++)
    byHour[String(h).padStart(2, '0') + 'h'] = compute((r) => r.hour === h);

  const byVol = {};
  for (const v of ['Low', 'Mid', 'High'])
    byVol[v] = compute((r) => r.atrRegime === v);

  return {
    bySrc,
    byBand,
    bySession,
    byHour,
    byVol,
    q1: atrReg.q1,
    q2: atrReg.q2,
  };
}

// ---------------- main ----------------
(async () => {
  const redis = createClient({ url: REDIS_URL });
  await redis.connect();
  const signals = await readSignals(redis, SYM, MAX_SIGNALS);
  if (!signals.length) {
    console.log('no signals');
    process.exit(0);
  }

  const minSigTs = signals[0].ts;
  const maxSigTs = signals[signals.length - 1].ts;

  const k1m = await readKlinesFromHist(redis, SYM);
  if (!k1m.length) {
    console.log(
      `[diag] signals: ${new Date(minSigTs).toISOString()} ~ ${new Date(maxSigTs).toISOString()}`,
    );
    console.log(
      `[diag] klines : (none from hist:win:1m) — 请先执行: node scripts/backfill_klines.mjs`,
    );
    process.exit(0);
  }

  const kStart = k1m[0].ts,
    kEnd = k1m[k1m.length - 1].ts;
  const covered =
    kStart <= minSigTs &&
    kEnd >= maxSigTs + Math.max(...HORIZONS_MIN, 10) * 60_000;
  console.log(
    `[diag] signals: ${new Date(minSigTs).toISOString()} ~ ${new Date(maxSigTs).toISOString()}`,
  );
  console.log(
    `[diag] klines : ${new Date(kStart).toISOString()} ~ ${new Date(kEnd).toISOString()}`,
  );
  console.log(`[diag] covered: ${covered ? 'YES' : 'PARTIAL'}`);

  const atr = rollingATR(k1m, 30);
  const atrReg = classifyATRRegime(atr);

  const A = evalA(signals, k1m);
  const B = evalB(signals, k1m);
  const G = groupStats(signals, k1m, atr, atrReg);

  console.log(
    `=== QA ${SYM} — signals=${signals.length}, k1m=${k1m.length}, price=${USE_CLOSE_PRICE ? 'close' : 'mid'}, fee=${FEE_BPS}bps ===`,
  );
  const orderH = [...HORIZONS_MIN].sort((a, b) => a - b);
  let totN = 0,
    acc = 0,
    means = 0;
  for (const h of orderH) {
    const r = A[h];
    totN += r.n;
    acc += (r.winRate || 0) * (r.n || 0);
    means += (r.mean || 0) * (r.n || 0);
  }
  console.log(
    `[A] total=${totN}  winRate=${pct(totN ? acc / totN : NaN)}  medianPnL=${pct(median(orderH.map((h) => A[h].median).filter(Number.isFinite)))}  meanPnL=${pct(totN ? means / totN : NaN)}`,
  );
  for (const h of orderH) {
    const r = A[h];
    console.log(
      `    h=${h}m  n=${r.n}  winRate=${pct(r.winRate)}  median=${pct(r.median)}  mean=${pct(r.mean)}`,
    );
  }
  console.log(
    `[B] trades=${B.trades}  winRate=${pct(B.winRate)}  medianPnL=${pct(B.median)}  meanPnL=${pct(B.mean)}`,
  );

  const printGroup = (title, obj) => {
    console.log(`\n-- ${title} --`);
    for (const k of Object.keys(obj)) {
      const row = obj[k];
      const line = orderH
        .map((h) => {
          const x = row[h] || { n: 0 };
          return `${h}m:n=${x.n},wr=${pct(x.winRate)},med=${pct(x.median)},mean=${pct(x.mean)}`;
        })
        .join(' | ');
      console.log(k.padEnd(12), line);
    }
  };
  printGroup('by src', G.bySrc);
  printGroup('by strength band', G.byBand);
  printGroup('by session(UTC)', G.bySession);
  printGroup('by hour(UTC)', G.byHour);
  console.log(
    `\n-- by volatility regime (ATR30; q1=${G.q1?.toFixed ? G.q1.toFixed(6) : G.q1}, q2=${G.q2?.toFixed ? G.q2.toFixed(6) : G.q2}) --`,
  );
  printGroup('vol', G.byVol);

  await redis.quit();
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
