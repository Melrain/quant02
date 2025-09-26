/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable @typescript-eslint/no-unsafe-return */
/* eslint-disable no-undef */
// scripts/sim.e2e.mjs
// Node >=18（原生 fetch），需要 npm: redis, yargs, xmlbuilder2（若要 JUnit 输出）
//
// 模式：
//   smoke  - 少量样本，验证链路连通、路由基本逻辑（pass/strength/cooldown）
//   load   - 指定速率与时长压测，校验 rate 与 drops、延迟是否在阈内
//   health - 在 smoke 基础上增加一组健康阈值断言（通过率区间、p95 延迟阈值）
//
// 用法（示例见文末）：node scripts/sim.e2e.mjs --mode smoke
/* eslint-disable no-console */
import { createClient } from 'redis';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import { create } from 'xmlbuilder2';

const argv = yargs(hideBin(process.argv))
  .option('mode', {
    type: 'string',
    default: process.env.MODE || 'smoke',
    choices: ['smoke', 'load', 'health'],
  })
  .option('prom', {
    type: 'string',
    default: process.env.PROM_URL || 'http://localhost:9090',
  })
  .option('redis', {
    type: 'string',
    default: process.env.REDIS_URL || 'redis://127.0.0.1:6379',
  })
  .option('syms', {
    type: 'string',
    default: process.env.SYMS || 'BTC-USDT-SWAP,ETH-USDT-SWAP',
  })
  .option('dirs', { type: 'string', default: process.env.DIRS || 'buy,sell' })
  .option('srcs', { type: 'string', default: process.env.SRCS || 'manual' })
  .option('scrapeWaitMs', {
    type: 'number',
    default: Number(process.env.SCRAPE_WAIT_MS || 7000),
  })
  .option('loadRate', {
    type: 'number',
    default: Number(process.env.LOAD_RATE || 4),
    desc: '每秒注入 detected 条数（load 模式）',
  })
  .option('loadSeconds', {
    type: 'number',
    default: Number(process.env.LOAD_SECONDS || 30),
  })
  .option('latencyP95MaxMs', {
    type: 'number',
    default: Number(process.env.LAT_P95_MAX_MS || 4000),
  })
  .option('passRatioMin', {
    type: 'number',
    default: Number(process.env.PASS_RATIO_MIN || 0.15),
  })
  .option('passRatioMax', {
    type: 'number',
    default: Number(process.env.PASS_RATIO_MAX || 0.95),
  })
  .option('junit', { type: 'string', default: process.env.JUNIT_XML || '' })
  .help().argv;

const PROM = argv.prom;
const REDIS_URL = argv.redis;
const SYMS = argv.syms
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);
const DIRS = argv.dirs
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);
const SRCS = argv.srcs
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);

const MET = {
  detected: 'quant_signals_detected_total',
  final: 'quant_signals_final_total',
  dropped: 'quant_router_dropped_total',
  latencyBucket: 'quant_router_latency_ms_bucket',
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function promQuery(q) {
  const url = `${PROM}/api/v1/query?query=${encodeURIComponent(q)}`;
  const res = await fetch(url);
  const j = await res.json();
  if (j.status !== 'success') throw new Error(`Prom query failed: ${q}`);
  return j.data.result;
}

function labels(obj = {}) {
  return (
    '{' +
    Object.entries(obj)
      .filter(([, v]) => v !== undefined && v !== null && v !== '')
      .map(([k, v]) => `${k}="${String(v)}"`)
      .join(',') +
    '}'
  );
}

async function getCounters(filter = {}) {
  const detQ = `${MET.detected}${labels(filter)}`;
  const finQ = `${MET.final}${labels(filter)}`;
  const dropStrengthQ = `${MET.dropped}${labels({ ...filter, reason: 'strength' })}`;
  const dropCooldownQ = `${MET.dropped}${labels({ ...filter, reason: 'cooldown' })}`;

  const [det, fin, ds, dc] = await Promise.all([
    promQuery(detQ).catch(() => []),
    promQuery(finQ).catch(() => []),
    promQuery(dropStrengthQ).catch(() => []),
    promQuery(dropCooldownQ).catch(() => []),
  ]);

  const get1 = (arr) =>
    Number((arr && arr[0] && arr[0].value && arr[0].value[1]) || 0);
  return {
    detected: get1(det),
    final: get1(fin),
    dropStrength: get1(ds),
    dropCooldown: get1(dc),
  };
}

async function waitForCounterBump({
  base,
  filter,
  expectKeys = ['detected', 'final', 'dropStrength', 'dropCooldown'],
  timeoutMs = argv.scrapeWaitMs,
  pollMs = 800,
}) {
  const t0 = Date.now();
  while (Date.now() - t0 < timeoutMs) {
    const cur = await getCounters(filter);
    const d = delta(cur, base);
    if (expectKeys.some((k) => (d[k] || 0) > 0))
      return { after: cur, delta: d };
    await sleep(pollMs);
  }
  const after = await getCounters(filter);
  return { after, delta: delta(after, base) };
}

async function getLatencyP(quantile = 0.95, range = '5m', extraFilter = {}) {
  // histogram_quantile(0.95, sum by (le) (rate(quant_router_latency_ms_bucket[5m])))
  const inner = `sum by (le) (rate(${MET.latencyBucket}${labels(extraFilter)}[${range}]))`;
  const q = `histogram_quantile(${quantile}, ${inner})`;
  const r = await promQuery(q);
  return Number(r[0]?.value?.[1] || NaN);
}

function buildDetectedKey(sym) {
  return `signal:detected:{${sym}}`;
}

async function getDynGate(redis, sym) {
  const h = await redis.hGetAll(`dyn:gate:{${sym}}`);
  return {
    effMin0: Number(h?.effMin0 ?? '0.65'),
    cooldownMs: Number(h?.cooldownMs ?? '6000'),
  };
}

async function xadd(redis, key, fields, maxlen = 5000) {
  const arr = [];
  for (const [k, v] of Object.entries(fields)) {
    arr.push(k, String(v));
  }
  await redis.xAdd(key, '*', arr, {
    TRIM: { strategy: 'MAXLEN', strategyModifier: '~', threshold: maxlen },
  });
}

/** ---- 基础注入：1条会通过，1条 strength drop，1条 cooldown drop ---- */
async function injectSmoke(redis, { sym, dir, src, effMin0, cooldownMs }) {
  const now = Date.now();
  const key = buildDetectedKey(sym);

  // 1) 会通过：强度足够，间隔足够
  await xadd(redis, key, {
    ts: String(now - cooldownMs - 1000),
    instId: sym,
    dir,
    strength: String(Math.max(effMin0 + 0.1, 0.8)),
    'evidence.src': src,
    'evidence.notional3s': '5000000',
  });

  // 2) strength drop：低于门槛
  await xadd(redis, key, {
    ts: String(now - 500),
    instId: sym,
    dir,
    strength: String(Math.max(0.1, effMin0 - 0.1)),
    'evidence.src': src,
    'evidence.notional3s': '10000',
  });

  // 3) cooldown drop：与 #1 同方向 & 在冷却内
  await xadd(redis, key, {
    ts: String(now - 200),
    instId: sym,
    dir,
    strength: String(Math.max(effMin0 + 0.05, 0.75)),
    'evidence.src': src,
    'evidence.notional3s': '4000000',
  });
}

/** ---- 压测注入：loadSeconds * loadRate 条 ---- */
async function injectLoad(
  redis,
  { sym, dir, src, effMin0, cooldownMs, rate, seconds },
) {
  const key = buildDetectedKey(sym);
  const endAt = Date.now() + seconds * 1000;
  const intervalMs = Math.max(10, Math.floor(1000 / Math.max(1, rate)));
  while (Date.now() < endAt) {
    const ts = Date.now();
    // 交替构造强度（多数略高于门槛，少数略低）以制造合理 drop
    const strong = Math.random() < 0.75;
    const st = strong ? effMin0 + 0.08 + Math.random() * 0.05 : effMin0 - 0.05;
    await xadd(redis, key, {
      ts: String(ts),
      instId: sym,
      dir,
      strength: String(st),
      'evidence.src': src,
      'evidence.notional3s': strong ? '3000000' : '50000',
    });
    await sleep(intervalMs);
  }
}

function delta(after, base) {
  const r = {};
  for (const k of Object.keys(after))
    r[k] = Number(after[k] || 0) - Number(base[k] || 0);
  return r;
}

function addCase(resultBag, name, ok, detail) {
  resultBag.push({ name, ok, detail });
}

function printSummary(cases) {
  const okN = cases.filter((c) => c.ok).length;
  const failN = cases.length - okN;
  console.log(
    `\n=== ASSERT SUMMARY: ${okN}/${cases.length} passed${failN ? `, ${failN} failed` : ''} ===`,
  );
  for (const c of cases) {
    console.log(
      `${c.ok ? '✅' : '❌'} ${c.name}${c.detail ? ` — ${c.detail}` : ''}`,
    );
  }
}

function toJUnitXml(suiteName, cases) {
  const root = create({ version: '1.0' }).ele('testsuite', {
    name: suiteName,
    tests: String(cases.length),
    failures: String(cases.filter((c) => !c.ok).length),
  });
  for (const c of cases) {
    const tc = root.ele('testcase', { name: c.name });
    if (!c.ok) tc.ele('failure').txt(c.detail || 'failed');
  }
  return root.end({ prettyPrint: true });
}

async function main() {
  console.log('== Sim start ==');
  console.log({
    MODE: argv.mode,
    PROM,
    REDIS_URL,
    SYMS,
    DIRS,
    SRCS,
  });

  const redis = createClient({ url: REDIS_URL });
  await redis.connect();

  const cases = [];

  for (const sym of SYMS) {
    const dyn = await getDynGate(redis, sym);
    console.log(
      `[dyn] ${sym} effMin0=${dyn.effMin0} cooldownMs=${dyn.cooldownMs}`,
    );

    for (const dir of DIRS) {
      for (const src of SRCS) {
        // 1) 读取基线
        const base = await getCounters({ sym, dir, src });
        console.log(`[base] ${sym} ${dir} ${src}`, base);

        // 2) 注入
        if (argv.mode === 'load') {
          await injectLoad(redis, {
            sym,
            dir,
            src,
            effMin0: dyn.effMin0,
            cooldownMs: dyn.cooldownMs,
            rate: argv.loadRate,
            seconds: argv.loadSeconds,
          });
        } else {
          await injectSmoke(redis, {
            sym,
            dir,
            src,
            effMin0: dyn.effMin0,
            cooldownMs: dyn.cooldownMs,
          });
        }

        // 3) 等 Prom 抓取
        console.log('signals pushed. waiting for Prometheus scrape…');
        const { after, delta: d } = await waitForCounterBump({
          base,
          filter: { sym, dir, src },
        });
        console.log(`[after] ${sym} ${dir} ${src}`, after);
        console.log(`[delta] ${sym} ${dir} ${src}`, d);

        // 5) 断言（各模式）
        if (argv.mode === 'smoke' || argv.mode === 'health') {
          addCase(
            cases,
            `${sym}/${dir}/${src} detected >=3`,
            d.detected >= 3,
            `got ${d.detected}`,
          );
          addCase(
            cases,
            `${sym}/${dir}/${src} final >=1`,
            d.final >= 1,
            `got ${d.final}`,
          );
          addCase(
            cases,
            `${sym}/${dir}/${src} drop.strength >=1`,
            d.dropStrength >= 1,
            `got ${d.dropStrength}`,
          );
          addCase(
            cases,
            `${sym}/${dir}/${src} drop.cooldown >=1`,
            d.dropCooldown >= 1,
            `got ${d.dropCooldown}`,
          );
        }

        if (argv.mode === 'load') {
          // 预期 final rate 大致接近注入 rate 的 10%~90%（根据门槛/冷却，留出余量）
          // 这里不做严格 rate 计算，给出方向性健康校验即可
          addCase(
            cases,
            `${sym}/${dir}/${src} detected increase`,
            d.detected > 0,
            `got ${d.detected}`,
          );
          addCase(
            cases,
            `${sym}/${dir}/${src} final increase`,
            d.final > 0,
            `got ${d.final}`,
          );
        }

        if (argv.mode === 'health') {
          // 通过率区间
          const passRatio = d.detected > 0 ? d.final / d.detected : 0;
          addCase(
            cases,
            `${sym}/${dir}/${src} passRatio in [${argv.passRatioMin}, ${argv.passRatioMax}]`,
            passRatio >= argv.passRatioMin && passRatio <= argv.passRatioMax,
            `ratio=${passRatio.toFixed(3)}, detected=${d.detected}, final=${d.final}`,
          );

          // p95 延迟阈值（全局，不分 dir/src）
          const p95 = await getLatencyP(0.95, '5m', {}); // 也可以加 {sym}
          addCase(
            cases,
            `${sym} router p95 <= ${argv.latencyP95MaxMs}ms`,
            Number.isFinite(p95) ? p95 <= argv.latencyP95MaxMs : false,
            `p95=${p95}`,
          );
        }
      }
    }
  }

  printSummary(cases);

  // JUnit（可选）
  if (argv.junit) {
    const xml = toJUnitXml(`quant-sim-${argv.mode}`, cases);
    await BunOrNodeWriteFile(argv.junit, xml);
    console.log(`JUnit written: ${argv.junit}`);
  }

  await redis.quit();

  // 失败时退出非零，便于 CI
  if (cases.some((c) => !c.ok)) process.exit(2);
}

async function BunOrNodeWriteFile(path, content) {
  // 兼容 Bun/Node 环境
  if (typeof Bun !== 'undefined' && Bun.write) {
    await Bun.write(path, content);
    return;
  }
  const fs = await import('node:fs/promises');
  await fs.writeFile(path, content);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
