/* eslint-disable @typescript-eslint/no-unsafe-return */
// scripts/sim_assert.mjs
import axios from 'axios';
import Redis from 'ioredis';

// ---- env ----
const PROM = process.env.PROM_URL || 'http://localhost:9090';
const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
const SYM = process.env.SYM || 'BTC-USDT-SWAP';
const DIR = process.env.DIR || 'buy';
const SRC = process.env.SRC || 'manual';
const SCRAPE_WAIT_MS = Number(process.env.SCRAPE_WAIT_MS || '7000');

// ---- utils ----
function oneSample(resp) {
  const data = resp?.data?.data;
  if (data?.resultType !== 'vector') return 0;
  if (!data?.result?.length) return 0;
  const v = Number(data.result[0].value?.[1]);
  return Number.isFinite(v) ? v : 0;
}

async function qInstant(expr) {
  return axios.get(`${PROM}/api/v1/query`, { params: { query: expr } });
}

async function getDynGate(redis) {
  const h = await redis.hgetall(`dyn:gate:{${SYM}}`);
  const effMin0 = Number(h?.effMin0 ?? '0.65');
  const cooldownMs = Number(h?.cooldownMs ?? '6000');
  return { effMin0, cooldownMs };
}

async function getCounters() {
  // detected/final 仍按 src 过滤（这些在服务里都有 src=flow/delta/manual）
  const detected = await qInstant(
    `quant_signals_detected_total{sym="${SYM}",dir="${DIR}",src="${SRC}"}`,
  );
  const final = await qInstant(
    `quant_signals_final_total{sym="${SYM}",dir="${DIR}",src="${SRC}"}`,
  );

  // drop 不按 src 过滤：不同地方打点可能是 unknown，这里用 sum by 聚合
  const dropStr = await qInstant(
    `sum by (sym,dir,reason) (quant_router_dropped_total{sym="${SYM}",dir="${DIR}",reason="strength"})`,
  );
  const dropCd = await qInstant(
    `sum by (sym,dir,reason) (quant_router_dropped_total{sym="${SYM}",dir="${DIR}",reason="cooldown"})`,
  );

  return {
    detected: oneSample(detected),
    final: oneSample(final),
    dropStrength: oneSample(dropStr),
    dropCooldown: oneSample(dropCd),
  };
}

async function xadd(redis, key, fields) {
  const flat = [];
  for (const [k, v] of Object.entries(fields)) flat.push(k, String(v));
  return redis.xadd(key, '*', ...flat);
}

async function main() {
  console.log('== Sim start ==');
  console.log({
    PROM,
    REDIS_URL,
    SYM,
    DIR,
    SRC,
  });

  const redis = new Redis(REDIS_URL);

  // 读取门槛，便于构造边界用例
  const dyn = await getDynGate(redis);
  console.log(`[dyn] effMin0=${dyn.effMin0} cooldownMs=${dyn.cooldownMs}`);

  // 基线
  const base = await getCounters();
  console.log('[base]', base);

  // 构造三条 detected：
  //  1) 强度高于门槛 -> 通过
  //  2) 强度低于门槛 -> strength drop
  //  3) 再来一条高于门槛，但在冷却期内 -> cooldown drop
  const now = Date.now();
  const key = `signal:detected:{${SYM}}`;
  const strong = Math.max(dyn.effMin0 + 0.1, dyn.effMin0 + 0.05);
  const weak = Math.max(dyn.effMin0 - 0.2, 0.1);

  // 1) 通过
  await xadd(redis, key, {
    ts: String(now - 1500),
    instId: SYM,
    dir: DIR,
    strength: String(strong.toFixed(3)),
    'evidence.src': SRC,
    'evidence.notional3s': '6000000',
  });

  // 2) 强度不够（必掉）
  await xadd(redis, key, {
    ts: String(now - 1200),
    instId: SYM,
    dir: DIR,
    strength: String(weak.toFixed(3)),
    'evidence.src': SRC,
    'evidence.notional3s': '1000000',
  });

  // 3) 冷却内（必掉）
  await xadd(redis, key, {
    ts: String(now - 800), // 与第一条间隔 < cooldownMs
    instId: SYM,
    dir: DIR,
    strength: String(strong.toFixed(3)),
    'evidence.src': SRC,
    'evidence.notional3s': '7000000',
  });

  console.log('signals pushed. waiting for Prometheus scrape…');
  await new Promise((r) => setTimeout(r, SCRAPE_WAIT_MS));

  const after = await getCounters();
  console.log('[after]', after);

  const delta = {
    detected: after.detected - base.detected,
    final: after.final - base.final,
    dropStrength: after.dropStrength - base.dropStrength,
    dropCooldown: after.dropCooldown - base.dropCooldown,
  };
  console.log('[delta]', delta);

  let ok = true;
  if (!(delta.detected >= 3)) {
    ok = false;
    console.log(
      `ASSERT FAILED:\n - detected delta expected >=3, got ${delta.detected}`,
    );
  }
  if (!(delta.final >= 1)) {
    ok = false;
    console.log(` - final delta expected >=1, got ${delta.final}`);
  }
  if (!(delta.dropStrength >= 1)) {
    ok = false;
    console.log(
      ` - dropStrength delta expected >=1, got ${delta.dropStrength}`,
    );
  }
  if (!(delta.dropCooldown >= 1)) {
    ok = false;
    console.log(
      ` - dropCooldown delta expected >=1, got ${delta.dropCooldown}`,
    );
  }

  if (ok) console.log('✅ ASSERT PASS');
  process.exit(ok ? 0 : 1);
}

main().catch((e) => {
  console.error(e);
  process.exit(2);
});
