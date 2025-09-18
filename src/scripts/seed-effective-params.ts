// src/scripts/seed-effective-params.ts
import 'dotenv/config';
import { confOf } from '../window/symbol.config';
import Redis from 'ioredis';

// 你可以改成从 args 或 env 读取
function toInstId(token: string): string {
  const t = token.trim();
  if (!t) return '';
  const u = t.toUpperCase();
  return u.includes('-') ? u : `${u}-USDT-SWAP`;
}

function parseSymbolsFromEnv(): string[] {
  const raw =
    process.env.SYMBOLS ?? // 显式列表优先
    process.env.OKX_ASSETS ?? // 短写，如 btc,eth,doge
    process.env.OKX_SYMBOLS ?? // 可混用
    'btc,eth';
  const list = raw
    .split(',')
    .map((s) => toInstId(s))
    .filter(Boolean);
  return Array.from(new Set(list));
}

const SYMS = parseSymbolsFromEnv();

function toHash(conf: ReturnType<typeof confOf>) {
  return {
    contractMultiplier: String(conf.contractMultiplier ?? 1),
    minNotional3s: String(conf.minNotional3s ?? 0),
    cooldownMs: String(conf.cooldownMs ?? 3000),
    dedupMs: String(conf.dedupMs ?? 1000),
    minStrength: String(conf.minStrength ?? 0.55),
    consensusBoost: String(conf.consensusBoost ?? 0.1),
    breakoutBandPct: String(conf.breakoutBandPct ?? 0.001),
    dynDeltaK: String(conf.dynDeltaK ?? 1.0),
    liqK: String(conf.liqK ?? 1.0),
    source: 'static',
    ts: String(Date.now()),
  };
}

async function main() {
  const redisUrl = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
  console.log(`[seed] redis=${redisUrl} symbols=${SYMS.join(',')}`);
  const redis = new Redis(redisUrl);
  await redis.connect?.();
  for (const sym of SYMS) {
    const conf = confOf(sym);
    const key = `qt:param:effective:${sym}`;
    const hash = toHash(conf);
    await redis.hset(key, hash as any);
    console.log(`[seed] ${sym} -> ${key}`, hash);
  }
  await redis.quit?.();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
