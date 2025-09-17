// src/example.ts

import { RedisClient } from './redis.client';

async function main() {
  const r = new RedisClient({
    url: 'redis://localhost:6379',
    keyPrefix: 'dev:',
    defaultTTL: 3600,
  });

  // 基础 CRUD
  await r.set('greet', 'hello', 10);
  console.log(await r.get('greet')); // 'hello'
  await r.hset('user:42', { name: 'Ada', age: 30 });
  console.log(await r.hgetall('user:42')); // { name: 'Ada', age: '30' }

  // JSON 便捷
  await r.setJSON('cfg:okx', { instId: 'BTC-USDT-SWAP', tf: '5m' });

  // ZSET 做时间序列索引
  const now = Date.now();
  await r.zadd('ohlcv:BTC', now, JSON.stringify({ t: now, c: 68000 }));
  console.log(await r.zrangeByScore('ohlcv:BTC', now - 60000, now));

  // 分布式锁
  const token = await r.lockWithRetry('lock:signal:BTC', 5000, 1000);
  if (token) {
    try {
      // 关键区：防重复下单 / 单资源串行
    } finally {
      await r.unlock('lock:signal:BTC', token);
    }
  }

  await r.quit();
}
void main();
