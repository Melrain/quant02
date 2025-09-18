// src/params/param.refresher.ts
import {
  Injectable,
  Logger,
  OnModuleInit,
  OnModuleDestroy,
} from '@nestjs/common';
import { RedisStreamsService } from 'src/redis-streams/redis-streams.service';
import { RedisClient } from 'src/redis/redis.client';
import { confOf } from 'src/window/symbol.config';
import { DynamicSug, EffectiveWrite } from './param.types';

function clamp(n: number, lo: number, hi: number) {
  return Math.max(lo, Math.min(hi, n));
}
function ema(prev: number, cur: number, alpha: number) {
  return alpha * cur + (1 - alpha) * prev;
}
const toNum = (x: any, d = 0) => (Number.isFinite(Number(x)) ? Number(x) : d);

@Injectable()
export class ParamRefresher implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(ParamRefresher.name);
  private running = false;

  /** 你已在 window-worker 里解析过 ENV；这里也可直接复用同一方法 */
  private parseSymbolsFromEnv(): string[] {
    const raw = process.env.SYMBOLS || 'BTC-USDT-SWAP,ETH-USDT-SWAP';
    return raw
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean);
  }
  private readonly symbols = this.parseSymbolsFromEnv();

  // 刷新节奏（你可按需调整）
  private readonly T_MIN_NOTIONAL = 20_000; // 20s
  private readonly T_COOLDOWN = 45_000; // 45s
  private readonly T_BAND = 60_000; // 1m
  private readonly T_K = 8_000; // 8s

  // 上次刷新的时间
  private lastMinNotional: Record<string, number> = {};
  private lastCooldown: Record<string, number> = {};
  private lastBand: Record<string, number> = {};
  private lastK: Record<string, number> = {};

  // 平滑缓存
  private emaStore = new Map<string, Record<string, number>>();

  constructor(
    private readonly stream: RedisStreamsService,
    private readonly redis: RedisClient,
  ) {}

  async onModuleInit() {
    this.running = true;
    void this.loop();
    this.logger.log(`ParamRefresher started for ${this.symbols.join(', ')}`);
  }
  async onModuleDestroy() {
    this.running = false;
  }

  private keyDynamic(sym: string) {
    return `qt:param:dynamic:${sym}`;
  }
  private keyEffective(sym: string) {
    return `qt:param:effective:${sym}`;
  }
  private keyOverride(sym: string) {
    return `qt:param:override:${sym}`;
  } // 手动覆盖（可选）
  private keyUseDynamic(sym: string) {
    return `qt:param:useDynamic:${sym}`;
  } // '1'|'0'

  private getSnapKey(sym: string, name: string) {
    // 例：你在 window-worker 里写的是 buildOutKey(sym, 'win:state:1m')
    // 这里直接拼：如需完全一致，用 stream.buildOutKey(sym, ...)
    return this.stream.buildOutKey(sym, name);
  }

  private getEma(sym: string, field: string, alpha: number, val: number) {
    const m =
      this.emaStore.get(sym) ??
      (this.emaStore.set(sym, {}), this.emaStore.get(sym)!);
    const prev = m[field];
    const v = prev === undefined ? val : ema(prev, val, alpha);
    m[field] = v;
    return v;
  }

  private async computeMinNotional3s(sym: string) {
    // 方案A：从 win:state:1m 的 vol/vbuy/vsell + 你在 signal 里写过 notional3s（可做近似）
    // 方案B（这里实现）：扫 trades 流最近若干条，做 3s 滑窗的分位（P80）。
    const keyTrades = this.stream.buildKey(sym, 'trades');
    // 取最近 ~600 条（约几十秒，按你的流量来）——注意 bounded
    const recent = await this.stream.xrevrange(keyTrades, '+', '-', 600);
    const points: Array<{ ts: number; buy: number; sell: number }> = [];
    for (const [, payload] of recent) {
      const p: any = payload;
      const ts = toNum(p.ts);
      const px = toNum(p.px ?? p.price);
      const qty = toNum(p.qty ?? p.size);
      const side = (p.side ?? p.dir ?? '').toString().toLowerCase();
      if (!ts || !px || !qty) continue;
      const notional = px * qty * (confOf(sym).contractMultiplier ?? 1); // 这里先用静态乘数
      points.push({
        ts,
        buy: side.startsWith('b') ? notional : 0,
        sell: side.startsWith('s') ? notional : 0,
      });
    }
    points.sort((a, b) => a.ts - b.ts);
    // 3s 滑窗聚合
    const win: Array<{ sum: number }> = [];
    let i = 0;
    for (let j = 0; j < points.length; j++) {
      const tEnd = points[j].ts;
      while (i < j && tEnd - points[i].ts > 3000) i++;
      let buy = 0,
        sell = 0;
      for (let k = i; k <= j; k++) {
        buy += points[k].buy;
        sell += points[k].sell;
      }
      win.push({ sum: buy + sell });
    }
    if (!win.length) return undefined;
    const sums = win.map((w) => w.sum).sort((a, b) => a - b);
    const p80 = sums[Math.floor(0.8 * (sums.length - 1))];
    // EMA 平滑 + clamp 到每品种安全线
    const hardFloor = /BTC/.test(sym)
      ? 1_000_000
      : /ETH/.test(sym)
        ? 600_000
        : 120_000; // 举例
    const emaVal = this.getEma(sym, 'minNotional3s', 0.2, p80);
    return Math.max(hardFloor, emaVal);
  }

  private async computeCooldownDedup(sym: string) {
    // 用 ATR(1m)/price 与当前 spread_bps 映射到 [3s,20s]
    const kState = this.getSnapKey(sym, 'win:state:1m');
    const h = await this.redis.hgetall(kState);
    const atr = toNum(h['atr14'], 0); // 如果你没写 atr14，可以用 std 或 TrueRange 近似
    const last = toNum(h['last'], toNum(h['close'], 0));
    const bookKey = this.stream.buildOutKey(sym, 'book:state'); // 如果你有书写盘口状态
    const bh = await this.redis.hgetall(bookKey);
    const spreadBp = toNum(bh['spreadBp'], 1.5);

    // 简单映射：低波+大点差 ⇒ 拉长；高波+小点差 ⇒ 缩短
    const vol = last > 0 ? atr / last : 0.001;
    const base = 6000; // ms
    const volAdj = clamp(0.002 / Math.max(0.0005, vol), 0.3, 2.5); // 波动小 → 放大
    const spdAdj = clamp(spreadBp / 2.0, 0.6, 2.0); // 点差大 → 放大
    const cooldown = clamp(base * volAdj * spdAdj, 3000, 20000);
    const dedup = clamp(cooldown * 0.35, 600, 5000);
    return { cooldownMs: Math.round(cooldown), dedupMs: Math.round(dedup) };
  }

  private async computeBreakoutBand(sym: string) {
    // band = k * ATR(1m)/price，k ~ 1.0，夹到 [0.05%, 0.40%]
    const h = await this.redis.hgetall(this.getSnapKey(sym, 'win:state:1m'));
    const atr = toNum(h['atr14'], 0);
    const last = toNum(h['last'], toNum(h['close'], 0));
    if (!last) return undefined;
    const k = 1.0;
    const pct = clamp(k * (atr / last), 0.0005, 0.004);
    // 轻微平滑
    const v = this.getEma(sym, 'breakoutBandPct', 0.2, pct);
    return v;
  }

  private async computeDynDeltaK(sym: string) {
    // OI / funding / basis / orderbook imbalance 合成，夹到 [0.8, 1.5]
    // 下面的 key 你可以换成你现在写入的快照键
    const oi = await this.redis.hgetall(
      this.stream.buildOutKey(sym, 'oi:state'),
    );
    const fund = await this.redis.hgetall(
      this.stream.buildOutKey(sym, 'funding:state'),
    );
    const basis = await this.redis.hgetall(
      this.stream.buildOutKey(sym, 'basis:state'),
    );
    const book = await this.redis.hgetall(
      this.stream.buildOutKey(sym, 'book:state'),
    );

    const oiRoc5m = toNum(oi['roc5m'], 0); // 你可以在桥里顺手写
    const fundDelta1h = toNum(fund['delta1h'], 0);
    const basisRank = toNum(basis['rank7d'], 0.5);
    const imb = toNum(book['imb5'], 0.5);

    // 非方向化合成：把“更难做”的情形抬高 K
    let k = 1.0;
    k += oiRoc5m < 0 ? 0.1 : -0.05;
    k += fundDelta1h > 0 ? 0.05 : -0.05; // 假设资金费朝多端变贵 → 做多更难
    k += basisRank > 0.6 ? 0.05 : basisRank < 0.4 ? -0.05 : 0;
    k += Math.abs(imb - 0.5) > 0.05 ? -0.05 : 0.05;
    return clamp(k, 0.8, 1.5);
  }

  private async computeLiqK(sym: string) {
    // 成交活跃度/深度 → 0.6~1.4
    const book = await this.redis.hgetall(
      this.stream.buildOutKey(sym, 'book:state'),
    );
    const depthNotional = toNum(book['topNNotional'], 0); // 若缺少可用 5 档累计
    const spreadBp = toNum(book['spreadBp'], 1.5);
    if (!depthNotional) return 1.0;
    let k = 1.0;
    k *= clamp(
      1.0 + (1_000_000 / Math.max(100_000, depthNotional)) * 0.2,
      0.7,
      1.3,
    );
    k *= clamp(1.0 + (spreadBp - 1.0) * 0.1, 0.8, 1.2);
    return clamp(k, 0.6, 1.4);
  }

  private async writeDynamic(sym: string, sug: DynamicSug) {
    await this.redis.hset(this.keyDynamic(sym), {
      ts: String(sug.ts),
      stale: String(sug.stale ?? 0),
      ...(sug.minNotional3s_sug !== undefined
        ? { minNotional3s_sug: String(Math.round(sug.minNotional3s_sug)) }
        : {}),
      ...(sug.cooldownMs_sug !== undefined
        ? { cooldownMs_sug: String(Math.round(sug.cooldownMs_sug)) }
        : {}),
      ...(sug.dedupMs_sug !== undefined
        ? { dedupMs_sug: String(Math.round(sug.dedupMs_sug)) }
        : {}),
      ...(sug.breakoutBandPct_sug !== undefined
        ? { breakoutBandPct_sug: String(sug.breakoutBandPct_sug) }
        : {}),
      ...(sug.dynDeltaK_sug !== undefined
        ? { dynDeltaK_sug: String(sug.dynDeltaK_sug) }
        : {}),
      ...(sug.liqK_sug !== undefined ? { liqK_sug: String(sug.liqK_sug) } : {}),
    });
    await this.redis.expire(this.keyDynamic(sym), 120); // 2 分钟失效
  }

  private async mergeEffective(sym: string) {
    const base = confOf(sym);
    const dyn = await this.redis.hgetall(this.keyDynamic(sym));
    const ov = await this.redis.hgetall(this.keyOverride(sym)); // 可选的人工覆盖
    const useDyn = await this.redis.get(this.keyUseDynamic(sym));

    const now = Date.now();
    let source: EffectiveWrite['source'] = 'static';
    let clamped: '1' | '0' = '0';

    // 取值：静态 → 动态建议（如果 useDynamic=1 才用）→ 覆盖
    const eff: EffectiveWrite = {
      contractMultiplier: base.contractMultiplier ?? 1,
      minNotional3s: base.minNotional3s ?? 0,
      cooldownMs: base.cooldownMs ?? 3000,
      dedupMs: base.dedupMs ?? 1000,
      minStrength: base.minStrength ?? 0.55,
      consensusBoost: base.consensusBoost ?? 0.1,
      breakoutBandPct: base.breakoutBandPct ?? 0.001,
      dynDeltaK: base.dynDeltaK ?? 1.0,
      liqK: base.liqK ?? 1.0,
      source: 'static',
      ts: now,
      useDynamic: useDyn === '1' ? '1' : '0',
    };

    // 动态建议
    if (useDyn === '1' && dyn && Object.keys(dyn).length) {
      source = 'dynamic';
      eff.minNotional3s = toNum(dyn.minNotional3s_sug, eff.minNotional3s);
      eff.cooldownMs = toNum(dyn.cooldownMs_sug, eff.cooldownMs);
      eff.dedupMs = toNum(dyn.dedupMs_sug, eff.dedupMs);
      eff.breakoutBandPct = toNum(dyn.breakoutBandPct_sug, eff.breakoutBandPct);
      eff.dynDeltaK = toNum(dyn.dynDeltaK_sug, eff.dynDeltaK);
      eff.liqK = toNum(dyn.liqK_sug, eff.liqK);

      // 上下限 clamp（安全护栏）
      const before = { ...eff };
      eff.cooldownMs = clamp(eff.cooldownMs, 3000, 20000);
      eff.dedupMs = clamp(eff.dedupMs, 300, 5000);
      eff.breakoutBandPct = clamp(eff.breakoutBandPct, 0.0005, 0.004);
      eff.dynDeltaK = clamp(eff.dynDeltaK, 0.8, 1.5);

      if (
        before.cooldownMs !== eff.cooldownMs ||
        before.dedupMs !== eff.dedupMs ||
        before.breakoutBandPct !== eff.breakoutBandPct ||
        before.dynDeltaK !== eff.dynDeltaK
      )
        clamped = '1';
    }

    // 人工覆盖（优先级最高）
    if (ov && Object.keys(ov).length) {
      source = 'override';
      for (const [k, v] of Object.entries(ov)) {
        if (k === 'ts') continue;
        (eff as any)[k] = toNum(v, (eff as any)[k]);
      }
    }

    eff.source = source;

    await this.redis.hset(this.keyEffective(sym), {
      ...Object.fromEntries(
        Object.entries(eff).map(([k, v]) => [k, String(v)]),
      ),
      clamped,
      ts: String(now),
    });
  }

  private async loop() {
    while (this.running) {
      const now = Date.now();

      await Promise.all(
        this.symbols.map(async (sym) => {
          // 新鲜度：简单地认为 win:state:1m 存在且 ts 接近 now 即 fresh
          const h1m = await this.redis.hgetall(
            this.getSnapKey(sym, 'win:state:1m'),
          );
          const ts1m = toNum(h1m['ts'], 0);
          const fresh = ts1m && now - ts1m < 30_000;

          // 1) minNotional3s（20s 刷一次）
          if (
            fresh &&
            (!this.lastMinNotional[sym] ||
              now - this.lastMinNotional[sym] >= this.T_MIN_NOTIONAL)
          ) {
            const v = await this.computeMinNotional3s(sym);
            if (v !== undefined) {
              await this.writeDynamic(sym, {
                ts: now,
                stale: 0,
                minNotional3s_sug: v,
              });
              this.lastMinNotional[sym] = now;
            }
          }

          // 2) cooldown/dedup（45s）
          if (
            fresh &&
            (!this.lastCooldown[sym] ||
              now - this.lastCooldown[sym] >= this.T_COOLDOWN)
          ) {
            const { cooldownMs, dedupMs } =
              await this.computeCooldownDedup(sym);
            await this.writeDynamic(sym, {
              ts: now,
              stale: 0,
              cooldownMs_sug: cooldownMs,
              dedupMs_sug: dedupMs,
            });
            this.lastCooldown[sym] = now;
          }

          // 3) breakoutBandPct（1m）
          if (
            fresh &&
            (!this.lastBand[sym] || now - this.lastBand[sym] >= this.T_BAND)
          ) {
            const pct = await this.computeBreakoutBand(sym);
            if (pct !== undefined) {
              await this.writeDynamic(sym, {
                ts: now,
                stale: 0,
                breakoutBandPct_sug: pct,
              });
              this.lastBand[sym] = now;
            }
          }

          // 4) dynDeltaK / liqK（8s）
          if (
            fresh &&
            (!this.lastK[sym] || now - this.lastK[sym] >= this.T_K)
          ) {
            const [k, lk] = await Promise.all([
              this.computeDynDeltaK(sym),
              this.computeLiqK(sym),
            ]);
            await this.writeDynamic(sym, {
              ts: now,
              stale: 0,
              dynDeltaK_sug: k,
              liqK_sug: lk,
            });
            this.lastK[sym] = now;
          }

          // 合成 effective（每轮都做一次即可）
          await this.mergeEffective(sym);
        }),
      );

      await new Promise((r) => setTimeout(r, 500)); // 小憩半秒
    }
  }
}
