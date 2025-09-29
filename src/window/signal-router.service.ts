/* eslint-disable @typescript-eslint/no-unused-vars */
import {
  Injectable,
  Logger,
  OnModuleInit,
  OnModuleDestroy,
} from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { MetricsService } from 'src/metrics/metrics.service';
import { RedisStreamsService } from 'src/redis-streams/redis-streams.service';
import { RedisClient } from 'src/redis/redis.client';
import { parseSymbolsFromEnv } from 'src/utils/utils';

type DetectedRow = Record<string, string>;

@Injectable()
export class SignalRouterService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(SignalRouterService.name);
  private running = false;

  private readonly symbols = parseSymbolsFromEnv();

  // ==== P0 配置（可走 .env）====
  private readonly MIN_SPACING_MS = Number(
    process.env.SIGNAL_MIN_SPACING_MS ?? '10000',
  ); // 10s
  private readonly HYST_HI = Number(process.env.SIGNAL_HYST_HI ?? '0.75'); // 切换方向所需更高阈值
  private readonly HYST_LO = Number(process.env.SIGNAL_HYST_LO ?? '0.55'); // 同向维持较低阈值

  // 幂等锁配置
  private readonly IDEM_BUCKET_MS = Number(
    process.env.SIGNAL_IDEM_BUCKET_MS ?? '8000',
  ); // 指纹时间桶
  private readonly IDEM_TTL_MS = Number(
    process.env.SIGNAL_IDEM_TTL_MS ?? '10000',
  ); // 锁 TTL（略大于 bucket）

  // 最近一次“最终发车”的方向（按标的）
  private lastEmitDir = new Map<string, 'buy' | 'sell'>(); // key = sym
  // 运行态：按 sym|dir 做冷却/去重（最终层面的）
  private lastEmitTs = new Map<string, number>(); // key=sym|dir
  private lastEmitKey = new Map<string, string>(); // key=sym|dir -> evidence.approx_key

  // 1s 级本地缓存 dyn gate
  private dynCache = new Map<
    string,
    { ts: number; effMin0: number; cooldownMs: number }
  >();

  // 环境/静态参数（可 .env 配置）
  private readonly ENABLED =
    (process.env.SIGNALS_ENABLED ?? 'true').toLowerCase() !== 'false';
  private readonly MIN_STRENGTH_FLOOR = Number(
    process.env.SIGNAL_MIN_STRENGTH_FLOOR ?? '0.6',
  ); // 最低兜底门槛
  private readonly EXTRA_COOLDOWN_MS = Number(
    process.env.SIGNAL_EXTRA_COOLDOWN_MS ?? '0',
  ); // 在 dyn.cooldownMs 基础上附加的“发布层”冷却

  constructor(
    private readonly redis: RedisStreamsService,
    private readonly bus: EventEmitter2,
    private readonly metrics: MetricsService,
    private readonly redisClient: RedisClient, // ✅ 新增注入：幂等锁
  ) {}

  async onModuleInit() {
    // 建组：读取 detected 流（每个 symbol 一个）
    const keys = this.symbols.map((s) =>
      this.redis.buildOutKey(s, 'signal:detected'),
    );
    await this.redis.ensureGroups(keys, 'cg:signal-router', '$');

    this.running = true;
    void this.loop();
    this.logger.log(
      `SignalRouter started (enabled=${this.ENABLED}) for ${this.symbols.join(', ')}`,
    );
  }

  async onModuleDestroy() {
    this.running = false;
  }

  private async loop() {
    const consumer = `router#${process.pid}`;
    while (this.running) {
      try {
        const keys = this.symbols.map((s) =>
          this.redis.buildOutKey(s, 'signal:detected'),
        );
        const batch = await this.redis.readGroup({
          group: 'cg:signal-router',
          consumer,
          keys,
          count: 200,
          blockMs: 500,
        });
        if (!batch) continue;

        const msgs = this.redis.normalizeBatch(batch);
        const ackMap = new Map<string, string[]>();

        for (const m of msgs) {
          try {
            const sym = m.symbol; // normalizeBatch 对 outKey 也能识别 {sym}
            const h = m.payload as DetectedRow;

            // —— 结构化字段（容错为主）——
            const ts = Number(h.ts);
            const dir =
              h.dir === 'buy' || h.dir === 'sell'
                ? h.dir
                : ((h['evidence.dir'] as any) ?? '');
            const strength = Number(h.strength);
            const src = String(h['evidence.src'] ?? h.src ?? 'unknown');
            const approxKey = String(h['evidence.approx_key'] ?? '');

            // ① 一收到 detected 流就记一笔（来源统计）
            if (sym && (dir === 'buy' || dir === 'sell')) {
              this.metrics.incDetected(sym, dir, src);
            }

            // ——入口时延指标（detected.ts -> router now）——
            if (Number.isFinite(ts)) {
              const lat = Date.now() - ts;
              if (lat >= 0 && lat < 24 * 3600_000 && sym) {
                this.metrics.observeRouterLatency(sym, lat, dir || 'na', src);
              }
            }

            // ……字段缺失：直接丢弃……
            if (
              !sym ||
              !Number.isFinite(ts) ||
              !(dir === 'buy' || dir === 'sell')
            ) {
              this.metrics.incDrop(sym || 'na', 'na' as any, 'bad_row', src);
              this.safeAck(ackMap, m.key, m.id);
              continue;
            }

            // 全局开关
            if (!this.ENABLED) {
              this.metrics.incDrop(sym, dir, 'disabled', src || 'unknown');
              this.safeAck(ackMap, m.key, m.id);
              continue;
            }

            // 读取 dyn gate（1s 级本地缓存）
            const { effMin0, cooldownMs } = await this.getDyn(sym);

            // 最终门槛：不得低于 floor
            const finalMin = Math.max(this.MIN_STRENGTH_FLOOR, effMin0 || 0);
            // 也把最新参数刷进 Gauge，便于面板观察
            this.metrics.setDynGate(sym, finalMin, cooldownMs);

            // 强度闸门
            if (!Number.isFinite(strength) || strength < finalMin) {
              this.metrics.incDrop(sym, dir, 'strength', src);
              this.logger.debug(
                `[drop.strength] ${sym} ${dir} st=${Number.isFinite(strength) ? strength.toFixed(3) : 'NaN'} < min=${finalMin.toFixed(
                  2,
                )} src=${src}`,
              );
              this.safeAck(ackMap, m.key, m.id);
              continue;
            }

            // 冷却闸门（发布层）：dyn.cooldownMs + extra
            const stateKey = `${sym}|${dir}`;
            const lastTs = this.lastEmitTs.get(stateKey) || 0;
            const cool = (cooldownMs || 0) + this.EXTRA_COOLDOWN_MS;
            if (ts - lastTs < cool) {
              this.metrics.incDrop(sym, dir, 'cooldown', src);
              this.logger.debug(
                `[drop.cooldown] ${sym} ${dir} dt=${ts - lastTs} < cool=${cool} src=${src}`,
              );
              this.safeAck(ackMap, m.key, m.id);
              continue;
            }

            // 去重（同方向、同 approx_key、冷却期内）
            const lastKey = this.lastEmitKey.get(stateKey);
            if (
              lastKey &&
              approxKey &&
              lastKey === approxKey &&
              ts - lastTs < cool
            ) {
              this.metrics.incDrop(sym, dir, 'dedup', src);
              this.logger.debug(
                `[drop.dedup] ${sym} ${dir} approx_key=${approxKey}`,
              );
              this.safeAck(ackMap, m.key, m.id);
              continue;
            }

            // ===== P0-2: min-spacing（同标的+同方向的最小间隔） =====
            const spacingKey = `${sym}|${dir}`;
            const sinceLast = Date.now() - lastTs;
            if (sinceLast < this.MIN_SPACING_MS) {
              this.logger.debug(
                `[drop.min_spacing] ${sym} ${dir} sinceLast=${sinceLast}ms < ${this.MIN_SPACING_MS}ms`,
              );
              this.metrics?.incDrop?.(sym, dir, 'min_spacing', src);
              this.safeAck(ackMap, m.key, m.id);
              continue;
            }

            // ===== P0-3: hysteresis（方向切换更高阈值，同向较低阈值） =====
            const lastDir = this.lastEmitDir.get(sym);
            const needHi = lastDir && lastDir !== dir; // 切换方向
            const passHysteresis = needHi
              ? strength >= this.HYST_HI
              : strength >= this.HYST_LO;
            if (!passHysteresis) {
              this.logger.debug(
                `[drop.hysteresis] ${sym} lastDir=${lastDir ?? 'NA'} -> ${dir}, strength=${strength} ` +
                  `(need ${needHi ? '>=HYST_HI' : '>=HYST_LO'})`,
              );
              this.metrics?.incDrop?.(sym, dir, 'hysteresis', src);
              this.safeAck(ackMap, m.key, m.id);
              continue;
            }

            // ===== P0-4: 幂等锁（指纹锁，防止重连/重试/同一动机重复发车） =====
            const idemKey = this.buildIdemKey(sym, dir, src, ts);
            const token = await this.redisClient.tryLock(
              idemKey,
              this.IDEM_TTL_MS,
            );
            if (!token) {
              this.logger.debug(
                `[drop.idempotent_lock] ${sym} ${dir} src=${src} key=${idemKey}`,
              );
              this.metrics?.incDrop?.(sym, dir, 'idempotent_lock', src);
              this.safeAck(ackMap, m.key, m.id);
              continue;
            }
            // 注：一次性锁，无需 unlock，TTL 到期自动释放

            // ===== P0-1：写入前计算 refPx_*（以 refPx_ts 为锚） =====
            const { refPx, refPx_source, refPx_ts, refPx_stale } =
              await this.getRefPx(sym);

            // price lag 观测（可选，不存在方法也不会报错）
            if (refPx_ts && Number.isFinite(ts)) {
              const lag = Number(refPx_ts) - ts;
              if (lag >= 0 && lag < 24 * 3600_000) {
                this.metrics?.observePriceLag?.(sym, lag, dir, src);
                this.logger.debug(
                  `[router.price_lag] ${sym} ${dir} lag_ms=${lag} refSrc=${refPx_source}`,
                );
              }
            }

            // ===== 发布到 final 流 =====
            const outKey = this.redis.buildOutKey(sym, 'signal:final');
            await this.redis.xadd(
              outKey,
              {
                ts: String(ts),
                instId: sym,
                dir,
                strength: String(strength),

                // 参考价（P0-1）
                ...(refPx ? { refPx } : {}),
                ...(refPx_source ? { refPx_source } : {}),
                ...(refPx_ts ? { refPx_ts } : {}),
                ...(refPx_stale ? { refPx_stale } : {}),

                // 透传证据...
                'evidence.src': src,
                'evidence.notional3s': h['evidence.notional3s'] ?? '',
                'evidence.delta3s': h['evidence.delta3s'] ?? '',
                'evidence.zLike': h['evidence.zLike'] ?? '',
                'evidence.buyShare3s': h['evidence.buyShare3s'] ?? '',
                'evidence.breakout': h['evidence.breakout'] ?? '',
                'evidence.band': h['evidence.band'] ?? '',
                'evidence.eps': h['evidence.eps'] ?? '',
                strategyId: h.strategyId ?? 'intra.v1',
                ttlMs: h.ttlMs ?? String(Math.max(3000, cooldownMs || 0)),
              },
              { maxlenApprox: 5_000 },
            );

            this.metrics.incFinal(sym, dir, src);

            // 记录发车状态（供下一次门控使用）
            this.lastEmitTs.set(spacingKey, Date.now());
            this.lastEmitDir.set(sym, dir);
            if (approxKey) this.lastEmitKey.set(stateKey, approxKey); // ✅ 发车后回写 approxKey

            // 事件广播（进程内）
            this.bus.emit('signal.final', {
              ts,
              instId: sym,
              dir,
              strength,
              src,
            });

            this.logger.log(
              `[signal.final] inst=${sym} dir=${dir} st=${strength.toFixed(3)} src=${src} cool=${cool}` +
                (refPx
                  ? ` refPx=${refPx} src=${refPx_source} lag=${refPx_ts ? Number(refPx_ts) - ts : 'NA'}`
                  : ''),
            );

            // ACK
            this.safeAck(ackMap, m.key, m.id);
          } catch (e) {
            // 失败先不 ACK（pending 让消费者组稍后重试）
            this.logger.warn(
              `process detected->final failed id=${m.id}: ${(e as Error).message}`,
            );
          }
        }

        // 统一 ACK
        for (const [key, ids] of ackMap) {
          if (ids.length) await this.redis.ack(key, 'cg:signal-router', ids);
        }
      } catch (e) {
        this.logger.error(`router loop error: ${(e as Error).message}`);
      }
    }
  }

  private safeAck(ackMap: Map<string, string[]>, key: string, id: string) {
    if (!ackMap.has(key)) ackMap.set(key, []);
    ackMap.get(key)!.push(id);
  }

  // 读取 dyn:gate 快照（1s 缓存），仅取我们需要的字段
  private async getDyn(sym: string): Promise<{
    effMin0: number;
    cooldownMs: number;
  }> {
    const now = Date.now();
    const c = this.dynCache.get(sym);
    if (c && now - c.ts < 1000) {
      return { effMin0: c.effMin0, cooldownMs: c.cooldownMs };
    }
    const key = `dyn:gate:{${sym}}`;
    const h = await this.redis.hgetall(key);
    // 兜底：未写入过 dyn:gate 时给出保守默认
    const effMin0 = Number(h?.effMin0 ?? '0.7');
    const cooldownMs = Number(h?.cooldownMs ?? '9000');
    this.dynCache.set(sym, { ts: now, effMin0, cooldownMs });
    return { effMin0, cooldownMs };
  }

  // 统一取参考价：优先 mid(盘口)，兜底 last(成交)
  private async getRefPx(sym: string): Promise<{
    refPx?: string;
    refPx_source?: 'mid' | 'last';
    refPx_ts?: string;
    refPx_stale?: 'true' | 'false';
  }> {
    const now = Date.now();
    // A) 从盘口 stream 取最近一条
    try {
      const bookKey = this.redis.buildKey(sym, 'book'); // ws:{sym}:book（已前缀）
      const rows = await this.redis.xrevrangeLatest(bookKey, 1);
      if (rows && rows.length) {
        const [id, fields] = rows[0];
        const obj: Record<string, string> = {};
        for (let i = 0; i < fields.length; i += 2)
          obj[fields[i]] = fields[i + 1];
        const bid = Number(obj['bid1.px']);
        const ask = Number(obj['ask1.px']);
        const ts = Number(obj.ts) || now;
        if (
          Number.isFinite(bid) &&
          Number.isFinite(ask) &&
          bid > 0 &&
          ask > 0
        ) {
          const mid = (bid + ask) / 2;
          return {
            refPx: String(mid),
            refPx_source: 'mid',
            refPx_ts: String(ts),
            refPx_stale: ts > 0 && now - ts > 200 ? 'true' : 'false',
          };
        }
      }
    } catch {
      /* empty */
    }

    // B) 兜底：从成交 stream 取最近一条
    try {
      const tradeKey = this.redis.buildKey(sym, 'trades'); // ws:{sym}:trades（已前缀）
      const rows = await this.redis.xrevrangeLatest(tradeKey, 1);
      if (rows && rows.length) {
        const [id, fields] = rows[0];
        const obj: Record<string, string> = {};
        for (let i = 0; i < fields.length; i += 2)
          obj[fields[i]] = fields[i + 1];
        const px = Number(obj.px);
        const ts = Number(obj.ts) || now;
        if (Number.isFinite(px) && px > 0) {
          return {
            refPx: String(px),
            refPx_source: 'last',
            refPx_ts: String(ts),
            refPx_stale: ts > 0 && now - ts > 200 ? 'true' : 'false',
          };
        }
      }
    } catch {
      /* empty */
    }

    // C) 实在没有，就不写 refPx 字段（回测可跳过该样本/标注未知）
    return {};
  }

  // 构建幂等指纹键：instId|dir|src|时间桶
  private buildIdemKey(
    sym: string,
    dir: 'buy' | 'sell',
    src?: string,
    ts?: number,
  ) {
    const bucket =
      Math.floor((ts ?? Date.now()) / this.IDEM_BUCKET_MS) *
      this.IDEM_BUCKET_MS;
    const s = src || 'na';
    return `idem:final:${sym}:${dir}:${s}:${bucket}`;
  }
}
