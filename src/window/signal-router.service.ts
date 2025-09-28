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
import { parseSymbolsFromEnv } from 'src/utils/utils';

type DetectedRow = Record<string, string>;

@Injectable()
export class SignalRouterService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(SignalRouterService.name);
  private running = false;

  private readonly symbols = parseSymbolsFromEnv();

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

            // 通过 -> 发布到 final 流
            const outKey = this.redis.buildOutKey(sym, 'signal:final');
            await this.redis.xadd(
              outKey,
              {
                ts: String(ts),
                instId: sym,
                dir,
                strength: String(strength),
                // 透传关键信息，便于回放
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

            // 计数：final 成功
            this.metrics.incFinal(sym, dir, src);

            // 状态记账
            this.lastEmitTs.set(stateKey, ts);
            if (approxKey) this.lastEmitKey.set(stateKey, approxKey);

            // 事件广播（进程内）
            this.bus.emit('signal.final', {
              ts,
              instId: sym,
              dir,
              strength,
              src,
            });

            this.logger.log(
              `[signal.final] inst=${sym} dir=${dir} st=${strength.toFixed(3)} src=${src} cool=${cool}`,
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
}
