import { Injectable, Logger } from '@nestjs/common';
import { RedisStreamsService } from 'src/redis-streams/redis-streams.service';
import { atr, detectSwing, fibLevels, FibLevels, near } from './indicators';
import { TF, WinRepo } from './win.repo';

export type Side = 'buy' | 'sell';
export interface FinalSignal {
  sym: string;
  side: Side;
  size: number;
  entryHint?: number;
  ts?: number; // 新增：将原始信号时间透传
  // ... 你的其它字段
}
interface OrderIntent {
  sym: string;
  side: Side;
  entry: number;
  size: number;
  sl?: number | null;
  tps?: number[];
  postOnly?: boolean;
  decision?: 'PASS' | 'DEFER' | 'REJECT';
  reason?: string;
  ts?: number; // 新增
}

@Injectable()
export class FibonacciFilterService {
  private readonly logger = new Logger(FibonacciFilterService.name);

  private TF_MAIN: TF = (process.env.FIB_TF_MAIN as TF) || '1m';
  private LOOKBACK = Number(process.env.FIB_LOOKBACK_BARS || 200);
  private MIN_SWING_PCT = Number(process.env.FIB_MIN_SWING_PCT || 0.8);
  private NEAR_ATR_K = Number(process.env.FIB_NEAR_ATR_K || 0.3);
  private SL_ATR_BUF = Number(process.env.FIB_SL_BUFFER_ATR || 0.4);
  private TP1 = Number(process.env.FIB_TP1 || 1.272);
  private TP2 = Number(process.env.FIB_TP2 || 1.618);
  private DEFER_TTL = Number(process.env.FIB_DEFER_TTL_SEC || 120);

  constructor(
    private readonly win: WinRepo,
    private readonly streams: RedisStreamsService,
  ) {}

  /** 让你的消费组在拿到 payload 后调用它 */
  async handleFinalSignal(msg: FinalSignal) {
    const { sym, side } = msg;

    // 1) 最近 N 根确认K
    const bars = await this.win.lastNBars(sym, this.TF_MAIN, this.LOOKBACK);
    if (bars.length < 20)
      return this.block(sym, { ...msg, reason: 'insufficient bars' });

    // 2) ATR
    const atrVal = atr(bars, 14);
    if (!atrVal || atrVal <= 0)
      return this.block(sym, { ...msg, reason: 'atr invalid' });

    // 3) Swing + Fib
    const swing = detectSwing(bars, this.MIN_SWING_PCT);
    if (!swing) return this.block(sym, { ...msg, reason: 'no valid swing' });
    const fib = fibLevels(swing);

    // 4) 草案 + 改写
    const last = bars[bars.length - 1].close;
    const entry0 = msg.entryHint ?? last;
    let intent: OrderIntent = {
      sym,
      side,
      entry: entry0,
      size: msg.size,
      sl: null,
      tps: [],
      ts: msg.ts, // 透传 ts
    };
    intent = this.rewriteByFib(intent, fib, atrVal);

    // 5) 输出（用 buildOutStreamKey 得到“已前缀”的真实 key）
    if (intent.decision === 'PASS') {
      await this.streams.xadd(
        this.streams.buildOutStreamKey(sym, 'order:intent'),
        serialize(intent),
      );
    } else if (intent.decision === 'DEFER') {
      await this.streams.xadd(
        this.streams.buildOutStreamKey(sym, 'order:defer'),
        serialize({ ...intent, ttl: this.DEFER_TTL }),
      );
    } else {
      await this.block(sym, intent);
    }

    // 6) 指标（HINCRBY）
    const raw = this.streams['redis'].raw();
    const hkey = this.streams['redis'].key(
      `fib:metrics:${sym}:${this.today()}`,
    );
    await (raw as any).call(
      'HINCRBY',
      hkey,
      `decision:${intent.decision}`,
      '1',
    );
  }

  private rewriteByFib(
    intent: OrderIntent,
    fib: FibLevels,
    atrVal: number,
  ): OrderIntent {
    const { side, entry } = intent;
    const near618 = near(entry, fib['0.618'], atrVal, this.NEAR_ATR_K);
    const near50 = near(entry, fib['0.5'], atrVal, this.NEAR_ATR_K);
    const near382 = near(entry, fib['0.382'], atrVal, this.NEAR_ATR_K);

    if (near618 || near50) {
      const sl =
        side === 'buy'
          ? fib['0.786'] - this.SL_ATR_BUF * atrVal
          : fib['0.786'] + this.SL_ATR_BUF * atrVal;
      const tp1 = this.TP1 === 1.272 ? fib['1.272'] : fib['1.618'];
      const tp2 = this.TP2 === 1.618 ? fib['1.618'] : fib['1.272'];
      const sized = intent.size * (near618 ? 1.5 : 1.2);
      return {
        ...intent,
        size: sized,
        sl,
        tps: [tp1, tp2],
        decision: 'PASS',
        reason: 'near 0.5/0.618',
      };
    }
    if (near382) {
      const sl =
        side === 'buy'
          ? fib['0.5'] - this.SL_ATR_BUF * atrVal
          : fib['0.5'] + this.SL_ATR_BUF * atrVal;
      const tp1 = this.TP1 === 1.272 ? fib['1.272'] : fib['1.618'];
      return {
        ...intent,
        size: intent.size * 0.7,
        sl,
        tps: [tp1],
        decision: 'PASS',
        reason: 'near 0.382 (light)',
      };
    }
    // 不在关键位 → 改挂 postOnly 限价到更近的 0.5 / 0.618
    const target =
      Math.abs(entry - fib['0.618']) < Math.abs(entry - fib['0.5'])
        ? fib['0.618']
        : fib['0.5'];
    return {
      ...intent,
      entry: target,
      postOnly: true,
      decision: 'DEFER',
      reason: 'snap to better fib level',
    };
  }

  private async block(sym: string, payload: any) {
    await this.streams.xadd(
      this.streams.buildOutStreamKey(sym, 'order:blocked'),
      serialize({ ...payload, decision: 'REJECT' }),
    );
  }

  private today() {
    const d = new Date();
    const y = d.getUTCFullYear(),
      m = String(d.getUTCMonth() + 1).padStart(2, '0'),
      dd = String(d.getUTCDate()).padStart(2, '0');
    return `${y}${m}${dd}`;
  }
}

function serialize(obj: Record<string, any>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(obj))
    out[k] = typeof v === 'object' ? JSON.stringify(v) : String(v);
  return out;
}
