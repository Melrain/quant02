/* eslint-disable @typescript-eslint/no-unsafe-return */
import { Injectable, Logger } from '@nestjs/common';
import { RedisStreamsService } from 'src/redis-streams/redis-streams.service';

export type TF = '1m' | '5m' | '15m';

export interface Bar {
  ts: number;
  open: number;
  high: number;
  low: number;
  close: number;
  vol?: number;
}

@Injectable()
export class WinRepo {
  private readonly logger = new Logger(WinRepo.name);
  constructor(private readonly streams: RedisStreamsService) {}

  private winKey(sym: string, tf: TF) {
    // window 属于“输出类 key”，你文档里要求：先走 RedisClient.key() 加前缀后再用于 raw stream 命令
    return this.streams['redis'].key(`win:${sym}:${tf}`);
  }

  /** 取最近 n 根 confirm=1 的K线（时间升序） */
  async lastNBars(sym: string, tf: TF, n: number): Promise<Bar[]> {
    const key = this.winKey(sym, tf);
    const count = Math.max(n * 3, n + 10);

    // 用你封装的“伪 XREVRANGE”（已返回 [id, Record<string,string>]，且 key 需已前缀）
    const tail = await this.streams.xrevrange(key, '+', '-', count);

    const out: Bar[] = [];
    for (const [id, f] of tail) {
      if ((f['confirm'] ?? '') !== '1') continue;
      const ts = Number(f['ts'] ?? id.split('-')[0]);
      const open = Number(f['open']),
        high = Number(f['high']);
      const low = Number(f['low']),
        close = Number(f['close']);
      const vol = f['vol'] ? Number(f['vol']) : undefined;
      if ([ts, open, high, low, close].some(Number.isNaN)) continue;
      out.push({ ts, open, high, low, close, vol });
      if (out.length >= n) break;
    }
    return out.reverse();
  }
}
