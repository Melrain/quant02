import { Module } from '@nestjs/common';
import { FibonacciFilterService } from './fibonacci-filter.service';
import { FibonacciOrderFilterService } from './fibonacci-order-filter.service';
import { WindowModule } from 'src/window/window.module';
import { RedisStreamsModule } from 'src/redis-streams/redis-streams.module';
import { WinRepo } from './win.repo';

@Module({
  imports: [WindowModule, RedisStreamsModule],
  controllers: [],
  providers: [FibonacciFilterService, FibonacciOrderFilterService, WinRepo],
  exports: [FibonacciFilterService, FibonacciOrderFilterService, WinRepo],
})
export class FiboModule {}
