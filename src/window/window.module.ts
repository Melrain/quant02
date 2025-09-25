import { Module } from '@nestjs/common';
import { WindowWorkerService } from './window-worker.service';
import { RedisStreamsModule } from 'src/redis-streams/redis-streams.module';
import { RedisModule } from 'src/redis/redis.module';
import { DynGateReaderModule } from 'src/dyn-gate-reader/dyn-gate-reader.module';
import { SignalRouterService } from './signal-router.service';
import { MetricsModule } from 'src/metrics/metrics.module';

@Module({
  imports: [
    RedisStreamsModule,
    RedisModule,
    DynGateReaderModule,
    MetricsModule,
  ],
  providers: [WindowWorkerService, SignalRouterService],
  exports: [SignalRouterService],
})
export class WindowModule {}
