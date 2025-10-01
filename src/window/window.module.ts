import { Module } from '@nestjs/common';
import { WindowWorkerService } from './window-worker.service';
import { RedisStreamsModule } from 'src/redis-streams/redis-streams.module';
import { RedisModule } from 'src/redis/redis.module';
import { DynGateReaderModule } from 'src/dyn-gate-reader/dyn-gate-reader.module';
import { SignalRouterService } from './signal-router.service';
import { MetricsModule } from 'src/metrics/metrics.module';
import { PriceResolverModule } from 'src/price-resolver/price-resolver.module';
import { SignalEvaluatorService } from './signal-evaluator.service';

@Module({
  imports: [
    RedisStreamsModule,
    RedisModule,
    DynGateReaderModule,
    MetricsModule,
    PriceResolverModule,
  ],

  providers: [WindowWorkerService, SignalEvaluatorService, SignalRouterService],
  exports: [SignalRouterService, SignalEvaluatorService],
})
export class WindowModule {}
