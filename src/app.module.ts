import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { EventEmitterModule } from '@nestjs/event-emitter';
import { WorkMarketModule } from './worker-market/work-market.module';
import { RedisModule } from './redis/redis.module';
import { RedisStreamsModule } from './redis-streams/redis-streams.module';
import { WindowModule } from './window/window.module';

import { ConfigModule } from '@nestjs/config';
import { ScheduleModule } from '@nestjs/schedule';
import { CronModule } from './cron/cron.module';
import { DynGateReaderModule } from './dyn-gate-reader/dyn-gate-reader.module';
import { ObservabilityModule } from './observability/obeservability.module';
import { MetricsModule } from './metrics/metrics.module';

@Module({
  imports: [
    ScheduleModule.forRoot(),
    ConfigModule.forRoot({ isGlobal: true, envFilePath: '.env' }),
    EventEmitterModule.forRoot({
      wildcard: true,
      delimiter: '.',
      maxListeners: 100,
    }),
    WorkMarketModule,
    RedisModule,
    RedisStreamsModule,
    WindowModule,
    CronModule,
    DynGateReaderModule,
    ObservabilityModule,
    MetricsModule,
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
