import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { EventEmitterModule } from '@nestjs/event-emitter';
import { WorkMarketModule } from './worker-market/work-market.module';
import { RedisModule } from './redis/redis.module';
import { RedisStreamsModule } from './redis-streams/redis-streams.module';
import { WindowModule } from './window/window.module';

import { ConfigModule } from '@nestjs/config';

@Module({
  imports: [
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
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
