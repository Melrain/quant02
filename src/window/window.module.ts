import { Module } from '@nestjs/common';
import { WindowWorkerService } from './window-worker.service';
import { RedisStreamsModule } from 'src/redis-streams/redis-streams.module';
import { RedisModule } from 'src/redis/redis.module';

@Module({
  imports: [RedisStreamsModule, RedisModule],
  providers: [WindowWorkerService],
  exports: [],
})
export class WindowModule {}
