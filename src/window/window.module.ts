import { Module } from '@nestjs/common';
import { WindowWorkerService } from './window-worker.service';
import { RedisStreamsModule } from 'src/redis-streams/redis-streams.module';
import { RedisModule } from 'src/redis/redis.module';
import { ParamModule } from 'src/params/params.module';

@Module({
  imports: [RedisStreamsModule, RedisModule, ParamModule],
  providers: [WindowWorkerService],
  exports: [],
})
export class WindowModule {}
