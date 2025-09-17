import { Global, Module } from '@nestjs/common';
import { RedisClient } from './redis.client';

@Global()
@Module({
  imports: [],
  providers: [RedisClient],
  exports: [RedisClient],
})
export class RedisModule {}
