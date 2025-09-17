// src/infra/redis/redis-streams.module.ts
import { Module, Global } from '@nestjs/common';
import { RedisStreamsService } from './redis-streams.service';
import { RedisClient } from 'src/redis/redis.client';

/**
 * 简洁做法：如果你的项目只有一个 Redis 连接，
 * 这里直接 new RedisClient，并设置为全局模块（@Global）
 * 这样所有地方都能直接注入 RedisStreamService 使用。
 */
@Global()
@Module({
  providers: [
    {
      provide: RedisClient,
      useFactory: () =>
        new RedisClient({
          url: process.env.REDIS_URL || 'redis://127.0.0.1:6379',
          keyPrefix: process.env.REDIS_PREFIX || '', // 如 'dev:' / 'prod:'
          enableAutoPipelining: true,
        }),
    },
    RedisStreamsService,
  ],
  exports: [RedisClient, RedisStreamsService],
})
export class RedisStreamsModule {}
