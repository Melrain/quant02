// src/params/param.module.ts
import { Module } from '@nestjs/common';
import { ParamRepository } from './param.repository';
import { RedisModule } from '../redis/redis.module';
import { ParamRefresher } from './param.refresher';

@Module({
  imports: [RedisModule],
  providers: [ParamRepository, ParamRefresher],
  exports: [ParamRepository, ParamRefresher],
})
export class ParamModule {}
