import { Module } from '@nestjs/common';
import { OkxWsService } from './okx-ws.service';
import { OkxWsClient } from './okx-ws-client';

@Module({
  imports: [],
  providers: [OkxWsService, OkxWsClient],
  exports: [],
})
export class WorkMarketModule {}
