import { Module } from '@nestjs/common';
import { OkxWsService } from './okx-ws.service';
import { OkxWsClient } from './okx-ws-client';
import { StreamsBridgeService } from './streams-bridge.service';

@Module({
  imports: [],
  providers: [OkxWsService, OkxWsClient, StreamsBridgeService],
  exports: [],
})
export class WorkMarketModule {}
