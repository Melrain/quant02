import { Module } from '@nestjs/common';
import { OkxWsService } from './okx-ws.service';
import { OkxWsClient } from './okx-ws-client';
import { StreamsBridgeService } from './streams-bridge.service';
import { OiBootstrapService } from './oi-bootstrap.service';
import { FundingBootstrapService } from './funding-bootstrap.service';

@Module({
  imports: [],
  providers: [
    OkxWsService,
    OiBootstrapService,
    OkxWsClient,
    StreamsBridgeService,
    FundingBootstrapService,
  ],
  exports: [OiBootstrapService, FundingBootstrapService],
})
export class WorkMarketModule {}
