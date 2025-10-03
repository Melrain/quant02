import { Module } from 'node_modules/@nestjs/common';
import { OkxRestTradeService } from './okx-rest-trade.service';

@Module({
  controllers: [],
  providers: [OkxRestTradeService],
  exports: [OkxRestTradeService],
  imports: [],
})
export class OkxAccountModule {}
