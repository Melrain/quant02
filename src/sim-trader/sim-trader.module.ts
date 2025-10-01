import { Module } from '@nestjs/common';
import { SimTraderService } from './sim-trader.service';
import { SimTraderController } from './sim-trader.controller';

@Module({
  controllers: [SimTraderController],
  providers: [SimTraderService],
})
export class SimTraderModule {}
