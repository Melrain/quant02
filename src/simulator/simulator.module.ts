import { Module } from '@nestjs/common';
import { SimTradeService } from './sim-trade.service';
import { PriceResolverModule } from 'src/price-resolver/price-resolver.module';
import { MetricsModule } from 'src/metrics/metrics.module';

@Module({
  imports: [PriceResolverModule, MetricsModule],
  controllers: [],
  providers: [SimTradeService],
  exports: [],
})
export class SimulatorModule {}
