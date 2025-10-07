import { Module } from '@nestjs/common';
import { SimTradeService } from './sim-trade.service';
import { PriceResolverModule } from 'src/price-resolver/price-resolver.module';
import { MetricsModule } from 'src/metrics/metrics.module';
import { SimTradeIntentService } from './sim-trade-intent.service';
import { FiboModule } from 'src/fibo/fibo.module';

@Module({
  imports: [PriceResolverModule, MetricsModule, FiboModule],
  controllers: [],
  providers: [SimTradeService, SimTradeIntentService],
  exports: [],
})
export class SimulatorModule {}
