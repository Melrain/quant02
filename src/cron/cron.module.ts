import { Module } from '@nestjs/common';
import { MarketEnvUpdaterService } from './market-env-updater.service';
import { WorkMarketModule } from 'src/worker-market/work-market.module';
import { MetricsModule } from 'src/metrics/metrics.module';

@Module({
  imports: [WorkMarketModule, MetricsModule],
  controllers: [],
  providers: [MarketEnvUpdaterService],
  exports: [],
})
export class CronModule {}
