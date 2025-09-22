import { Module } from '@nestjs/common';
import { MarketEnvUpdaterService } from './market-env-updater.service';

@Module({
  imports: [],
  controllers: [],
  providers: [MarketEnvUpdaterService],
  exports: [],
})
export class CronModule {}
