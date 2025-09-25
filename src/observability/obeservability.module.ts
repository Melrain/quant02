import { Module } from '@nestjs/common';
import { ObservabilityController } from './observability.controller';
import { ObservabilityService } from './observability.service';

@Module({
  imports: [],
  controllers: [ObservabilityController],
  providers: [ObservabilityService],
  exports: [],
})
export class ObservabilityModule {}
