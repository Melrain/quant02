import { Module, Global } from '@nestjs/common';
import { MetricsService } from './metrics.service';
import { MetricsController } from './metrics.controller';
import { SignalEvaluatorService } from 'src/window/signal-evaluator.service';

@Global()
@Module({
  providers: [MetricsService, SignalEvaluatorService],
  controllers: [MetricsController],
  exports: [MetricsService, SignalEvaluatorService],
})
export class MetricsModule {}
