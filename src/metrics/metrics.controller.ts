import { Controller, Get, Header } from '@nestjs/common';
import { MetricsService } from './metrics.service';

@Controller()
export class MetricsController {
  constructor(private readonly metrics: MetricsService) {}

  @Get('/metrics')
  @Header('Content-Type', 'text/plain; version=0.0.4')
  async scrape() {
    return this.metrics.getRegistry().metrics();
  }
}
