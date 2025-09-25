// src/observability/observability.controller.ts
import { Controller, Get, Param, ParseBoolPipe, Query } from '@nestjs/common';
import { ObservabilityService } from './observability.service';

@Controller('obs')
export class ObservabilityController {
  constructor(private readonly obs: ObservabilityService) {}

  // 动态门槛快照
  @Get('dyngate/:sym')
  async getDynGate(@Param('sym') sym: string) {
    return this.obs.getDynGate(sym);
  }

  // 信号统计（近期 N 条/或时间窗）
  @Get('signals/stats')
  async getSignalStats(
    @Query('limit') limit?: string,
    @Query('withExamples', ParseBoolPipe) withExamples = false,
  ) {
    const n = Math.max(1, Math.min(2000, Number(limit) || 200));
    return this.obs.getSignalStats(n, withExamples);
  }
}
