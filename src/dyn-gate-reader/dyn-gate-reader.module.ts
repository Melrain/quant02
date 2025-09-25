import { Module } from '@nestjs/common';
import { DynGateReaderService } from './dyn-gate-reader.service';

@Module({
  exports: [DynGateReaderService],
  imports: [],
  controllers: [],
  providers: [DynGateReaderService],
})
export class DynGateReaderModule {}
