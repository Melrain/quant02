import { Module } from '@nestjs/common';
import { WindowWorkerService } from './window-worker.service';

@Module({
  imports: [],
  providers: [WindowWorkerService],
  exports: [],
})
export class WindowModule {}
