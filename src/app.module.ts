import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { EventEmitterModule } from '@nestjs/event-emitter';
import { WorkMarketModule } from './worker-market/work-market.module';

@Module({
  imports: [
    EventEmitterModule.forRoot({
      wildcard: true,
      delimiter: '.',
      maxListeners: 100,
    }),
    WorkMarketModule,
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
