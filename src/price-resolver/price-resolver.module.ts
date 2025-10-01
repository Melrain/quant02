import { Module } from '@nestjs/common';
import { PriceResolverService } from './price-resolver.service';
import { PriceResolverController } from './price-resolver.controller';

@Module({
  imports: [],
  controllers: [PriceResolverController],
  providers: [PriceResolverService],
  exports: [PriceResolverService],
})
export class PriceResolverModule {}
