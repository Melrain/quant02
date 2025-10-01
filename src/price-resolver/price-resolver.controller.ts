import { Controller } from '@nestjs/common';
import { PriceResolverService } from './price-resolver.service';

@Controller('price-resolver')
export class PriceResolverController {
  constructor(private readonly priceResolverService: PriceResolverService) {}
}
