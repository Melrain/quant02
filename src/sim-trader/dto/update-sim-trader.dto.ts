import { PartialType } from '@nestjs/mapped-types';
import { CreateSimTraderDto } from './create-sim-trader.dto';

export class UpdateSimTraderDto extends PartialType(CreateSimTraderDto) {}
