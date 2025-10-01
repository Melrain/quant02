import { Controller, Get, Post, Body, Patch, Param, Delete } from '@nestjs/common';
import { SimTraderService } from './sim-trader.service';
import { CreateSimTraderDto } from './dto/create-sim-trader.dto';
import { UpdateSimTraderDto } from './dto/update-sim-trader.dto';

@Controller('sim-trader')
export class SimTraderController {
  constructor(private readonly simTraderService: SimTraderService) {}

  @Post()
  create(@Body() createSimTraderDto: CreateSimTraderDto) {
    return this.simTraderService.create(createSimTraderDto);
  }

  @Get()
  findAll() {
    return this.simTraderService.findAll();
  }

  @Get(':id')
  findOne(@Param('id') id: string) {
    return this.simTraderService.findOne(+id);
  }

  @Patch(':id')
  update(@Param('id') id: string, @Body() updateSimTraderDto: UpdateSimTraderDto) {
    return this.simTraderService.update(+id, updateSimTraderDto);
  }

  @Delete(':id')
  remove(@Param('id') id: string) {
    return this.simTraderService.remove(+id);
  }
}
