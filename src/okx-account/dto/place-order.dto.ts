// dto/place-order.dto.ts
import { IsIn, IsOptional, IsString, ValidateNested } from 'class-validator';
import { Transform, Type } from 'class-transformer';
import { OkxPrivateCreds } from '../types';

export type TriggerPxType = 'last' | 'mark' | 'index';

export class AttachAlgoOrd {
  // 止盈
  @IsOptional() @IsString() tpTriggerPx?: string; // 触发价
  @IsOptional() @IsString() tpOrdPx?: string; // 下单价，市价用 '-1'
  @IsOptional()
  @IsIn(['last', 'mark', 'index'])
  tpTriggerPxType?: TriggerPxType;

  // 止损
  @IsOptional() @IsString() slTriggerPx?: string;
  @IsOptional() @IsString() slOrdPx?: string; // 市价用 '-1'
  @IsOptional()
  @IsIn(['last', 'mark', 'index'])
  slTriggerPxType?: TriggerPxType;
}

export class PlaceOrderDto {
  // 外层单笔下单才需要 creds；批量时会被去掉
  creds!: OkxPrivateCreds;

  @IsString() instId!: string;
  @IsIn(['cross', 'isolated', 'cash'])
  tdMode!: 'cross' | 'isolated' | 'cash';
  @IsIn(['buy', 'sell']) side!: 'buy' | 'sell';
  @IsIn(['limit', 'market', 'post_only', 'fok', 'ioc', 'optimal_limit_ioc'])
  ordType!:
    | 'limit'
    | 'market'
    | 'post_only'
    | 'fok'
    | 'ioc'
    | 'optimal_limit_ioc';

  @IsString() sz!: string; // OKX 偏好字符串

  @IsOptional() @IsString() px?: string; // limit 才需要
  @IsOptional() @IsIn(['long', 'short']) posSide?: 'long' | 'short';

  // OKX 接口更偏好字符串；这里兼容 boolean，发送前已转字符串
  @IsOptional()
  @Transform(({ value }) => (value === undefined ? undefined : String(value)))
  reduceOnly?: string;

  @IsOptional() @IsString() clOrdId?: string;

  // 强类型 + 嵌套校验
  @IsOptional()
  @ValidateNested({ each: true })
  @Type(() => AttachAlgoOrd)
  attachAlgoOrds?: AttachAlgoOrd[];
}
