export type OkxInstType =
  | 'SPOT'
  | 'MARGIN'
  | 'SWAP'
  | 'FUTURES'
  | 'OPTION'
  | 'ANY';
// 私有频道订阅参数（与文档一致：args 为对象数组）
export type OkxPrivateSubArg =
  | { channel: 'account'; ccy?: string }
  | { channel: 'balance_and_position' }
  | {
      channel: 'positions';
      instType?: OkxInstType;
      instFamily?: string;
      instId?: string;
    }
  | {
      channel: 'orders';
      instType?: OkxInstType;
      instFamily?: string;
      instId?: string;
    }
  | {
      channel: 'orders-algo';
      instType?: OkxInstType;
      instFamily?: string;
      instId?: string;
    }
  // 兜底，便于扩展其它私有频道
  | ({ channel: string } & Record<string, string>);

// 订阅/退订 payload
export type OkxPrivateSubscribePayload = {
  op: 'subscribe' | 'unsubscribe';
  args: OkxPrivateSubArg[];
  id?: string;
};

// 登录 payload（私有 WS 需先 login 再订阅）
export type OkxLoginArgs = {
  apiKey: string;
  passphrase: string;
  timestamp: string; // 秒级时间戳字符串
  sign: string; // Base64(HMAC_SHA256(secretKey, timestamp + 'GET' + '/users/self/verify'))
  simulated?: '1';
};
export type OkxLoginPayload = { op: 'login'; args: [OkxLoginArgs] };

// 入站消息（简化）
export type OkxPrivateInbound =
  | { event: string; arg?: any; code?: string; msg?: string; connId?: string }
  | { arg: any; data: any[] }
  | string;

/*****************************************manager **************************************************** */
export type TenantKey = string;

export type OkxPrivateCreds = {
  apiKey: string;
  secretKey: string;
  passphrase: string;
  simulated?: boolean;
};

export type SubscribeArg =
  | { channel: string; [k: string]: any }
  | Array<{ channel: string; [k: string]: any }>;

export type ManagerStatus =
  | 'INIT'
  | 'CONNECTING'
  | 'OPEN'
  | 'LOGGING_IN'
  | 'READY'
  | 'CLOSED'
  | 'ERROR'
  | 'ENV_MISMATCH';

export interface PlaceOrderInput {
  // --- 账号 & 路由 ---
  creds: OkxPrivateCreds;

  // --- 订单必需字段 ---
  instId: string; // 例：'BTC-USDT-SWAP'
  tdMode: 'cross' | 'isolated' | 'cash'; // 全仓/逐仓/现货
  side: 'buy' | 'sell';
  ordType:
    | 'limit'
    | 'market'
    | 'post_only'
    | 'fok'
    | 'ioc'
    | 'optimal_limit_ioc';
  sz: string; // 数量或张数（OKX 要求 string）
  px?: string; // 限价单需要

  // --- 订单可选字段（按你的账户模式决定是否必填）---
  posSide?: 'long' | 'short'; // 双向持仓才需要；净持仓不要传
  reduceOnly?: boolean; // 合约/跨币保/币币杠杆可用
  ccy?: string; // 部分逐仓或全仓杠杆订单需要
  tgtCcy?: 'base_ccy' | 'quote_ccy'; // 仅币币市价单
  clOrdId?: string;
  tag?: string;

  // 止盈止损（附带）
  attachAlgoOrds?: Array<{
    attachAlgoClOrdId?: string;
    tpTriggerPx?: string;
    tpOrdPx?: string;
    tpOrdKind?: 'condition' | 'limit';
    slTriggerPx?: string;
    slOrdPx?: string; // -1 表示市价
    tpTriggerPxType?: 'last' | 'index' | 'mark';
    slTriggerPxType?: 'last' | 'index' | 'mark';
    sz?: string;
    amendPxOnTriggerType?: '0' | '1';
  }>;
}

export interface SetLeverageInput {
  creds: OkxPrivateCreds;
  lever: string; // '10'
  mgnMode: 'cross' | 'isolated';
  instId?: string; // 永续/交割建议传 instId
  ccy?: string; // 现货/杠杆可能用到
  posSide?: 'long' | 'short'; // 双向时可分方向
}
