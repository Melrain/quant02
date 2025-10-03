/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable @typescript-eslint/no-unsafe-return */
import {
  Injectable,
  Logger,
  OnModuleInit,
  OnModuleDestroy,
} from '@nestjs/common';
import axios, { AxiosInstance } from 'axios';
import https from 'https';
import { createHmac } from 'crypto';
import * as types from './dto/types';
import { PlaceOrderDto } from './dto/place-order.dto';

const API_PREFIX = '/api/v5';

// ---- 时钟漂移（与 OKX 服务器时间的差值，毫秒） ----
let clockSkewMs = 0;

@Injectable()
export class OkxRestTradeService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(OkxRestTradeService.name);
  private readonly http: AxiosInstance;

  // instruments 缓存（ctVal/lotSz/tickSz/…）
  private instCache = new Map<
    string,
    {
      ctVal?: number;
      lotSz?: string;
      tickSz?: string;
      ts: number;
      raw?: any;
    }
  >();
  private readonly instTtlMs = 5 * 60 * 1000;

  // 触发价类型：默认 mark，可通过 env 覆盖
  private readonly triggerPxType: 'last' | 'mark' | 'index' = (() => {
    const v = (process.env.TRIGGER_PX_TYPE || 'mark').toLowerCase();
    return v === 'last' || v === 'index' || v === 'mark' ? (v as any) : 'mark';
  })();

  constructor() {
    this.http = axios.create({
      baseURL: process.env.OKX_BASE_URL || 'https://www.okx.com',
      timeout: Number(process.env.OKX_TIMEOUT_MS || 10_000),
      // 为撮合接口准备更高连接复用能力
      httpsAgent: new https.Agent({
        keepAlive: true,
        maxSockets: 256,
        maxFreeSockets: 64,
      }),
      // 不跟随 3xx（避免错误跳转污染签名）
      maxRedirects: 0,
      validateStatus: () => true, // 交给代码判断
    });
  }

  async onModuleInit() {
    // 启动时做一次时钟同步（失败不影响）
    await this.syncServerTimeSafe();
  }

  async onModuleDestroy() {
    // nothing
  }

  // ---------- 公共工具 ----------

  private async syncServerTimeSafe() {
    try {
      const url = `${API_PREFIX}/public/time`;
      const resp = await this.http.get(url);
      const serverMs = Number(resp?.data?.data?.[0]?.ts ?? Date.now());
      clockSkewMs = serverMs - Date.now();
      this.logger.log(`OKX clock skew set to ${clockSkewMs}ms`);
    } catch (e) {
      this.logger.warn('syncServerTime failed (safe ignore).');
    }
  }

  private nowIsoWithSkew(): string {
    return new Date(Date.now() + clockSkewMs).toISOString();
  }

  /** 统一签名头（把 query 拼进 requestPath，一定要与真实请求完全一致） */
  private buildSignedHeaders(
    creds: types.OkxPrivateCreds,
    method: 'GET' | 'POST' | 'DELETE',
    requestPath: string,
    bodyObj?: unknown,
  ) {
    const timestamp = this.nowIsoWithSkew();
    const body = bodyObj ? JSON.stringify(bodyObj) : '';
    const prehash = `${timestamp}${method}${requestPath}${body}`;
    const sign = createHmac('sha256', creds.secretKey)
      .update(prehash)
      .digest('base64');

    const headers: Record<string, string> = {
      'OK-ACCESS-KEY': creds.apiKey,
      'OK-ACCESS-SIGN': sign,
      'OK-ACCESS-TIMESTAMP': timestamp,
      'OK-ACCESS-PASSPHRASE': creds.passphrase,
      'Content-Type': 'application/json',
    };
    if ((creds as any)?.simulated) headers['x-simulated-trading'] = '1';
    return headers;
  }

  /** 构造 query 字符串（null/undefined 过滤） */
  private toQuery(qs?: Record<string, any>): string {
    if (!qs) return '';
    const pairs = Object.entries(qs)
      .filter(([, v]) => v !== undefined && v !== null)
      .map(
        ([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`,
      );
    return pairs.length ? `?${pairs.join('&')}` : '';
  }

  // axios 错误是否可重试
  private isRetriable(err: any): { ok: boolean; waitMs?: number } {
    const status = err?.response?.status as number | undefined;
    const code = err?.code as string | undefined;

    // 429/限频：若带 Retry-After 则按秒休眠
    if (status === 429) {
      const ra = Number(err?.response?.headers?.['retry-after']);
      if (Number.isFinite(ra) && ra > 0) return { ok: true, waitMs: ra * 1000 };
      return { ok: true }; // 没有 RA 用指数退避
    }

    // 5xx
    if (status && status >= 500) return { ok: true };

    // 常见网络错误 & 超时
    const retriableCodes = new Set([
      'ECONNRESET',
      'ETIMEDOUT',
      'EAI_AGAIN',
      'ERR_BAD_RESPONSE',
      'ECONNABORTED',
      'ENOTFOUND',
      'EHOSTUNREACH',
    ]);
    if (code && retriableCodes.has(code)) return { ok: true };

    return { ok: false };
  }

  // 统一 GET（带签名、可重试）
  private async privateGet<T>(
    creds: types.OkxPrivateCreds,
    path: string,
    qs?: Record<string, any>,
    retryOpt?: { max?: number; baseDelayMs?: number },
  ): Promise<T> {
    const max = retryOpt?.max ?? Number(process.env.RETRY_5XX_TIMES || 3);
    const base = retryOpt?.baseDelayMs ?? 300;
    const qsStr = this.toQuery(qs);
    const requestPath = `${API_PREFIX}${path}${qsStr}`;

    for (let attempt = 1; attempt <= max; attempt++) {
      const headers = this.buildSignedHeaders(creds, 'GET', requestPath);
      const resp = await this.http.get(requestPath, { headers });
      if (resp.status === 200) return resp.data as T;

      // 非 200 统一处理
      const fakeErr = { response: resp };
      const { ok, waitMs } = this.isRetriable(fakeErr);
      this.logger.warn(
        `GET ${path} attempt=${attempt} status=${resp.status} retriable=${ok}`,
      );

      if (ok && attempt < max) {
        await new Promise((r) =>
          setTimeout(
            r,
            waitMs ?? base * attempt + Math.floor(Math.random() * 150),
          ),
        );
        // 时间戳相关（401/400）尝试同步时间
        if (resp.status === 401 || resp.status === 400)
          await this.syncServerTimeSafe();
        continue;
      }

      throw new Error(
        `GET ${path} failed status=${resp.status} body=${JSON.stringify(resp.data)}`,
      );
    }
    // 理论到不了
    throw new Error('unreachable');
  }

  // 统一 POST（带签名、可重试、指数退避+抖动）
  private async privatePost<T>(
    creds: types.OkxPrivateCreds,
    path: string,
    body?: any,
    retryOpt?: { max?: number; baseDelayMs?: number },
  ): Promise<T> {
    const max = retryOpt?.max ?? Number(process.env.RETRY_5XX_TIMES || 3);
    const base = retryOpt?.baseDelayMs ?? 300;
    const requestPath = `${API_PREFIX}${path}`;

    for (let attempt = 1; attempt <= max; attempt++) {
      const headers = this.buildSignedHeaders(creds, 'POST', requestPath, body);
      try {
        const resp = await this.http.post(requestPath, body, { headers });
        if (resp.status === 200) return resp.data as T;

        const fakeErr = { response: resp };
        const { ok, waitMs } = this.isRetriable(fakeErr);
        this.logger.error(
          `POST ${path} attempt=${attempt} status=${resp.status} retriable=${ok}`,
        );

        if (ok && attempt < max) {
          await new Promise((r) =>
            setTimeout(
              r,
              waitMs ?? base * attempt + Math.floor(Math.random() * 150),
            ),
          );
          if (resp.status === 401 || resp.status === 400)
            await this.syncServerTimeSafe();
          continue;
        }

        throw new Error(
          `POST ${path} failed status=${resp.status} body=${JSON.stringify(resp.data)}`,
        );
      } catch (err: any) {
        // axios 异常（如超时/网络错误）
        const { ok, waitMs } = this.isRetriable(err);
        this.logger.error(
          `POST ${path} attempt=${attempt} code=${err?.code || ''} retriable=${ok}`,
        );
        if (ok && attempt < max) {
          await new Promise((r) =>
            setTimeout(
              r,
              waitMs ?? base * attempt + Math.floor(Math.random() * 150),
            ),
          );
          // 可能时间戳问题
          if (err?.response?.status === 401 || err?.response?.status === 400) {
            await this.syncServerTimeSafe();
          }
          continue;
        }
        const payload = err?.response?.data ?? err?.message ?? err;
        throw new Error(`POST ${path} failed: ${JSON.stringify(payload)}`);
      }
    }
    throw new Error('unreachable');
  }

  // ---------- 规格化工具（可选开启） ----------

  /** 把值四舍五入到 step 的整数倍（step 为字符串/小数，如 '0.001'） */
  private normalizeToStep(
    value: string | number,
    step?: string,
  ): string | number {
    if (!step) return value;
    const v = Number(value);
    const s = Number(step);
    if (!Number.isFinite(v) || !Number.isFinite(s) || s <= 0) return value;
    const n = Math.round(v / s) * s;
    // 避免 0.30000000004 之类的尾差
    const dp = (step.split('.')[1] || '').length;
    return Number(n.toFixed(dp));
  }

  // ---------- 对外 API ----------

  /** 现价等行情 */
  async getTicker(instId: string) {
    const { data } = await this.http.get(`${API_PREFIX}/market/ticker`, {
      params: { instId },
    });
    return data;
  }

  /** instruments 元信息，含 lotSz/tickSz/ctVal（缓存） */
  async getInstrumentMeta(instId: string) {
    const now = Date.now();
    const cache = this.instCache.get(instId);
    if (cache && now - cache.ts < this.instTtlMs) return cache.raw;

    const { data } = await this.http.get(`${API_PREFIX}/public/instruments`, {
      params: { instType: 'SWAP', instId },
    });
    const row = data?.data?.[0];
    const ctVal = Number(row?.ctVal ?? row?.ctMult ?? 1);
    const lotSz = row?.lotSz as string | undefined;
    const tickSz = row?.tickSz as string | undefined;

    this.instCache.set(instId, {
      ctVal: Number.isFinite(ctVal) && ctVal > 0 ? ctVal : 1,
      lotSz,
      tickSz,
      ts: now,
      raw: row,
    });
    return row;
  }

  /** 合约面值（缓存） */
  async getContractValue(instId: string): Promise<number> {
    const meta = await this.getInstrumentMeta(instId);
    const ctVal = Number(meta?.ctVal ?? meta?.ctMult ?? 1);
    return Number.isFinite(ctVal) && ctVal > 0 ? ctVal : 1;
  }

  /** 下单（支持 attachAlgoOrds / 重试 / 幂等 clOrdId / 触发价类型注入） */
  async placeOrder(input: PlaceOrderDto) {
    const {
      creds,
      reduceOnly,
      attachAlgoOrds,
      clOrdId,
      instId,
      px,
      sz,
      ...rest
    } = input;

    const meta = await this.getInstrumentMeta(instId).catch(() => undefined);

    const body: any = {
      ...rest,
      instId,
      // 幂等 clOrdId
      clOrdId:
        clOrdId ??
        `nb-${instId}-${Date.now()}-${Math.floor(Math.random() * 1e6)}`,
      // OKX 文档要求 reduceOnly 为字符串
      ...(reduceOnly !== undefined ? { reduceOnly: String(reduceOnly) } : {}),
    };

    // 规格化（可选）：若不想规格化，可注释掉这两行
    body.sz = this.normalizeToStep(sz, meta?.lotSz);
    if (px !== undefined) body.px = this.normalizeToStep(px, meta?.tickSz);

    // 空数组不发送
    if (Array.isArray(attachAlgoOrds) && attachAlgoOrds.length > 0) {
      body.attachAlgoOrds = attachAlgoOrds.map((a) => ({
        tpTriggerPxType: a.tpTriggerPxType ?? this.triggerPxType,
        slTriggerPxType: a.slTriggerPxType ?? this.triggerPxType,
        ...a,
      }));
    }

    return this.privatePost(creds, '/trade/order', body, {
      max: Number(process.env.RETRY_5XX_TIMES || 3),
      baseDelayMs: 300,
    });
  }

  /** 批量下单（自动注入 triggerPxType / clOrdId） */
  async batchOrders(
    creds: types.OkxPrivateCreds,
    orders: Omit<PlaceOrderDto, 'creds'>[],
  ) {
    const now = Date.now();
    const body = await Promise.all(
      orders.map(
        async (
          { reduceOnly, attachAlgoOrds, clOrdId, instId, px, sz, ...o },
          i,
        ) => {
          const meta = await this.getInstrumentMeta(instId).catch(
            () => undefined,
          );
          return {
            ...o,
            instId,
            clOrdId:
              clOrdId ??
              `nb-${instId}-${now}-${i}-${Math.floor(Math.random() * 1e6)}`,
            ...(reduceOnly !== undefined
              ? { reduceOnly: String(reduceOnly) }
              : {}),
            sz: this.normalizeToStep(sz, meta?.lotSz),
            ...(px !== undefined
              ? { px: this.normalizeToStep(px, meta?.tickSz) }
              : {}),
            ...(Array.isArray(attachAlgoOrds) && attachAlgoOrds.length > 0
              ? {
                  attachAlgoOrds: attachAlgoOrds.map((a) => ({
                    tpTriggerPxType: a.tpTriggerPxType ?? this.triggerPxType,
                    slTriggerPxType: a.slTriggerPxType ?? this.triggerPxType,
                    ...a,
                  })),
                }
              : {}),
          };
        },
      ),
    );

    return this.privatePost(creds, '/trade/batch-orders', body);
  }

  /** 撤单（ordId 或 clOrdId 二选一） */
  async cancelOrder(
    creds: types.OkxPrivateCreds,
    args: { instId: string; ordId?: string; clOrdId?: string },
  ) {
    return this.privatePost(creds, '/trade/cancel-order', args);
  }

  /** 设置杠杆 */
  async setLeverage(input: types.SetLeverageInput) {
    const { creds, ...body } = input;
    return this.privatePost(creds, '/account/set-leverage', body);
  }

  /** 设置持仓模式（净/双向） */
  async setPositionMode(
    creds: types.OkxPrivateCreds,
    posMode: 'net_mode' | 'long_short_mode',
  ) {
    const body = { posMode };
    return this.privatePost(creds, '/account/set-position-mode', body);
  }

  /** 查未成交挂单 */
  async getOpenOrders(creds: types.OkxPrivateCreds, instId?: string) {
    return this.privateGet(
      creds,
      '/trade/orders-pending',
      instId ? { instId } : undefined,
    );
  }

  /** 获取所有仓位信息 */
  async getPositions(creds: types.OkxPrivateCreds, instId?: string) {
    return this.privateGet(
      creds,
      '/account/positions',
      instId ? { instId } : undefined,
    );
  }
}
