/* eslint-disable @typescript-eslint/no-unnecessary-type-assertion */
/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable @typescript-eslint/no-unsafe-return */
import { Injectable, Logger } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { MetricsService } from 'src/metrics/metrics.service';
import { RedisStreamsService } from 'src/redis-streams/redis-streams.service';
import { parseSymbolsFromEnv } from 'src/utils/utils';
import { FundingBootstrapService } from 'src/worker-market/funding-bootstrap.service';

/* ===================== 内部类型（非 DTO） ===================== */
type EnvFactors = {
  volPct: number; // 0~1：相对波动分位（近5m/15m）
  liqPct: number; // 0~1：相对活跃度分位（成交额）
  rateExc: number; // >=0：信号速率异常度
  eventFlag: 0 | 1; // funding临近等事件窗口
  oiRegime: -1 | 0 | 1; // OI 增减仓状态
  updatedAt: number; // 毫秒
};

type DynGateParams = {
  effMin0: number;
  minNotional3s: number;
  minMoveBp: number;
  minMoveAtrRatio: number;
  cooldownMs: number;
  dedupMs: number;
  breakoutBandPct: number;

  // evidence
  volPct: number;
  liqPct: number;
  rateExc: number;
  eventFlag: number;
  oiRegime: number;
  updated_at: number;
  version: string;
};

/* ===================== 常量参数（可调） ===================== */
const VOL_LOOKBACK = 48; // 计算分位所用窗口（条），5m*48 ≈ 4小时
const LIQ_LOOKBACK = 48; // 同上：成交额分位
const OI_LOOKBACK = 30; // 取最近 30 条 OI（OKX推送频率较低）
const RATE_WIN_MS = 60_000; // 近 60s 信号速率
const RATE_BASE_MS = 15 * 60_000; // 近 15m 作为基线
const FUNDING_NEAR_MS = 10 * 60_000; // funding 前 10 分钟置 1
const EPS = 1e-9;

/* ===================== 小工具 ===================== */
const clip01 = (x: number) => Math.max(0, Math.min(1, x));
const safeNum = (v: any): number => {
  const n = Number(v);
  return Number.isFinite(n) ? n : NaN;
};
const pctRank = (series: number[], x: number): number => {
  if (!series.length) return 0.5;
  const s = series.slice().sort((a, b) => a - b);
  let lo = 0,
    hi = s.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (s[mid] <= x) lo = mid + 1;
    else hi = mid;
  }
  return clip01(lo / s.length);
};
const pctChange = (a: number, b: number) => (b - a) / (Math.abs(a) + EPS);

/* ===================== Service ===================== */
@Injectable()
export class MarketEnvUpdaterService {
  private readonly logger = new Logger(MarketEnvUpdaterService.name);
  private readonly symbols = parseSymbolsFromEnv();
  private oiState = new Map<string, { dir: -1 | 0 | 1; since: number }>();

  constructor(
    private readonly streams: RedisStreamsService,
    private readonly fundingBootstrapService: FundingBootstrapService,
    private readonly metrics: MetricsService, // <—— 新增
  ) {}

  async onModuleInit() {
    const symbols = parseSymbolsFromEnv();
    for (const s of symbols) {
      await this.fundingBootstrapService.bootstrapFundingHistory(s);
    }
  }

  /** 每 10s 跑一次（你也可以改成 */ @Cron('*/10 * * * * *') /** ）*/
  async updateMarketEnv() {
    const now = Date.now();
    this.logger.log(
      `MarketEnvUpdater triggered @${new Date(now).toISOString()}`,
    );

    for (const sym of this.symbols) {
      try {
        const factors = await this.computeEnvFactors(sym, now);
        const params = this.mapFactorsToParams(sym, factors, now);

        // 🔥 统一走 MetricsService
        this.metrics.setDynGateExtended(sym, {
          effMin0: params.effMin0,
          minNotional3s: params.minNotional3s,
          volPct: params.volPct,
          liqPct: params.liqPct,
          oiRegime: params.oiRegime,
          cooldownMs: params.cooldownMs,
          breakoutBandPct: params.breakoutBandPct,
        });

        await this.writeSnapshot(sym, params);
        this.logger.log(
          `[dyn.gate] ${sym} effMin0=${params.effMin0.toFixed(2)} minNotional3s=${params.minNotional3s} ` +
            `cooldown=${params.cooldownMs} band=${params.breakoutBandPct.toFixed(4)} ` +
            `(volPct=${factors.volPct.toFixed(2)} liqPct=${factors.liqPct.toFixed(2)} oiRegime=${factors.oiRegime})`,
        );
      } catch (e) {
        this.logger.error(
          `[dyn.gate] ${sym} update failed: ${(e as Error).message}`,
        );
      }
    }
  }

  /* ===================== 核心：计算环境因子 ===================== */
  private async computeEnvFactors(
    sym: string,
    now: number,
  ): Promise<EnvFactors> {
    // ---- 1) 读取 5m / 15m k线（来自 exchange WS）----
    const k5Key = this.streams.buildKlineKey(sym, '5m'); // ws:{sym}:kline5m
    const k15Key = this.streams.buildKlineKey(sym, '15m'); // ws:{sym}:kline15m
    const k5 = await this.streams.readLatestAsObjects(k5Key, VOL_LOOKBACK);
    const k15 = await this.streams.readLatestAsObjects(k15Key, VOL_LOOKBACK);

    // 计算 5m/15m 的“当下”波动与历史分布分位
    const vol5 = this.volNowAndHist(k5);
    const vol15 = this.volNowAndHist(k15);
    // 综合：取较大的那一个更保守
    const volPct = clip01(Math.max(vol5.pct, vol15.pct));

    // 成交额（报价币）分位（若有 volCcyQuote，没有则退化用 vol*close 近似）
    const liq5 = this.liqNowAndHist(k5);
    const liq15 = this.liqNowAndHist(k15);
    const liqPct = clip01(Math.max(liq5.pct, liq15.pct));

    // ---- 2) OI regime：用最近 OI 斜率/涨跌判断 ----
    const oiKey = this.streams.buildKey(sym, 'oi'); // ws:{sym}:oi
    const oiRows = await this.streams.readLatestAsObjects(oiKey, OI_LOOKBACK);
    // --- OI Regime：取 45min 窗口，稳健些 ---
    // const lookbackMs = 45 * 60_000;
    const lookbackMs = 90 * 60_000; // 近 90 分钟，样本更充足
    const series = await this.readOiSeries(sym, now - lookbackMs, now);
    const rawRegime = this.calcOiRegime(series); // 仍然用严格 AND 阈值
    let oiRegime = this.enforcePersistence(sym, rawRegime, now);

    // 环境共识：低波动/低活跃直接屏蔽 OI 影响
    if (volPct < 0.4 || liqPct < 0.4) oiRegime = 0;

    // ---- 3) Funding 事件近窗 ----
    const fundStateKey = this.streams.buildOutKey(sym, 'state:funding');
    const fundState = await this.streams.hgetall(fundStateKey);
    const eventFlag = this.computeFundingEventFlag(fundState!, now);

    // ---- 4) 信号速率异常：近 60s / 近 15m 基线 ----
    const sigKey = this.streams.buildOutKey(sym, 'signal:detected'); // WindowWorker 的输出
    const rateExc = await this.computeSignalRateExc(sigKey, now);

    return { volPct, liqPct, rateExc, eventFlag, oiRegime, updatedAt: now };
  }

  /* ===================== 因子 → 动态参数映射 ===================== */
  private mapFactorsToParams(
    sym: string,
    f: EnvFactors,
    now: number,
  ): DynGateParams {
    // TODO: base 可从 symbol.config.ts 读取（这里先放默认）
    const baseMinStrength = 0.65;
    const baseMinNotional3s = 2_000;
    const baseCooldownMs = 6_000;
    const baseBandPct = 0.02; // 2.0%
    const baseDedupMs = 3_000;

    // 强度门槛：高波动/事件/速率异常/加减仓 → 抬高
    const effMin0 = Math.min(
      0.78, // 顶
      Math.max(
        0.6, // 底
        baseMinStrength +
          0.05 * (f.volPct > 0.8 ? 1 : 0) +
          0.05 * Math.min(1, f.rateExc) +
          0.08 * f.eventFlag +
          0.02 * (f.oiRegime !== 0 ? 1 : 0), // OI 只加一点点
      ),
    );

    // 活跃度门槛：随 liqPct 上下浮动
    const minNotional3s = Math.max(
      baseMinNotional3s,
      Math.round(baseMinNotional3s * (0.9 + 0.35 * f.liqPct)),
    );

    // 位移阈：随波动提高
    const minMoveBp = Math.round(2 + 4 * f.volPct); // 2~6 bp
    const minMoveAtrRatio = +(0.15 + 0.2 * f.volPct).toFixed(3); // 0.15~0.35

    // 冷却：事件/速率异常 → 拉长
    const cooldownMs = Math.round(
      baseCooldownMs * (1 + 0.6 * Math.min(1, f.rateExc) + 0.6 * f.eventFlag),
    );

    // 突破确认带宽：高波动 → 略增
    const breakoutBandPct = +Math.min(
      0.05,
      baseBandPct * (1 + 0.5 * f.volPct),
    ).toFixed(4);

    return {
      effMin0,
      minNotional3s,
      minMoveBp,
      minMoveAtrRatio,
      cooldownMs,
      dedupMs: baseDedupMs,
      breakoutBandPct,

      volPct: f.volPct,
      liqPct: f.liqPct,
      rateExc: f.rateExc,
      eventFlag: f.eventFlag,
      oiRegime: f.oiRegime,

      updated_at: now,
      version: 'v1.1',
    };
  }

  /* ===================== 写入快照 + 审计流 ===================== */
  private async writeSnapshot(sym: string, p: DynGateParams) {
    const hashKey = `dyn:gate:{${sym}}`;
    const logKey = `dyn:gate:log:{${sym}}`;

    await this.streams.hset(hashKey, {
      ...Object.fromEntries(Object.entries(p).map(([k, v]) => [k, String(v)])),
    });
    await this.streams.xadd(
      logKey,
      {
        ...Object.fromEntries(
          Object.entries(p).map(([k, v]) => [k, String(v)]),
        ),
      },
      { maxlenApprox: 2000 },
    );
  }

  /* ===================== 具体计算（波动/活跃度/OI/事件/速率） ===================== */

  /** 计算“当前波动值”与“在历史分布中的分位”，基于 k线数组 */

  private volNowAndHist(rows: Record<string, string>[]): {
    now: number;
    pct: number;
  } {
    // 用 ATR 近似：hlRange 与 prevClose 的 gap
    if (!rows.length) return { now: 0, pct: 0.5 };
    const nums = rows
      .map((r) => ({
        h: safeNum(r.h ?? r.high),
        l: safeNum(r.l ?? r.low),
        c: safeNum(r.c ?? r.close),
      }))
      .filter(
        (x) =>
          Number.isFinite(x.h) && Number.isFinite(x.l) && Number.isFinite(x.c),
      );

    if (nums.length < 3) return { now: 0, pct: 0.5 };

    const trSeries: number[] = [];
    for (let i = 1; i < nums.length; i++) {
      const prev = nums[i - 1],
        cur = nums[i];
      const tr = Math.max(
        cur.h - cur.l,
        Math.abs(cur.h - prev.c),
        Math.abs(cur.l - prev.c),
      );
      // 归一到 bp（相对价格），减少面值影响
      const denom = Math.max(cur.c, EPS);
      trSeries.push((tr / denom) * 10_000); // bp
    }
    const nowVal = trSeries[trSeries.length - 1] ?? 0;
    const hist = trSeries.slice(0, -1); // 历史分布
    const pct = pctRank(hist.length ? hist : trSeries, nowVal);
    return { now: nowVal, pct };
  }

  /** 计算“当前成交额分位”：优先 volCcyQuote，否则 vol*close 近似 */
  private liqNowAndHist(rows: Record<string, string>[]): {
    now: number;
    pct: number;
  } {
    if (!rows.length) return { now: 0, pct: 0.5 };
    const ser = rows
      .map((r) => {
        const q = safeNum((r as any).volCcyQuote ?? (r as any).quoteVol);
        if (Number.isFinite(q)) return q;
        // 退化：vol * close
        const vol = safeNum(r.vol);
        const c = safeNum(r.c ?? r.close);
        if (Number.isFinite(vol) && Number.isFinite(c)) return vol * c;
        return NaN;
      })
      .filter(Number.isFinite) as number[];

    if (ser.length < 3) return { now: 0, pct: 0.5 };
    const nowVal = ser[ser.length - 1]!;
    const hist = ser.slice(0, -1);
    const pct = pctRank(hist.length ? hist : ser, nowVal);
    return { now: nowVal, pct };
  }

  /** 自适应判别 OI regime：-1/0/1
   * 思路：
   *  - 取最近 60min 的按分钟序列，分成两个 15min 桶：A=最近15min，B=之前的15min
   *  - 比较 A/B 的相对变化率 pct = (A-B)/max(中位数,1)
   *  - 同时计算单位时间差分的中位数绝对偏差 MAD，做一个 z-like = lastDiff / (1.4826*MAD+eps)
   *  - 满足：|pct| > pctTh 且 |z| > zTh 时触发；否则 0
   */
  private calcOiRegime(series: Array<{ ts: number; oi: number }>): -1 | 0 | 1 {
    if (!series || series.length < 8) {
      this.logger.debug(
        `[dyn.oi] insufficient series: n=${series?.length ?? 0}`,
      );
      return 0;
    }

    const vals = series.map((s) => s.oi);
    const median =
      (() => {
        const s = vals.slice().sort((a, b) => a - b);
        const n = s.length,
          m = n >> 1;
        return n % 2 ? s[m] : 0.5 * (s[m - 1] + s[m]);
      })() || 1;

    const lastTs = series[series.length - 1].ts;
    const Afrom = lastTs - 15 * 60_000;
    const Bfrom = lastTs - 30 * 60_000;
    const Bto = lastTs - 15 * 60_000;

    const Avals = series.filter((p) => p.ts >= Afrom).map((p) => p.oi);
    const Bvals = series
      .filter((p) => p.ts >= Bfrom && p.ts < Bto)
      .map((p) => p.oi);

    const A = Avals.length
      ? Avals.reduce((s, x) => s + x, 0) / Avals.length
      : NaN;
    const B = Bvals.length
      ? Bvals.reduce((s, x) => s + x, 0) / Bvals.length
      : NaN;

    if (!Number.isFinite(A) || !Number.isFinite(B)) {
      this.logger.debug(
        `[dyn.oi] no bucket data: A=${Avals.length}, B=${Bvals.length}`,
      );
      return 0;
    }

    const pct = (A - B) / Math.max(1, median);

    const diffs: number[] = [];
    for (let i = 1; i < series.length; i++)
      diffs.push(series[i].oi - series[i - 1].oi);
    const abs = diffs.map(Math.abs).sort((a, b) => a - b);
    const mad = abs.length ? abs[abs.length >> 1] : 0;
    const scale = 1.4826 * mad + 1e-9;
    const lastDiff = diffs.length ? diffs[diffs.length - 1] : 0;
    const zLike = lastDiff / scale;

    this.logger.debug(
      `[dyn.oi] n=${series.length} A=${A.toFixed(0)} B=${B.toFixed(0)} ` +
        `pct=${(pct * 100).toFixed(2)}% z=${zLike.toFixed(2)}`,
    );

    const pctTh = 0.012;
    const zTh = 2.0;

    if (pct >= pctTh && zLike >= zTh) return 1;
    if (pct <= -pctTh && zLike <= -zTh) return -1;
    return 0;
  }
  // 在 calcOiRegime 之后，加一个“持续性滤波”
  private enforcePersistence(
    sym: string,
    raw: -1 | 0 | 1,
    now: number,
  ): -1 | 0 | 1 {
    const cur = this.oiState.get(sym) ?? { dir: 0, since: now };
    if (raw === 0) {
      // 一旦回到0，清空状态更保守
      this.oiState.set(sym, { dir: 0, since: now });
      return 0;
    }
    if (raw === cur.dir) {
      // 同向累计时长
      const heldMs = now - cur.since;
      const NEED_MS = 10 * 60_000; // 持续10分钟
      if (heldMs >= NEED_MS) return raw; // 通过持续性门槛
      // 未达标前，仍视作 0
      return 0;
    } else {
      // 切向，重新计时
      this.oiState.set(sym, { dir: raw, since: now });
      return 0; // 先观察，不立刻生效
    }
  }

  /** 读取最近一段时间的 OI 数列（旧→新），优先用 oiCcy，没有再用 oi */
  private async readOiSeries(sym: string, startMs: number, endMs: number) {
    const key = this.streams.buildKey(sym, 'oi'); // ws:{sym}:oi
    const rows = await this.streams.readWindowAsObjects(
      key,
      startMs,
      endMs,
      2000,
    );

    const series = rows
      .map((r) => {
        const ts = Number(r.ts);
        const raw = r.oiCcy ?? r.oi; // ★ 优先 oiCcy
        const val = Number(raw);
        return Number.isFinite(ts) && Number.isFinite(val)
          ? { ts, oi: val }
          : null;
      })
      .filter((x): x is { ts: number; oi: number } => !!x);

    // OKX 的 OI 推送不一定是固定频率，做一下“按分钟取末样本”的下采样，降噪
    const byMin = new Map<number, { ts: number; oi: number }>();
    for (const p of series) {
      const bucket = Math.floor(p.ts / 60_000);
      const prev = byMin.get(bucket);
      if (!prev || p.ts >= prev.ts) byMin.set(bucket, p);
    }
    return Array.from(byMin.values()).sort((a, b) => a.ts - b.ts);
  }

  /** funding 事件近窗（Hash state:funding:{sym} 里有 nextFundingTime/ts/rate） */
  private computeFundingEventFlag(
    hash: Record<string, string>,
    now: number,
  ): 0 | 1 {
    if (!hash) return 0;
    const nft = safeNum(hash.nextFundingTime);
    if (!Number.isFinite(nft)) return 0;
    const dt = nft - now;
    return dt >= 0 && dt <= FUNDING_NEAR_MS ? 1 : 0;
  }

  /** 信号速率异常度：近 60s 与近 15m 基线的比值-1（>=0），读取 signal:detected stream */
  private async computeSignalRateExc(
    sigKey: string,
    now: number,
  ): Promise<number> {
    const winStart = now - RATE_WIN_MS;
    const baseStart = now - RATE_BASE_MS;

    const recent = await this.streams.readWindowAsObjects(
      sigKey,
      winStart,
      now,
    );
    const base = await this.streams.readWindowAsObjects(sigKey, baseStart, now);

    const r = recent.length / (RATE_WIN_MS / 1000); // 条/秒
    const b = base.length / (RATE_BASE_MS / 1000); // 条/秒

    if (b <= EPS) return r > 0 ? 1 : 0; // 无基线但有触发，给 1
    return Math.max(0, r / b - 1); // 相对异常强度
  }
}
