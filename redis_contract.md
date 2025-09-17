# Redis 契约表（Contract Spec）

## 命名与总则

- **Key 前缀**：统一由 `RedisClient.keyPrefix` 注入（如 `dev:` / `prod:`），下文省略不写。
- **哈希标签**：为 Cluster 友好，所有按标的分片的 key 均使用：`{<instId>}`，如 `ws:{BTC-USDT-SWAP}:trades`。
- **时间戳**：**毫秒 UTC**。  
  - 原子事件 `ts`=交易所毫秒；  
  - Bar `ts`=**bar close**（例：1m bar 的 `ts`=该分钟结束时刻）。
- **数值类型**：**全部以 string 写入**（价格、数量、比率等），避免小数精度问题。  
- **Streams 修剪**：统一用 `MAXLEN ~` 近似修剪，防止内存无限长。
- **消费语义**：默认 **At-least-once**；下游靠幂等键去重。

---

## 1) 原始行情流（WS → Bridge）

### 1.1 trades（逐笔成交）
- **Key**：`ws:{instId}:trades`（MAXLEN~ **10_000**）
- **Fields**：
  | 字段 | 说明 |
  |---|---|
  | `type` | 固定 `market.trade` |
  | `src` | 数据源，例如 `okx` |
  | `instId` | 交易对/合约 ID |
  | `ts` | 交易所毫秒 |
  | `px` | 成交价 |
  | `qty` | 成交量 |
  | `side` | `buy`/`sell` |
  | `taker` | `1`（吃单）/`0`（挂单），OKX 逐笔一般为 `1` |
  | `tradeId` | 交易所逐笔 ID（可空） |
  | `recvTs` | 本地接收毫秒（可空） |
  | `ingestId` | 采集链路追踪 ID（可空） |

### 1.2 book（盘口轻量帧）
- **Key**：`ws:{instId}:book`（MAXLEN~ **2_000**）
- **Fields**：
  | 字段 | 说明 |
  |---|---|
  | `type`/`src`/`instId`/`ts`/`recvTs`/`ingestId` | 同上 |
  | `bid1.px` / `bid1.sz` | 最优买一价/量 |
  | `ask1.px` / `ask1.sz` | 最优卖一价/量 |
  | `bidSz10` / `askSz10` | 前 10 档合计量（可空） |
  | `spread` | `ask1.px - bid1.px`（可空） |
  | `snapshot` | `1`/`0` |
  | `u` / `pu` | 序号（OKX） |
  | `checksum` | 校验（OKX） |
  | `action` | `snapshot`/`update` |

### 1.3 ticker（可选）
- **Key**：`ws:{instId}:ticker`（MAXLEN~ **2_000**）
- **Fields**：`last`、`bid1.*`、`ask1.*`、`open24h`、`high24h`、`low24h`、`vol24h`、`volCcy24h` 等。

### 1.4 oi（持仓量）
- **Key**：`ws:{instId}:oi`（MAXLEN~ **1_000**）
- **Fields**：`oi`、`oiCcy`，以及通用字段。

### 1.5 funding（资金费）
- **Key**：`ws:{instId}:funding`（MAXLEN~ **1_000**）
- **Fields**：`rate`、`nextFundingTime?`，以及通用字段。

### 1.6 kline（若从交易所订阅）
- **Key**：`ws:{instId}:kline{tf}`（如 `kline1m`，MAXLEN~ **2_000**）
- **Fields**：`tf`、`o/h/l/c/vol`、`confirm?`、通用字段。
- **说明**：系统内**还是以自聚合 1m 为准**；交易所 kline 仅作校验/回补。

---

## 2) 窗口条（本地封桶）

### 2.1 进行中窗口（Hash）
- **Key**：`win:state:1m:{instId}`（Hash）
- **Fields**：
  | 字段 | 说明 |
  |---|---|
  | `startTs` / `closeTs` | 当前窗口起止（毫秒） |
  | `open`/`high`/`low`/`last` | O/H/L/C（last 表示当前最新价） |
  | `vol` | 总量 |
  | `vbuy`/`vsell` | 买/卖量 |
  | `vwapNum`/`vwapDen` | 计算 VWAP 用 |
  | `tickN` | 本窗口内逐笔条数 |

### 2.2 已封 1m 条（Stream）
- **Key**：`win:1m:{instId}`（MAXLEN~ **2_000**）
- **Fields**：
  | 字段 | 说明 |
  |---|---|
  | `ts` | **bar close** 毫秒 |
  | `open`/`high`/`low`/`close` | OHLC |
  | `vol` / `vbuy` / `vsell` | 量能 |
  | `vwap` | `vwapNum / vwapDen` |
  | `tickN` | 聚合的逐笔数 |
  | `gap` | `0/1`（是否存在缺口/回补） |

### 2.3 上位 TF（Stream）
- **Key**：`win:{tf}:{instId}`（如 `win:5m:{sym}` / `win:15m:{sym}`；MAXLEN~ **2_000**）  
- **Fields**：同 1m；由 1m 向上滚动生成。

---

## 3) 指标帧（Features）

- **Key**：`feat:{tf}:{instId}`（如 `feat:1m:{sym}` / `feat:5m:{sym}`；MAXLEN~ **2_000**）
- **Fields（示例）**：
  | 字段 | 说明 |
  |---|---|
  | `ts` | bar close 毫秒 |
  | `ema20`/`ema60` | 指数均线 |
  | `rsi14` | RSI |
  | `atr14` | ATR |
  | `trend5m`/`trend15m` | 趋势方向（`1/-1/0`） |
  | `volZ`/`basisZ`/`oiRoc` | 量能/基差/持仓变化率 |
  | `dq.gap`/`dq.statsFresh` | 数据质量标记（`0/1`） |

---

## 4) 信号（Strategy Signals）

### detected（桶内 tick 级）
- **Key**：`signal:detected:{instId}`（MAXLEN~ **5_000**）  
- **Fields**：  
  | 字段 | 说明 |
  |---|---|
  | `ts` | 触发时刻 |
  | `kind` | 固定 `intra` |
  | `dir` | `buy`/`sell` |
  | `strength` | `[0,1]` 字符串 |
  | `evidence.*` | 证据（如 `delta3s`, `buyShare3s`） |
  | `ttlMs` | 有效期（毫秒） |
  | `strategyId` | 策略标识 |

### candidate（bar 级合成）
- **Key**：`signal:candidate:{instId}`（MAXLEN~ **5_000**）  
- **Fields**：`ts`、`kind='bar'`、`dir`、`strength`、`usedTF`、`usedStats`、`strategyId`、`dq.*`。

### accepted（通过风控，进入执行）
- **Key**：`signal:accepted:{instId}`（MAXLEN~ **5_000**）  
- **Fields**：`ts`、`dir`、`size`、`planId`、`traceId`、`weights.*`。

---

## 5) 交易大数据（REST 缓存）

- **最新快取（Hash）**：`stat:latest:{metric}:{instId}`  
  - 例：`stat:latest:retail_ls:{sym}` → `{ ts, ls }`  
  - 例：`stat:latest:elite_ls:{sym}` → `{ ts, ls }`  
  - 例：`stat:latest:oi:{sym}` → `{ ts, oi, oiRoc1m, oiRoc5m }`  
- **时间序列（Stream，可选）**：`stat:{metric}:{instId}`（MAXLEN~ **1_000**）  
- **新鲜度要求**：读使用时校验 `now - ts <= ttl(metric)`；过期则忽略/降权。

---

## 消费组（建议）

| 组名 | 订阅 Key 前缀 | 作用 |
|---|---|---|
| `cg:window` | `ws:{*}:trades`（可附带 `book`） | 1m 封桶（产生 `win:*`） |
| `cg:indicator` | `win:*` | 计算 `feat:*` |
| `cg:analysis` | `feat:*` + `signal:detected:*` | 合成 `candidate/accepted` |

---

## 幂等与审计

- **幂等键**  
  - `barId = {instId}|{tf}|{ts}`（用于 `win:*`）  
  - `featId = {instId}|{tf}|{ts}|{featVersion}`  
  - `signalId = {strategyId}|{instId}|{ts}|{kind}`  
- **审计链路**：所有写入携带 `traceId/ingestId`，订单 `tag` 带上 `planId` 与所用指标版本与新鲜度。

---

## 保留策略（MAXLEN 建议）

| 流 | MAXLEN ~ |
|---|---|
| `ws:{sym}:trades` | 10_000 |
| `ws:{sym}:book` | 2_000 |
| `ws:{sym}:kline{tf}` | 2_000 |
| `ws:{sym}:oi` / `funding` / `ticker` | 1_000 ~ 2_000 |
| `win:{tf}:{sym}` | 2_000 |
| `feat:{tf}:{sym}` | 2_000 |
| `signal:*:{sym}` | 5_000 |
| `stat:{metric}:{sym}` | 1_000 |

---

## 事件（可选 Pub/Sub）

- `event.window.sealed:{tf}:{instId}`：封桶后发布。

---

## 生产注意

- **延迟预算**：tick→detected ≤ 50–150ms；1m 封桶→candidate ≤ 50–100ms。
- **故障恢复**：优先回补 `trades` 重算窗口；兜不齐则标记 `gap=1`。  
- **Cluster**：坚持 `{instId}` 哈希标签，避免跨槽 pipeline。
- **错误通道**：无法解析/多次投递失败的消息写 `dlq:{stream}` 并告警。
