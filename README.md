1. 信号日志字段（[signal] ...）
   [signal] inst=BTC-USDT-SWAP dir=buy strength=0.792 src=flow
   notional3s=17894071 (buy=17889579 sell=4492 min=3000000)
   delta3s=17885086.70 zLike= buyShare3s=1.000 breakout=
   | params: cm=1 minNotional3s=3000000 band=0.012 dynK=1.8 liqK=1.2
   minStrength=0.7 cb=0.1 cooldown=9000 dedup=4000 dynAbsDelta=12376520

inst
合约/标的，例如 BTC-USDT-SWAP
dir
信号方向，buy 或 sell
strength
信号强度，范围 0~1；越高表示越强
src
信号来源 detector：flow（资金流）、delta（统计异常）、breakout（价格突破）
notional3s
近 3 秒总名义额（计价币）
(buy=..., sell=...)
近 3 秒买入 / 卖出名义额拆分
min=...
动态参数里的 最小名义额门槛（低于此值时信号通常不触发）
delta3s
近 3 秒买卖差（buy - sell）
zLike
类似 Z-score 的统计量（大于 2 表示较强的统计异常）
buyShare3s
买方占比：buy / (buy+sell)
breakout
是否突破，1 表示突破成立，空表示无
cm
contractMultiplier：合约乘数，用于把成交量换算为名义额
minNotional3s
生效的最小 3s 名义额门槛
band
突破带宽（相对价格百分比），例如 0.012 表示 ±1.2%
dynK
动态阈值缩放系数（越大表示阈值更严格）
liqK
流动性缩放系数（按深度/流动性调整的权重）
minStrength
信号最小强度门槛（低于此值不会发出信号）
cb
consensusBoost：共识折扣系数，多 detector 同向时可降低门槛
cooldown
同向信号冷却时间（毫秒）
dedup
去重窗口（毫秒），避免短期内重复信号
dynAbsDelta
动态参考指标：EWMA

2. 环境更新日志字段（[dyn.gate] ...）
   [dyn.gate] BTC-USDT-SWAP effMin0=0.70 minNotional3s=2000
   cooldown=9600 band=0.0241 (volPct=0.41 liqPct=0.00 oiRegime=0)

字段解释:
effMin0
动态计算的有效信号门槛（最终 minStrength 起点），例如 0.70
minNotional3s
动态设定的最小 3s 名义额门槛（低于此值不触发信号）
cooldown
动态计算后的冷却时间（毫秒），高波动期通常更长
band
动态突破带宽，通常基于波动度调整
volPct
波动百分位，0~1，数值越高说明当前波动率接近历史高分位
liqPct
流动性百分位，0~1，越低表示流动性稀薄
oiRegime
持仓量 regime，0 表示中性，还没识别出明显 regime；后期可能为 ±1 表示强趋势/强背离状态

3. 使用建议
   • 看 strength + effMin0：
   - strength >= effMin0 才是有效信号
   - effMin0 会随环境动态变化，比固定门槛更合理
     • 看 notional3s vs minNotional3s：
   - 小币种低于门槛会被过滤
   - 大币种必须显著超过门槛才算强信号
     • 看 volPct 与 band：
   - 高波动时 band 会变宽，避免假突破
   - volPct≈1.0 表示市场极度活跃，冷却时间也更长
     • 看 oiRegime 与 liqPct：
   - OI regime 用于长期趋势判别（目前初版大多为 0）
   - liqPct≈0 表示盘口几乎没深度，信号可靠性要打折

4. [MarketEnvUpdaterService] [dyn.oi] n=18 A=680853 B=679863 pct=0.15% z=0.56
   n=18
   已收集到的样本点数量（比如近 18 个 5m/15m 桶里的 OI 数据）
   A=680853
   序列前半段（历史一侧）的平均 OI 值或加权和
   B=679863
   序列后半段（近期一侧）的平均 OI 值或加权和
   pct=0.15%
   (B - A) / A，即近期 OI 相对过去的变化百分比
   z=0.56
   类 Z-score，表示 OI 变化相对于序列波动的标准化程度；绝对值越大越“异常”
