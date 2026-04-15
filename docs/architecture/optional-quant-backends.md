# 可选量化后端接入说明：CCXT / vectorbt / skfolio

这轮接入的原则是：默认主链保持轻量，扩展能力显式开启。

默认状态仍然是：

- 市场数据：继续使用仓内已有 connector，例如 Binance、Bitstamp、smoke data
- 研究回测：`research_backend = "native"`
- 组合构建：`portfolio_method = "proportional"`

`CCXT`、`vectorbt`、`skfolio` 都是可选能力。它们不会在默认安装、默认回测、官方多模态 benchmark 里自动接管主流程。

## 安装方式

只安装你当前需要的部分：

```powershell
python -m pip install -e .[exchange]
python -m pip install -e .[research_backends]
python -m pip install -e .[portfolio_opt]
```

通常建议先只装 CCXT：

```powershell
python -m pip install -e .[exchange]
```

原因很简单：`exchange` 只负责交易所数据，重量可控；`vectorbt` 和 `skfolio` 更偏研究/优化栈，应该只在明确要跑这些路径时再装。

## CCXT 市场数据

CCXT 现在是市场数据域的一等 vendor：

- `data_domain`: `market`
- `source_vendor`: `ccxt`
- `exchange`: CCXT 交易所 id，例如 `okx`、`binance`
- `symbols`: 交易所原生格式，例如 `BTC/USDT`
- `frequency`: 现有 K 线频率，例如 `1h`、`1d`
- `symbol_type`: `spot`、`future`、`futures`、`swap`、`margin`

实际链路是：

```text
DatasetAcquisitionRequest
  -> MarketAcquisitionHandler
  -> DomainIngestionCoordinator
  -> CcxtMarketConnector
  -> ingestion cache
  -> PrepareWorkflowService
```

这意味着：

- `CcxtMarketConnector` 只负责交易所 IO 和标准化。
- 缓存、snapshot、增量补洞仍然由 `DomainIngestionCoordinator` 管。
- CCXT cache 已按 `exchange + market_type + symbol` 分开，`okx:spot:BTC/USDT` 不会和 `binance:spot:BTC/USDT` 或 `okx:swap:BTC/USDT` 混在一起。
- 如果没安装 `ccxt`，会明确报 `dependency_missing`，不会偷偷切回别的数据源。

已做真实 smoke：

```text
vendor=ccxt
exchange=okx
symbol=BTC/USDT
frequency=1h
第一次请求：live_fetch
第二次请求：cache_hit
```

## vectorbt 研究后端

`vectorbt` 不是默认官方回测引擎，而是一个旁路研究后端。

显式开启：

```json
{
  "research_backend": "vectorbt"
}
```

默认仍然是：

```json
{
  "research_backend": "native"
}
```

它适合：

- 多资产、矩阵化比较清楚的信号面板
- 想把 native 研究引擎和 vectorbt 做并排对比
- 明确知道这不是官方多模态 rolling benchmark 的默认路径

它不适合：

- 单纯想跑官方 benchmark
- 输入不是规整矩阵，或者市场数据/信号无法按时间和资产对齐
- 没安装 `research_backends` extra

如果依赖缺失或输入不满足矩阵条件，应该显式失败，不会静默退回 native。

## skfolio 组合构建

`skfolio` 是可选组合构建方法，用来把信号转换成更受约束的目标权重。

显式开启：

```json
{
  "portfolio_method": "skfolio_mean_risk"
}
```

默认仍然是：

```json
{
  "portfolio_method": "proportional"
}
```

第一版支持范围刻意收窄：

- 必须是多资产
- 必须能构造同频、对齐后的 return matrix
- 必须有足够历史样本
- 只映射现有 `RiskConstraintSet` 中最核心的约束
- 单资产、样本太短、矩阵缺失、依赖缺失都会明确拒绝

它适合：

- 你想测试“多个信号如何形成受约束组合”
- 你有足够多资产和历史窗口
- 你关心 gross、net、单名上限、long-only/long-short 这类约束

它不适合：

- 单资产 BTC 回测
- 官方多模态 benchmark 默认链
- 数据还不够稳定时强行优化组合

## 借鉴 Qlib 的地方

本项目没有引入 Qlib 包本身，只保守借鉴它的分层方法：

- data adapter：交易所或 vendor IO
- handler：把请求变成 panel，并处理窗口/多资产聚合
- alpha/signal：模型输出转换成可交易信号
- portfolio construction：信号权重转换成目标权重
- backtest/report：执行、评估、产物展示

这样做的价值是：CCXT、vectorbt、skfolio 不会散落在多模态代码里，也不会让 `PrepareWorkflowService` 或 backtest engine 继续无限膨胀。

## 使用建议

优先用 CCXT：

- 需要 OKX 或其它仓内 connector 没覆盖的交易所
- 想用 `BTC/USDT` 这种交易所原生 symbol
- 想让外部交易所 K 线也进入现有 dataset/cache/workflow 主链

谨慎用 vectorbt：

- 只在需要旁路研究对比时开启
- 不要把它当官方 benchmark 默认引擎

谨慎用 skfolio：

- 只在多资产且数据对齐充分时开启
- 不要让它替代单资产 proportional 默认路径

一句话总结：这三个接入对项目是“扩展能力”，不是“替换主线”。默认主线保持轻，显式开启才变重。
