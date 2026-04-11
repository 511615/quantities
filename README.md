# Quant Platform

面向数字资产量化研究的平台基础仓库。

当前版本提供：

- 模块化单体仓库骨架
- `Hydra + Pydantic` 配置体系
- 数据、特征、数据集、模型、训练、回测、Agent 的显式契约
- 最小可串联的训练、回测与 Agent smoke path
- 面向可复现研究的 manifest、digest 与 seed 管理

## 设计原则

- 研究、训练、回测、Agent 分层隔离
- 所有跨层交互都通过结构化 contract，而不是裸 `DataFrame` 或隐式全局状态
- 任何运行都可由 `config + manifest + seed + code version` 重放
- 防止数据泄漏和前视偏差是默认约束，而不是事后检查

## 快速开始

```bash
python -m pip install -e .[dev]
python -m quant_platform.cli.main info
python -m pytest
```

## Local Web Workbench

For the live multi-domain flow `request -> merged dataset -> train -> backtest`:

1. Copy `.env.example` to `.env`.
2. Set `FRED_API_KEY` in `.env` if you want real `macro/fred` ingestion.
3. Restart the backend after changing `.env`.
4. Start the API with `python -m quant_platform.webapi.main`.
5. Start the web app from `apps/web` with `npm install` and `npm run dev`.

Without `FRED_API_KEY`, the backend will explicitly fail the macro stage instead of silently falling back.

或使用脚本入口：

```bash
quant-platform info
quant-platform train smoke
quant-platform backtest smoke
quant-platform agent smoke
```

## 目录说明

- `src/quant_platform/`: 核心代码
- `conf/`: Hydra 配置
- `docs/contracts/`: 公开 contract 文档
- `tests/contract/`: 契约测试
- `tests/smoke/`: 最小链路验证

## 当前边界

这是基础设施实现，不包含生产级策略逻辑、复杂特征工程或真实交易执行。
首版重点是把系统边界、可复现性和扩展点先固定住。
