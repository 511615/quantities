from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from quant_platform.backtest.contracts.backtest import (
    BacktestRequest,
    BenchmarkSpec,
    CalendarSpec,
    CostModel,
    PortfolioConfig,
    StrategyConfig,
)
from quant_platform.data.contracts.ingestion import (
    ConnectorRegistration,
    DataConnector,
    IngestionCoverage,
    IngestionRequest,
    IngestionResult,
)
from quant_platform.data.contracts.market import NormalizedMarketBar
from quant_platform.common.enums.core import LabelKind
from quant_platform.training.contracts.training import (
    PredictionScope,
    TrackingContext,
    TrainerConfig,
)
from quant_platform.workflows.contracts.requests import (
    BacktestWorkflowRequest,
    DatasetAcquisitionRequest,
    DatasetBuildConfig,
    DatasetSymbolSelector,
    DatasetTimeWindow,
    PredictWorkflowRequest,
    PredictionInputRef,
    TrainWorkflowRequest,
)
from quant_platform.workflows.services.prepare import PrepareWorkflowService


class _FakeCcxtMarketConnector(DataConnector):
    def __init__(self) -> None:
        self.registration = ConnectorRegistration(
            data_domain="market",
            vendor="ccxt",
            display_name="fake ccxt integration",
            status="active",
        )

    def ingest(self, request: IngestionRequest) -> IngestionResult:
        symbol = request.identifiers[0]
        base_price = 100.0 if symbol == "BTC/USDT" else 80.0
        rows: list[dict[str, object]] = []
        cursor = request.time_range.start
        offset = 0
        while cursor <= request.time_range.end:
            price = base_price + offset
            rows.append(
                NormalizedMarketBar(
                    event_time=cursor,
                    available_time=cursor,
                    symbol=symbol,
                    venue=str(request.options.get("exchange", "okx")),
                    open=price,
                    high=price + 1.0,
                    low=price - 1.0,
                    close=price + 0.5,
                    volume=10.0 + offset,
                ).model_dump(mode="json")
            )
            cursor = (
                cursor.replace(hour=cursor.hour + 1)
                if cursor.hour < 23
                else cursor.replace(day=cursor.day + 1, hour=0)
            )
            offset += 1
        return IngestionResult(
            request_id=request.request_id,
            data_domain="market",
            vendor="ccxt",
            storage_uri="",
            normalized_uri="",
            coverage=IngestionCoverage(
                start_time=request.time_range.start,
                end_time=request.time_range.end,
                complete=True,
            ),
            metadata={
                "exchange": request.options.get("exchange", "okx"),
                "rows": rows,
            },
        )


class _FakeVectorbtPortfolio:
    def __init__(self, close: pd.DataFrame) -> None:
        self._close = close

    def value(self) -> pd.Series:
        values = [100_000.0 + float(index) * 250.0 for index in range(len(self._close.index))]
        return pd.Series(values, index=self._close.index)


class _FakeVectorbtModule:
    class Portfolio:
        @staticmethod
        def from_orders(*, close, **kwargs):  # type: ignore[no-untyped-def]
            _ = kwargs
            return _FakeVectorbtPortfolio(close)


def test_prepare_workflow_materializes_ccxt_dataset_end_to_end(workflow_runtime) -> None:
    workflow_runtime.ingestion_service.register(_FakeCcxtMarketConnector())
    service = PrepareWorkflowService(workflow_runtime)
    request = DatasetAcquisitionRequest(
        request_name="ccxt integration dataset",
        data_domain="market",
        dataset_type="training_panel",
        asset_mode="multi_asset",
        time_window=DatasetTimeWindow(
            start_time=datetime(2024, 1, 1, 0, tzinfo=UTC),
            end_time=datetime(2024, 1, 1, 6, tzinfo=UTC),
        ),
        symbol_selector=DatasetSymbolSelector(
            symbol_type="spot",
            selection_mode="explicit",
            symbols=["BTC/USDT", "ETH/USDT"],
            symbol_count=2,
            tags=["ccxt", "integration"],
        ),
        source_vendor="ccxt",
        exchange="okx",
        frequency="1h",
        build_config=DatasetBuildConfig(
            feature_set_id="baseline_market_features",
            label_horizon=1,
            label_kind=LabelKind.REGRESSION,
            split_strategy="time_series",
        ),
    )

    prepare_request = service.build_prepare_request_from_dataset_request(request)
    result = service.prepare(prepare_request)

    assert result.dataset_ref.dataset_id == "ccxt_integration_dataset"
    assert result.dataset_ref.dataset_id in workflow_runtime.dataset_store
    assert Path(result.dataset_manifest_uri).exists()
    manifest_payload = json.loads(Path(result.dataset_manifest_uri).read_text(encoding="utf-8"))
    acquisition_profile = manifest_payload["acquisition_profile"]
    assert acquisition_profile["source_vendor"] == "ccxt"
    assert acquisition_profile["exchange"] == "okx"
    assert acquisition_profile["symbols"] == ["BTC/USDT", "ETH/USDT"]
    assert result.dataset_ref.entity_scope == "multi_asset"
    assert result.dataset_ref.entity_count == 2


def test_backtest_workflow_runs_vectorbt_when_explicitly_selected(
    monkeypatch,
    facade,
) -> None:
    prepare_result = facade.prepare_workflow.prepare(facade.prepare_workflow.build_smoke_request())
    train_result = facade.train_workflow.train(
        TrainWorkflowRequest(
            dataset_ref=prepare_result.dataset_ref,
            model_specs=[facade.build_smoke_model_spec()],
            trainer_config=TrainerConfig(
                runner="local",
                epochs=1,
                batch_size=32,
                deterministic=True,
            ),
            tracking_context=TrackingContext(
                backend="file",
                experiment_name="ccxt-vectorbt-integration",
                tracking_uri=str(facade.artifact_root / "tracking"),
            ),
            seed=7,
            run_id_prefix="ccxt-vectorbt-integration",
        )
    )
    predict_result = facade.predict_workflow.predict(
        PredictWorkflowRequest(
            dataset_ref=prepare_result.dataset_ref,
            fit_results=[train_result.items[0].fit_result],
            prediction_scope=PredictionScope(
                scope_name="full",
                as_of_time=prepare_result.dataset_ref.feature_view_ref.as_of_time,
            ),
        )
    )

    def _fake_import(name: str):
        if name == "pandas":
            return pd
        if name == "vectorbt":
            return _FakeVectorbtModule
        raise ModuleNotFoundError(name)

    monkeypatch.setattr(
        "quant_platform.backtest.engines.vectorbt_adapter.importlib.import_module",
        _fake_import,
    )

    result = facade.backtest_workflow.backtest(
        BacktestWorkflowRequest(
            prediction_inputs=[
                PredictionInputRef(
                    model_name=train_result.items[0].model_name,
                    run_id=train_result.items[0].fit_result.run_id,
                    prediction_frame_uri=predict_result.items[0].prediction_frame_uri,
                )
            ],
            backtest_request_template=BacktestRequest(
                prediction_frame_uri=predict_result.items[0].prediction_frame_uri,
                research_backend="vectorbt",
                strategy_config=StrategyConfig(
                    name="sign_strategy",
                    portfolio_method="proportional",
                ),
                portfolio_config=PortfolioConfig(
                    initial_cash=100000.0,
                    max_gross_leverage=1.0,
                    max_net_leverage=1.0,
                    max_position_weight=1.0,
                ),
                cost_model=CostModel(fee_bps=0.0, slippage_bps=0.0),
                benchmark_spec=BenchmarkSpec(name="buy_and_hold", symbol="BTCUSDT"),
                calendar_spec=CalendarSpec(timezone="UTC", frequency="1h"),
            ),
            dataset_ref=prepare_result.dataset_ref,
            benchmark_name="vectorbt_explicit",
            data_source="smoke",
            market_bars=facade.build_smoke_market_bars(),
        )
    )

    assert result.items[0].research_backtest_result.engine_type == "research"
    diagnostics = facade.store.read_json(result.items[0].research_backtest_result.diagnostics_uri or "")
    assert diagnostics["performance_metrics"]["cumulative_return"] > 0.0
    assert Path(result.items[0].research_backtest_result.report_uri).exists()
