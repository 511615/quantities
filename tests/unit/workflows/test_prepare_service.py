from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from quant_platform.common.enums.core import LabelKind
from quant_platform.data.contracts.ingestion import (
    ConnectorRegistration,
    DataConnector,
    IngestionCoverage,
    IngestionRequest,
    IngestionResult,
)
from quant_platform.data.contracts.market import NormalizedMarketBar
from quant_platform.workflows.contracts.requests import (
    DatasetAcquisitionRequest,
    DatasetBuildConfig,
    DatasetSymbolSelector,
    DatasetTimeWindow,
)
from quant_platform.workflows.services.prepare import PrepareWorkflowService


class _FakeCcxtMarketConnector(DataConnector):
    def __init__(self) -> None:
        self.requests: list[IngestionRequest] = []
        self.registration = ConnectorRegistration(
            data_domain="market",
            vendor="ccxt",
            display_name="fake ccxt",
            status="active",
        )

    def ingest(self, request: IngestionRequest) -> IngestionResult:
        self.requests.append(request)
        symbol = request.identifiers[0]
        rows: list[dict[str, object]] = []
        price = 100.0 if symbol == "BTC/USDT" else 80.0
        cursor = request.time_range.start
        while cursor <= request.time_range.end:
            rows.append(
                NormalizedMarketBar(
                    event_time=cursor,
                    available_time=cursor,
                    symbol=symbol,
                    venue=str(request.options.get("exchange", "binance")),
                    open=price,
                    high=price + 1.0,
                    low=price - 1.0,
                    close=price + 0.5,
                    volume=10.0,
                ).model_dump(mode="json")
            )
            cursor = cursor.replace(hour=cursor.hour + 1) if cursor.hour < 23 else cursor.replace(day=cursor.day + 1, hour=0)
            price += 1.0
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
            metadata={"rows": rows},
        )


def test_prepare_service_builds_dataset_chain_and_artifacts(workflow_runtime) -> None:
    service = PrepareWorkflowService(workflow_runtime)

    result = service.prepare(service.build_smoke_request())

    assert result.data_asset_ref.asset_id == "market_ohlcv_btcusdt_1h"
    assert result.feature_view_ref.feature_set_id == "baseline_market_features"
    assert result.dataset_ref.dataset_id == "smoke_dataset"
    assert result.dataset_ref.dataset_id in workflow_runtime.dataset_store
    assert Path(result.dataset_manifest_uri).exists()
    assert Path(result.quality_report_uri).exists()
    artifact_kinds = {artifact.kind for artifact in result.stage_result.artifacts}
    assert artifact_kinds >= {
        "feature_view_ref",
        "dataset_ref",
        "dataset_manifest",
        "quality_report",
    }


def test_prepare_service_builds_multi_asset_request_contract(workflow_runtime) -> None:
    service = PrepareWorkflowService(workflow_runtime)

    request = DatasetAcquisitionRequest(
        request_name="multi asset smoke request",
        data_domain="market",
        asset_mode="multi_asset",
        time_window=DatasetTimeWindow(
            start_time=service.build_smoke_market_bars()[0].event_time,
            end_time=service.build_smoke_market_bars()[-1].event_time.replace(hour=23),
        ),
        symbol_selector=DatasetSymbolSelector(
            symbol_type="spot",
            selection_mode="explicit",
            symbols=["BTCUSDT", "ETHUSDT"],
            symbol_count=2,
            tags=["smoke"],
        ),
        source_vendor="internal_smoke",
        exchange="binance",
        frequency="1h",
        build_config=DatasetBuildConfig(
            feature_set_id="baseline_market_features",
            label_horizon=1,
            label_kind=LabelKind.REGRESSION,
            split_strategy="time_series",
        ),
    )

    prepared = service.build_prepare_request_from_dataset_request(request)

    assert prepared.entity_scope == "multi_asset"
    assert prepared.sample_policy.universe == "multi_asset"
    assert prepared.alignment_policy["join_key"] == ["entity_key", "timestamp"]
    assert prepared.acquisition_profile["symbols"] == ["BTCUSDT", "ETHUSDT"]


def test_prepare_service_materializes_ccxt_market_request(workflow_runtime) -> None:
    connector = _FakeCcxtMarketConnector()
    workflow_runtime.ingestion_service.register(connector)
    service = PrepareWorkflowService(workflow_runtime)
    request = DatasetAcquisitionRequest(
        request_name="ccxt multi asset request",
        data_domain="market",
        asset_mode="multi_asset",
        time_window=DatasetTimeWindow(
            start_time=datetime(2024, 1, 1, 0, tzinfo=UTC),
            end_time=datetime(2024, 1, 1, 3, tzinfo=UTC),
        ),
        symbol_selector=DatasetSymbolSelector(
            symbol_type="swap",
            selection_mode="explicit",
            symbols=["BTC/USDT", "ETH/USDT"],
            symbol_count=2,
            tags=["ccxt"],
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

    prepared = service.build_prepare_request_from_dataset_request(request)

    assert prepared.data_source == "ccxt"
    assert prepared.entity_scope == "multi_asset"
    assert prepared.acquisition_profile["source_vendor"] == "ccxt"
    assert prepared.acquisition_profile["exchange"] == "okx"
    assert prepared.acquisition_profile["symbols"] == ["BTC/USDT", "ETH/USDT"]
    assert {request.options.get("market_type") for request in connector.requests} == {"swap"}
