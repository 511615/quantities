from __future__ import annotations

from pathlib import Path

from quant_platform.common.enums.core import LabelKind
from quant_platform.workflows.contracts.requests import (
    DatasetAcquisitionRequest,
    DatasetBuildConfig,
    DatasetSymbolSelector,
    DatasetTimeWindow,
)
from quant_platform.workflows.services.prepare import PrepareWorkflowService


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
