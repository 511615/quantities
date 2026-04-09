from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from quant_platform.api.facade import QuantPlatformFacade
from quant_platform.common.enums.core import LabelKind, ModelFamily
from quant_platform.common.types.core import SchemaField
from quant_platform.data.contracts.market import NormalizedMarketBar
from quant_platform.datasets.builders.dataset_builder import DatasetBuilder
from quant_platform.datasets.contracts.dataset import DatasetRef, LabelSpec, SamplePolicy
from quant_platform.datasets.labeling.forward_return import ForwardReturnLabeler
from quant_platform.datasets.splits.time_series import TimeSeriesSplitPlanner
from quant_platform.features.transforms.market_features import MarketFeatureBuilder
from quant_platform.models.contracts.model_spec import ModelSpec
from quant_platform.workflows.runtime import WorkflowRuntime
from quant_platform.workflows.services import WorkflowPipelineService


@pytest.fixture
def artifact_root(tmp_path: Path) -> Path:
    return tmp_path / "artifacts"


@pytest.fixture
def facade(artifact_root: Path) -> QuantPlatformFacade:
    return QuantPlatformFacade(artifact_root)


@pytest.fixture
def workflow_runtime(artifact_root: Path) -> WorkflowRuntime:
    return WorkflowRuntime.build(artifact_root)


@pytest.fixture
def workflow_pipeline(workflow_runtime: WorkflowRuntime) -> WorkflowPipelineService:
    return WorkflowPipelineService(workflow_runtime)


@pytest.fixture
def market_bars() -> list[NormalizedMarketBar]:
    base = datetime(2024, 1, 1, tzinfo=UTC)
    closes = [100.0, 101.0, 100.5, 102.0, 103.5, 102.5]
    volumes = [10.0, 11.0, 9.5, 15.0, 16.0, 12.0]
    rows: list[NormalizedMarketBar] = []
    for idx, close in enumerate(closes):
        open_price = closes[idx - 1] if idx > 0 else close
        rows.append(
            NormalizedMarketBar(
                event_time=base.replace(hour=idx),
                available_time=base.replace(hour=idx),
                symbol="BTCUSDT",
                venue="binance",
                open=open_price,
                high=max(open_price, close) + 0.5,
                low=min(open_price, close) - 0.5,
                close=close,
                volume=volumes[idx],
            )
        )
    return rows


@pytest.fixture
def smoke_dataset_ref(facade: QuantPlatformFacade) -> DatasetRef:
    return facade.build_smoke_dataset()


@pytest.fixture
def smoke_model_spec() -> ModelSpec:
    return ModelSpec(
        model_name="elastic_net",
        family=ModelFamily.LINEAR,
        version="0.1.0",
        input_schema=[
            SchemaField(name="lag_return_1", dtype="float"),
            SchemaField(name="volume_zscore", dtype="float"),
        ],
        output_schema=[SchemaField(name="prediction", dtype="float")],
        hyperparams={"alpha": 0.001, "l1_ratio": 0.5, "random_state": 7},
    )


@pytest.fixture
def built_dataset(
    market_bars: list[NormalizedMarketBar],
    facade: QuantPlatformFacade,
) -> tuple[DatasetRef, list]:
    as_of_time = datetime(2024, 1, 5, tzinfo=UTC)
    data_ref, _ = facade.data_catalog.register_market_asset(
        asset_id="market_ohlcv_btcusdt_1h",
        source="internal",
        frequency="1h",
        rows=market_bars,
    )
    feature_result = MarketFeatureBuilder().build(
        feature_set_id="baseline_market_features",
        data_ref=data_ref,
        bars=market_bars,
        as_of_time=as_of_time,
    )
    labels = ForwardReturnLabeler().build(
        feature_result.rows,
        {bar.event_time: bar.close for bar in market_bars},
        horizon=1,
    )
    split_manifest = TimeSeriesSplitPlanner.single_split(
        timestamps=[row.timestamp for row in feature_result.rows],
        train_end_index=1,
        valid_end_index=2,
    )
    dataset_ref, samples, _ = DatasetBuilder.build_dataset(
        dataset_id="dataset_fixture",
        feature_result=feature_result,
        labels=labels,
        label_spec=LabelSpec(target_column="future_return_1", horizon=1, kind=LabelKind.REGRESSION),
        split_manifest=split_manifest,
        sample_policy=SamplePolicy(min_history_bars=10),
    )
    return dataset_ref, samples
