from __future__ import annotations

from datetime import datetime

from typing import Any

from pydantic import AliasChoices, Field, model_validator

from quant_platform.agents.contracts.base import GuardrailPolicy
from quant_platform.backtest.contracts.backtest import BacktestRequest
from quant_platform.common.enums.core import LabelKind, SplitStrategy
from quant_platform.common.types.core import ArtifactRef, FrozenModel
from quant_platform.data.contracts.data_asset import DataAssetRef
from quant_platform.data.contracts.market import NormalizedMarketBar
from quant_platform.datasets.contracts.dataset import (
    DatasetRef,
    LabelSpec,
    SamplePolicy,
    SplitManifest,
)
from quant_platform.models.contracts.model_spec import ModelSpec
from quant_platform.training.contracts.training import (
    FitResult,
    PredictionFrame,
    PredictionScope,
    TrackingContext,
    TrainerConfig,
)
from quant_platform.workflows.contracts.state import WorkflowStageName


class RollingWindowWorkflowSpec(FrozenModel):
    train_size: int = Field(gt=0)
    valid_size: int = Field(gt=0)
    test_size: int = Field(gt=0)
    step_size: int = Field(default=1, gt=0)
    embargo: int = Field(default=0, ge=0)
    purge_gap: int = Field(default=0, ge=0)
    expanding_train: bool = False


class PredictionInputRef(FrozenModel):
    model_name: str | None = None
    run_id: str | None = None
    prediction_frame_uri: str | None = None
    prediction_frame: PredictionFrame | None = None

    @model_validator(mode="after")
    def validate_reference(self) -> "PredictionInputRef":
        if self.prediction_frame_uri is None and self.prediction_frame is None:
            raise ValueError("prediction input requires prediction_frame_uri or prediction_frame")
        return self


class PrepareWorkflowRequest(FrozenModel):
    data_source: str
    asset_id: str
    frequency: str
    dataset_id: str
    feature_set_id: str
    label_spec: LabelSpec
    sample_policy: SamplePolicy
    as_of_time: datetime
    market_bars: list[NormalizedMarketBar] = Field(default_factory=list)
    split_strategy: SplitStrategy = SplitStrategy.TIME_SERIES
    split_manifest: SplitManifest | None = None
    train_end_index: int | None = None
    valid_end_index: int | None = None
    entity_scope: str = "single_asset"
    input_data_refs: list[DataAssetRef] = Field(default_factory=list)
    alignment_policy: dict[str, object] = Field(default_factory=dict)
    missing_feature_policy: dict[str, object] = Field(default_factory=dict)
    normalization_profile: dict[str, object] = Field(default_factory=dict)
    dataset_tags: list[str] = Field(default_factory=list)
    acquisition_profile: dict[str, object] = Field(default_factory=dict)


class DatasetTimeWindow(FrozenModel):
    start_time: datetime
    end_time: datetime

    @model_validator(mode="after")
    def validate_order(self) -> "DatasetTimeWindow":
        if self.end_time <= self.start_time:
            raise ValueError("time_window end_time must be greater than start_time")
        return self


class DatasetSymbolSelector(FrozenModel):
    symbol_type: str = "venue_symbol"
    selection_mode: str = "explicit"
    symbols: list[str] = Field(default_factory=list)
    symbol_count: int | None = None
    tags: list[str] = Field(default_factory=list)


class DatasetBuildConfig(FrozenModel):
    feature_set_id: str
    label_horizon: int = Field(gt=0)
    label_kind: LabelKind
    split_strategy: str = "time_series"
    sample_policy_name: str = "training_panel_strict"
    alignment_policy_name: str = "event_time_inner"
    missing_feature_policy_name: str = "drop_if_missing"
    sample_policy: dict[str, object] = Field(default_factory=dict)
    alignment_policy: dict[str, object] = Field(default_factory=dict)
    missing_feature_policy: dict[str, object] = Field(default_factory=dict)


class DatasetAcquisitionSourceRequest(FrozenModel):
    data_domain: str
    source_vendor: str | None = Field(
        default=None,
        validation_alias=AliasChoices("source_vendor", "vendor"),
    )
    exchange: str | None = None
    frequency: str
    symbol_selector: DatasetSymbolSelector | None = None
    identifier: str | None = None
    filters: dict[str, Any] = Field(default_factory=dict)

    @property
    def vendor(self) -> str:
        return self.source_vendor or "internal"


class DatasetAcquisitionRequest(FrozenModel):
    request_name: str
    data_domain: str | None = "market"
    dataset_type: str = "training_panel"
    asset_mode: str = "single_asset"
    time_window: DatasetTimeWindow
    symbol_selector: DatasetSymbolSelector | None = None
    selection_mode: str = "explicit"
    source_vendor: str | None = "internal"
    exchange: str | None = "binance"
    frequency: str | None = "1h"
    filters: dict[str, Any] = Field(default_factory=dict)
    build_config: DatasetBuildConfig
    sources: list[DatasetAcquisitionSourceRequest] = Field(default_factory=list)
    merge_policy_name: str | None = None

    @model_validator(mode="after")
    def validate_contract(self) -> "DatasetAcquisitionRequest":
        if self.sources:
            return self
        missing: list[str] = []
        if self.symbol_selector is None:
            missing.append("symbol_selector")
        if not self.source_vendor:
            missing.append("source_vendor")
        if not self.exchange:
            missing.append("exchange")
        if not self.frequency:
            missing.append("frequency")
        if missing:
            raise ValueError(
                "dataset acquisition request requires either sources[] or legacy single-domain "
                f"fields: {', '.join(missing)}"
            )
        return self

    def normalized_sources(self) -> list[DatasetAcquisitionSourceRequest]:
        if self.sources:
            return list(self.sources)
        return [
            DatasetAcquisitionSourceRequest(
                data_domain=self.data_domain or "market",
                source_vendor=self.source_vendor,
                exchange=self.exchange,
                frequency=self.frequency or "1h",
                symbol_selector=self.symbol_selector,
                filters=dict(self.filters),
            )
        ]


class TrainWorkflowRequest(FrozenModel):
    dataset_ref: DatasetRef | None = None
    model_specs: list[ModelSpec]
    trainer_config: TrainerConfig
    tracking_context: TrackingContext
    seed: int
    run_id_prefix: str = "workflow-train"
    ranking_metric: str = "mae"
    lower_is_better: bool = True


class PredictWorkflowRequest(FrozenModel):
    dataset_ref: DatasetRef | None = None
    fit_results: list[FitResult] = Field(default_factory=list)
    fit_result_refs: list[str] = Field(default_factory=list)
    prediction_scope: PredictionScope


class BenchmarkWorkflowRequest(FrozenModel):
    dataset_ref: DatasetRef | None = None
    model_specs: list[ModelSpec]
    trainer_config: TrainerConfig
    tracking_context: TrackingContext
    seed: int
    prediction_scope: PredictionScope
    rolling_window_spec: RollingWindowWorkflowSpec | None = None
    benchmark_name: str = "workflow_benchmark"
    data_source: str | None = None


class BacktestWorkflowRequest(FrozenModel):
    prediction_inputs: list[PredictionInputRef] = Field(default_factory=list)
    backtest_request_template: BacktestRequest
    dataset_ref: DatasetRef | None = None
    benchmark_name: str | None = None
    data_source: str | None = None
    market_bars: list[NormalizedMarketBar] = Field(default_factory=list)
    summary_row_metadata: dict[str, object] = Field(default_factory=dict)


class ReviewWorkflowRequest(FrozenModel):
    request_id: str
    goal: str
    task_type: str = "summarize_experiment"
    experiment_refs: list[ArtifactRef] = Field(default_factory=list)
    input_artifacts: list[ArtifactRef] = Field(default_factory=list)
    comparison_mode: str | None = None
    allowed_tools: list[str] = Field(default_factory=list)
    guardrail_policy: GuardrailPolicy = Field(default_factory=GuardrailPolicy)


class WorkflowRunRequest(FrozenModel):
    workflow_id: str
    stages: list[WorkflowStageName]
    prepare: PrepareWorkflowRequest | None = None
    train: TrainWorkflowRequest | None = None
    predict: PredictWorkflowRequest | None = None
    benchmark: BenchmarkWorkflowRequest | None = None
    backtest: BacktestWorkflowRequest | None = None
    review: ReviewWorkflowRequest | None = None
