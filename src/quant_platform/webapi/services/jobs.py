from __future__ import annotations

import json
import threading
import time
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pydantic import ValidationError

from quant_platform.api.facade import QuantPlatformFacade
from quant_platform.backtest.contracts.backtest import (
    BacktestRequest,
    BenchmarkSpec,
    CalendarSpec,
    CostModel,
    PortfolioConfig,
    StrategyConfig,
)
from quant_platform.backtest.contracts.scenario import ScenarioSpec
from quant_platform.common.hashing.digest import stable_digest
from quant_platform.common.types.core import TimeRange
from quant_platform.common.types.core import SchemaField
from quant_platform.data.contracts.data_asset import DataAssetRef
from quant_platform.datasets.contracts.dataset import DatasetRef, DatasetSample
from quant_platform.features.contracts.feature_view import FeatureViewRef
from quant_platform.models.contracts.model_spec import ModelSpec
from quant_platform.training.contracts.training import (
    PredictionFrame,
    PredictionMetadata,
    PredictionRow,
    PredictionScope,
    PredictRequest,
    TrackingContext,
    TrainerConfig,
)
from quant_platform.webapi.schemas.launch import (
    BacktestLaunchPreflightView,
    BacktestLaunchOptionsView,
    LaunchBacktestRequest,
    LaunchBacktestPreflightRequest,
    LaunchJobResponse,
    LaunchModelCompositionRequest,
    LaunchTrainRequest,
    PresetOptionView,
    TrainLaunchOptionsView,
)
from quant_platform.webapi.schemas.views import (
    BacktestTemplateView,
    DatasetAcquisitionRequest,
    DatasetAcquisitionSourceRequest,
    DatasetFusionRequest,
    DatasetFusionSourceRequest,
    DatasetPipelinePlanView,
    DatasetPipelineRequest,
    DatasetRequestOptionsView,
    DeepLinkView,
    JobListResponse,
    JobResultView,
    JobStageView,
    JobStatusView,
    ModelTemplateView,
    PipelineStageView,
    PipelineSummaryView,
    StableSummaryView,
)
from quant_platform.webapi.services.backtest_protocol import (
    OFFICIAL_BACKTEST_TEMPLATE_ID,
    OFFICIAL_DEFAULT_WINDOW_DAYS,
    OFFICIAL_MARKET_BENCHMARK_DATASET_ID,
    OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID,
    OFFICIAL_WINDOW_OPTIONS,
    build_custom_backtest_request,
    build_official_backtest_request,
    build_protocol_metadata,
    custom_backtest_template,
    derive_lookback_bucket,
    official_backtest_template,
)
from quant_platform.webapi.services.catalog import (
    OFFICIAL_MULTIMODAL_STANDARD_SCHEMA_VERSION,
    ResearchWorkbenchService,
)
from quant_platform.workflows.contracts.requests import (
    BacktestWorkflowRequest,
    DatasetAcquisitionRequest as WorkflowDatasetAcquisitionRequest,
    PredictionInputRef,
    TrainWorkflowRequest,
)


class JobExecutionError(RuntimeError):
    """Raised when a launch job cannot be executed safely."""


OFFICIAL_MIN_SOURCE_PREDICTION_ROWS = 24
OFFICIAL_MIN_ALIGNED_MULTIMODAL_ROWS = 24


@dataclass
class JobContext:
    service: JobService
    job_id: str

    def start_stage(self, name: str, summary: str = "") -> None:
        self.service._update_stage(
            self.job_id,
            name=name,
            status="running",
            summary=summary,
            started_at=datetime.now(UTC),
            finished_at=None,
        )

    def finish_stage(self, name: str, summary: str = "") -> None:
        self.service._update_stage(
            self.job_id,
            name=name,
            status="success",
            summary=summary,
            started_at=None,
            finished_at=datetime.now(UTC),
        )


@dataclass(frozen=True)
class DatasetPipelinePlan:
    requested_stages: list[str]
    final_stage: str
    fusion_enabled: bool
    training_enabled: bool


@dataclass(frozen=True)
class DatasetMaterializationResult:
    dataset_ref: DatasetRef
    dataset_manifest_uri: str
    quality_report_uri: str | None = None


class JobService:
    def __init__(
        self,
        *,
        artifact_root: Path,
        workbench: ResearchWorkbenchService,
        facade: QuantPlatformFacade,
    ) -> None:
        self.artifact_root = artifact_root.resolve()
        self.workbench = workbench
        self.facade = facade
        self.jobs_root = self.artifact_root / "webapi" / "jobs"
        self.jobs_root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="workbench-jobs")

    def list_jobs(self) -> JobListResponse:
        items = [self._read_job(path) for path in sorted(self.jobs_root.glob("*.json"))]
        items.sort(key=lambda item: item.updated_at, reverse=True)
        return JobListResponse(items=items)

    def get_job(self, job_id: str) -> JobStatusView | None:
        path = self.jobs_root / f"{job_id}.json"
        if not path.exists():
            return None
        return self._read_job(path)

    def launch_train(self, request: LaunchTrainRequest) -> LaunchJobResponse:
        return self._create_job(
            "train",
            lambda context: self._run_train_job(context, request),
        )

    def launch_backtest(self, request: LaunchBacktestRequest) -> LaunchJobResponse:
        return self._create_job(
            "backtest",
            lambda context: self._run_backtest_job(context, request),
        )

    def launch_model_composition(self, request: LaunchModelCompositionRequest) -> LaunchJobResponse:
        return self._create_job(
            "model_composition",
            lambda context: self._run_model_composition_job(context, request),
        )

    def launch_dataset_request(self, request: DatasetAcquisitionRequest) -> LaunchJobResponse:
        return self._create_job(
            "dataset_request",
            lambda context: self._run_dataset_request_job(context, request),
        )

    def launch_dataset_pipeline(self, request: DatasetPipelineRequest) -> DatasetPipelinePlanView:
        plan = self._build_dataset_pipeline_plan(request)
        launch = self._create_job(
            "dataset_pipeline",
            lambda context: self._run_dataset_pipeline_job(context, request, plan),
            initial_result=JobResultView(requested_stages=plan.requested_stages),
        )
        return DatasetPipelinePlanView(
            job_id=launch.job_id,
            status=launch.status,
            job_api_path=launch.job_api_path,
            tracking_token=launch.tracking_token,
            submitted_at=launch.submitted_at,
            requested_stages=plan.requested_stages,
            final_stage=plan.final_stage,
            fusion_enabled=plan.fusion_enabled,
            training_enabled=plan.training_enabled,
            base_request_name=request.base_request.request_name,
        )

    def get_train_options(self) -> TrainLaunchOptionsView:
        template_response = self.workbench.list_model_templates()
        model_names = sorted({item.model_name for item in template_response.items if item.deleted_at is None})
        return TrainLaunchOptionsView(
            dataset_presets=[
                PresetOptionView(
                    value="smoke",
                    label="Smoke dataset",
                    description="Fast synthetic dataset for workbench validation.",
                    recommended=True,
                ),
                PresetOptionView(
                    value="real_benchmark",
                    label="Real benchmark dataset",
                    description="Cached real-market benchmark dataset.",
                ),
            ],
            model_options=[
                PresetOptionView(
                    value=name,
                    label=name,
                    description=f"{name} option from model registry.",
                    recommended=name == "elastic_net",
                )
                for name in model_names
            ],
            template_options=[
                PresetOptionView(
                    value=item.template_id,
                    label=item.name,
                    description=item.description or f"{item.model_name} template.",
                    recommended=item.template_id == "registry::elastic_net",
                )
                for item in template_response.items
                if item.deleted_at is None
            ],
            trainer_presets=[
                PresetOptionView(
                    value="fast",
                    label="Fast",
                    description="Single-pass deterministic training for the MVP workbench.",
                    recommended=True,
                )
            ],
            default_seed=7,
            constraints={
                "max_models_per_launch": 5,
                "selection_mode": "template_preferred",
                "mutually_exclusive_fields": [["template_id", "model_names"]],
                "default_template_id": "registry::elastic_net",
                "default_dataset_preset": "smoke",
                "default_trainer_preset": "fast",
                "dataset_selector": {
                    "accepted_fields": ["dataset_id", "dataset_preset"],
                    "priority": "dataset_id_gt_dataset_preset",
                },
                "model_options_source": "registry",
                "supports_template_launch": True,
                "legacy_model_names_supported": True,
            },
        )

    def get_dataset_request_options(self) -> DatasetRequestOptionsView:
        return self.workbench.get_dataset_request_options()

    def get_backtest_options(self) -> BacktestLaunchOptionsView:
        official_template = official_backtest_template()
        return BacktestLaunchOptionsView(
            default_mode="official",
            official_template_id=official_template.template_id,
            official_multimodal_schema_version=OFFICIAL_MULTIMODAL_STANDARD_SCHEMA_VERSION,
            official_multimodal_feature_names=self.workbench.official_multimodal_feature_names_v1(),
            template_options=[official_template],
            official_window_options=[
                PresetOptionView(
                    value=str(days),
                    label=f"Recent {days}d",
                    description=f"Evaluate the latest {days} days under the official rolling benchmark.",
                    recommended=days == OFFICIAL_DEFAULT_WINDOW_DAYS,
                )
                for days in OFFICIAL_WINDOW_OPTIONS
            ],
            dataset_presets=[
                PresetOptionView(
                    value="smoke",
                    label="Smoke dataset",
                    description="Fast synthetic dataset for workbench validation.",
                    recommended=True,
                ),
                PresetOptionView(
                    value="real_benchmark",
                    label="Real benchmark dataset",
                    description="Cached real-market benchmark dataset.",
                ),
            ],
            prediction_scopes=[
                PresetOptionView(value="full", label="Full", recommended=True),
                PresetOptionView(value="test", label="Test"),
            ],
            strategy_presets=[
                PresetOptionView(
                    value="sign",
                    label="Sign strategy",
                    description="Default prediction-to-position strategy template.",
                    recommended=True,
                )
            ],
            portfolio_presets=[
                PresetOptionView(
                    value="research_default",
                    label="Research default",
                    description="Default portfolio constraints for MVP research workbench.",
                    recommended=True,
                )
            ],
            cost_presets=[
                PresetOptionView(
                    value="standard",
                    label="Standard cost",
                    description="5 bps fee plus 2 bps slippage.",
                    recommended=True,
                )
            ],
            research_backends=[
                PresetOptionView(
                    value="native",
                    label="Native research",
                    description="Current in-repo research engine.",
                    recommended=True,
                ),
                PresetOptionView(
                    value="vectorbt",
                    label="vectorbt",
                    description="Optional matrix backtest adapter for explicit side-by-side research runs.",
                ),
            ],
            portfolio_methods=[
                PresetOptionView(
                    value="proportional",
                    label="Proportional",
                    description="Current signal-normalization portfolio construction.",
                    recommended=True,
                ),
                PresetOptionView(
                    value="skfolio_mean_risk",
                    label="skfolio mean-risk",
                    description="Optional signal-informed mean-risk optimizer for multi-asset runs.",
                ),
            ],
            default_benchmark_symbol="BTCUSDT",
            default_official_window_days=OFFICIAL_DEFAULT_WINDOW_DAYS,
            constraints={
                "required_fields": ["run_id"],
                "dataset_selector": {
                    "accepted_fields": ["dataset_id", "dataset_ids", "dataset_preset"],
                    "priority": "dataset_id_gt_run_manifest_gt_dataset_preset",
                },
                "default_prediction_scope_by_mode": {
                    "official": official_template.fixed_prediction_scope,
                    "custom": "full",
                },
                "research_backend": {
                    "default": "native",
                    "allowed_values": ["native", "vectorbt"],
                },
                "portfolio_method": {
                    "default": "proportional",
                    "allowed_values": ["proportional", "skfolio_mean_risk"],
                },
                "official_window_days": {
                    "default": OFFICIAL_DEFAULT_WINDOW_DAYS,
                    "allowed_values": list(OFFICIAL_WINDOW_OPTIONS),
                    "ranking_scope": "same_window_only",
                },
                "official_locked_fields": [
                    "prediction_scope",
                    "strategy_preset",
                    "portfolio_preset",
                    "cost_preset",
                ],
                "official_template_id": official_template.template_id,
            },
        )

    def get_backtest_preflight(
        self,
        request: LaunchBacktestPreflightRequest,
    ) -> BacktestLaunchPreflightView:
        template_id = request.template_id or official_backtest_template().template_id
        if request.mode != "official":
            return BacktestLaunchPreflightView(
                compatible=True,
                mode=request.mode,
                template_id=template_id,
                official_window_days=request.official_window_days or OFFICIAL_DEFAULT_WINDOW_DAYS,
            )
        run_manifest = self.workbench.get_run_manifest(request.run_id)
        run_metadata = self.workbench.get_run_model_metadata(request.run_id)
        return self._evaluate_official_backtest_preflight(
            request=LaunchBacktestRequest(
                run_id=request.run_id,
                mode=request.mode,
                template_id=template_id,
                official_window_days=request.official_window_days,
            ),
            run_manifest=run_manifest,
            run_metadata=run_metadata,
        )

    def _create_job(
        self,
        job_type: str,
        runner: Callable[[JobContext], JobResultView],
        *,
        initial_result: JobResultView | None = None,
    ) -> LaunchJobResponse:
        job_id = uuid.uuid4().hex
        now = datetime.now(UTC)
        status = JobStatusView(
            job_id=job_id,
            job_type=job_type,
            status="queued",
            created_at=now,
            updated_at=now,
            stages=[],
            result=initial_result or JobResultView(),
            error_message=None,
        )
        self._write_status(status)
        self._executor.submit(self._execute_job, job_id, runner)
        return LaunchJobResponse(
            job_id=job_id,
            status="queued",
            job_api_path=f"/api/jobs/{job_id}",
            tracking_token=job_id,
            submitted_at=now,
        )

    def _execute_job(
        self,
        job_id: str,
        runner: Callable[[JobContext], JobResultView],
    ) -> None:
        try:
            self._update_status(job_id, status="running", error_message=None)
            result = runner(JobContext(service=self, job_id=job_id))
            self._update_status(job_id, status="success", result=result, error_message=None)
        except Exception as exc:  # noqa: BLE001
            self._mark_active_stage_failed(job_id, str(exc))
            self._update_status(job_id, status="failed", error_message=str(exc))

    def _run_train_job(self, context: JobContext, request: LaunchTrainRequest) -> JobResultView:
        template = self._resolve_template_for_request(request)
        if request.dataset_id:
            context.start_stage("prepare", f"Loading dataset '{request.dataset_id}'")
            dataset_ref = self._load_dataset_from_artifacts(request.dataset_id)
            context.finish_stage("prepare", f"Loaded dataset '{request.dataset_id}'")
        else:
            dataset_preset = request.dataset_preset or (template.dataset_preset if template else "smoke")
            prepare_request, dataset_name = self._prepare_request_for_dataset(dataset_preset)
            context.start_stage("prepare", f"Preparing dataset preset: {dataset_name}")
            prepare_result = self.facade.prepare_workflow.prepare(prepare_request)
            dataset_ref = prepare_result.dataset_ref
            context.finish_stage("prepare", f"Prepared dataset '{dataset_ref.dataset_id}'")

        run_ids, fit_result_uris, readiness = self._execute_train_stage(
            context,
            dataset_ref=dataset_ref,
            request=request,
            stage_name="train",
        )
        return JobResultView(
            dataset_id=dataset_ref.dataset_id,
            run_ids=run_ids,
            fit_result_uris=fit_result_uris,
            summary=StableSummaryView(
                status=("warning" if readiness.readiness_status == "warning" else "success"),
                headline=f"Training launched on dataset {dataset_ref.dataset_id}",
                warnings=readiness.warnings,
            ),
        )

    def _run_backtest_job(
        self,
        context: JobContext,
        request: LaunchBacktestRequest,
    ) -> JobResultView:
        template = self._resolve_backtest_template_for_launch(request)
        effective_prediction_scope = (
            template.fixed_prediction_scope or request.prediction_scope
        )
        run_manifest = self.workbench.get_run_manifest(request.run_id)
        run_metadata = self.workbench.get_run_model_metadata(request.run_id)
        official_preflight = (
            self._evaluate_official_backtest_preflight(
                request=request,
                run_manifest=run_manifest,
                run_metadata=run_metadata,
            )
            if template.official
            else None
        )
        if official_preflight is not None and not official_preflight.compatible:
            raise JobExecutionError(
                official_preflight.blocking_reasons[0]
                if official_preflight.blocking_reasons
                else "Official backtest preflight failed."
            )
        if isinstance(run_manifest.get("composition"), dict):
            return self._run_composed_backtest_job(
                context,
                request=request,
                template=template,
                effective_prediction_scope=effective_prediction_scope,
                run_manifest=run_manifest,
                run_metadata=run_metadata,
                official_preflight=official_preflight,
            )
        if template.official:
            return self._run_official_backtest_job(
                context,
                request=request,
                template=template,
                effective_prediction_scope=effective_prediction_scope,
                run_manifest=run_manifest,
                run_metadata=run_metadata,
                preflight=official_preflight,
            )
        readiness = None
        model_artifact_uri = self.workbench.resolve_run_model_artifact_uri(request.run_id)
        if model_artifact_uri is None:
            raise JobExecutionError(
                f"Unable to resolve model artifact for run '{request.run_id}'."
            )
        manifest_dataset_id = self._str(run_manifest.get("dataset_id"))
        selected_dataset_id = request.dataset_id or manifest_dataset_id
        if selected_dataset_id is None:
            prepare_request, dataset_name = self._prepare_request_for_dataset(request.dataset_preset)
            context.start_stage("prepare", f"Preparing debug dataset preset: {dataset_name}")
            prepare_result = self.facade.prepare_workflow.prepare(prepare_request)
            dataset_ref = prepare_result.dataset_ref
            market_bars = prepare_request.market_bars
            data_source = prepare_request.data_source
            readiness = self.workbench.get_dataset_readiness(dataset_ref.dataset_id)
            context.finish_stage("prepare", f"Prepared dataset '{dataset_ref.dataset_id}'")
        else:
            context.start_stage("prepare", f"Loading dataset '{selected_dataset_id}'")
            dataset_ref = self._load_dataset_from_artifacts(selected_dataset_id)
            if manifest_dataset_id and selected_dataset_id != manifest_dataset_id:
                raise JobExecutionError(
                    f"Schema mismatch: backtest dataset '{selected_dataset_id}' does not match the training dataset '{manifest_dataset_id}'."
                )
            readiness = self.workbench.get_dataset_readiness(selected_dataset_id)
            if readiness is None:
                raise JobExecutionError(
                    f"Unable to resolve readiness for dataset '{selected_dataset_id}'."
                )
            if readiness.readiness_status == "not_ready":
                raise JobExecutionError(
                    f"Dataset '{selected_dataset_id}' is not ready for backtest: "
                    f"{'; '.join(readiness.blocking_issues or ['readiness_failed'])}"
                )
            manifest_feature_hash = self._str(run_manifest.get("feature_schema_hash"))
            if manifest_feature_hash and dataset_ref.feature_schema_hash != manifest_feature_hash:
                raise JobExecutionError(
                    f"Dataset '{selected_dataset_id}' feature schema does not match training run '{request.run_id}'."
                )
            manifest_entity_scope = self._str(run_manifest.get("entity_scope"))
            if manifest_entity_scope and dataset_ref.entity_scope != manifest_entity_scope:
                raise JobExecutionError(
                    f"Dataset '{selected_dataset_id}' entity scope '{dataset_ref.entity_scope}' is incompatible "
                    f"with training scope '{manifest_entity_scope}'."
                )
            model_feature_names = run_metadata.get("feature_names")
            if isinstance(model_feature_names, list):
                dataset_samples = self.facade.dataset_store.get(selected_dataset_id, [])
                if dataset_samples:
                    dataset_feature_names = list(dataset_samples[0].features.keys())
                else:
                    dataset_feature_names = [field.name for field in dataset_ref.feature_view_ref.feature_schema]
                if dataset_feature_names != [str(item) for item in model_feature_names]:
                    raise JobExecutionError(
                        f"Dataset '{selected_dataset_id}' feature order does not match training run '{request.run_id}'."
                    )
            market_bars = self.workbench.load_market_bars_for_dataset(selected_dataset_id)
            if not market_bars:
                raise JobExecutionError(
                    f"Dataset '{selected_dataset_id}' does not expose real market bars for backtest."
                )
            dataset_payload = self.workbench.get_dataset_payload(selected_dataset_id) or {}
            acquisition_profile = (
                (self.workbench.get_dataset_detail(selected_dataset_id).acquisition_profile)
                if self.workbench.get_dataset_detail(selected_dataset_id) is not None
                else {}
            )
            data_source = (
                self._str(acquisition_profile.get("source_vendor"))
                or self._str((dataset_payload.get("feature_view_ref") or {}).get("source"))
                or "dataset_artifact"
            )
            context.finish_stage("prepare", f"Loaded dataset '{dataset_ref.dataset_id}'")
        if readiness is None:
            raise JobExecutionError(
                f"Unable to resolve readiness for dataset '{dataset_ref.dataset_id}'."
            )
        if template.official and readiness.official_nlp_gate_status == "failed":
            raise JobExecutionError(
                "Official backtest template is blocked by the NLP quality gate: "
                + "; ".join(readiness.official_nlp_gate_reasons or ["official_nlp_gate_failed"])
            )

        benchmark_symbol = self._resolve_benchmark_symbol(
            request=request,
            dataset_id=dataset_ref.dataset_id,
        )
        metadata_summary = self._build_backtest_metadata_summary(
            run_manifest=run_manifest,
            run_metadata=run_metadata,
            dataset_id=dataset_ref.dataset_id,
            readiness=readiness,
        )
        protocol_metadata = build_protocol_metadata(
            template=template,
            launch_mode=request.mode,
            prediction_scope=effective_prediction_scope,
            dataset_id=dataset_ref.dataset_id,
            dataset_frequency=self._dataset_frequency(dataset_ref.dataset_id),
            target_name=self._label_target_name(dataset_ref.dataset_id),
            label_horizon=self._dataset_label_horizon(dataset_ref.dataset_id),
            lookback_bucket=self._dataset_lookback_bucket(dataset_ref.dataset_id),
            metadata_summary=metadata_summary,
        )
        protocol_metadata.update(
            {
                "actual_market_start_time": readiness.market_window_start_time.isoformat()
                if readiness.market_window_start_time is not None
                else None,
                "actual_market_end_time": readiness.market_window_end_time.isoformat()
                if readiness.market_window_end_time is not None
                else None,
                "actual_backtest_start_time": readiness.official_backtest_start_time.isoformat()
                if readiness.official_backtest_start_time is not None
                else None,
                "actual_backtest_end_time": readiness.official_backtest_end_time.isoformat()
                if readiness.official_backtest_end_time is not None
                else None,
                "actual_nlp_start_time": readiness.nlp_actual_start_time.isoformat()
                if readiness.nlp_actual_start_time is not None
                else None,
                "actual_nlp_end_time": readiness.nlp_actual_end_time.isoformat()
                if readiness.nlp_actual_end_time is not None
                else None,
                "nlp_gate_status": readiness.official_nlp_gate_status,
                "nlp_gate_reasons": list(readiness.official_nlp_gate_reasons),
                "primary_dataset_id": dataset_ref.dataset_id,
                "dataset_ids": [dataset_ref.dataset_id],
                "dataset_roles": {dataset_ref.dataset_id: "market"},
                "dataset_modalities": {
                    dataset_ref.dataset_id: self._dataset_modality(dataset_ref.dataset_id) or "market"
                },
                "alignment_status": "single_dataset",
                "alignment_notes": ["Single-modality backtest reused one dataset only."],
            }
        )

        context.start_stage("predict", f"Generating predictions for scope '{effective_prediction_scope}'")
        prediction_frame = self.facade.prediction_runner.predict(
            PredictRequest(
                model_artifact_uri=model_artifact_uri,
                dataset_ref=dataset_ref,
                prediction_scope=PredictionScope(
                    scope_name=effective_prediction_scope,
                    as_of_time=dataset_ref.feature_view_ref.as_of_time,
                ),
            )
        )
        prediction_artifact = self.facade.store.write_model(
            f"predictions/{request.run_id}/{effective_prediction_scope}.json",
            prediction_frame,
        )
        context.finish_stage("predict", "Prediction artifact written")

        context.start_stage("backtest", "Running research and simulation backtests")
        backtest_result = self.facade.backtest_workflow.backtest(
            BacktestWorkflowRequest(
                prediction_inputs=[
                    PredictionInputRef(
                        model_name=request.run_id,
                        run_id=request.run_id,
                        prediction_frame_uri=prediction_artifact.uri,
                    )
                ],
                backtest_request_template=self._backtest_template(
                    prediction_frame_uri=prediction_artifact.uri,
                    benchmark_symbol=benchmark_symbol,
                    mode=request.mode,
                    research_backend=request.research_backend,
                    portfolio_method=request.portfolio_method,
                ),
                dataset_ref=dataset_ref,
                benchmark_name="workbench_backtest",
                data_source=data_source,
                market_bars=market_bars,
                summary_row_metadata=protocol_metadata,
            )
        )
        backtest_ids = [item.backtest_result.backtest_id for item in backtest_result.items]
        context.finish_stage("backtest", f"Completed {len(backtest_ids)} backtest run(s)")
        return JobResultView(
            dataset_id=dataset_ref.dataset_id,
            dataset_ids=[dataset_ref.dataset_id],
            run_ids=[request.run_id],
            backtest_ids=backtest_ids,
            prediction_scope=effective_prediction_scope,
            template_id=template.template_id,
            template_name=template.name,
            official=template.official,
            protocol_version=template.protocol_version,
            research_backend=request.research_backend,
            portfolio_method=request.portfolio_method,
        )

    def _run_composed_backtest_job(
        self,
        context: JobContext,
        *,
        request: LaunchBacktestRequest,
        template: BacktestTemplateView,
        effective_prediction_scope: str,
        run_manifest: dict[str, object],
        run_metadata: dict[str, object],
        official_preflight: BacktestLaunchPreflightView | None = None,
    ) -> JobResultView:
        if template.official:
            return self._run_official_composed_backtest_job(
                context,
                request=request,
                template=template,
                effective_prediction_scope=effective_prediction_scope,
                run_manifest=run_manifest,
                run_metadata=run_metadata,
                preflight=official_preflight,
            )
        composition = run_manifest.get("composition")
        if not isinstance(composition, dict):
            raise JobExecutionError(f"Run '{request.run_id}' is missing composition metadata.")
        source_runs = composition.get("source_runs")
        if not isinstance(source_runs, list) or len(source_runs) < 2:
            raise JobExecutionError(f"Run '{request.run_id}' does not declare valid source runs.")

        requested_dataset_ids = [
            dataset_id
            for dataset_id in request.dataset_ids
            if isinstance(dataset_id, str) and dataset_id.strip()
        ]
        context.start_stage("prepare", "Resolving multimodal source datasets")
        selected_sources = self._resolve_composed_dataset_assignments(
            source_runs=source_runs,
            requested_dataset_ids=requested_dataset_ids,
        )
        market_source = next((item for item in selected_sources if item["modality"] == "market"), None)
        if market_source is None:
            raise JobExecutionError("Composed run must include a market source for backtest execution.")

        source_prediction_frames: list[dict[str, object]] = []
        market_dataset_ref = None
        market_bars = None
        market_readiness = None
        market_data_source = None
        dataset_roles: dict[str, str] = {}
        dataset_modalities: dict[str, str] = {}

        for source in selected_sources:
            selected_dataset_id = source["selected_dataset_id"]
            dataset_roles[selected_dataset_id] = source["modality"]
            dataset_modalities[selected_dataset_id] = source["modality"]
            (
                dataset_ref,
                readiness,
                data_source,
            ) = self._load_compatible_dataset_for_source_run(
                source_run_id=source["run_id"],
                selected_dataset_id=selected_dataset_id,
            )
            if template.official and readiness.official_nlp_gate_status == "failed":
                raise JobExecutionError(
                    f"Dataset '{selected_dataset_id}' failed the official NLP quality gate: "
                    + "; ".join(readiness.official_nlp_gate_reasons or ["official_nlp_gate_failed"])
                )
            source_model_artifact_uri = self.workbench.resolve_run_model_artifact_uri(source["run_id"])
            if source_model_artifact_uri is None:
                raise JobExecutionError(
                    f"Unable to resolve model artifact for source run '{source['run_id']}'."
                )
            prediction_frame = self.facade.prediction_runner.predict(
                PredictRequest(
                    model_artifact_uri=source_model_artifact_uri,
                    dataset_ref=dataset_ref,
                    prediction_scope=PredictionScope(
                        scope_name=effective_prediction_scope,
                        as_of_time=dataset_ref.feature_view_ref.as_of_time,
                    ),
                )
            )
            source_prediction_frames.append(
                {
                    "run_id": source["run_id"],
                    "model_name": source["model_name"],
                    "modality": source["modality"],
                    "dataset_id": selected_dataset_id,
                    "weight": source["weight"],
                    "frame": prediction_frame,
                }
            )
            if source["modality"] == "market":
                market_dataset_ref = dataset_ref
                market_bars = self.workbench.load_market_bars_for_dataset(selected_dataset_id)
                if not market_bars:
                    raise JobExecutionError(
                        f"Dataset '{selected_dataset_id}' does not expose real market bars for backtest."
                    )
                market_readiness = readiness
                market_data_source = data_source
        if market_dataset_ref is None or market_readiness is None or market_data_source is None or market_bars is None:
            raise JobExecutionError("Unable to resolve the market source required for multimodal backtest.")

        context.finish_stage("prepare", f"Resolved {len(selected_sources)} modality datasets")

        metadata_summary = self._build_backtest_metadata_summary(
            run_manifest=run_manifest,
            run_metadata=run_metadata,
            dataset_id=market_dataset_ref.dataset_id,
            readiness=market_readiness,
        )
        protocol_metadata = build_protocol_metadata(
            template=template,
            launch_mode=request.mode,
            prediction_scope=effective_prediction_scope,
            dataset_id=market_dataset_ref.dataset_id,
            dataset_frequency=self._dataset_frequency(market_dataset_ref.dataset_id),
            target_name=self._label_target_name(market_dataset_ref.dataset_id),
            label_horizon=self._dataset_label_horizon(market_dataset_ref.dataset_id),
            lookback_bucket=self._dataset_lookback_bucket(market_dataset_ref.dataset_id),
            metadata_summary=metadata_summary,
        )

        context.start_stage("predict", f"Generating multimodal predictions for scope '{effective_prediction_scope}'")
        merged_prediction_frame, alignment_notes = self._blend_prediction_frames(
            run_id=request.run_id,
            source_prediction_frames=source_prediction_frames,
        )
        prediction_artifact = self.facade.store.write_model(
            f"predictions/{request.run_id}/{effective_prediction_scope}.json",
            merged_prediction_frame,
        )
        context.finish_stage("predict", "Multimodal prediction artifact written")

        protocol_metadata.update(
            {
                "actual_market_start_time": market_readiness.market_window_start_time.isoformat()
                if market_readiness.market_window_start_time is not None
                else None,
                "actual_market_end_time": market_readiness.market_window_end_time.isoformat()
                if market_readiness.market_window_end_time is not None
                else None,
                "actual_backtest_start_time": market_readiness.official_backtest_start_time.isoformat()
                if market_readiness.official_backtest_start_time is not None
                else None,
                "actual_backtest_end_time": market_readiness.official_backtest_end_time.isoformat()
                if market_readiness.official_backtest_end_time is not None
                else None,
                "actual_nlp_start_time": market_readiness.nlp_actual_start_time.isoformat()
                if market_readiness.nlp_actual_start_time is not None
                else None,
                "actual_nlp_end_time": market_readiness.nlp_actual_end_time.isoformat()
                if market_readiness.nlp_actual_end_time is not None
                else None,
                "nlp_gate_status": market_readiness.official_nlp_gate_status,
                "nlp_gate_reasons": list(market_readiness.official_nlp_gate_reasons),
                "fusion_strategy": self._str(composition.get("fusion_strategy")) or "late_score_blend",
                "primary_dataset_id": market_dataset_ref.dataset_id,
                "dataset_ids": [str(item["selected_dataset_id"]) for item in selected_sources],
                "dataset_roles": dataset_roles,
                "dataset_modalities": dataset_modalities,
                "alignment_status": "strict_intersection",
                "alignment_notes": alignment_notes,
            }
        )

        benchmark_symbol = self._resolve_benchmark_symbol(
            request=request,
            dataset_id=market_dataset_ref.dataset_id,
        )
        model_display_name = self._str(run_metadata.get("model_name")) or request.run_id

        context.start_stage("backtest", "Running multimodal research and simulation backtests")
        backtest_result = self.facade.backtest_workflow.backtest(
            BacktestWorkflowRequest(
                prediction_inputs=[
                    PredictionInputRef(
                        model_name=model_display_name,
                        run_id=request.run_id,
                        prediction_frame_uri=prediction_artifact.uri,
                    )
                ],
                backtest_request_template=self._backtest_template(
                    prediction_frame_uri=prediction_artifact.uri,
                    benchmark_symbol=benchmark_symbol,
                    mode=request.mode,
                    research_backend=request.research_backend,
                    portfolio_method=request.portfolio_method,
                ),
                dataset_ref=market_dataset_ref,
                benchmark_name="workbench_backtest",
                data_source=market_data_source,
                market_bars=market_bars,
                summary_row_metadata=protocol_metadata,
            )
        )
        backtest_ids = [item.backtest_result.backtest_id for item in backtest_result.items]
        context.finish_stage("backtest", f"Completed {len(backtest_ids)} multimodal backtest run(s)")
        return JobResultView(
            dataset_id=market_dataset_ref.dataset_id,
            dataset_ids=[str(item["selected_dataset_id"]) for item in selected_sources],
            run_ids=[request.run_id],
            backtest_ids=backtest_ids,
            prediction_scope=effective_prediction_scope,
            template_id=template.template_id,
            template_name=template.name,
            official=template.official,
            protocol_version=template.protocol_version,
            research_backend=request.research_backend,
            portfolio_method=request.portfolio_method,
        )

    def _run_model_composition_job(
        self,
        context: JobContext,
        request: LaunchModelCompositionRequest,
    ) -> JobResultView:
        context.start_stage("inspect", "Validating source runs for multimodal composition")
        source_specs = self._validate_composition_sources(
            source_run_ids=request.source_run_ids,
            weights=request.weights,
        )
        context.finish_stage("inspect", f"Validated {len(source_specs)} source runs")

        run_id = (
            f"multimodal-compose-{datetime.now(UTC):%Y%m%d%H%M%S}-{uuid.uuid4().hex[:6]}"
        )
        display_name = request.composition_name.strip() if request.composition_name else "multimodal_fusion"
        primary_source = next(item for item in source_specs if item["modality"] == "market")
        primary_dataset_id = primary_source["dataset_ids"][0]
        primary_detail = self.workbench.get_dataset_detail(primary_dataset_id)
        primary_payload = self.workbench.get_dataset_payload(primary_dataset_id) or {}
        primary_readiness = self.workbench.get_dataset_readiness(primary_dataset_id)
        if primary_detail is None:
            raise JobExecutionError(f"Primary market dataset '{primary_dataset_id}' was not found.")
        context.start_stage("compose", f"Materializing composed run '{display_name}'")

        source_dataset_ids = list(
            dict.fromkeys(
                dataset_id
                for source in source_specs
                for dataset_id in source["dataset_ids"]
            )
        )
        official_contract = self._official_composition_contract_summary(source_specs)
        model_dir = self.artifact_root / "models" / run_id
        model_dir.mkdir(parents=True, exist_ok=True)
        train_manifest = {
            "run_id": run_id,
            "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "dataset_ref_uri": f"dataset://{primary_dataset_id}",
            "dataset_id": primary_dataset_id,
            "dataset_manifest_uri": str(
                (self.artifact_root / "datasets" / f"{primary_dataset_id}_dataset_manifest.json").resolve()
            ),
            "dataset_type": primary_detail.dataset.dataset_type,
            "data_domain": primary_detail.dataset.data_domain,
            "data_domains": list(primary_detail.dataset.data_domains),
            "snapshot_version": primary_detail.dataset.snapshot_version,
            "entity_scope": primary_detail.dataset.entity_scope,
            "entity_count": primary_detail.dataset.entity_count,
            "feature_schema_hash": primary_readiness.feature_schema_hash if primary_readiness else None,
            "dataset_readiness_status": primary_readiness.readiness_status if primary_readiness else None,
            "dataset_readiness_warnings": list(primary_readiness.warnings) if primary_readiness else [],
            "source_dataset_ids": source_dataset_ids,
            "fusion_domains": [str(item["modality"]) for item in source_specs],
            "composition": {
                "fusion_strategy": request.fusion_strategy,
                "official_template_eligible": True,
                "official_blocking_reasons": [],
                "official_contract": official_contract,
                "source_runs": [
                    {
                        "run_id": item["run_id"],
                        "model_name": item["model_name"],
                        "modality": item["modality"],
                        "weight": item["weight"],
                        "dataset_ids": list(item["dataset_ids"]),
                    }
                    for item in source_specs
                ],
                "rules": [
                    "Use two or more existing single-modality runs only.",
                    "Late fusion uses strict timestamp + entity-key intersection.",
                    "Backtest selects one compatible dataset per modality and never persists a merged dataset.",
                ],
            },
            "model_artifact": {
                "kind": "composed_multimodal_manifest",
                "uri": str((model_dir / "metadata.json").resolve()),
                "content_hash": None,
                "metadata": {
                    "model_name": display_name,
                    "registry_model_name": "multimodal_reference",
                    "fusion_strategy": request.fusion_strategy,
                },
            },
            "metrics": {},
        }
        metadata = {
            "run_id": run_id,
            "model_name": display_name,
            "registry_model_name": "multimodal_reference",
            "model_family": "multimodal",
            "advanced_kind": "multimodal",
            "model_spec": {
                "model_name": "multimodal_reference",
                "family": "multimodal",
                "version": "0.1.0",
                "input_schema": [],
                "output_schema": [{"name": "prediction", "dtype": "float", "nullable": False}],
                "task_type": source_specs[0]["task_type"],
                "lookback": None,
                "target_horizon": source_specs[0]["label_horizon"],
                "prediction_type": "return",
                "hyperparams": {
                    "fusion_strategy": request.fusion_strategy,
                    "source_run_ids": [item["run_id"] for item in source_specs],
                    "weights": {item["run_id"]: item["weight"] for item in source_specs},
                },
            },
            "registration": {
                "model_name": "multimodal_reference",
                "family": "multimodal",
                "advanced_kind": "multimodal",
                "input_adapter_key": "composed_multimodal",
                "prediction_adapter_key": "standard_prediction",
                "artifact_adapter_key": "json_manifest",
                "capabilities": ["composed_from_existing_runs", "late_score_blend"],
                "benchmark_eligible": True,
                "default_eligible": False,
                "enabled": False,
            },
            "artifact_uri": str((model_dir / "metadata.json").resolve()),
            "artifact_dir": str(model_dir.resolve()),
            "state_uri": None,
            "backend": "late_score_blend",
            "training_sample_count": 0,
            "feature_names": [],
            "training_config": {},
            "training_metrics": {},
            "best_epoch": None,
            "trained_steps": None,
            "checkpoint_tag": "composed",
            "input_metadata": {
                "source_run_ids": [item["run_id"] for item in source_specs],
                "source_dataset_ids": source_dataset_ids,
                "official_template_eligible": True,
                "official_blocking_reasons": [],
                "official_contract": official_contract,
            },
            "prediction_metadata": {
                "fusion_strategy": request.fusion_strategy,
                "source_run_ids": [item["run_id"] for item in source_specs],
                "source_dataset_ids": source_dataset_ids,
                "dataset_ids": source_dataset_ids,
                "official_template_eligible": True,
                "official_contract": official_contract,
            },
            "source_dataset_ids": source_dataset_ids,
        }
        evaluation_summary = {
            "run_id": run_id,
            "dataset_id": primary_dataset_id,
            "task_type": source_specs[0]["task_type"],
            "selected_scope": "full",
            "sample_count": 0,
            "regression_metrics": {},
            "split_metrics": {},
            "coverage": {
                "available_scopes": ["full"],
                "sample_count": 0,
                "start_time": primary_detail.dataset.freshness.data_start_time.isoformat()
                if primary_detail.dataset.freshness.data_start_time is not None
                else None,
                "end_time": primary_detail.dataset.freshness.data_end_time.isoformat()
                if primary_detail.dataset.freshness.data_end_time is not None
                else None,
            },
            "series": {},
            "composition": train_manifest["composition"],
        }
        self.facade.store.write_json(f"tracking/{run_id}.json", {
            "created_at": train_manifest["created_at"],
            "metrics": {},
            "params": {
                "dataset_id": primary_dataset_id,
                "model_name": display_name,
                "registry_model_name": "multimodal_reference",
                "fusion_strategy": request.fusion_strategy,
            },
            "run_id": run_id,
        })
        self.facade.store.write_json(f"models/{run_id}/train_manifest.json", train_manifest)
        self.facade.store.write_json(f"models/{run_id}/metadata.json", metadata)
        self.facade.store.write_json(f"models/{run_id}/evaluation_summary.json", evaluation_summary)
        self.facade.store.write_model(
            f"predictions/{run_id}/full.json",
            PredictionFrame(
                rows=[],
                metadata=PredictionMetadata(
                    feature_view_ref=None,
                    prediction_time=datetime.now(UTC),
                    inference_latency_ms=0,
                    target_horizon=source_specs[0]["label_horizon"],
                ),
            ),
        )
        context.finish_stage("compose", f"Composed run '{run_id}' is ready")
        return JobResultView(
            dataset_id=primary_dataset_id,
            dataset_ids=source_dataset_ids,
            run_ids=[run_id],
            summary=StableSummaryView(
                status="success",
                headline=f"Composed multimodal run {run_id}",
                detail=f"Fusion strategy: {request.fusion_strategy}",
            ),
        )

    def _validate_composition_sources(
        self,
        *,
        source_run_ids: list[str],
        weights: dict[str, float],
    ) -> list[dict[str, object]]:
        ordered_run_ids: list[str] = []
        seen_run_ids: set[str] = set()
        for run_id in source_run_ids:
            normalized = run_id.strip()
            if not normalized or normalized in seen_run_ids:
                continue
            ordered_run_ids.append(normalized)
            seen_run_ids.add(normalized)
        if len(ordered_run_ids) < 2:
            raise JobExecutionError("Multimodal composition requires at least two distinct source runs.")

        specs: list[dict[str, object]] = []
        signature: dict[str, str | int | None] | None = None
        signature_run_id: str | None = None
        seen_modalities: set[str] = set()
        for run_id in ordered_run_ids:
            detail = self.workbench.get_run_detail(run_id)
            if detail is None:
                raise JobExecutionError(f"Source run '{run_id}' was not found.")
            manifest = self.workbench.get_run_manifest(run_id)
            if isinstance(manifest.get("composition"), dict):
                raise JobExecutionError(
                    f"Source run '{run_id}' is already a multimodal composition and cannot be nested."
                )
            source_dataset_ids = list(detail.dataset_ids or ([detail.dataset_id] if detail.dataset_id else []))
            if not source_dataset_ids:
                raise JobExecutionError(f"Source run '{run_id}' does not expose any dataset ids.")
            primary_dataset_id = source_dataset_ids[0]
            source_dataset_detail = self.workbench.get_dataset_detail(primary_dataset_id)
            source_metadata = self.workbench.get_run_model_metadata(run_id)
            try:
                source_dataset_ref = self._load_dataset_from_artifacts(primary_dataset_id)
            except JobExecutionError:
                source_dataset_ref = None
            source_feature_names = self._feature_names_for_run(source_metadata, source_dataset_ref)
            data_domain = (
                source_dataset_detail.dataset.data_domain
                if source_dataset_detail is not None
                else self._str((detail.dataset_summary or {}).get("data_domain"))
            )
            modality = self._infer_composition_source_modality(
                run_id=run_id,
                data_domain=data_domain,
                feature_names=source_feature_names,
            )
            if modality in seen_modalities:
                raise JobExecutionError(
                    f"Source runs must have distinct modalities, but '{run_id}' duplicates modality '{modality}'."
                )
            seen_modalities.add(modality)
            candidate_signature = {
                "task_type": detail.task_type,
                "entity_scope": self._str((detail.dataset_summary or {}).get("entity_scope")),
                "frequency": (
                    source_dataset_detail.dataset.frequency if source_dataset_detail is not None else None
                ),
                "label_horizon": (
                    source_dataset_detail.dataset.label_horizon if source_dataset_detail is not None else None
                ),
            }
            if signature is None:
                signature = candidate_signature
                signature_run_id = run_id
            elif candidate_signature != signature:
                raise JobExecutionError(
                    self._composition_signature_mismatch_message(
                        first_run_id=signature_run_id or ordered_run_ids[0],
                        first_signature=signature,
                        second_run_id=run_id,
                        second_signature=candidate_signature,
                    )
                )
            weight = weights.get(run_id, 1.0)
            if weight <= 0:
                raise JobExecutionError(f"Source run '{run_id}' must have a positive fusion weight.")
            specs.append(
                {
                    "run_id": run_id,
                    "model_name": detail.model_name,
                    "modality": modality,
                    "weight": float(weight),
                    "dataset_ids": source_dataset_ids,
                    "task_type": detail.task_type,
                    "label_horizon": candidate_signature["label_horizon"],
                    "feature_names": source_feature_names,
                }
            )
        if "market" not in seen_modalities:
            raise JobExecutionError("At least one market source run is required for multimodal composition.")
        return self._normalize_source_weights(specs)

    def _resolve_composed_dataset_assignments(
        self,
        *,
        source_runs: list[object],
        requested_dataset_ids: list[str],
    ) -> list[dict[str, object]]:
        specs: list[dict[str, object]] = []
        for item in source_runs:
            if not isinstance(item, dict):
                continue
            run_id = self._str(item.get("run_id"))
            modality = self._normalize_modality(self._str(item.get("modality")))
            if not run_id:
                continue
            if not modality:
                raise JobExecutionError(f"Composed source run '{run_id}' is missing modality metadata.")
            dataset_ids = [
                str(dataset_id)
                for dataset_id in item.get("dataset_ids", [])
                if isinstance(dataset_id, str) and dataset_id
            ]
            specs.append(
                {
                    "run_id": run_id,
                    "model_name": self._str(item.get("model_name")) or run_id,
                    "modality": modality,
                    "weight": float(item.get("weight", 1.0) or 1.0),
                    "dataset_ids": dataset_ids,
                }
            )
        if not specs:
            raise JobExecutionError("Composed run does not contain any usable source-run metadata.")

        dataset_by_modality: dict[str, str] = {}
        for dataset_id in requested_dataset_ids:
            detail = self.workbench.get_dataset_detail(dataset_id)
            if detail is None:
                raise JobExecutionError(f"Dataset '{dataset_id}' was not found.")
            modality = self._normalize_modality(detail.dataset.data_domain)
            if modality in dataset_by_modality:
                raise JobExecutionError(f"Only one dataset can be selected per modality, but '{modality}' was repeated.")
            dataset_by_modality[modality] = dataset_id

        selected_specs: list[dict[str, object]] = []
        for spec in specs:
            selected_dataset_id = dataset_by_modality.get(str(spec["modality"]))
            if selected_dataset_id is None:
                fallback_dataset_ids = spec["dataset_ids"] if isinstance(spec["dataset_ids"], list) else []
                selected_dataset_id = fallback_dataset_ids[0] if fallback_dataset_ids else None
            if not selected_dataset_id:
                raise JobExecutionError(
                    f"Please provide a dataset for modality '{spec['modality']}' when launching multimodal backtest."
                )
            selected_specs.append({**spec, "selected_dataset_id": selected_dataset_id})
        return self._normalize_source_weights(selected_specs)

    def _load_compatible_dataset_for_source_run(
        self,
        *,
        source_run_id: str,
        selected_dataset_id: str,
    ) -> tuple[DatasetRef, object, str]:
        dataset_ref = self._load_dataset_from_artifacts(selected_dataset_id)
        readiness = self.workbench.get_dataset_readiness(selected_dataset_id)
        if readiness is None:
            raise JobExecutionError(f"Unable to resolve readiness for dataset '{selected_dataset_id}'.")
        if readiness.readiness_status == "not_ready":
            raise JobExecutionError(
                f"Dataset '{selected_dataset_id}' is not ready for multimodal backtest: "
                f"{'; '.join(readiness.blocking_issues or ['readiness_failed'])}"
            )
        source_manifest = self.workbench.get_run_manifest(source_run_id)
        source_metadata = self.workbench.get_run_model_metadata(source_run_id)
        manifest_feature_hash = self._str(source_manifest.get("feature_schema_hash"))
        if manifest_feature_hash and dataset_ref.feature_schema_hash != manifest_feature_hash:
            raise JobExecutionError(
                f"Dataset '{selected_dataset_id}' feature schema does not match source run '{source_run_id}'."
            )
        manifest_entity_scope = self._str(source_manifest.get("entity_scope"))
        if manifest_entity_scope and dataset_ref.entity_scope != manifest_entity_scope:
            raise JobExecutionError(
                f"Dataset '{selected_dataset_id}' entity scope '{dataset_ref.entity_scope}' is incompatible "
                f"with source run '{source_run_id}' scope '{manifest_entity_scope}'."
            )
        model_feature_names = source_metadata.get("feature_names")
        if isinstance(model_feature_names, list):
            dataset_samples = self.facade.dataset_store.get(selected_dataset_id, [])
            if dataset_samples:
                dataset_feature_names = list(dataset_samples[0].features.keys())
            else:
                dataset_feature_names = [field.name for field in dataset_ref.feature_view_ref.feature_schema]
            if dataset_feature_names != [str(item) for item in model_feature_names]:
                raise JobExecutionError(
                    f"Dataset '{selected_dataset_id}' feature order does not match source run '{source_run_id}'."
                )
        dataset_payload = self.workbench.get_dataset_payload(selected_dataset_id) or {}
        dataset_detail = self.workbench.get_dataset_detail(selected_dataset_id)
        acquisition_profile = dataset_detail.acquisition_profile if dataset_detail is not None else {}
        data_source = (
            self._str(acquisition_profile.get("source_vendor"))
            or self._str((dataset_payload.get("feature_view_ref") or {}).get("source"))
            or "dataset_artifact"
        )
        return dataset_ref, readiness, data_source

    def _blend_prediction_frames(
        self,
        *,
        run_id: str,
        source_prediction_frames: list[dict[str, object]],
    ) -> tuple[PredictionFrame, list[str]]:
        if not source_prediction_frames:
            raise JobExecutionError("No source predictions were produced for multimodal fusion.")
        keyed_rows: list[dict[tuple[str, tuple[tuple[str, str], ...]], PredictionRow]] = []
        for item in source_prediction_frames:
            frame = item["frame"]
            if not isinstance(frame, PredictionFrame):
                raise JobExecutionError("Invalid prediction frame encountered during multimodal fusion.")
            row_map: dict[tuple[str, tuple[tuple[str, str], ...]], PredictionRow] = {}
            for row in frame.rows:
                row_map[self._prediction_row_key(row)] = row
            keyed_rows.append(row_map)
        intersection = set(keyed_rows[0].keys())
        for row_map in keyed_rows[1:]:
            intersection &= set(row_map.keys())
        if not intersection:
            raise JobExecutionError(
                "Selected modality datasets could not be aligned on the same timestamp and entity keys."
            )
        ordered_keys = sorted(intersection, key=lambda item: (item[0], item[1]))
        if len(ordered_keys) < OFFICIAL_MIN_ALIGNED_MULTIMODAL_ROWS:
            raise JobExecutionError(
                "Official multimodal benchmark produced only "
                f"{len(ordered_keys)} aligned predictions; at least {OFFICIAL_MIN_ALIGNED_MULTIMODAL_ROWS} are required."
            )
        normalized_sources = self._normalize_source_weights(source_prediction_frames)
        merged_rows: list[PredictionRow] = []
        for key in ordered_keys:
            aligned_rows = [row_map[key] for row_map in keyed_rows]
            blended_prediction = 0.0
            blended_confidence = 0.0
            for source, row in zip(normalized_sources, aligned_rows, strict=True):
                weight = float(source["weight"])
                blended_prediction += weight * row.prediction
                blended_confidence += weight * row.confidence
            anchor_row = aligned_rows[0]
            feature_available_time = max(
                [(row.feature_available_time or row.timestamp) for row in aligned_rows]
            )
            merged_rows.append(
                PredictionRow(
                    entity_keys=dict(anchor_row.entity_keys),
                    timestamp=anchor_row.timestamp,
                    prediction=blended_prediction,
                    confidence=max(0.0, min(blended_confidence, 1.0)),
                    model_run_id=run_id,
                    feature_available_time=feature_available_time,
                )
            )
        market_frame = next(
            (
                item["frame"]
                for item in normalized_sources
                if item.get("modality") == "market" and isinstance(item.get("frame"), PredictionFrame)
            ),
            None,
        )
        market_metadata = market_frame.metadata if isinstance(market_frame, PredictionFrame) else None
        notes = [
            f"Strict alignment kept {len(ordered_keys)} shared rows across {len(source_prediction_frames)} modalities.",
        ]
        for source, row_map in zip(normalized_sources, keyed_rows, strict=True):
            dataset_id = self._str(source.get("selected_dataset_id")) or self._str(source.get("dataset_id")) or "--"
            notes.append(
                f"{source['run_id']} ({source['modality']}, dataset {dataset_id}) contributed {len(intersection)}/{len(row_map)} aligned rows."
            )
        return (
            PredictionFrame(
                rows=merged_rows,
                metadata=PredictionMetadata(
                    feature_view_ref=market_metadata.feature_view_ref if market_metadata is not None else None,
                    prediction_time=datetime.now(UTC),
                    inference_latency_ms=0,
                    target_horizon=market_metadata.target_horizon if market_metadata is not None else None,
                ),
            ),
            notes,
        )

    def _normalize_source_weights(self, specs: list[dict[str, object]]) -> list[dict[str, object]]:
        total_weight = sum(float(item.get("weight", 0.0) or 0.0) for item in specs)
        if total_weight <= 0:
            raise JobExecutionError("Fusion weights must sum to a positive value.")
        normalized: list[dict[str, object]] = []
        for item in specs:
            normalized.append({**item, "weight": float(item.get("weight", 0.0) or 0.0) / total_weight})
        return normalized

    @staticmethod
    def _modality_sort_key(modality: str) -> int:
        order = {"market": 0, "macro": 1, "on_chain": 2, "derivatives": 3, "nlp": 4}
        return order.get(modality, len(order))

    def _feature_modalities(self, feature_names: list[str]) -> list[str]:
        detected: set[str] = set()
        for feature_name in feature_names:
            normalized = feature_name.strip().lower()
            if normalized.startswith(("sentiment_", "text_", "news_")):
                detected.add("nlp")
                continue
            if normalized.startswith("macro_"):
                detected.add("macro")
                continue
            if normalized.startswith(("onchain_", "on_chain_")):
                detected.add("on_chain")
                continue
            if normalized.startswith(("derivatives_", "derivative_", "futures_", "perp_")):
                detected.add("derivatives")
                continue
            detected.add("market")
        if not detected:
            return ["market"]
        return sorted(detected, key=self._modality_sort_key)

    def _has_market_features(self, feature_names: list[str]) -> bool:
        prefixes = ("sentiment_", "text_", "news_")
        return any(not name.startswith(prefixes) for name in feature_names)

    def _infer_composition_source_modality(
        self,
        *,
        run_id: str,
        data_domain: str | None,
        feature_names: list[str],
    ) -> str:
        normalized = self._normalize_modality(data_domain)
        feature_modalities = self._feature_modalities(feature_names)
        if len(feature_modalities) > 1:
            raise JobExecutionError(
                f"Source run '{run_id}' mixes modalities {', '.join(feature_modalities)} and cannot be used as a single-modality source."
            )
        detected = feature_modalities[0] if feature_modalities else "market"
        if normalized in {"unknown", detected}:
            return detected
        if normalized == "market" and detected == "market":
            return "market"
        raise JobExecutionError(
            f"Source run '{run_id}' declares modality '{normalized}' but its feature schema resolves to '{detected}'."
        )

    @staticmethod
    def _prediction_row_key(row: PredictionRow) -> tuple[str, tuple[tuple[str, str], ...]]:
        return (
            row.timestamp.astimezone(UTC).isoformat(),
            tuple(sorted((str(key), str(value)) for key, value in row.entity_keys.items())),
        )

    @staticmethod
    def _normalize_modality(data_domain: str | None) -> str:
        normalized = (data_domain or "").strip().lower()
        if normalized == "market":
            return "market"
        if normalized == "macro":
            return "macro"
        if normalized in {"on_chain", "onchain", "on-chain"}:
            return "on_chain"
        if normalized in {"derivatives", "derivative", "futures", "perp", "perpetual"}:
            return "derivatives"
        if normalized in {"sentiment_events", "sentiment", "text", "news", "nlp"}:
            return "nlp"
        return normalized or "unknown"

    @staticmethod
    def _composition_signature_mismatch_message(
        *,
        first_run_id: str,
        first_signature: dict[str, str | int | None],
        second_run_id: str,
        second_signature: dict[str, str | int | None],
    ) -> str:
        labels = {
            "task_type": "task type",
            "frequency": "frequency",
            "label_horizon": "label horizon",
            "entity_scope": "entity scope",
        }
        mismatches: list[str] = []
        for key in ["task_type", "frequency", "label_horizon", "entity_scope"]:
            if first_signature.get(key) == second_signature.get(key):
                continue
            mismatches.append(
                f"{labels[key]} mismatch: "
                f"{first_run_id}={first_signature.get(key) or '--'} vs "
                f"{second_run_id}={second_signature.get(key) or '--'}"
            )
        if not mismatches:
            return "All source runs must share the same task type, frequency, label horizon, and entity scope."
        return "Cannot compose runs with incompatible signatures. " + "; ".join(mismatches) + "."

    def _run_dataset_request_job(
        self,
        context: JobContext,
        request: DatasetAcquisitionRequest,
    ) -> JobResultView:
        prepare_result, readiness, summary_artifacts, _request_summary = (
            self._execute_dataset_request_stages(
                context,
                request=request,
                acquire_stage="acquire",
                prepare_stage="prepare",
                readiness_stage="readiness",
            )
        )
        return JobResultView(
            dataset_id=prepare_result.dataset_ref.dataset_id,
            summary_artifacts=summary_artifacts,
            result_links=[
                DeepLinkView(
                    kind="dataset_detail",
                    label=f"Dataset {prepare_result.dataset_ref.dataset_id}",
                    href=f"/datasets/{prepare_result.dataset_ref.dataset_id}",
                    api_path=f"/api/datasets/{prepare_result.dataset_ref.dataset_id}",
                )
            ],
            summary=StableSummaryView(
                status=readiness.readiness_status,
                headline=f"Dataset {prepare_result.dataset_ref.dataset_id} prepared",
                warnings=readiness.warnings,
                recommended_actions=readiness.recommended_next_actions,
            ),
        )

    def _run_dataset_pipeline_job(
        self,
        context: JobContext,
        request: DatasetPipelineRequest,
        plan: DatasetPipelinePlan,
    ) -> JobResultView:
        result = JobResultView(requested_stages=plan.requested_stages)
        prepare_result, base_readiness, summary_artifacts, _request_summary = (
            self._execute_dataset_request_stages(
                context,
                request=request.base_request,
                acquire_stage="acquire_base",
                prepare_stage="prepare_base",
                readiness_stage="readiness_base",
            )
        )
        base_dataset_id = prepare_result.dataset_ref.dataset_id
        result = result.model_copy(
            update={
                "dataset_id": base_dataset_id,
                "base_dataset_id": base_dataset_id,
                "summary_artifacts": summary_artifacts,
            }
        )
        self._update_status(context.job_id, result=result)
        if base_readiness.readiness_status == "not_ready":
            self._update_stage(
                context.job_id,
                name="readiness_base",
                status="failed",
                summary=(
                    f"Readiness={base_readiness.readiness_status}; "
                    f"{'; '.join(base_readiness.blocking_issues or ['readiness_failed'])}"
                ),
                started_at=None,
                finished_at=datetime.now(UTC),
            )
            raise JobExecutionError(
                f"Base dataset '{base_dataset_id}' is not ready for pipeline continuation: "
                f"{'; '.join(base_readiness.blocking_issues or ['readiness_failed'])}"
            )

        final_dataset_id = base_dataset_id
        final_readiness = base_readiness
        if plan.fusion_enabled:
            fusion_request = self._build_fusion_request_from_pipeline(
                request.fusion,
                base_dataset_id=base_dataset_id,
            )
            context.start_stage(
                "build_fusion",
                f"Building fusion dataset from '{base_dataset_id}'",
            )
            fusion_response = self.workbench.build_fusion_dataset(fusion_request)
            context.finish_stage(
                "build_fusion",
                f"Built fusion dataset '{fusion_response.dataset_id}'",
            )
            context.start_stage("readiness_fusion", "Evaluating fusion dataset readiness")
            fusion_readiness = self.workbench.get_dataset_readiness(fusion_response.dataset_id)
            if fusion_readiness is None:
                raise JobExecutionError(
                    f"Unable to resolve readiness for fusion dataset '{fusion_response.dataset_id}'."
                )
            context.finish_stage(
                "readiness_fusion",
                f"Readiness={fusion_readiness.readiness_status}",
            )
            final_dataset_id = fusion_response.dataset_id
            final_readiness = fusion_readiness
            result = result.model_copy(
                update={
                    "dataset_id": final_dataset_id,
                    "fusion_dataset_id": final_dataset_id,
                    "summary_artifacts": [
                        *result.summary_artifacts,
                        *[
                            uri
                            for uri in (
                                fusion_response.feature_view_uri,
                                fusion_response.dataset_manifest_uri,
                            )
                            if uri
                        ],
                    ],
                }
            )
            self._update_status(context.job_id, result=result)
            if fusion_readiness.readiness_status == "not_ready":
                self._update_stage(
                    context.job_id,
                    name="readiness_fusion",
                    status="failed",
                    summary=(
                        f"Readiness={fusion_readiness.readiness_status}; "
                        f"{'; '.join(fusion_readiness.blocking_issues or ['readiness_failed'])}"
                    ),
                    started_at=None,
                    finished_at=datetime.now(UTC),
                )
                raise JobExecutionError(
                    f"Fusion dataset '{final_dataset_id}' is not ready for pipeline continuation: "
                    f"{'; '.join(fusion_readiness.blocking_issues or ['readiness_failed'])}"
                )

        if plan.training_enabled:
            train_request = self._build_train_request_from_pipeline(
                request.training,
                dataset_id=final_dataset_id,
            )
            dataset_ref = self._load_dataset_from_artifacts(final_dataset_id)
            run_ids, fit_result_uris, final_readiness = self._execute_train_stage(
                context,
                dataset_ref=dataset_ref,
                request=train_request,
                stage_name="train",
            )
            result = result.model_copy(
                update={
                    "dataset_id": final_dataset_id,
                    "run_ids": run_ids,
                    "fit_result_uris": fit_result_uris,
                }
            )
            self._update_status(context.job_id, result=result)

        headline = f"Dataset pipeline completed for {final_dataset_id}"
        if result.run_ids:
            headline = (
                f"Dataset pipeline completed with {len(result.run_ids)} training run(s)"
            )
        return result.model_copy(
            update={
                "summary": StableSummaryView(
                    status=(
                        "warning"
                        if final_readiness.readiness_status == "warning"
                        else "success"
                    ),
                    headline=headline,
                    detail=f"Final dataset: {final_dataset_id}",
                    warnings=final_readiness.warnings,
                    recommended_actions=final_readiness.recommended_next_actions,
                )
            }
        )

    def _execute_dataset_request_stages(
        self,
        context: JobContext,
        *,
        request: DatasetAcquisitionRequest,
        acquire_stage: str,
        prepare_stage: str,
        readiness_stage: str,
    ):
        context.start_stage(acquire_stage, f"Acquiring dataset request '{request.request_name}'")
        request_artifact = self.facade.store.write_json(
            f"webapi/dataset_requests/{context.job_id}_{acquire_stage}.json",
            request.model_dump(mode="json"),
        )
        workflow_request = self._workflow_dataset_request(request)
        normalized_sources = workflow_request.normalized_sources()
        request_summary = self._acquisition_request_summary(normalized_sources)
        context.finish_stage(acquire_stage, f"Resolved request from {request_summary}")

        if len(normalized_sources) == 1:
            single_source = normalized_sources[0]
            if single_source.data_domain == "market":
                prepare_request = self.facade.prepare_workflow.build_prepare_request_from_dataset_request(
                    workflow_request
                )
                context.start_stage(
                    prepare_stage, f"Preparing dataset '{prepare_request.dataset_id}'"
                )
                prepare_result = self.facade.prepare_workflow.prepare(prepare_request)
            elif single_source.data_domain == "sentiment_events":
                api_source = DatasetAcquisitionSourceRequest.model_validate(
                    single_source.model_dump(mode="json")
                )
                context.start_stage(
                    prepare_stage,
                    f"Preparing sentiment dataset '{request.request_name}'",
                )
                final_dataset_id = self.workbench.build_sentiment_dataset_from_request(
                    request,
                    api_source,
                )
                prepare_result = DatasetMaterializationResult(
                    dataset_ref=self._load_dataset_from_artifacts(final_dataset_id),
                    dataset_manifest_uri=str(
                        self.artifact_root
                        / "datasets"
                        / f"{final_dataset_id}_dataset_manifest.json"
                    ),
                    quality_report_uri=None,
                )
            else:
                raise JobExecutionError(
                    "Single-domain dataset requests currently support market and sentiment_events."
                )
        elif all(source.data_domain == "sentiment_events" for source in normalized_sources):
            api_sources = [
                DatasetAcquisitionSourceRequest.model_validate(source.model_dump(mode="json"))
                for source in normalized_sources
            ]
            context.start_stage(
                prepare_stage,
                f"Preparing sentiment dataset '{request.request_name}' from {len(api_sources)} sources",
            )
            final_dataset_id = self.workbench.build_sentiment_dataset_from_sources(
                request,
                api_sources,
            )
            prepare_result = DatasetMaterializationResult(
                dataset_ref=self._load_dataset_from_artifacts(final_dataset_id),
                dataset_manifest_uri=str(
                    self.artifact_root / "datasets" / f"{final_dataset_id}_dataset_manifest.json"
                ),
                quality_report_uri=None,
            )
        else:
            context.start_stage(prepare_stage, "Preparing market anchor and merged dataset")
            final_dataset_id = self._execute_multi_domain_dataset_request(workflow_request)
            prepare_result = DatasetMaterializationResult(
                dataset_ref=self._load_dataset_from_artifacts(final_dataset_id),
                dataset_manifest_uri=str(
                    self.artifact_root / "datasets" / f"{final_dataset_id}_dataset_manifest.json"
                ),
                quality_report_uri=None,
            )
        context.finish_stage(
            prepare_stage,
            f"Prepared dataset '{prepare_result.dataset_ref.dataset_id}' from {request_summary}",
        )
        context.start_stage(readiness_stage, "Evaluating dataset readiness")
        readiness = self.workbench.get_dataset_readiness(prepare_result.dataset_ref.dataset_id)
        if readiness is None:
            raise JobExecutionError(
                f"Unable to resolve readiness for dataset '{prepare_result.dataset_ref.dataset_id}'."
            )
        context.finish_stage(readiness_stage, f"Readiness={readiness.readiness_status}")
        summary_artifacts = [request_artifact.uri, prepare_result.dataset_manifest_uri]
        if prepare_result.quality_report_uri:
            summary_artifacts.append(prepare_result.quality_report_uri)
        return prepare_result, readiness, summary_artifacts, request_summary

    def _execute_train_stage(
        self,
        context: JobContext,
        *,
        dataset_ref: DatasetRef,
        request: LaunchTrainRequest,
        stage_name: str,
    ) -> tuple[list[str], list[str], object]:
        context.start_stage(stage_name, "Starting training workflow")
        template = self._resolve_template_for_request(request)
        self._validate_train_selection(request=request, template=template)
        model_specs = self._resolve_model_specs_for_request(
            request=request,
            feature_schema=dataset_ref.feature_view_ref.feature_schema,
            template=template,
        )
        if not model_specs:
            raise JobExecutionError("No supported models selected for training.")
        readiness = self.workbench.get_dataset_readiness(dataset_ref.dataset_id)
        if readiness is None:
            raise JobExecutionError(
                f"Unable to resolve readiness for dataset '{dataset_ref.dataset_id}'."
            )
        if readiness.readiness_status == "not_ready":
            raise JobExecutionError(
                f"Dataset '{dataset_ref.dataset_id}' is not ready for training: "
                f"{'; '.join(readiness.blocking_issues or ['readiness_failed'])}"
            )
        if not dataset_ref.feature_schema_hash:
            raise JobExecutionError(
                f"Dataset '{dataset_ref.dataset_id}' is missing feature_schema_hash."
            )
        if dataset_ref.entity_scope not in {"single_asset", "multi_asset"}:
            raise JobExecutionError(
                f"Dataset '{dataset_ref.dataset_id}' has unsupported entity_scope '{dataset_ref.entity_scope}'."
            )
        run_id_prefix = request.run_id_prefix or f"workbench-train-{datetime.now(UTC):%Y%m%d%H%M%S}"
        train_result = self.facade.train_workflow.train(
            TrainWorkflowRequest(
                dataset_ref=dataset_ref,
                model_specs=model_specs,
                trainer_config=self._trainer_config(
                    request.trainer_preset or (template.trainer_preset if template else "fast")
                ),
                tracking_context=TrackingContext(
                    backend="file",
                    experiment_name=request.experiment_name,
                    tracking_uri=str(self.artifact_root / "tracking"),
                ),
                seed=request.seed,
                run_id_prefix=run_id_prefix,
            )
        )
        run_ids = [item.fit_result.run_id for item in train_result.items]
        fit_result_uris = [item.fit_result_uri for item in train_result.items]
        context.finish_stage(stage_name, f"Completed {len(run_ids)} training run(s)")
        return run_ids, fit_result_uris, readiness

    def _prepare_request_for_dataset(self, dataset_preset: str):
        if dataset_preset == "real_benchmark":
            return self.facade.prepare_workflow.build_real_benchmark_request()
        return self.facade.prepare_workflow.build_smoke_request(), "smoke"

    def _workflow_dataset_request(
        self,
        request: DatasetAcquisitionRequest,
    ) -> WorkflowDatasetAcquisitionRequest:
        try:
            return WorkflowDatasetAcquisitionRequest.model_validate(request.model_dump())
        except Exception as exc:  # noqa: BLE001
            raise JobExecutionError(str(exc)) from exc

    def _acquisition_request_summary(self, sources) -> str:
        fragments: list[str] = []
        for source in sources:
            domain = getattr(source, "data_domain", "unknown")
            vendor = getattr(source, "vendor", None) or getattr(source, "source_vendor", None) or "internal"
            identifier = getattr(source, "identifier", None)
            frequency = getattr(source, "frequency", None) or "unknown"
            label = f"{domain}/{vendor}/{frequency}"
            if identifier:
                label = f"{label}/{identifier}"
            fragments.append(label)
        return ", ".join(fragments) if fragments else "unknown_request"

    def _execute_multi_domain_dataset_request(
        self,
        request: WorkflowDatasetAcquisitionRequest,
    ) -> str:
        sources = request.normalized_sources()
        market_sources = [source for source in sources if source.data_domain == "market"]
        if len(market_sources) != 1:
            raise JobExecutionError(
                "Multi-domain trainable requests require exactly one market source."
            )
        unique_frequencies = {source.frequency for source in sources}
        if len(unique_frequencies) != 1:
            raise JobExecutionError(
                "Multi-domain trainable requests require the same frequency across all sources."
            )
        merge_policy_name = request.merge_policy_name or "available_time_safe_asof"
        if merge_policy_name not in {"strict_timestamp_inner", "available_time_safe_asof"}:
            raise JobExecutionError(
                f"Unsupported merge policy '{merge_policy_name}'. "
                "Only strict_timestamp_inner and available_time_safe_asof are allowed."
            )

        market_source = market_sources[0]
        anchor_request = request.model_copy(
            update={
                "request_name": f"{request.request_name}_market_anchor",
                "data_domain": "market",
                "dataset_type": "training_panel",
                "source_vendor": market_source.vendor,
                "exchange": market_source.exchange or request.exchange or "binance",
                "frequency": market_source.frequency,
                "symbol_selector": market_source.symbol_selector or request.symbol_selector,
                "filters": dict(market_source.filters),
                "sources": [],
                "merge_policy_name": None,
            }
        )
        prepare_request = self.facade.prepare_workflow.build_prepare_request_from_dataset_request(
            anchor_request
        )
        anchor_dataset_id = f"{self._dataset_id_from_request_name(request.request_name)}_market_anchor"
        unique_timestamps = len({bar.event_time for bar in prepare_request.market_bars})
        effective_min_history = min(
            prepare_request.sample_policy.min_history_bars,
            max(unique_timestamps - 3, 1),
        )
        prepare_request = prepare_request.model_copy(
            update={
                "dataset_id": anchor_dataset_id,
                "sample_policy": prepare_request.sample_policy.model_copy(
                    update={"min_history_bars": effective_min_history}
                ),
            }
        )
        prepare_result = self.facade.prepare_workflow.prepare(prepare_request)
        self.workbench._update_dataset_acquisition_profile(
            anchor_dataset_id,
            {
                "request_name": anchor_request.request_name,
                "request_origin": "multi_domain_market_anchor",
                "internal_visibility": "hidden",
                "data_domain": "market",
                "data_domains": ["market"],
                "source_specs": [
                    {
                        "data_domain": source.data_domain,
                        "source_vendor": source.vendor,
                        "exchange": source.exchange,
                        "frequency": source.frequency,
                        "identifier": source.identifier,
                        "filters": dict(source.filters),
                        "symbol_selector": (
                            source.symbol_selector.model_dump(mode="json")
                            if source.symbol_selector is not None
                            else None
                        ),
                    }
                    for source in sources
                ],
            },
        )
        anchor_readiness = self.workbench.get_dataset_readiness(anchor_dataset_id)
        if anchor_readiness is None:
            raise JobExecutionError(
                f"Unable to resolve readiness for market anchor dataset '{anchor_dataset_id}'."
            )
        if anchor_readiness.readiness_status == "not_ready":
            raise JobExecutionError(
                f"Market anchor dataset '{anchor_dataset_id}' is not ready for merge: "
                f"{'; '.join(anchor_readiness.blocking_issues or ['readiness_failed'])}"
            )
        api_sources = [
            DatasetAcquisitionSourceRequest.model_validate(source.model_dump(mode="json"))
            for source in sources
        ]
        merged = self.workbench.build_merged_dataset_from_sources(
            request_name=request.request_name,
            market_anchor_dataset_id=prepare_result.dataset_ref.dataset_id,
            sources=api_sources,
            merge_policy_name=merge_policy_name,
        )
        return merged.dataset_id

    def _prepare_request_for_acquisition_request(
        self,
        request: DatasetAcquisitionRequest,
    ):
        workflow_request = self._workflow_dataset_request(request)
        try:
            prepared_request = self.facade.prepare_workflow.build_prepare_request_from_dataset_request(workflow_request)
        except Exception as exc:  # noqa: BLE001
            raise JobExecutionError(str(exc)) from exc
        source_summary = str(prepared_request.acquisition_profile.get("request_origin", prepared_request.data_source))
        return prepared_request, source_summary

    def _build_dataset_pipeline_plan(
        self,
        request: DatasetPipelineRequest,
    ) -> DatasetPipelinePlan:
        requested_stages = ["acquire_base", "prepare_base", "readiness_base"]
        if request.fusion.enabled:
            requested_stages.extend(["build_fusion", "readiness_fusion"])
        if request.training.enabled:
            requested_stages.append("train")
        return DatasetPipelinePlan(
            requested_stages=requested_stages,
            final_stage=requested_stages[-1],
            fusion_enabled=request.fusion.enabled,
            training_enabled=request.training.enabled,
        )

    def _build_fusion_request_from_pipeline(
        self,
        config,
        *,
        base_dataset_id: str,
    ) -> DatasetFusionRequest:
        missing_feature_policy = dict(config.missing_feature_policy)
        missing_feature_policy.setdefault(
            "min_feature_coverage_ratio",
            config.min_feature_coverage_ratio,
        )
        sources = (
            list(config.sources)
            if config.sources
            else self._default_pipeline_fusion_sources()
        )
        return DatasetFusionRequest(
            request_name=config.request_name or f"{base_dataset_id}_fusion",
            base_dataset_id=base_dataset_id,
            dataset_type="fusion_training_panel",
            sample_policy_name="fusion_training_panel_strict",
            alignment_policy_name=config.alignment_policy_name,
            missing_feature_policy_name=config.missing_feature_policy_name,
            alignment_policy=dict(config.alignment_policy),
            missing_feature_policy=missing_feature_policy,
            sources=sources,
        )

    def _build_train_request_from_pipeline(
        self,
        config,
        *,
        dataset_id: str,
    ) -> LaunchTrainRequest:
        return LaunchTrainRequest(
            dataset_preset="smoke",
            dataset_id=dataset_id,
            template_id=config.template_id,
            template_overrides=dict(config.template_overrides),
            model_names=list(config.model_names),
            trainer_preset=config.trainer_preset,
            seed=config.seed,
            experiment_name=config.experiment_name,
            run_id_prefix=config.run_id_prefix or f"{dataset_id}-pipeline-train",
        )

    def _default_pipeline_fusion_sources(self) -> list[DatasetFusionSourceRequest]:
        return [
            DatasetFusionSourceRequest(
                data_domain="macro",
                vendor="fred",
                identifier="DFF",
                frequency="1d",
                metric_name="value",
                feature_name="macro_dff_value",
            ),
            DatasetFusionSourceRequest(
                data_domain="on_chain",
                vendor="defillama",
                identifier="tvl",
                frequency="1d",
                metric_name="value",
                feature_name="onchain_tvl_value",
            ),
        ]

    def _read_dataset_ref(self, dataset_id: str) -> DatasetRef:
        dataset_ref_path = self.artifact_root / "datasets" / f"{dataset_id}_dataset_ref.json"
        if not dataset_ref_path.exists():
            raise JobExecutionError(f"Dataset '{dataset_id}' does not exist.")
        return self.facade.store.read_model(str(dataset_ref_path), DatasetRef)

    def _load_dataset_from_artifacts(self, dataset_id: str) -> DatasetRef:
        dataset_ref = self._read_dataset_ref(dataset_id)
        if dataset_id not in self.facade.dataset_store:
            samples_path = self.artifact_root / "datasets" / f"{dataset_id}_dataset_samples.json"
            if not samples_path.exists():
                raise JobExecutionError(f"Dataset samples for '{dataset_id}' are missing.")
            payload = self.facade.store.read_json(str(samples_path))
            feature_order = [field.name for field in dataset_ref.feature_view_ref.feature_schema]
            samples = [DatasetSample.model_validate(item) for item in payload.get("samples", [])]
            if feature_order:
                normalized_samples: list[DatasetSample] = []
                for sample in samples:
                    normalized_features = {
                        name: float(sample.features.get(name, 0.0))
                        for name in feature_order
                    }
                    normalized_samples.append(
                        sample.model_copy(update={"features": normalized_features})
                    )
                samples = normalized_samples
            self.facade.dataset_store[dataset_id] = samples
        readiness = self.workbench.get_dataset_readiness(dataset_id)
        if readiness is None:
            return dataset_ref
        return dataset_ref.model_copy(
            update={
                "readiness_status": readiness.readiness_status,
                "entity_scope": readiness.entity_scope or dataset_ref.entity_scope,
                "entity_count": readiness.entity_count or dataset_ref.entity_count,
            }
        )

    def _trainer_config(self, trainer_preset: str) -> TrainerConfig:
        if trainer_preset == "fast":
            return TrainerConfig(runner="local", epochs=1, batch_size=32, deterministic=True)
        return TrainerConfig(runner="local", epochs=1, batch_size=32, deterministic=True)

    def _backtest_template(
        self,
        *,
        prediction_frame_uri: str,
        benchmark_symbol: str,
        mode: str,
        research_backend: str,
        portfolio_method: str,
    ) -> BacktestRequest:
        if mode == "official":
            return build_official_backtest_request(
                prediction_frame_uri=prediction_frame_uri,
                benchmark_symbol=benchmark_symbol,
                research_backend=research_backend,
                portfolio_method=portfolio_method,
            )
        return build_custom_backtest_request(
            prediction_frame_uri=prediction_frame_uri,
            benchmark_symbol=benchmark_symbol,
            research_backend=research_backend,
            portfolio_method=portfolio_method,
        )

    def _resolve_backtest_template_for_launch(
        self,
        request: LaunchBacktestRequest,
    ) -> BacktestTemplateView:
        if request.mode == "official" or request.template_id == OFFICIAL_BACKTEST_TEMPLATE_ID:
            return official_backtest_template()
        return custom_backtest_template()

    def _resolve_benchmark_symbol(
        self,
        *,
        request: LaunchBacktestRequest,
        dataset_id: str | None,
    ) -> str:
        if request.benchmark_symbol.strip():
            return request.benchmark_symbol.strip()
        if dataset_id:
            detail = self.workbench.get_dataset_detail(dataset_id)
            if detail is not None:
                asset_id = detail.dataset.asset_id
                if asset_id:
                    return asset_id
                symbols = detail.dataset.symbols_preview
                if symbols:
                    return symbols[0]
        return "BTCUSDT"

    def _dataset_modality(self, dataset_id: str) -> str | None:
        detail = self.workbench.get_dataset_detail(dataset_id)
        if detail is not None:
            if detail.dataset.data_domain:
                return detail.dataset.data_domain
            if detail.dataset.data_domains:
                return detail.dataset.data_domains[0]
        payload = self.workbench.get_dataset_payload(dataset_id) or {}
        data_domain = self._str(payload.get("data_domain"))
        if data_domain:
            return data_domain
        data_domains = payload.get("data_domains")
        if isinstance(data_domains, list):
            for item in data_domains:
                normalized = self._str(item)
                if normalized:
                    return normalized
        return None

    def _resolve_official_window_days(self, request: LaunchBacktestRequest) -> int:
        if request.official_window_days in OFFICIAL_WINDOW_OPTIONS:
            return int(request.official_window_days)
        return OFFICIAL_DEFAULT_WINDOW_DAYS

    def _ensure_official_benchmark_sync(
        self,
        *,
        requires_multimodal_benchmark: bool,
        window_days: int | None = None,
    ) -> None:
        windows_to_validate = (
            [int(window_days)]
            if window_days in OFFICIAL_WINDOW_OPTIONS
            else [int(item) for item in OFFICIAL_WINDOW_OPTIONS]
        )
        unresolved_windows: list[tuple[int, str]] = []
        for candidate_window_days in windows_to_validate:
            try:
                self._resolve_official_benchmark_context(
                    window_days=candidate_window_days,
                    requires_multimodal_benchmark=requires_multimodal_benchmark,
                )
            except JobExecutionError as exc:
                unresolved_windows.append((candidate_window_days, str(exc)))

        if not unresolved_windows:
            return

        if requires_multimodal_benchmark:
            try:
                self.workbench.ensure_official_multimodal_benchmark()
            except Exception as exc:  # pragma: no cover - defensive wrapper for materialization failures
                raise JobExecutionError(f"Official multimodal benchmark sync failed: {exc}") from exc
        for candidate_window_days in windows_to_validate:
            try:
                self._resolve_official_benchmark_context(
                    window_days=candidate_window_days,
                    requires_multimodal_benchmark=requires_multimodal_benchmark,
                )
            except JobExecutionError as exc:
                raise JobExecutionError(
                    f"Official benchmark sync is incomplete for the {candidate_window_days}d window: {exc}"
                ) from exc

    def _resolve_official_benchmark_context(
        self,
        *,
        window_days: int,
        requires_multimodal_benchmark: bool,
    ) -> dict[str, object]:
        market_dataset_id = OFFICIAL_MARKET_BENCHMARK_DATASET_ID
        multimodal_dataset_id = OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID
        market_detail = self.workbench.get_dataset_detail(market_dataset_id)
        multimodal_detail = self.workbench.get_dataset_detail(multimodal_dataset_id)
        multimodal_readiness = self.workbench.get_dataset_readiness(multimodal_dataset_id)
        if market_detail is None:
            raise JobExecutionError(
                f"Official market benchmark dataset '{market_dataset_id}' is not available."
            )
        if multimodal_detail is None and requires_multimodal_benchmark:
            raise JobExecutionError(
                f"Official multimodal benchmark dataset '{multimodal_dataset_id}' is not available."
            )
        if multimodal_readiness is None and requires_multimodal_benchmark:
            raise JobExecutionError(
                f"Official multimodal benchmark dataset '{multimodal_dataset_id}' has no readiness record."
            )
        market_end = market_detail.dataset.freshness.data_end_time
        multimodal_end = (
            (
                multimodal_readiness.nlp_actual_end_time
                if multimodal_readiness is not None
                else None
            )
            or (multimodal_detail.dataset.freshness.data_end_time if multimodal_detail is not None else None)
        )
        market_start = market_detail.dataset.freshness.data_start_time
        multimodal_start = (
            multimodal_detail.dataset.freshness.data_start_time if multimodal_detail is not None else None
        )
        if market_end is None or market_start is None:
            raise JobExecutionError("Official rolling benchmark is missing required time-range metadata.")
        if requires_multimodal_benchmark and (multimodal_end is None or multimodal_start is None):
            raise JobExecutionError("Official rolling benchmark is missing required multimodal time-range metadata.")
        window_end = min(market_end, multimodal_end) if requires_multimodal_benchmark and multimodal_end else market_end
        window_start = window_end - timedelta(days=window_days)
        if window_start < market_start:
            raise JobExecutionError(
                f"Official market benchmark does not have {window_days} days of recent history."
            )
        if requires_multimodal_benchmark and multimodal_start is not None and window_start < multimodal_start:
            raise JobExecutionError(
                f"Official multimodal benchmark does not have {window_days} days of recent history."
            )
        benchmark_version = (
            f"{multimodal_dataset_id}:{multimodal_detail.dataset.snapshot_version or 'latest'}"
            if multimodal_detail is not None
            else f"{market_dataset_id}:{market_detail.dataset.snapshot_version or 'latest'}"
        )
        return {
            "market_dataset_id": market_dataset_id,
            "multimodal_dataset_id": multimodal_dataset_id,
            "benchmark_version": benchmark_version,
            "window_days": window_days,
            "window_start": window_start,
            "window_end": window_end,
            "multimodal_readiness": multimodal_readiness,
        }

    @staticmethod
    def _contract_token(value: object | None) -> str | None:
        if value is None:
            return None
        token = str(value).strip()
        return token.upper() if token else None

    @staticmethod
    def _contract_vendor(value: object | None) -> str | None:
        if value is None:
            return None
        token = str(value).strip().lower()
        return token or None

    def _market_symbol_set(self, detail) -> set[str]:
        candidates: list[str] = []
        dataset_symbols = getattr(getattr(detail, "dataset", None), "symbols_preview", None) or []
        candidates.extend(str(item) for item in dataset_symbols if item)
        acquisition_profile = getattr(detail, "acquisition_profile", {}) or {}
        candidates.extend(str(item) for item in (acquisition_profile.get("symbols") or []) if item)
        for spec in acquisition_profile.get("source_specs") or []:
            if not isinstance(spec, dict):
                continue
            selector = spec.get("symbol_selector") or {}
            candidates.extend(str(item) for item in (selector.get("symbols") or []) if item)
        return {token for item in candidates if (token := self._contract_token(item))}

    def _dataset_identifier_set(self, detail) -> set[str]:
        acquisition_profile = getattr(detail, "acquisition_profile", {}) or {}
        identifiers: list[str] = []
        identifiers.extend(str(item) for item in (acquisition_profile.get("identifiers") or []) if item)
        identifier = acquisition_profile.get("identifier")
        if identifier:
            identifiers.append(str(identifier))
        for spec in acquisition_profile.get("source_specs") or []:
            if not isinstance(spec, dict):
                continue
            spec_identifier = spec.get("identifier")
            if spec_identifier:
                identifiers.append(str(spec_identifier))
        return {token for item in identifiers if (token := self._contract_token(item))}

    def _official_expected_market_contract(self) -> tuple[str | None, set[str]]:
        detail = self.workbench.get_dataset_detail(OFFICIAL_MARKET_BENCHMARK_DATASET_ID)
        if detail is None:
            raise JobExecutionError(
                f"Official market benchmark dataset '{OFFICIAL_MARKET_BENCHMARK_DATASET_ID}' is not available."
            )
        vendor = self._contract_vendor(detail.dataset.source_vendor)
        symbols = self._market_symbol_set(detail)
        return vendor, symbols

    def _official_expected_nlp_contract(self) -> tuple[str | None, set[str]]:
        detail = self.workbench.get_dataset_detail(OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID)
        if detail is None:
            raise JobExecutionError(
                f"Official multimodal benchmark dataset '{OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID}' is not available."
            )
        acquisition_profile = detail.acquisition_profile or {}
        sentiment_spec = next(
            (
                item
                for item in (acquisition_profile.get("source_specs") or [])
                if isinstance(item, dict) and self._str(item.get("data_domain")) == "sentiment_events"
            ),
            None,
        )
        vendor = self._contract_vendor(
            (sentiment_spec or {}).get("source_vendor") if isinstance(sentiment_spec, dict) else None
        )
        identifiers = self._dataset_identifier_set(detail)
        return vendor, identifiers

    def _official_expected_auxiliary_contracts(self) -> dict[str, tuple[str | None, set[str]]]:
        detail = self.workbench.get_dataset_detail(OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID)
        if detail is None:
            raise JobExecutionError(
                f"Official multimodal benchmark dataset '{OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID}' is not available."
            )
        acquisition_profile = detail.acquisition_profile or {}
        contracts: dict[str, tuple[str | None, set[str]]] = {}
        for modality in ("macro", "on_chain", "derivatives"):
            matched_specs = [
                item
                for item in (acquisition_profile.get("source_specs") or [])
                if isinstance(item, dict) and self._normalize_modality(self._str(item.get("data_domain"))) == modality
            ]
            vendor = self._contract_vendor(matched_specs[0].get("source_vendor")) if matched_specs else None
            identifiers = {
                token
                for item in matched_specs
                for value in [item.get("identifier")]
                if (token := self._contract_token(value))
            }
            contracts[modality] = (vendor, identifiers)
        return contracts

    def _source_dataset_id_for_modality(self, item: dict[str, object], modality: str) -> str | None:
        datasets = item.get("datasets")
        if isinstance(datasets, list):
            for candidate in datasets:
                if not isinstance(candidate, dict):
                    continue
                candidate_modality = self._normalize_modality(self._str(candidate.get("modality")))
                dataset_id = self._str(candidate.get("dataset_id"))
                if candidate_modality == modality and dataset_id:
                    return dataset_id
        dataset_ids = item.get("dataset_ids")
        if isinstance(dataset_ids, list):
            for candidate in dataset_ids:
                dataset_id = self._str(candidate)
                if dataset_id and self._dataset_modality(dataset_id) == modality:
                    return dataset_id
            for candidate in dataset_ids:
                dataset_id = self._str(candidate)
                if dataset_id:
                    return dataset_id
        return None

    def _official_source_contract_reasons(
        self,
        *,
        request_run_id: str,
        run_manifest: dict[str, object],
        required_modalities: list[str],
    ) -> list[str]:
        composition = run_manifest.get("composition")
        source_specs: list[dict[str, object]] = []
        if isinstance(composition, dict):
            if composition.get("official_template_eligible") is not True:
                stored_reasons = composition.get("official_blocking_reasons")
                blocking_reasons = [
                    str(item).strip()
                    for item in (stored_reasons or [])
                    if isinstance(item, str) and str(item).strip()
                ]
                return blocking_reasons or [
                    f"Run '{request_run_id}' predates the official multimodal composition contract and must be recomposed."
                ]
            source_runs = composition.get("source_runs")
            if not isinstance(source_runs, list) or not source_runs:
                raise JobExecutionError("Composed run is missing source run metadata required for official preflight.")
            for item in source_runs:
                if not isinstance(item, dict):
                    continue
                modality = self._normalize_modality(self._str(item.get("modality")))
                source_run_id = self._str(item.get("run_id"))
                if not modality or not source_run_id:
                    raise JobExecutionError("Composed run contains a source without run_id/modality metadata.")
                source_specs.append(
                    {
                        "run_id": source_run_id,
                        "modality": modality,
                        "dataset_id": self._source_dataset_id_for_modality(item, modality),
                    }
                )
        else:
            dataset_id = self._str(run_manifest.get("dataset_id"))
            dataset_ids = run_manifest.get("dataset_ids")
            if not dataset_id and isinstance(dataset_ids, list):
                for candidate in dataset_ids:
                    resolved = self._str(candidate)
                    if resolved:
                        dataset_id = resolved
                        break
            if not dataset_id:
                raise JobExecutionError(
                    f"Run '{request_run_id}' is missing dataset metadata required for official preflight."
                )
            resolved_modalities = [
                self._normalize_modality(modality)
                for modality in required_modalities
                if self._normalize_modality(modality) != "unknown"
            ]
            if not resolved_modalities:
                inferred = self._normalize_modality(self._dataset_modality(dataset_id))
                resolved_modalities = [inferred if inferred != "unknown" else "market"]
            for modality in dict.fromkeys(resolved_modalities):
                source_specs.append(
                    {
                        "run_id": request_run_id,
                        "modality": modality,
                        "dataset_id": dataset_id,
                        "dataset_ids": [dataset_id],
                    }
                )
        return self._official_source_contract_reasons_for_specs(
            request_run_id=request_run_id,
            source_specs=source_specs,
        )

    def _official_source_contract_reasons_for_specs(
        self,
        *,
        request_run_id: str,
        source_specs: list[dict[str, object]],
    ) -> list[str]:
        official_market_vendor, official_market_symbols = self._official_expected_market_contract()
        official_nlp_vendor, official_nlp_identifiers = self._official_expected_nlp_contract()
        official_aux_contracts = self._official_expected_auxiliary_contracts()
        reasons: list[str] = []
        for spec in source_specs:
            source_run_id = self._str(spec.get("run_id")) or request_run_id
            modality = self._normalize_modality(self._str(spec.get("modality")))
            dataset_id = self._str(spec.get("dataset_id"))
            if not dataset_id:
                dataset_ids = spec.get("dataset_ids")
                if isinstance(dataset_ids, list):
                    for candidate in dataset_ids:
                        resolved = self._str(candidate)
                        if resolved:
                            dataset_id = resolved
                            break
            if not dataset_id:
                reasons.append(f"Source run '{source_run_id}' is missing a dataset for modality '{modality}'.")
                continue
            detail = self.workbench.get_dataset_detail(dataset_id)
            if detail is None:
                reasons.append(f"Source dataset '{dataset_id}' for run '{source_run_id}' is not available.")
                continue
            acquisition_profile = detail.acquisition_profile or {}
            source_specs_profile = [
                item
                for item in (acquisition_profile.get("source_specs") or [])
                if isinstance(item, dict)
            ]
            if modality == "market":
                market_spec = next(
                    (
                        item
                        for item in source_specs_profile
                        if self._normalize_modality(self._str(item.get("data_domain"))) == "market"
                    ),
                    None,
                )
                selector = market_spec.get("symbol_selector") if isinstance(market_spec, dict) else {}
                source_vendor = self._contract_vendor(
                    (market_spec or {}).get("source_vendor") if isinstance(market_spec, dict) else None
                ) or self._contract_vendor(detail.dataset.source_vendor) or self._contract_vendor(
                    acquisition_profile.get("source_vendor")
                )
                source_symbols = (
                    {
                        token
                        for item in ((selector or {}).get("symbols") or [])
                        if (token := self._contract_token(item))
                    }
                    or self._market_symbol_set(detail)
                )
                if source_vendor != official_market_vendor:
                    reasons.append(
                        f"Source run '{source_run_id}' uses market vendor '{source_vendor or '--'}' instead of the official vendor '{official_market_vendor or '--'}'."
                    )
                if source_symbols != official_market_symbols:
                    reasons.append(
                        f"Source run '{source_run_id}' uses market symbols {sorted(source_symbols) or ['--']} instead of the official symbols {sorted(official_market_symbols) or ['--']}."
                    )
                continue

            if modality == "nlp":
                sentiment_spec = next(
                    (
                        item
                        for item in source_specs_profile
                        if self._normalize_modality(self._str(item.get("data_domain"))) == "nlp"
                        or self._normalize_modality(self._str(item.get("data_domain"))) == "sentiment_events"
                    ),
                    None,
                )
                source_vendor = self._contract_vendor(
                    (sentiment_spec or {}).get("source_vendor") if isinstance(sentiment_spec, dict) else None
                ) or self._contract_vendor(detail.dataset.source_vendor) or self._contract_vendor(
                    acquisition_profile.get("source_vendor")
                )
                fusion_source_identifiers = [
                    item.get("identifier")
                    for item in (acquisition_profile.get("fusion_sources") or [])
                    if isinstance(item, dict)
                    and self._normalize_modality(self._str(item.get("data_domain"))) in {"nlp", "sentiment_events"}
                ]
                source_identifiers = (
                    {
                        token
                        for value in [
                            (sentiment_spec or {}).get("identifier") if isinstance(sentiment_spec, dict) else None,
                            *fusion_source_identifiers,
                        ]
                        if (token := self._contract_token(value))
                    }
                    or self._dataset_identifier_set(detail)
                )
                if source_vendor != official_nlp_vendor:
                    reasons.append(
                        f"Source run '{source_run_id}' uses NLP vendor '{source_vendor or '--'}' instead of the official vendor '{official_nlp_vendor or '--'}'."
                    )
                if source_identifiers != official_nlp_identifiers:
                    reasons.append(
                        f"Source run '{source_run_id}' uses NLP identifiers {sorted(source_identifiers) or ['--']} instead of the official identifiers {sorted(official_nlp_identifiers) or ['--']}."
                    )
                market_anchor_dataset_id = self._str(acquisition_profile.get("market_anchor_dataset_id"))
                if not market_anchor_dataset_id:
                    for candidate in acquisition_profile.get("source_dataset_ids") or []:
                        resolved = self._str(candidate)
                        if resolved and self._dataset_modality(resolved) == "market":
                            market_anchor_dataset_id = resolved
                            break
                if market_anchor_dataset_id:
                    market_anchor_detail = self.workbench.get_dataset_detail(market_anchor_dataset_id)
                    if market_anchor_detail is None:
                        reasons.append(
                            f"Source run '{source_run_id}' references market anchor dataset '{market_anchor_dataset_id}' but it is not available."
                        )
                    else:
                        anchor_vendor = self._contract_vendor(
                            market_anchor_detail.dataset.source_vendor
                        ) or self._contract_vendor((market_anchor_detail.acquisition_profile or {}).get("source_vendor"))
                        anchor_symbols = self._market_symbol_set(market_anchor_detail)
                        if anchor_vendor != official_market_vendor:
                            reasons.append(
                                f"Source run '{source_run_id}' uses market anchor vendor '{anchor_vendor or '--'}' instead of the official vendor '{official_market_vendor or '--'}'."
                            )
                        if anchor_symbols != official_market_symbols:
                            reasons.append(
                                f"Source run '{source_run_id}' uses market anchor symbols {sorted(anchor_symbols) or ['--']} instead of the official symbols {sorted(official_market_symbols) or ['--']}."
                            )
                label_source_vendor = self._contract_vendor(acquisition_profile.get("label_source_vendor"))
                if label_source_vendor and label_source_vendor != official_market_vendor:
                    reasons.append(
                        f"Source run '{source_run_id}' uses label source vendor '{label_source_vendor}' instead of the official market vendor '{official_market_vendor or '--'}'."
                    )
                continue

            if modality in {"macro", "on_chain", "derivatives"}:
                matched_specs = [
                    item
                    for item in source_specs_profile
                    if self._normalize_modality(self._str(item.get("data_domain"))) == modality
                ]
                source_vendor = self._contract_vendor(
                    matched_specs[0].get("source_vendor") if matched_specs else None
                ) or self._contract_vendor(detail.dataset.source_vendor) or self._contract_vendor(
                    acquisition_profile.get("source_vendor")
                )
                source_identifiers = {
                    token
                    for value in [item.get("identifier") for item in matched_specs]
                    if (token := self._contract_token(value))
                } or self._dataset_identifier_set(detail)
                official_vendor, official_identifiers = official_aux_contracts.get(modality, (None, set()))
                if source_vendor != official_vendor:
                    reasons.append(
                        f"Source run '{source_run_id}' uses {modality} vendor '{source_vendor or '--'}' instead of the official vendor '{official_vendor or '--'}'."
                    )
                if source_identifiers != official_identifiers:
                    reasons.append(
                        f"Source run '{source_run_id}' uses {modality} identifiers {sorted(source_identifiers) or ['--']} instead of the official identifiers {sorted(official_identifiers) or ['--']}."
                    )
                continue
        return list(dict.fromkeys(reasons))

    def _official_composition_contract_summary(
        self,
        source_specs: list[dict[str, object]],
    ) -> dict[str, object]:
        official_market_vendor, official_market_symbols = self._official_expected_market_contract()
        official_nlp_vendor, official_nlp_identifiers = self._official_expected_nlp_contract()
        official_aux_contracts = self._official_expected_auxiliary_contracts()
        return {
            "contract_version": "official_multimodal_composition_v1",
            "official_market_dataset_id": OFFICIAL_MARKET_BENCHMARK_DATASET_ID,
            "official_multimodal_dataset_id": OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID,
            "official_market_vendor": official_market_vendor,
            "official_market_symbols": sorted(official_market_symbols),
            "official_nlp_vendor": official_nlp_vendor,
            "official_nlp_identifiers": sorted(official_nlp_identifiers),
            "official_auxiliary_contracts": {
                modality: {
                    "vendor": vendor,
                    "identifiers": sorted(identifiers),
                }
                for modality, (vendor, identifiers) in official_aux_contracts.items()
            },
            "source_run_ids": [
                self._str(item.get("run_id"))
                for item in source_specs
                if self._str(item.get("run_id"))
            ],
            "source_dataset_ids": list(
                dict.fromkeys(
                    dataset_id
                    for item in source_specs
                    for dataset_id in (
                        [
                            self._str(item.get("dataset_id")),
                            *[
                                self._str(candidate)
                                for candidate in (item.get("dataset_ids") or [])
                            ],
                        ]
                    )
                    if dataset_id
                )
            ),
        }

    @staticmethod
    def _feature_names_for_run(run_metadata: dict[str, object], dataset_ref: DatasetRef | None) -> list[str]:
        feature_names = run_metadata.get("feature_names")
        if isinstance(feature_names, list) and feature_names:
            return [str(item) for item in feature_names if isinstance(item, str) and item]
        model_spec = run_metadata.get("model_spec")
        if isinstance(model_spec, dict):
            input_schema = model_spec.get("input_schema")
            if isinstance(input_schema, list) and input_schema:
                resolved = [
                    str(item.get("name"))
                    for item in input_schema
                    if isinstance(item, dict) and isinstance(item.get("name"), str) and item.get("name")
                ]
                if resolved:
                    return resolved
        if dataset_ref is None:
            return []
        return [field.name for field in dataset_ref.feature_view_ref.feature_schema]

    @staticmethod
    def _uses_text_features(feature_names: list[str]) -> bool:
        prefixes = ("sentiment_", "text_", "news_")
        return any(name.startswith(prefixes) for name in feature_names)

    @staticmethod
    def _uses_auxiliary_features(feature_names: list[str]) -> bool:
        prefixes = ("macro_", "onchain_", "on_chain_", "derivatives_", "derivative_", "futures_", "perp_")
        return any(name.startswith(prefixes) for name in feature_names)

    @staticmethod
    def _dedupe_feature_names(feature_names: list[str]) -> list[str]:
        deduped: list[str] = []
        seen: set[str] = set()
        for name in feature_names:
            if name in seen:
                continue
            seen.add(name)
            deduped.append(name)
        return deduped

    def _required_official_feature_names(
        self,
        *,
        run_manifest: dict[str, object],
        run_metadata: dict[str, object],
    ) -> tuple[list[str], list[str], bool, bool]:
        composition = run_manifest.get("composition")
        if not isinstance(composition, dict):
            fallback_ref = self._read_dataset_ref(OFFICIAL_MARKET_BENCHMARK_DATASET_ID)
            feature_names = self._feature_names_for_run(run_metadata, fallback_ref)
            required_modalities = self._feature_modalities(feature_names)
            requires_text_features = "nlp" in required_modalities
            requires_multimodal_benchmark = any(
                modality != "market" for modality in required_modalities
            )
            return (
                feature_names,
                required_modalities,
                requires_text_features,
                requires_multimodal_benchmark,
            )
        source_runs = composition.get("source_runs")
        if not isinstance(source_runs, list) or not source_runs:
            raise JobExecutionError("Composed run is missing source run metadata required for official preflight.")
        required_feature_names: list[str] = []
        required_modalities: list[str] = []
        requires_text_features = False
        requires_multimodal_benchmark = False
        for item in source_runs:
            if not isinstance(item, dict):
                continue
            source_run_id = self._str(item.get("run_id"))
            modality = self._normalize_modality(self._str(item.get("modality")))
            if not source_run_id or not modality:
                raise JobExecutionError(
                    "Composed run contains a source without run_id/modality metadata."
                )
            source_metadata = self.workbench.get_run_model_metadata(source_run_id)
            fallback_dataset_id = (
                OFFICIAL_MARKET_BENCHMARK_DATASET_ID
                if modality == "market"
                else OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID
            )
            fallback_ref = self._read_dataset_ref(fallback_dataset_id)
            source_feature_names = self._feature_names_for_run(source_metadata, fallback_ref)
            required_feature_names.extend(source_feature_names)
            required_modalities.append(modality)
            requires_text_features = requires_text_features or self._uses_text_features(source_feature_names) or modality == "nlp"
            requires_multimodal_benchmark = (
                requires_multimodal_benchmark
                or modality != "market"
                or self._uses_auxiliary_features(source_feature_names)
                or self._uses_text_features(source_feature_names)
            )
        return (
            self._dedupe_feature_names(required_feature_names),
            list(dict.fromkeys(sorted(required_modalities, key=self._modality_sort_key))),
            requires_text_features,
            requires_multimodal_benchmark,
        )

    def _evaluate_official_backtest_preflight(
        self,
        *,
        request: LaunchBacktestRequest,
        run_manifest: dict[str, object],
        run_metadata: dict[str, object],
    ) -> BacktestLaunchPreflightView:
        template_id = request.template_id or official_backtest_template().template_id
        window_days = self._resolve_official_window_days(request)
        (
            required_feature_names,
            required_modalities,
            requires_text_features,
            requires_multimodal_benchmark,
        ) = self._required_official_feature_names(
            run_manifest=run_manifest,
            run_metadata=run_metadata,
        )
        requires_auxiliary_features = any(
            modality in {"macro", "on_chain", "derivatives"} for modality in required_modalities
        )
        target_dataset_id = (
            OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID if requires_multimodal_benchmark else OFFICIAL_MARKET_BENCHMARK_DATASET_ID
        )
        official_dataset_ids = [OFFICIAL_MARKET_BENCHMARK_DATASET_ID]
        if requires_multimodal_benchmark:
            official_dataset_ids.append(OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID)
        available_feature_names: list[str] = []
        blocking_reasons: list[str] = []
        nlp_gate_status = "not_required"
        nlp_gate_reasons: list[str] = []
        official_window_start_time: datetime | None = None
        official_window_end_time: datetime | None = None
        official_benchmark_version: str | None = None
        missing_feature_names: list[str] = []

        try:
            target_dataset_ref = self._read_dataset_ref(target_dataset_id)
            available_feature_names = [
                field.name for field in target_dataset_ref.feature_view_ref.feature_schema
            ]
        except JobExecutionError as exc:
            blocking_reasons.append(str(exc))

        if not blocking_reasons:
            missing_feature_names = [
                name for name in required_feature_names if name not in set(available_feature_names)
            ]

        if missing_feature_names:
            blocking_reasons.append(
                "Official benchmark dataset is missing features: "
                + ", ".join(missing_feature_names)
            )

        blocking_reasons.extend(
            self._official_source_contract_reasons(
                request_run_id=request.run_id,
                run_manifest=run_manifest,
                required_modalities=required_modalities,
            )
        )

        try:
            official_context = self._resolve_official_benchmark_context(
                window_days=window_days,
                requires_multimodal_benchmark=requires_multimodal_benchmark,
            )
            official_window_start_time = official_context["window_start"]
            official_window_end_time = official_context["window_end"]
            official_benchmark_version = self._stringify(official_context["benchmark_version"])
            if requires_text_features:
                multimodal_readiness = official_context["multimodal_readiness"]
                if multimodal_readiness is not None:
                    nlp_gate_status = multimodal_readiness.official_nlp_gate_status or "unknown"
                    nlp_gate_reasons = list(multimodal_readiness.official_nlp_gate_reasons or [])
        except JobExecutionError as exc:
            blocking_reasons.append(str(exc))

        if requires_text_features and nlp_gate_status == "failed":
            blocking_reasons.append(
                "Official backtest template is blocked by the NLP quality gate: "
                + "; ".join(nlp_gate_reasons or ["official_nlp_gate_failed"])
            )
        blocking_reasons = list(dict.fromkeys(blocking_reasons))

        return BacktestLaunchPreflightView(
            compatible=len(blocking_reasons) == 0,
            mode=request.mode,
            template_id=template_id,
            official_window_days=window_days,
            official_benchmark_version=official_benchmark_version,
            official_market_dataset_id=OFFICIAL_MARKET_BENCHMARK_DATASET_ID,
            official_multimodal_dataset_id=OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID,
            official_dataset_ids=official_dataset_ids,
            required_modalities=required_modalities,
            official_window_start_time=official_window_start_time,
            official_window_end_time=official_window_end_time,
            requires_text_features=requires_text_features,
            requires_nlp_features=requires_text_features,
            requires_auxiliary_features=requires_auxiliary_features,
            requires_multimodal_benchmark=requires_multimodal_benchmark,
            required_feature_names=required_feature_names,
            available_official_feature_names=available_feature_names,
            missing_official_feature_names=missing_feature_names,
            blocking_reasons=blocking_reasons,
            nlp_gate_status=nlp_gate_status,
            nlp_gate_reasons=nlp_gate_reasons,
        )

    def _official_base_dataset_id_for_features(self, feature_names: list[str]) -> str:
        preferred_ids = (
            [OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID]
            if self._uses_text_features(feature_names) or self._uses_auxiliary_features(feature_names)
            else [OFFICIAL_MARKET_BENCHMARK_DATASET_ID, OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID]
        )
        for dataset_id in preferred_ids:
            dataset_ref = self._read_dataset_ref(dataset_id)
            available = {field.name for field in dataset_ref.feature_view_ref.feature_schema}
            if all(name in available for name in feature_names):
                return dataset_id
        raise JobExecutionError(
            "Official rolling benchmark does not expose the feature schema required by this run."
        )

    def _build_official_windowed_dataset(
        self,
        *,
        base_dataset_id: str,
        feature_names: list[str],
        window_start: datetime,
        window_end: datetime,
        alias_dataset_id: str,
    ) -> DatasetRef:
        base_dataset_ref = self._load_dataset_from_artifacts(base_dataset_id)
        base_samples = list(self.facade.dataset_store.get(base_dataset_id, []))
        if not base_samples:
            raise JobExecutionError(f"Official benchmark dataset '{base_dataset_id}' has no samples.")
        schema_by_name = {
            field.name: field for field in base_dataset_ref.feature_view_ref.feature_schema
        }
        missing_features = [name for name in feature_names if name not in schema_by_name]
        if missing_features:
            raise JobExecutionError(
                f"Official benchmark dataset '{base_dataset_id}' is missing features: {', '.join(missing_features)}."
            )
        end_exclusive = window_end + timedelta(microseconds=1)
        filtered_samples = [
            sample.model_copy(
                update={
                    "features": {
                        name: float(sample.features.get(name, 0.0))
                        for name in feature_names
                    }
                }
            )
            for sample in base_samples
            if window_start <= sample.timestamp < end_exclusive
        ]
        if not filtered_samples:
            raise JobExecutionError(
                f"Official benchmark dataset '{base_dataset_id}' has no samples inside the selected rolling window."
            )
        feature_schema = [schema_by_name[name] for name in feature_names]
        input_refs: list[DataAssetRef] = []
        for ref in base_dataset_ref.feature_view_ref.input_data_refs:
            clipped_start = max(ref.time_range.start, window_start)
            clipped_end = min(ref.time_range.end, window_end)
            if clipped_end <= clipped_start:
                continue
            input_refs.append(
                ref.model_copy(update={"time_range": TimeRange(start=clipped_start, end=clipped_end)})
            )
        if not input_refs:
            input_refs = list(base_dataset_ref.feature_view_ref.input_data_refs)
        feature_view_ref = base_dataset_ref.feature_view_ref.model_copy(
            update={
                "feature_schema": feature_schema,
                "input_data_refs": input_refs,
                "as_of_time": base_dataset_ref.feature_view_ref.as_of_time,
                "storage_uri": f"memory://official-window/{alias_dataset_id}",
            }
        )
        boundary = window_start - timedelta(seconds=1)
        split_manifest = base_dataset_ref.split_manifest.model_copy(
            update={
                "train_range": TimeRange(start=boundary - timedelta(seconds=2), end=boundary - timedelta(seconds=1)),
                "valid_range": TimeRange(start=boundary - timedelta(seconds=1), end=boundary),
                "test_range": TimeRange(start=window_start, end=end_exclusive),
            }
        )
        windowed_dataset_ref = base_dataset_ref.model_copy(
            update={
                "dataset_id": alias_dataset_id,
                "feature_view_ref": feature_view_ref,
                "split_manifest": split_manifest,
                "dataset_hash": stable_digest(
                    {
                        "base_dataset_id": base_dataset_id,
                        "window_start": window_start.isoformat(),
                        "window_end": window_end.isoformat(),
                        "feature_names": feature_names,
                    }
                ),
                "feature_schema_hash": stable_digest(feature_names),
            }
        )
        self.facade.dataset_store[alias_dataset_id] = filtered_samples
        return windowed_dataset_ref

    @staticmethod
    def _slice_market_bars_for_window(
        market_bars: list[object],
        *,
        window_start: datetime,
        window_end: datetime,
    ) -> list[object]:
        return [
            bar
            for bar in market_bars
            if hasattr(bar, "event_time") and window_start <= bar.event_time <= window_end
        ]

    def _run_official_backtest_job(
        self,
        context: JobContext,
        *,
        request: LaunchBacktestRequest,
        template: BacktestTemplateView,
        effective_prediction_scope: str,
        run_manifest: dict[str, object],
        run_metadata: dict[str, object],
        preflight: BacktestLaunchPreflightView | None,
    ) -> JobResultView:
        if preflight is None:
            raise JobExecutionError("Official backtest preflight result is required.")
        model_artifact_uri = self.workbench.resolve_run_model_artifact_uri(request.run_id)
        if model_artifact_uri is None:
            raise JobExecutionError(
                f"Unable to resolve model artifact for run '{request.run_id}'."
            )
        window_days = self._resolve_official_window_days(request)
        self._ensure_official_benchmark_sync(
            requires_multimodal_benchmark=bool(preflight.requires_multimodal_benchmark),
            window_days=window_days,
        )
        official_context = self._resolve_official_benchmark_context(
            window_days=window_days,
            requires_multimodal_benchmark=bool(preflight.requires_multimodal_benchmark),
        )
        context.start_stage("prepare", "Resolving official rolling benchmark")
        base_dataset_id = self._official_base_dataset_id_for_features(preflight.required_feature_names)
        base_feature_names = preflight.required_feature_names
        required_modalities = [
            self._normalize_modality(modality)
            for modality in preflight.required_modalities
            if self._normalize_modality(modality) != "unknown"
        ] or ["market"]
        non_market_modalities = [modality for modality in required_modalities if modality != "market"]
        bundle_modality = (
            non_market_modalities[0] if len(non_market_modalities) == 1 else "multimodal_bundle"
        )
        official_dataset_ids = [OFFICIAL_MARKET_BENCHMARK_DATASET_ID]
        if base_dataset_id == OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID:
            official_dataset_ids.append(OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID)
        base_readiness = self.workbench.get_dataset_readiness(base_dataset_id)
        if base_readiness is None:
            raise JobExecutionError(
                f"Official benchmark dataset '{base_dataset_id}' has no readiness record."
            )
        if preflight.requires_text_features and base_readiness.official_nlp_gate_status == "failed":
            raise JobExecutionError(
                "Official backtest template is blocked by the NLP quality gate: "
                + "; ".join(base_readiness.official_nlp_gate_reasons or ["official_nlp_gate_failed"])
            )
        official_dataset_ref = self._build_official_windowed_dataset(
            base_dataset_id=base_dataset_id,
            feature_names=base_feature_names,
            window_start=official_context["window_start"],
            window_end=official_context["window_end"],
            alias_dataset_id=f"{request.run_id}__official_{window_days}d",
        )
        market_bars = self.workbench.load_market_bars_for_dataset(
            OFFICIAL_MARKET_BENCHMARK_DATASET_ID
        )
        market_bars = self._slice_market_bars_for_window(
            market_bars,
            window_start=official_context["window_start"],
            window_end=official_context["window_end"],
        )
        if not market_bars:
            raise JobExecutionError("Official market benchmark has no bars inside the selected rolling window.")
        context.finish_stage("prepare", "Resolved official rolling benchmark")
        metadata_summary = self._build_backtest_metadata_summary(
            run_manifest=run_manifest,
            run_metadata=run_metadata,
            dataset_id=base_dataset_id,
            readiness=base_readiness,
        )
        protocol_metadata = build_protocol_metadata(
            template=template,
            launch_mode=request.mode,
            prediction_scope=effective_prediction_scope,
            dataset_id=base_dataset_id,
            dataset_frequency=self._dataset_frequency(base_dataset_id),
            target_name=self._label_target_name(base_dataset_id),
            label_horizon=self._dataset_label_horizon(base_dataset_id),
            lookback_bucket=self._dataset_lookback_bucket(base_dataset_id),
            metadata_summary=metadata_summary,
            required_modalities=required_modalities,
            official_benchmark_version=str(official_context["benchmark_version"]),
            official_window_days=window_days,
            official_window_start_time=official_context["window_start"].isoformat(),
            official_window_end_time=official_context["window_end"].isoformat(),
            official_market_dataset_id=OFFICIAL_MARKET_BENCHMARK_DATASET_ID,
            official_multimodal_dataset_id=(
                OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID
                if base_dataset_id == OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID
                else None
            ),
            official_dataset_ids=official_dataset_ids,
        )
        protocol_metadata.update(
            {
                "actual_market_start_time": base_readiness.market_window_start_time.isoformat()
                if base_readiness.market_window_start_time is not None
                else None,
                "actual_market_end_time": base_readiness.market_window_end_time.isoformat()
                if base_readiness.market_window_end_time is not None
                else None,
                "actual_backtest_start_time": official_context["window_start"].isoformat(),
                "actual_backtest_end_time": official_context["window_end"].isoformat(),
                "actual_nlp_start_time": base_readiness.nlp_actual_start_time.isoformat()
                if base_readiness.nlp_actual_start_time is not None
                else None,
                "actual_nlp_end_time": base_readiness.nlp_actual_end_time.isoformat()
                if base_readiness.nlp_actual_end_time is not None
                else None,
                "nlp_gate_status": base_readiness.official_nlp_gate_status,
                "nlp_gate_reasons": list(base_readiness.official_nlp_gate_reasons),
                "primary_dataset_id": OFFICIAL_MARKET_BENCHMARK_DATASET_ID,
                "dataset_ids": official_dataset_ids,
                "dataset_roles": {
                    OFFICIAL_MARKET_BENCHMARK_DATASET_ID: "market_anchor",
                    OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID: "official_multimodal_bundle"
                    if base_dataset_id == OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID
                    else "unused",
                },
                "dataset_modalities": {
                    OFFICIAL_MARKET_BENCHMARK_DATASET_ID: "market",
                    OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID: bundle_modality,
                },
                "alignment_status": "official_rolling_window",
                "alignment_notes": [
                    f"Official rolling benchmark version {official_context['benchmark_version']} with a {window_days}d window.",
                    f"Window spans {official_context['window_start'].isoformat()} to {official_context['window_end'].isoformat()}.",
                ],
            }
        )
        context.start_stage(
            "predict",
            f"Generating official predictions for the latest {window_days}d window",
        )
        prediction_frame = self.facade.prediction_runner.predict(
            PredictRequest(
                model_artifact_uri=model_artifact_uri,
                dataset_ref=official_dataset_ref,
                prediction_scope=PredictionScope(
                    scope_name=effective_prediction_scope,
                    as_of_time=official_dataset_ref.feature_view_ref.as_of_time,
                ),
            )
        )
        prediction_artifact = self.facade.store.write_model(
            f"predictions/{request.run_id}/{effective_prediction_scope}.json",
            prediction_frame,
        )
        context.finish_stage("predict", "Official prediction artifact written")
        benchmark_symbol = self._resolve_benchmark_symbol(
            request=request,
            dataset_id=OFFICIAL_MARKET_BENCHMARK_DATASET_ID,
        )
        context.start_stage("backtest", "Running official rolling-window backtest")
        backtest_result = self.facade.backtest_workflow.backtest(
            BacktestWorkflowRequest(
                prediction_inputs=[
                    PredictionInputRef(
                        model_name=self._str(run_metadata.get("model_name")) or request.run_id,
                        run_id=request.run_id,
                        prediction_frame_uri=prediction_artifact.uri,
                    )
                ],
                backtest_request_template=self._backtest_template(
                    prediction_frame_uri=prediction_artifact.uri,
                    benchmark_symbol=benchmark_symbol,
                    mode=request.mode,
                    research_backend=request.research_backend,
                    portfolio_method=request.portfolio_method,
                ),
                dataset_ref=official_dataset_ref,
                benchmark_name="workbench_backtest",
                data_source="official_rolling_benchmark",
                market_bars=market_bars,
                summary_row_metadata=protocol_metadata,
            )
        )
        backtest_ids = [item.backtest_result.backtest_id for item in backtest_result.items]
        context.finish_stage("backtest", f"Completed {len(backtest_ids)} official backtest run(s)")
        result_dataset_ids = protocol_metadata["dataset_ids"]
        return JobResultView(
            dataset_id=result_dataset_ids[0] if isinstance(result_dataset_ids, list) and result_dataset_ids else None,
            dataset_ids=list(result_dataset_ids) if isinstance(result_dataset_ids, list) else [],
            run_ids=[request.run_id],
            backtest_ids=backtest_ids,
            prediction_scope=effective_prediction_scope,
            template_id=template.template_id,
            template_name=template.name,
            official=template.official,
            protocol_version=template.protocol_version,
            research_backend=request.research_backend,
            portfolio_method=request.portfolio_method,
        )

    def _run_official_composed_backtest_job(
        self,
        context: JobContext,
        *,
        request: LaunchBacktestRequest,
        template: BacktestTemplateView,
        effective_prediction_scope: str,
        run_manifest: dict[str, object],
        run_metadata: dict[str, object],
        preflight: BacktestLaunchPreflightView | None,
    ) -> JobResultView:
        if preflight is None:
            raise JobExecutionError("Official backtest preflight result is required.")
        composition = run_manifest.get("composition")
        if not isinstance(composition, dict):
            raise JobExecutionError(f"Run '{request.run_id}' is missing composition metadata.")
        source_runs = composition.get("source_runs")
        if not isinstance(source_runs, list) or len(source_runs) < 2:
            raise JobExecutionError(f"Run '{request.run_id}' does not declare valid source runs.")
        window_days = self._resolve_official_window_days(request)
        self._ensure_official_benchmark_sync(
            requires_multimodal_benchmark=bool(preflight.requires_multimodal_benchmark),
            window_days=window_days,
        )
        official_context = self._resolve_official_benchmark_context(
            window_days=window_days,
            requires_multimodal_benchmark=bool(preflight.requires_multimodal_benchmark),
        )
        multimodal_readiness = official_context["multimodal_readiness"]
        if preflight.requires_text_features and getattr(multimodal_readiness, "official_nlp_gate_status", None) == "failed":
            raise JobExecutionError(
                "Official backtest template is blocked by the NLP quality gate: "
                + "; ".join(multimodal_readiness.official_nlp_gate_reasons or ["official_nlp_gate_failed"])
            )
        context.start_stage("prepare", "Resolving official multimodal rolling benchmark")
        source_prediction_frames: list[dict[str, object]] = []
        market_projection_ref: DatasetRef | None = None
        projected_dataset_ids: list[str] = []
        projected_dataset_roles: dict[str, str] = {}
        projected_dataset_modalities: dict[str, str] = {}
        for index, item in enumerate(source_runs):
            if not isinstance(item, dict):
                continue
            source_run_id = self._str(item.get("run_id"))
            modality = self._normalize_modality(self._str(item.get("modality")))
            if not source_run_id or not modality:
                raise JobExecutionError("Composed run contains a source without run_id/modality metadata.")
            source_metadata = self.workbench.get_run_model_metadata(source_run_id)
            base_dataset_id = (
                OFFICIAL_MARKET_BENCHMARK_DATASET_ID
                if modality == "market"
                else OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID
            )
            feature_names = self._feature_names_for_run(
                source_metadata,
                self._load_dataset_from_artifacts(base_dataset_id),
            )
            projected_ref = self._build_official_windowed_dataset(
                base_dataset_id=base_dataset_id,
                feature_names=feature_names,
                window_start=official_context["window_start"],
                window_end=official_context["window_end"],
                alias_dataset_id=f"{request.run_id}__{source_run_id}_official_{window_days}d",
            )
            projected_dataset_ids.append(projected_ref.dataset_id)
            projected_dataset_roles[projected_ref.dataset_id] = (
                "market_anchor" if modality == "market" else f"official_{modality}"
            )
            projected_dataset_modalities[projected_ref.dataset_id] = modality
            source_model_artifact_uri = self.workbench.resolve_run_model_artifact_uri(source_run_id)
            if source_model_artifact_uri is None:
                raise JobExecutionError(
                    f"Unable to resolve model artifact for source run '{source_run_id}'."
                )
            prediction_frame = self.facade.prediction_runner.predict(
                PredictRequest(
                    model_artifact_uri=source_model_artifact_uri,
                    dataset_ref=projected_ref,
                    prediction_scope=PredictionScope(
                        scope_name=effective_prediction_scope,
                        as_of_time=projected_ref.feature_view_ref.as_of_time,
                    ),
                )
            )
            if len(prediction_frame.rows) < OFFICIAL_MIN_SOURCE_PREDICTION_ROWS:
                raise JobExecutionError(
                    f"Source run '{source_run_id}' produced only {len(prediction_frame.rows)} predictions in the "
                    f"{window_days}d official window; at least {OFFICIAL_MIN_SOURCE_PREDICTION_ROWS} are required."
                )
            if modality == "market":
                market_projection_ref = projected_ref
            source_prediction_frames.append(
                {
                    "run_id": source_run_id,
                    "model_name": self._str(item.get("model_name")) or source_run_id,
                    "modality": modality,
                    "dataset_id": projected_ref.dataset_id,
                    "selected_dataset_id": projected_ref.dataset_id,
                    "weight": float(item.get("weight", 1.0) or 1.0),
                    "frame": prediction_frame,
                }
            )
        if not source_prediction_frames:
            raise JobExecutionError("Official multimodal benchmark produced no source predictions.")
        if market_projection_ref is None:
            market_projection_ref = self._build_official_windowed_dataset(
                base_dataset_id=OFFICIAL_MARKET_BENCHMARK_DATASET_ID,
                feature_names=[
                    field.name
                    for field in self._load_dataset_from_artifacts(
                        OFFICIAL_MARKET_BENCHMARK_DATASET_ID
                    ).feature_view_ref.feature_schema
                ],
                window_start=official_context["window_start"],
                window_end=official_context["window_end"],
                alias_dataset_id=f"{request.run_id}__market_anchor_official_{window_days}d",
            )
        market_bars = self.workbench.load_market_bars_for_dataset(
            OFFICIAL_MARKET_BENCHMARK_DATASET_ID
        )
        market_bars = self._slice_market_bars_for_window(
            market_bars,
            window_start=official_context["window_start"],
            window_end=official_context["window_end"],
        )
        if not market_bars:
            raise JobExecutionError("Official market benchmark has no bars inside the selected rolling window.")
        context.finish_stage("prepare", f"Resolved official multimodal benchmark for {len(source_prediction_frames)} sources")
        required_modalities = [
            self._normalize_modality(modality)
            for modality in preflight.required_modalities
            if self._normalize_modality(modality) != "unknown"
        ]
        metadata_summary = self._build_backtest_metadata_summary(
            run_manifest=run_manifest,
            run_metadata=run_metadata,
            dataset_id=OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID,
            readiness=multimodal_readiness,
        )
        protocol_metadata = build_protocol_metadata(
            template=template,
            launch_mode=request.mode,
            prediction_scope=effective_prediction_scope,
            dataset_id=OFFICIAL_MARKET_BENCHMARK_DATASET_ID,
            dataset_frequency=self._dataset_frequency(OFFICIAL_MARKET_BENCHMARK_DATASET_ID),
            target_name=self._label_target_name(OFFICIAL_MARKET_BENCHMARK_DATASET_ID),
            label_horizon=self._dataset_label_horizon(OFFICIAL_MARKET_BENCHMARK_DATASET_ID),
            lookback_bucket=self._dataset_lookback_bucket(OFFICIAL_MARKET_BENCHMARK_DATASET_ID),
            metadata_summary=metadata_summary,
            required_modalities=required_modalities,
            official_benchmark_version=str(official_context["benchmark_version"]),
            official_window_days=window_days,
            official_window_start_time=official_context["window_start"].isoformat(),
            official_window_end_time=official_context["window_end"].isoformat(),
            official_market_dataset_id=OFFICIAL_MARKET_BENCHMARK_DATASET_ID,
            official_multimodal_dataset_id=OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID,
            official_dataset_ids=[
                OFFICIAL_MARKET_BENCHMARK_DATASET_ID,
                OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID,
            ],
        )
        protocol_metadata.update(
            {
                "actual_market_start_time": multimodal_readiness.market_window_start_time.isoformat()
                if multimodal_readiness.market_window_start_time is not None
                else None,
                "actual_market_end_time": multimodal_readiness.market_window_end_time.isoformat()
                if multimodal_readiness.market_window_end_time is not None
                else None,
                "actual_backtest_start_time": official_context["window_start"].isoformat(),
                "actual_backtest_end_time": official_context["window_end"].isoformat(),
                "actual_nlp_start_time": multimodal_readiness.nlp_actual_start_time.isoformat()
                if multimodal_readiness.nlp_actual_start_time is not None
                else None,
                "actual_nlp_end_time": multimodal_readiness.nlp_actual_end_time.isoformat()
                if multimodal_readiness.nlp_actual_end_time is not None
                else None,
                "nlp_gate_status": multimodal_readiness.official_nlp_gate_status,
                "nlp_gate_reasons": list(multimodal_readiness.official_nlp_gate_reasons),
                "fusion_strategy": self._str(composition.get("fusion_strategy")) or "late_score_blend",
                "primary_dataset_id": OFFICIAL_MARKET_BENCHMARK_DATASET_ID,
                "dataset_ids": projected_dataset_ids,
                "dataset_roles": projected_dataset_roles,
                "dataset_modalities": projected_dataset_modalities,
                "alignment_status": "official_rolling_window",
            }
        )
        context.start_stage(
            "predict",
            f"Generating multimodal official predictions for the latest {window_days}d window",
        )
        merged_prediction_frame, alignment_notes = self._blend_prediction_frames(
            run_id=request.run_id,
            source_prediction_frames=source_prediction_frames,
        )
        protocol_metadata["alignment_notes"] = alignment_notes
        prediction_artifact = self.facade.store.write_model(
            f"predictions/{request.run_id}/{effective_prediction_scope}.json",
            merged_prediction_frame,
        )
        context.finish_stage("predict", "Official multimodal prediction artifact written")
        context.start_stage("backtest", "Running official multimodal rolling-window backtest")
        backtest_result = self.facade.backtest_workflow.backtest(
            BacktestWorkflowRequest(
                prediction_inputs=[
                    PredictionInputRef(
                        model_name=self._str(run_metadata.get("model_name")) or request.run_id,
                        run_id=request.run_id,
                        prediction_frame_uri=prediction_artifact.uri,
                    )
                ],
                backtest_request_template=self._backtest_template(
                    prediction_frame_uri=prediction_artifact.uri,
                    benchmark_symbol=self._resolve_benchmark_symbol(
                        request=request,
                        dataset_id=OFFICIAL_MARKET_BENCHMARK_DATASET_ID,
                    ),
                    mode=request.mode,
                    research_backend=request.research_backend,
                    portfolio_method=request.portfolio_method,
                ),
                dataset_ref=market_projection_ref,
                benchmark_name="workbench_backtest",
                data_source="official_rolling_benchmark",
                market_bars=market_bars,
                summary_row_metadata=protocol_metadata,
            )
        )
        backtest_ids = [item.backtest_result.backtest_id for item in backtest_result.items]
        context.finish_stage("backtest", f"Completed {len(backtest_ids)} official multimodal backtest run(s)")
        return JobResultView(
            dataset_id=OFFICIAL_MARKET_BENCHMARK_DATASET_ID,
            dataset_ids=projected_dataset_ids,
            run_ids=[request.run_id],
            backtest_ids=backtest_ids,
            prediction_scope=effective_prediction_scope,
            template_id=template.template_id,
            template_name=template.name,
            official=template.official,
            protocol_version=template.protocol_version,
            research_backend=request.research_backend,
            portfolio_method=request.portfolio_method,
        )

    def _build_backtest_metadata_summary(
        self,
        *,
        run_manifest: dict[str, object],
        run_metadata: dict[str, object],
        dataset_id: str | None,
        readiness,
    ) -> dict[str, str | None]:
        detail = self.workbench.get_dataset_detail(dataset_id) if dataset_id else None
        dataset_start = detail.dataset.freshness.data_start_time if detail is not None else None
        dataset_end = detail.dataset.freshness.data_end_time if detail is not None else None
        model_spec = run_metadata.get("model_spec") if isinstance(run_metadata.get("model_spec"), dict) else {}
        hyperparams = model_spec.get("hyperparams") if isinstance(model_spec, dict) else {}
        if not isinstance(hyperparams, dict):
            hyperparams = {}
        tracking_seed = self._str((run_manifest.get("repro_context") or {}).get("seed")) if isinstance(run_manifest.get("repro_context"), dict) else None
        data_domains = detail.dataset.data_domains if detail is not None else []
        prediction_metadata = (
            run_metadata.get("prediction_metadata")
            if isinstance(run_metadata.get("prediction_metadata"), dict)
            else {}
        )
        actual_market_start = (
            readiness.market_window_start_time.isoformat()
            if readiness is not None and readiness.market_window_start_time is not None
            else None
        )
        actual_market_end = (
            readiness.market_window_end_time.isoformat()
            if readiness is not None and readiness.market_window_end_time is not None
            else None
        )
        actual_backtest_start = (
            readiness.official_backtest_start_time.isoformat()
            if readiness is not None and readiness.official_backtest_start_time is not None
            else None
        )
        actual_backtest_end = (
            readiness.official_backtest_end_time.isoformat()
            if readiness is not None and readiness.official_backtest_end_time is not None
            else None
        )
        actual_nlp_start = (
            readiness.nlp_actual_start_time.isoformat()
            if readiness is not None and readiness.nlp_actual_start_time is not None
            else None
        )
        actual_nlp_end = (
            readiness.nlp_actual_end_time.isoformat()
            if readiness is not None and readiness.nlp_actual_end_time is not None
            else None
        )
        return {
            "train_start_time": dataset_start.isoformat() if dataset_start is not None else None,
            "train_end_time": dataset_end.isoformat() if dataset_end is not None else None,
            "lookback_window": self._stringify(hyperparams.get("lookback")),
            "label_horizon": self._stringify(
                detail.dataset.label_horizon if detail is not None else run_manifest.get("label_horizon")
            ),
            "modalities": ", ".join(data_domains) if data_domains else None,
            "fusion_summary": (
                self._str(prediction_metadata.get("fusion_strategy"))
                or (
                    "late_score_blend"
                    if self._str(run_metadata.get("advanced_kind")) == "multimodal"
                    else None
                )
                or (self._str(detail.acquisition_profile.get("merge_policy_name")) if detail is not None else None)
            ),
            "random_seed": tracking_seed,
            "tuning_trials": self._stringify((run_manifest.get("tracking") or {}).get("trial_count")) if isinstance(run_manifest.get("tracking"), dict) else None,
            "external_pretraining": self._stringify(run_metadata.get("pretrained")),
            "synthetic_data": self._stringify(run_manifest.get("synthetic_reference")),
            "actual_market_start_time": actual_market_start,
            "actual_market_end_time": actual_market_end,
            "actual_backtest_start_time": actual_backtest_start,
            "actual_backtest_end_time": actual_backtest_end,
            "actual_nlp_start_time": actual_nlp_start,
            "actual_nlp_end_time": actual_nlp_end,
            "nlp_gate_status": (
                readiness.official_nlp_gate_status if readiness is not None else None
            ),
        }

    def _dataset_detail(self, dataset_id: str | None):
        if not dataset_id:
            return None
        return self.workbench.get_dataset_detail(dataset_id)

    def _dataset_frequency(self, dataset_id: str | None) -> str | None:
        detail = self._dataset_detail(dataset_id)
        return detail.dataset.frequency if detail is not None else None

    def _dataset_label_horizon(self, dataset_id: str | None) -> int | None:
        detail = self._dataset_detail(dataset_id)
        return detail.dataset.label_horizon if detail is not None else None

    def _label_target_name(self, dataset_id: str | None) -> str | None:
        detail = self._dataset_detail(dataset_id)
        if detail is None:
            return None
        label_columns = detail.label_columns
        if label_columns:
            return label_columns[0]
        label_spec = detail.label_spec if isinstance(detail.label_spec, dict) else {}
        return self._str(label_spec.get("label_name")) or self._str(label_spec.get("name"))

    def _dataset_lookback_bucket(self, dataset_id: str | None) -> str | None:
        detail = self._dataset_detail(dataset_id)
        if detail is None:
            return None
        return derive_lookback_bucket(
            detail.dataset.freshness.data_start_time,
            detail.dataset.freshness.data_end_time,
        )

    @staticmethod
    def _stringify(value: object) -> str | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float, str)):
            text = str(value).strip()
            return text or None
        return None

    def _update_stage(
        self,
        job_id: str,
        *,
        name: str,
        status: str,
        summary: str,
        started_at: datetime | None,
        finished_at: datetime | None,
    ) -> None:
        with self._lock:
            current = self.get_job(job_id)
            if current is None:
                raise JobExecutionError(f"Job '{job_id}' does not exist.")
            stages = list(current.stages)
            for index, stage in enumerate(stages):
                if stage.name == name:
                    stages[index] = JobStageView(
                        name=name,
                        status=status,
                        summary=summary or stage.summary,
                        started_at=started_at or stage.started_at,
                        finished_at=finished_at,
                    )
                    break
            else:
                stages.append(
                    JobStageView(
                        name=name,
                        status=status,
                        summary=summary,
                        started_at=started_at,
                        finished_at=finished_at,
                    )
                )
            updated = current.model_copy(
                update={
                    "stages": stages,
                    "updated_at": datetime.now(UTC),
                }
            )
            self._write_status(updated)

    def _update_status(
        self,
        job_id: str,
        *,
        status: str | None = None,
        result: JobResultView | None = None,
        error_message: str | None = None,
    ) -> None:
        with self._lock:
            current = self.get_job(job_id)
            if current is None:
                raise JobExecutionError(f"Job '{job_id}' does not exist.")
            updated = current.model_copy(
                update={
                    "status": status or current.status,
                    "result": result if result is not None else current.result,
                    "error_message": error_message,
                    "updated_at": datetime.now(UTC),
                }
            )
            self._write_status(updated)

    def _mark_active_stage_failed(self, job_id: str, error_message: str) -> None:
        with self._lock:
            current = self.get_job(job_id)
            if current is None:
                return
            stages = list(current.stages)
            for index in range(len(stages) - 1, -1, -1):
                stage = stages[index]
                if stage.status == "running":
                    stages[index] = stage.model_copy(
                        update={
                            "status": "failed",
                            "summary": error_message or stage.summary,
                            "finished_at": datetime.now(UTC),
                        }
                    )
                    break
            self._write_status(current.model_copy(update={"stages": stages, "updated_at": datetime.now(UTC)}))

    def _read_job(self, path: Path) -> JobStatusView:
        payload = json.loads(path.read_text(encoding="utf-8"))
        try:
            raw = JobStatusView.model_validate(self._normalize_job_payload(payload))
        except ValidationError as exc:
            raw = self._build_invalid_job_status(path, exc)
        return self._enrich_job(raw)

    def _normalize_job_payload(self, payload: object) -> dict[str, object]:
        if not isinstance(payload, dict):
            return {}

        normalized = {
            field_name: payload[field_name]
            for field_name in JobStatusView.model_fields
            if field_name in payload
        }

        result = payload.get("result")
        if isinstance(result, dict):
            normalized["result"] = {
                field_name: result[field_name]
                for field_name in JobResultView.model_fields
                if field_name in result
            }

        stages = payload.get("stages")
        if isinstance(stages, list):
            normalized["stages"] = [
                {
                    field_name: stage[field_name]
                    for field_name in JobStageView.model_fields
                    if isinstance(stage, dict) and field_name in stage
                }
                for stage in stages
                if isinstance(stage, dict)
            ]

        return normalized

    def _build_invalid_job_status(self, path: Path, exc: ValidationError) -> JobStatusView:
        timestamp = datetime.fromtimestamp(path.stat().st_mtime, UTC)
        return JobStatusView(
            job_id=path.stem,
            job_type="unknown",
            status="failed",
            created_at=timestamp,
            updated_at=timestamp,
            stages=[],
            result=JobResultView(),
            error_message=f"Stored job payload is incompatible with current schema: {exc.errors()[0]['msg']}",
        )

    def _enrich_job(self, job: JobStatusView) -> JobStatusView:
        result_links = self._result_links(job.result)
        pipeline_summary = self._pipeline_summary(job)
        summary = self._job_summary(job, result_links)
        result = job.result.model_copy(
            update={
                "result_links": result_links,
                "deeplinks": self._deeplink_map(result_links),
                "pipeline_summary": pipeline_summary,
                "summary": summary,
            }
        )
        return job.model_copy(update={"result": result})

    def _result_links(self, result: JobResultView) -> list[DeepLinkView]:
        links: list[DeepLinkView] = []
        if result.dataset_id:
            links.append(
                DeepLinkView(
                    kind="dataset_detail",
                    label=f"Dataset {result.dataset_id}",
                    href=f"/datasets/{result.dataset_id}",
                    api_path=f"/api/datasets/{result.dataset_id}",
                )
            )
        if result.base_dataset_id:
            links.append(
                DeepLinkView(
                    kind="base_dataset_detail",
                    label=f"Base dataset {result.base_dataset_id}",
                    href=f"/datasets/{result.base_dataset_id}",
                    api_path=f"/api/datasets/{result.base_dataset_id}",
                )
            )
        if result.fusion_dataset_id:
            links.append(
                DeepLinkView(
                    kind="fusion_dataset_detail",
                    label=f"Fusion dataset {result.fusion_dataset_id}",
                    href=f"/datasets/{result.fusion_dataset_id}",
                    api_path=f"/api/datasets/{result.fusion_dataset_id}",
                )
            )
        for run_id in result.run_ids:
            links.append(
                DeepLinkView(
                    kind="run_detail",
                    label=f"Run {run_id}",
                    href=f"/runs/{run_id}",
                    api_path=f"/api/runs/{run_id}",
                )
            )
        for backtest_id in result.backtest_ids:
            links.append(
                DeepLinkView(
                    kind="backtest_detail",
                    label=f"Backtest {backtest_id}",
                    href=f"/backtests/{backtest_id}",
                    api_path=f"/api/backtests/{backtest_id}",
                )
            )
        for benchmark_name in result.benchmark_names:
            links.append(
                DeepLinkView(
                    kind="benchmark_detail",
                    label=f"Benchmark {benchmark_name}",
                    href=f"/benchmarks/{benchmark_name}",
                    api_path=f"/api/benchmarks/{benchmark_name}",
                )
            )
        return links

    def _pipeline_summary(self, job: JobStatusView) -> PipelineSummaryView:
        stage_order = [
            "acquire_base",
            "prepare_base",
            "readiness_base",
            "build_fusion",
            "readiness_fusion",
            "acquire",
            "prepare",
            "train",
            "predict",
            "backtest",
            "readiness",
            "review",
        ]
        requested = list(job.result.requested_stages) or self._requested_stages(job.job_type)
        existing = {stage.name: stage for stage in job.stages}
        current_stage = next((stage.name for stage in job.stages if stage.status == "running"), None)
        stages: list[PipelineStageView] = []
        completed: list[str] = []
        for stage_name in stage_order:
            stage = existing.get(stage_name)
            if stage is not None:
                status = stage.status
                summary = stage.summary
                started_at = stage.started_at
                finished_at = stage.finished_at
            elif stage_name in requested:
                if job.status in {"queued", "running"}:
                    status = "queued"
                elif job.status == "failed":
                    status = "blocked"
                else:
                    status = "not_requested"
                summary = ""
                started_at = None
                finished_at = None
            else:
                status = "not_requested"
                summary = ""
                started_at = None
                finished_at = None
            if status == "success":
                completed.append(stage_name)
            stages.append(
                PipelineStageView(
                    stage=stage_name,
                    status=status,
                    summary=summary,
                    started_at=started_at,
                    finished_at=finished_at,
                )
            )
        return PipelineSummaryView(
            status=job.status,
            current_stage=current_stage,
            requested_stages=requested,
            completed_stages=completed,
            stages=stages,
        )

    def _requested_stages(self, job_type: str) -> list[str]:
        if job_type == "backtest":
            return ["prepare", "predict", "backtest"]
        if job_type == "dataset_request":
            return ["acquire", "prepare", "readiness"]
        if job_type == "dataset_pipeline":
            return [
                "acquire_base",
                "prepare_base",
                "readiness_base",
                "build_fusion",
                "readiness_fusion",
                "train",
            ]
        if job_type == "train":
            return ["prepare", "train"]
        return []

    def _job_summary(self, job: JobStatusView, result_links: list[DeepLinkView]) -> StableSummaryView:
        if job.status == "failed":
            return StableSummaryView(
                status="failed",
                headline=f"{job.job_type.title()} job failed",
                detail=job.error_message,
                warnings=[job.error_message] if job.error_message else [],
            )
        if job.job_type == "train":
            run_count = len(job.result.run_ids)
            return StableSummaryView(
                status=job.status,
                headline=f"Training produced {run_count} run(s)",
                detail=(
                    f"Dataset: {job.result.dataset_id}" if job.result.dataset_id is not None else None
                ),
                highlights=[f"Run IDs: {', '.join(job.result.run_ids)}"] if job.result.run_ids else [],
                recommended_actions=(
                    ["Open the latest run detail and launch a backtest."]
                    if result_links
                    else []
                ),
            )
        if job.job_type == "backtest":
            backtest_count = len(job.result.backtest_ids)
            return StableSummaryView(
                status=job.status,
                headline=f"Backtest produced {backtest_count} report(s)",
                detail=(
                    f"Dataset: {job.result.dataset_id}" if job.result.dataset_id is not None else None
                ),
                highlights=(
                    [f"Backtest IDs: {', '.join(job.result.backtest_ids)}"]
                    if job.result.backtest_ids
                    else []
                ),
                recommended_actions=(
                    ["Open the latest backtest detail and inspect consistency warnings."]
                    if result_links
                    else []
                ),
            )
        if job.job_type == "dataset_request":
            return StableSummaryView(
                status=job.status,
                headline="Dataset request completed",
                detail=(
                    f"Dataset: {job.result.dataset_id}"
                    if job.result.dataset_id is not None
                    else None
                ),
                recommended_actions=(
                    ["Open the dataset detail and verify readiness."]
                    if result_links
                    else []
                ),
            )
        if job.job_type == "dataset_pipeline":
            final_dataset_id = (
                job.result.fusion_dataset_id
                or job.result.dataset_id
                or job.result.base_dataset_id
            )
            highlights = []
            if job.result.base_dataset_id:
                highlights.append(f"Base dataset: {job.result.base_dataset_id}")
            if job.result.fusion_dataset_id:
                highlights.append(f"Fusion dataset: {job.result.fusion_dataset_id}")
            if job.result.run_ids:
                highlights.append(f"Run IDs: {', '.join(job.result.run_ids)}")
            return StableSummaryView(
                status=job.status,
                headline="Dataset pipeline completed",
                detail=(f"Final dataset: {final_dataset_id}" if final_dataset_id else None),
                highlights=highlights,
                recommended_actions=(
                    ["Open the final dataset or the latest training run."]
                    if result_links
                    else []
                ),
            )
        return StableSummaryView(status=job.status, headline=f"{job.job_type.title()} job {job.status}")

    def _write_status(self, status: JobStatusView) -> None:
        target = self.jobs_root / f"{status.job_id}.json"
        temp_target = target.with_suffix(".json.tmp")
        temp_target.write_text(
            self._enrich_job(status).model_dump_json(indent=2),
            encoding="utf-8",
        )
        last_error: OSError | None = None
        for attempt in range(5):
            try:
                temp_target.replace(target)
                return
            except PermissionError as exc:
                last_error = exc
                time.sleep(0.05 * (attempt + 1))
        if last_error is not None:
            raise last_error

    def _resolve_model_specs_for_request(
        self,
        *,
        request: LaunchTrainRequest,
        feature_schema: list[SchemaField],
        template: ModelTemplateView | None = None,
    ) -> list[ModelSpec]:
        selected: list[tuple[str, dict[str, object]]] = []
        if template is not None:
            merged = {**template.hyperparams, **request.template_overrides}
            selected.append((template.model_name, merged))
        elif request.model_names:
            selected.extend((name, {}) for name in request.model_names)
        else:
            options = self.workbench.list_model_templates().items
            if not options:
                raise JobExecutionError("No available model templates found.")
            default_template = next(
                (item for item in options if item.template_id == "registry::elastic_net"),
                options[0],
            )
            selected.append((default_template.model_name, default_template.hyperparams))

        model_specs: list[ModelSpec] = []
        for model_name, extra_hyperparams in selected:
            try:
                registration = self.facade.model_registry.resolve_registration(model_name)
            except KeyError as exc:
                raise JobExecutionError(
                    f"Model '{model_name}' is not available in current registry."
                ) from exc
            model_specs.append(
                ModelSpec(
                    model_name=model_name,
                    family=registration.family,
                    version="0.1.0",
                    input_schema=feature_schema,
                    output_schema=[SchemaField(name="prediction", dtype="float")],
                    hyperparams={**registration.default_hyperparams, **extra_hyperparams},
                )
            )
        return model_specs

    def _resolve_template_for_request(
        self,
        request: LaunchTrainRequest,
    ) -> ModelTemplateView | None:
        if not request.template_id:
            return None
        template = self.workbench.get_model_template(request.template_id)
        if template is None:
            raise JobExecutionError(f"Template '{request.template_id}' does not exist.")
        if template.deleted_at is not None:
            raise JobExecutionError(f"Template '{request.template_id}' has been deleted.")
        return template

    def _validate_train_selection(
        self,
        *,
        request: LaunchTrainRequest,
        template: ModelTemplateView | None,
    ) -> None:
        if template is not None and request.model_names:
            raise JobExecutionError(
                "template_id and model_names cannot be supplied together."
            )
        if len(request.model_names) > 5:
            raise JobExecutionError(
                "No more than 5 model_names can be launched in a single request."
            )

    def _deeplink_map(self, links: list[DeepLinkView]) -> dict[str, str | None]:
        values: dict[str, str | None] = {
            "dataset_detail": None,
            "base_dataset_detail": None,
            "fusion_dataset_detail": None,
            "run_detail": None,
            "backtest_detail": None,
            "review_detail": None,
        }
        for link in links:
            if link.kind in values and values[link.kind] is None:
                values[link.kind] = link.href
        return values

    def _dataset_id_from_request_name(self, request_name: str) -> str:
        sanitized = "".join(
            character.lower() if character.isalnum() else "_"
            for character in request_name.strip()
        ).strip("_")
        sanitized = "_".join(segment for segment in sanitized.split("_") if segment)
        return sanitized or f"dataset_request_{datetime.now(UTC):%Y%m%d%H%M%S}"

    @staticmethod
    def _str(value: object) -> str | None:
        return value if isinstance(value, str) and value else None
