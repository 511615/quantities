from __future__ import annotations

import json
import threading
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
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
from quant_platform.common.types.core import SchemaField
from quant_platform.datasets.contracts.dataset import DatasetRef, DatasetSample
from quant_platform.models.contracts.model_spec import ModelSpec
from quant_platform.training.contracts.training import (
    PredictionScope,
    PredictRequest,
    TrackingContext,
    TrainerConfig,
)
from quant_platform.webapi.schemas.launch import (
    BacktestLaunchOptionsView,
    LaunchBacktestRequest,
    LaunchJobResponse,
    LaunchTrainRequest,
    PresetOptionView,
    TrainLaunchOptionsView,
)
from quant_platform.webapi.schemas.views import (
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
    PipelineStageView,
    PipelineSummaryView,
    StableSummaryView,
)
from quant_platform.webapi.services.catalog import ResearchWorkbenchService
from quant_platform.workflows.contracts.requests import (
    BacktestWorkflowRequest,
    DatasetAcquisitionRequest as WorkflowDatasetAcquisitionRequest,
    PredictionInputRef,
    TrainWorkflowRequest,
)


class JobExecutionError(RuntimeError):
    """Raised when a launch job cannot be executed safely."""


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
        baseline_specs = self.facade.benchmark_workflow.build_baseline_model_specs()
        model_names = sorted({spec.model_name for spec in baseline_specs})
        template_response = self.workbench.list_model_templates()
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
                "required_fields": ["template_id", "trainer_preset"],
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
        return BacktestLaunchOptionsView(
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
            default_benchmark_symbol="BTCUSDT",
            constraints={
                "required_fields": ["run_id", "prediction_scope", "strategy_preset"],
                "dataset_selector": {
                    "accepted_fields": ["dataset_id", "dataset_preset"],
                    "priority": "dataset_id_gt_run_manifest_gt_dataset_preset",
                },
            },
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
        if request.dataset_id:
            context.start_stage("prepare", f"Loading dataset '{request.dataset_id}'")
            dataset_ref = self._load_dataset_from_artifacts(request.dataset_id)
            context.finish_stage("prepare", f"Loaded dataset '{request.dataset_id}'")
        else:
            prepare_request, dataset_name = self._prepare_request_for_dataset(request.dataset_preset)
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
        model_artifact_uri = self.workbench.resolve_run_model_artifact_uri(request.run_id)
        if model_artifact_uri is None:
            raise JobExecutionError(
                f"Unable to resolve model artifact for run '{request.run_id}'."
            )
        run_manifest = self.workbench.get_run_manifest(request.run_id)
        run_metadata = self.workbench.get_run_model_metadata(request.run_id)
        manifest_dataset_id = self._str(run_manifest.get("dataset_id"))
        selected_dataset_id = request.dataset_id or manifest_dataset_id
        if selected_dataset_id is None:
            prepare_request, dataset_name = self._prepare_request_for_dataset(request.dataset_preset)
            context.start_stage("prepare", f"Preparing debug dataset preset: {dataset_name}")
            prepare_result = self.facade.prepare_workflow.prepare(prepare_request)
            dataset_ref = prepare_result.dataset_ref
            market_bars = prepare_request.market_bars
            data_source = prepare_request.data_source
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

        context.start_stage("predict", f"Generating predictions for scope '{request.prediction_scope}'")
        prediction_frame = self.facade.prediction_runner.predict(
            PredictRequest(
                model_artifact_uri=model_artifact_uri,
                dataset_ref=dataset_ref,
                prediction_scope=PredictionScope(
                    scope_name=request.prediction_scope,
                    as_of_time=dataset_ref.feature_view_ref.as_of_time,
                ),
            )
        )
        prediction_artifact = self.facade.store.write_model(
            f"predictions/{request.run_id}/{request.prediction_scope}.json",
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
                    benchmark_symbol=request.benchmark_symbol,
                ),
                dataset_ref=dataset_ref,
                benchmark_name="workbench_backtest",
                data_source=data_source,
                market_bars=market_bars,
            )
        )
        backtest_ids = [item.backtest_result.backtest_id for item in backtest_result.items]
        context.finish_stage("backtest", f"Completed {len(backtest_ids)} backtest run(s)")
        return JobResultView(
            dataset_id=dataset_ref.dataset_id,
            run_ids=[request.run_id],
            backtest_ids=backtest_ids,
            prediction_scope=request.prediction_scope,
        )

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
            prepare_request = self.facade.prepare_workflow.build_prepare_request_from_dataset_request(
                workflow_request
            )
            context.start_stage(prepare_stage, f"Preparing dataset '{prepare_request.dataset_id}'")
            prepare_result = self.facade.prepare_workflow.prepare(prepare_request)
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
        model_specs = self._resolve_model_specs_for_request(
            request=request,
            feature_schema=dataset_ref.feature_view_ref.feature_schema,
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
                trainer_config=self._trainer_config(request.trainer_preset),
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
        merge_policy_name = request.merge_policy_name or "strict_timestamp_inner"
        if merge_policy_name != "strict_timestamp_inner":
            raise JobExecutionError(
                f"Unsupported merge policy '{merge_policy_name}'. Only strict_timestamp_inner is allowed."
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

    def _load_dataset_from_artifacts(self, dataset_id: str) -> DatasetRef:
        dataset_ref_path = self.artifact_root / "datasets" / f"{dataset_id}_dataset_ref.json"
        if not dataset_ref_path.exists():
            raise JobExecutionError(f"Dataset '{dataset_id}' does not exist.")
        dataset_ref = self.facade.store.read_model(str(dataset_ref_path), DatasetRef)
        if dataset_id not in self.facade.dataset_store:
            samples_path = self.artifact_root / "datasets" / f"{dataset_id}_dataset_samples.json"
            if not samples_path.exists():
                raise JobExecutionError(f"Dataset samples for '{dataset_id}' are missing.")
            payload = self.facade.store.read_json(str(samples_path))
            self.facade.dataset_store[dataset_id] = [
                DatasetSample.model_validate(item) for item in payload.get("samples", [])
            ]
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
    ) -> BacktestRequest:
        return BacktestRequest(
            prediction_frame_uri=prediction_frame_uri,
            strategy_config=StrategyConfig(name="sign_strategy"),
            portfolio_config=PortfolioConfig(
                initial_cash=100000.0,
                max_gross_leverage=1.0,
                max_position_weight=1.0,
            ),
            cost_model=CostModel(fee_bps=5.0, slippage_bps=2.0),
            benchmark_spec=BenchmarkSpec(name="buy_and_hold", symbol=benchmark_symbol),
            calendar_spec=CalendarSpec(timezone="UTC", frequency="1h"),
        )

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
        temp_target.replace(target)

    def _resolve_model_specs_for_request(
        self,
        *,
        request: LaunchTrainRequest,
        feature_schema: list[SchemaField],
    ) -> list[ModelSpec]:
        selected: list[tuple[str, dict[str, object]]] = []
        if request.template_id:
            template = self.workbench.get_model_template(request.template_id)
            if template is None:
                raise JobExecutionError(f"Template '{request.template_id}' does not exist.")
            if template.deleted_at is not None:
                raise JobExecutionError(f"Template '{request.template_id}' has been deleted.")
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
