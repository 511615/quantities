from __future__ import annotations

from datetime import UTC, datetime

from quant_platform.common.hashing.digest import stable_digest
from quant_platform.common.types.core import ArtifactRef
from quant_platform.training.contracts.training import FitRequest
from quant_platform.workflows.contracts.requests import TrainWorkflowRequest
from quant_platform.workflows.contracts.results import (
    TrainLeaderboardEntry,
    TrainWorkflowItem,
    TrainWorkflowResult,
)
from quant_platform.workflows.contracts.state import (
    WorkflowStageName,
    WorkflowStageResult,
    WorkflowStageStatus,
)
from quant_platform.workflows.runtime import WorkflowRuntime


class TrainWorkflowService:
    def __init__(self, runtime: WorkflowRuntime) -> None:
        self.runtime = runtime

    def train(self, request: TrainWorkflowRequest) -> TrainWorkflowResult:
        started_at = datetime.now(UTC)
        if request.dataset_ref is None:
            raise ValueError("train workflow requires dataset_ref")
        items: list[TrainWorkflowItem] = []
        artifact_refs: list[ArtifactRef] = []
        multiple_models = len(request.model_specs) > 1
        for index, model_spec in enumerate(request.model_specs):
            run_id = (
                f"{request.run_id_prefix}-{model_spec.model_name}-{index:02d}"
                if multiple_models
                else request.run_id_prefix
            )
            fit_result = self.runtime.training_runner.fit(
                FitRequest(
                    run_id=run_id,
                    dataset_ref=request.dataset_ref,
                    model_spec=model_spec,
                    trainer_config=request.trainer_config,
                    seed=request.seed,
                    tracking_context=request.tracking_context,
                )
            )
            fit_result_artifact = self.runtime.store.write_model(
                f"workflows/train/{run_id}_fit_result.json",
                fit_result,
            )
            items.append(
                TrainWorkflowItem(
                    model_name=model_spec.model_name,
                    fit_result_uri=fit_result_artifact.uri,
                    fit_result=fit_result,
                )
            )
            artifact_refs.extend(
                [
                    ArtifactRef(kind="fit_result", uri=fit_result_artifact.uri),
                    ArtifactRef(kind="train_manifest", uri=fit_result.train_manifest_uri),
                ]
            )
        ranked_items = sorted(
            items,
            key=lambda item: item.fit_result.metrics.get(request.ranking_metric, float("inf")),
            reverse=not request.lower_is_better,
        )
        leaderboard = [
            TrainLeaderboardEntry(
                rank=index + 1,
                model_name=item.model_name,
                metric_name=request.ranking_metric,
                metric_value=float(item.fit_result.metrics.get(request.ranking_metric, 0.0)),
                fit_result_uri=item.fit_result_uri,
            )
            for index, item in enumerate(ranked_items)
        ]
        stage_result = WorkflowStageResult(
            stage=WorkflowStageName.TRAIN,
            status=WorkflowStageStatus.SUCCESS,
            request_digest=stable_digest(request),
            started_at=started_at,
            finished_at=datetime.now(UTC),
            artifacts=artifact_refs,
            summary=f"trained {len(items)} model(s) on dataset '{request.dataset_ref.dataset_id}'",
        )
        return TrainWorkflowResult(
            stage_result=stage_result,
            dataset_ref=request.dataset_ref,
            items=items,
            leaderboard=leaderboard,
        )
