from __future__ import annotations

from datetime import UTC, datetime

from quant_platform.common.hashing.digest import stable_digest
from quant_platform.common.types.core import ArtifactRef
from quant_platform.training.contracts.training import FitResult, PredictRequest
from quant_platform.workflows.contracts.requests import PredictWorkflowRequest
from quant_platform.workflows.contracts.results import (
    PredictWorkflowItem,
    PredictWorkflowResult,
)
from quant_platform.workflows.contracts.state import (
    WorkflowStageName,
    WorkflowStageResult,
    WorkflowStageStatus,
)
from quant_platform.workflows.runtime import WorkflowRuntime


class PredictWorkflowService:
    def __init__(self, runtime: WorkflowRuntime) -> None:
        self.runtime = runtime

    def predict(self, request: PredictWorkflowRequest) -> PredictWorkflowResult:
        started_at = datetime.now(UTC)
        if request.dataset_ref is None:
            raise ValueError("predict workflow requires dataset_ref")
        fit_results = self._resolve_fit_results(request)
        items: list[PredictWorkflowItem] = []
        artifacts: list[ArtifactRef] = []
        for fit_result in fit_results:
            frame = self.runtime.prediction_runner.predict(
                PredictRequest(
                    model_artifact_uri=fit_result.model_artifact_uri,
                    dataset_ref=request.dataset_ref,
                    prediction_scope=request.prediction_scope,
                )
            )
            artifact = self.runtime.store.write_model(
                f"predictions/{fit_result.run_id}/{request.prediction_scope.scope_name}.json",
                frame,
            )
            items.append(
                PredictWorkflowItem(
                    model_name=fit_result.model_name,
                    run_id=fit_result.run_id,
                    prediction_frame_uri=artifact.uri,
                    prediction_frame=frame,
                )
            )
            artifacts.append(ArtifactRef(kind="prediction_frame", uri=artifact.uri))
        stage_result = WorkflowStageResult(
            stage=WorkflowStageName.PREDICT,
            status=WorkflowStageStatus.SUCCESS,
            request_digest=stable_digest(request),
            started_at=started_at,
            finished_at=datetime.now(UTC),
            artifacts=artifacts,
            summary=f"generated predictions for {len(items)} trained run(s)",
        )
        return PredictWorkflowResult(
            stage_result=stage_result,
            dataset_ref=request.dataset_ref,
            items=items,
        )

    def _resolve_fit_results(self, request: PredictWorkflowRequest) -> list[FitResult]:
        fit_results = list(request.fit_results)
        fit_results.extend(
            self.runtime.store.read_model(uri, FitResult) for uri in request.fit_result_refs
        )
        if not fit_results:
            raise ValueError("predict workflow requires fit_results or fit_result_refs")
        return fit_results
