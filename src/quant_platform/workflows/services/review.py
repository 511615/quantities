from __future__ import annotations

from datetime import UTC, datetime

from quant_platform.agents.contracts.orchestration import (
    OrchestrationRequest,
    WorkflowArtifactBundle,
)
from quant_platform.agents.contracts.research import ResearchAgentRequest
from quant_platform.common.enums.core import AgentTaskType
from quant_platform.common.hashing.digest import stable_digest
from quant_platform.common.types.core import ArtifactRef
from quant_platform.workflows.contracts.requests import ReviewWorkflowRequest
from quant_platform.workflows.contracts.results import ReviewWorkflowResult
from quant_platform.workflows.contracts.results import ReviewWorkflowRecord
from quant_platform.workflows.contracts.state import (
    WorkflowStageName,
    WorkflowStageResult,
    WorkflowStageStatus,
)
from quant_platform.workflows.runtime import WorkflowRuntime


class ReviewWorkflowService:
    def __init__(self, runtime: WorkflowRuntime) -> None:
        self.runtime = runtime

    def review(self, request: ReviewWorkflowRequest) -> ReviewWorkflowResult:
        started_at = datetime.now(UTC)
        task_type = (
            AgentTaskType(request.task_type)
            if request.task_type in AgentTaskType._value2member_map_
            else AgentTaskType.SUMMARIZE_EXPERIMENT
        )
        orchestration_result = self.runtime.agent_service.orchestrate(
            OrchestrationRequest(
                request=ResearchAgentRequest(
                    request_id=request.request_id,
                    task_type=task_type,
                    goal=request.goal,
                    input_artifacts=request.input_artifacts,
                    experiment_refs=request.experiment_refs,
                    comparison_mode=request.comparison_mode,
                    allowed_tools=request.allowed_tools,
                    guardrail_policy=request.guardrail_policy,
                ),
                workflow_id=request.request_id,
                artifact_bundle=WorkflowArtifactBundle(
                    workflow_id=request.request_id,
                    artifacts=[*request.experiment_refs, *request.input_artifacts],
                    train_manifest_refs=list(request.experiment_refs),
                    backtest_report_refs=[
                        artifact
                        for artifact in request.input_artifacts
                        if artifact.kind == "backtest_report"
                    ],
                    backtest_result_refs=[
                        artifact
                        for artifact in request.input_artifacts
                        if artifact.kind == "backtest_result"
                    ],
                ),
            )
        )
        response = orchestration_result.final_response
        response_artifact = self.runtime.store.write_model(
            f"workflows/review/{request.request_id}.json",
            response,
        )
        self.runtime.store.write_model(
            f"workflows/review/{request.request_id}_record.json",
            ReviewWorkflowRecord(
                request_id=request.request_id,
                goal=request.goal,
                created_at=started_at,
                experiment_refs=request.experiment_refs,
                input_artifacts=request.input_artifacts,
                comparison_mode=request.comparison_mode,
                response_uri=response_artifact.uri,
                audit_log_uri=response.audit_log_uri,
            ),
        )
        stage_result = WorkflowStageResult(
            stage=WorkflowStageName.REVIEW,
            status=(
                WorkflowStageStatus.SUCCESS
                if response.status == "success"
                else WorkflowStageStatus.FAILED
            ),
            request_digest=stable_digest(request),
            started_at=started_at,
            finished_at=datetime.now(UTC),
            artifacts=[
                ArtifactRef(kind="review_response", uri=response_artifact.uri),
                ArtifactRef(kind="review_audit", uri=response.audit_log_uri),
            ],
            summary=f"review completed for request '{request.request_id}'",
        )
        return ReviewWorkflowResult(
            stage_result=stage_result,
            response_uri=response_artifact.uri,
            response=response,
        )
