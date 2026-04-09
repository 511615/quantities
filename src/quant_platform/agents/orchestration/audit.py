from __future__ import annotations

from quant_platform.agents.contracts.base import AgentResponse
from quant_platform.agents.contracts.orchestration import (
    ArtifactBundleSummary,
    ArtifactProfile,
    AuditBundle,
    AuditCompletenessChecks,
    AuthorizationSnapshot,
    FailureEvent,
    HandoffRecord,
    RouteDecision,
    WorkflowArtifactBundle,
)
from quant_platform.agents.contracts.tooling import ToolCall, ToolResult
from quant_platform.common.hashing.digest import stable_digest
from quant_platform.common.types.core import ArtifactRef


class AuditBundleBuilder:
    def build(
        self,
        *,
        request_id: str,
        workflow_id: str | None,
        route_decision: RouteDecision,
        authorization_snapshots: list[AuthorizationSnapshot],
        artifact_bundle: WorkflowArtifactBundle | None,
        tool_calls: list[ToolCall],
        tool_results: list[ToolResult],
        handoffs: list[HandoffRecord],
        response_refs: list[ArtifactRef],
        failure_events: list[FailureEvent],
        final_response: AgentResponse | None,
    ) -> AuditBundle:
        summary = self._build_summary(route_decision.artifact_profile, artifact_bundle)
        checks = self._build_checks(
            authorization_snapshots=authorization_snapshots,
            artifact_summary=summary,
            tool_calls=tool_calls,
            tool_results=tool_results,
            handoffs=handoffs,
            final_response=final_response,
        )
        return AuditBundle(
            bundle_id=stable_digest(
                {
                    "request_id": request_id,
                    "workflow_id": workflow_id,
                    "route_decision": route_decision,
                    "response_refs": response_refs,
                    "handoffs": handoffs,
                }
            ),
            request_id=request_id,
            workflow_id=workflow_id,
            route_decision=route_decision,
            authorization_snapshots=authorization_snapshots,
            artifact_bundle_summary=summary,
            tool_calls=tool_calls,
            tool_results=tool_results,
            handoffs=handoffs,
            agent_response_refs=response_refs,
            failure_events=failure_events,
            completeness_checks=checks,
        )

    def _build_summary(
        self,
        artifact_profile: ArtifactProfile,
        artifact_bundle: WorkflowArtifactBundle | None,
    ) -> ArtifactBundleSummary:
        refs: list[ArtifactRef] = []
        if artifact_bundle is not None:
            refs.extend(artifact_bundle.artifacts)
            refs.extend(artifact_bundle.train_manifest_refs)
            refs.extend(artifact_bundle.fit_result_refs)
            refs.extend(artifact_bundle.prediction_refs)
            refs.extend(artifact_bundle.backtest_result_refs)
            refs.extend(artifact_bundle.backtest_report_refs)
        return ArtifactBundleSummary(
            workflow_id=artifact_bundle.workflow_id if artifact_bundle is not None else None,
            artifact_profile=artifact_profile,
            artifact_refs=refs,
        )

    def _build_checks(
        self,
        *,
        authorization_snapshots: list[AuthorizationSnapshot],
        artifact_summary: ArtifactBundleSummary,
        tool_calls: list[ToolCall],
        tool_results: list[ToolResult],
        handoffs: list[HandoffRecord],
        final_response: AgentResponse | None,
    ) -> AuditCompletenessChecks:
        missing: list[str] = []
        has_auth = bool(authorization_snapshots)
        has_tool_trace = len(tool_calls) == len(tool_results)
        has_input_artifacts = bool(
            artifact_summary.artifact_refs or artifact_summary.artifact_profile.total_artifacts > 0
        )
        has_final_response = final_response is not None
        if not has_auth:
            missing.append("authorization_snapshots")
        if not has_tool_trace:
            missing.append("tool_trace")
        if not has_input_artifacts:
            missing.append("input_artifacts")
        if not has_final_response:
            missing.append("final_response")
        return AuditCompletenessChecks(
            has_route_decision=True,
            has_authorization_snapshots=has_auth,
            has_handoff_records=handoffs is not None,
            has_tool_trace=has_tool_trace,
            has_input_artifacts=has_input_artifacts,
            has_final_response=has_final_response,
            missing_sections=missing,
            is_complete=not missing,
        )
