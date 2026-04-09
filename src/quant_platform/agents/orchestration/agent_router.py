from __future__ import annotations

from quant_platform.agents.contracts.base import AgentRequest
from quant_platform.agents.contracts.orchestration import (
    ArtifactProfile,
    RouteDecision,
    RoutePolicy,
    WorkflowArtifactBundle,
)
from quant_platform.common.enums.core import AgentKind


class AgentRouter:
    """Resolve an orchestration request into an explicit route decision."""

    def route(
        self,
        request: AgentRequest,
        artifact_bundle: WorkflowArtifactBundle | None = None,
        route_policy: RoutePolicy | None = None,
    ) -> RouteDecision:
        policy = route_policy or RoutePolicy()
        artifact_profile = self._build_artifact_profile(request, artifact_bundle)

        if request.agent_kind == AgentKind.RESEARCH:
            return RouteDecision(
                requested_agent=request.agent_kind,
                resolved_agent=AgentKind.RESEARCH,
                task_type=request.task_type,
                artifact_profile=artifact_profile,
                decision_reason="research agent is compatible with train/backtest review artifacts",
            )

        if request.agent_kind == AgentKind.STRATEGY:
            planned_handoffs = (
                [AgentKind.RISK]
                if policy.enable_default_handoffs
                and (
                    artifact_profile.backtest_result_count > 0
                    or artifact_profile.backtest_report_count > 0
                    or request.metadata.get("handoff_to") == AgentKind.RISK.value
                )
                else []
            )
            return RouteDecision(
                requested_agent=request.agent_kind,
                resolved_agent=AgentKind.STRATEGY,
                task_type=request.task_type,
                artifact_profile=artifact_profile,
                decision_reason="strategy agent accepted explicit strategy planning request",
                requires_handoff=bool(planned_handoffs),
                planned_handoff_chain=planned_handoffs,
            )

        if request.agent_kind == AgentKind.RISK:
            planned_handoffs = (
                [AgentKind.EXECUTION]
                if policy.enable_default_handoffs
                and (
                    "order_intents" in artifact_profile.artifact_kinds
                    or request.metadata.get("handoff_to") == AgentKind.EXECUTION.value
                )
                else []
            )
            return RouteDecision(
                requested_agent=request.agent_kind,
                resolved_agent=AgentKind.RISK,
                task_type=request.task_type,
                artifact_profile=artifact_profile,
                decision_reason="risk agent accepted explicit risk review request",
                requires_handoff=bool(planned_handoffs),
                planned_handoff_chain=planned_handoffs,
            )

        if request.agent_kind == AgentKind.EXECUTION:
            if any(tool.startswith("execution.") for tool in request.allowed_tools):
                return RouteDecision(
                    requested_agent=request.agent_kind,
                    resolved_agent=AgentKind.EXECUTION,
                    task_type=request.task_type,
                    artifact_profile=artifact_profile,
                    decision_reason="execution agent accepted explicit execution planning request",
                )
            return RouteDecision(
                requested_agent=request.agent_kind,
                resolved_agent=AgentKind.EXECUTION,
                task_type=request.task_type,
                artifact_profile=artifact_profile,
                decision_reason="execution request is missing execution tool authorization",
                warnings=["execution route has no execution tools enabled"],
                is_allowed=False,
                blocked_reason="execution agent requires at least one execution tool",
            )

        return RouteDecision(
            requested_agent=request.agent_kind,
            resolved_agent=request.agent_kind,
            task_type=request.task_type,
            artifact_profile=artifact_profile,
            decision_reason="unsupported agent route",
            warnings=["requested agent kind is not routable"],
            is_allowed=False,
            blocked_reason="unsupported agent kind",
        )

    def _build_artifact_profile(
        self,
        request: AgentRequest,
        artifact_bundle: WorkflowArtifactBundle | None,
    ) -> ArtifactProfile:
        artifacts = list(request.input_artifacts)
        if artifact_bundle is not None:
            artifacts.extend(artifact_bundle.artifacts)
            artifacts.extend(artifact_bundle.train_manifest_refs)
            artifacts.extend(artifact_bundle.fit_result_refs)
            artifacts.extend(artifact_bundle.prediction_refs)
            artifacts.extend(artifact_bundle.backtest_result_refs)
            artifacts.extend(artifact_bundle.backtest_report_refs)
        kinds = [artifact.kind for artifact in artifacts]
        return ArtifactProfile(
            artifact_kinds=sorted(set(kinds)),
            total_artifacts=len(artifacts),
            train_manifest_count=sum(
                1 for artifact in artifacts if artifact.kind == "train_manifest"
            ),
            fit_result_count=sum(1 for artifact in artifacts if artifact.kind == "fit_result"),
            prediction_count=sum(
                1 for artifact in artifacts if artifact.kind == "prediction_frame"
            ),
            backtest_result_count=sum(
                1 for artifact in artifacts if artifact.kind == "backtest_result"
            ),
            backtest_report_count=sum(
                1 for artifact in artifacts if artifact.kind == "backtest_report"
            ),
        )
