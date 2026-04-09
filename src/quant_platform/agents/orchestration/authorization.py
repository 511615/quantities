from __future__ import annotations

from quant_platform.agents.contracts.base import AgentRequest
from quant_platform.agents.contracts.orchestration import (
    ArtifactAccessScope,
    AuthorizationPolicy,
    AuthorizationSnapshot,
    WorkflowArtifactBundle,
)
from quant_platform.agents.tool_registry.registry import ToolRegistry
from quant_platform.common.enums.core import AgentKind, ToolSideEffectLevel
from quant_platform.common.types.core import ArtifactRef


class ToolAuthorizationService:
    _DEFAULT_ARTIFACT_KINDS: dict[AgentKind, set[str]] = {
        AgentKind.RESEARCH: {
            "train_manifest",
            "fit_result",
            "prediction_frame",
            "backtest_result",
            "backtest_report",
            "review_audit",
            "audit_log",
        },
        AgentKind.STRATEGY: {
            "feature_view",
            "signal_recipe",
            "portfolio_constraints",
            "backtest_report",
        },
        AgentKind.RISK: {"backtest_result", "backtest_report", "strategy_draft", "risk_policy"},
        AgentKind.EXECUTION: {
            "order_intents",
            "venue_constraints",
            "risk_limits",
            "execution_plan",
        },
    }

    def __init__(self, registry: ToolRegistry) -> None:
        self.registry = registry

    def authorize(
        self,
        request: AgentRequest,
        *,
        resolved_agent: AgentKind,
        artifact_bundle: WorkflowArtifactBundle | None = None,
        policy: AuthorizationPolicy | None = None,
    ) -> AuthorizationSnapshot:
        authorization_policy = policy or AuthorizationPolicy()
        effective_tools: list[str] = []
        denied_tools: list[str] = []
        side_effect_budget = (
            ToolSideEffectLevel.EXTERNAL_ACTION
            if request.guardrail_policy.allow_external_action_tools
            else ToolSideEffectLevel.PROPOSAL_ONLY
        )
        for tool_name in request.allowed_tools:
            if not self.registry.is_registered(tool_name):
                denied_tools.append(tool_name)
                continue
            spec = self.registry.get_spec(tool_name)
            if resolved_agent not in spec.allowed_agent_kinds:
                denied_tools.append(tool_name)
                continue
            if spec.side_effect_level == ToolSideEffectLevel.EXTERNAL_ACTION and (
                not request.guardrail_policy.allow_external_action_tools
                or authorization_policy.max_side_effect_level != ToolSideEffectLevel.EXTERNAL_ACTION
            ):
                denied_tools.append(tool_name)
                continue
            effective_tools.append(tool_name)
        artifact_scope = ArtifactAccessScope(
            allowed_artifact_kinds=sorted(
                self._allowed_artifact_kinds(resolved_agent, artifact_bundle)
            ),
            artifact_refs=self._artifact_refs(request, artifact_bundle),
            reason="scoped to explicit request artifacts and agent-readable workflow artifacts",
        )
        return AuthorizationSnapshot(
            request_id=request.request_id,
            agent_kind=resolved_agent,
            allowed_tools_requested=list(request.allowed_tools),
            allowed_tools_effective=effective_tools,
            denied_tools=denied_tools,
            guardrail_policy=request.guardrail_policy,
            artifact_access_scope=artifact_scope,
            side_effect_budget=side_effect_budget,
            authorization_reason=(
                "effective tools are reduced to registered, agent-allowed, "
                "side-effect-safe tools"
            ),
        )

    def _allowed_artifact_kinds(
        self,
        agent_kind: AgentKind,
        artifact_bundle: WorkflowArtifactBundle | None,
    ) -> set[str]:
        allowed = set(self._DEFAULT_ARTIFACT_KINDS.get(agent_kind, set()))
        if artifact_bundle is not None:
            allowed.update(artifact.kind for artifact in artifact_bundle.artifacts)
            allowed.update(artifact.kind for artifact in artifact_bundle.train_manifest_refs)
            allowed.update(artifact.kind for artifact in artifact_bundle.fit_result_refs)
            allowed.update(artifact.kind for artifact in artifact_bundle.prediction_refs)
            allowed.update(artifact.kind for artifact in artifact_bundle.backtest_result_refs)
            allowed.update(artifact.kind for artifact in artifact_bundle.backtest_report_refs)
        return allowed

    @staticmethod
    def _artifact_refs(
        request: AgentRequest,
        artifact_bundle: WorkflowArtifactBundle | None,
    ) -> list[ArtifactRef]:
        refs = list(request.input_artifacts)
        if artifact_bundle is not None:
            refs.extend(artifact_bundle.artifacts)
            refs.extend(artifact_bundle.train_manifest_refs)
            refs.extend(artifact_bundle.fit_result_refs)
            refs.extend(artifact_bundle.prediction_refs)
            refs.extend(artifact_bundle.backtest_result_refs)
            refs.extend(artifact_bundle.backtest_report_refs)
        return refs
