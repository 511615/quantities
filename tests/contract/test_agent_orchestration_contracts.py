from __future__ import annotations

from quant_platform.agents.contracts.orchestration import (
    ArtifactProfile,
    RouteDecision,
    WorkflowArtifactBundle,
)
from quant_platform.agents.contracts.research import ResearchAgentRequest
from quant_platform.agents.orchestration.audit import AuditBundleBuilder
from quant_platform.agents.orchestration.authorization import ToolAuthorizationService
from quant_platform.agents.orchestration.handoff import AgentHandoff
from quant_platform.agents.tool_registry.adapters import register_default_tools
from quant_platform.agents.tool_registry.registry import ToolRegistry
from quant_platform.agents.services import (
    BacktestQueryService,
    ExecutionProposalService,
    RiskQueryService,
    StrategyProposalService,
    TrainingQueryService,
)
from quant_platform.common.enums.core import AgentKind
from quant_platform.common.types.core import ArtifactRef


def test_route_decision_contract_supports_blocked_state() -> None:
    decision = RouteDecision(
        requested_agent=AgentKind.EXECUTION,
        resolved_agent=AgentKind.EXECUTION,
        task_type="execute",
        artifact_profile=ArtifactProfile(artifact_kinds=["order_intents"], total_artifacts=1),
        decision_reason="execution request missing authorized tools",
        is_allowed=False,
        blocked_reason="execution agent requires at least one execution tool",
    )
    assert decision.is_allowed is False
    assert decision.blocked_reason == "execution agent requires at least one execution tool"


def test_handoff_rejects_illegal_transition() -> None:
    handoff = AgentHandoff().build_record(
        request_id="req-1",
        from_agent=AgentKind.RESEARCH,
        to_agent=AgentKind.EXECUTION,
        reason="illegal jump",
        triggered_by="test",
        input_artifacts=[],
        carryover_context_refs=[],
        carryover_allowed_tools=[],
        upstream_response_ref=ArtifactRef(kind="research_response", uri="memory://research"),
    )
    assert handoff.status == "blocked"
    assert handoff.blocked_reason == "illegal handoff transition"


def test_authorization_snapshot_denies_unregistered_tools(tmp_path) -> None:
    registry = ToolRegistry()
    register_default_tools(
        registry,
        training_queries=TrainingQueryService(tmp_path),
        backtest_queries=BacktestQueryService(tmp_path),
        risk_queries=RiskQueryService(tmp_path),
        strategy_proposals=StrategyProposalService(tmp_path),
        execution_proposals=ExecutionProposalService(tmp_path),
    )
    auth = ToolAuthorizationService(registry).authorize(
        request=ResearchAgentRequest(
            request_id="req-1",
            task_type="summarize_experiment",
            goal="review",
            allowed_tools=["research.read_experiment_summary", "unknown.tool"],
        ),
        resolved_agent=AgentKind.RESEARCH,
        artifact_bundle=WorkflowArtifactBundle(),
    )
    assert "research.read_experiment_summary" in auth.allowed_tools_effective
    assert "unknown.tool" in auth.denied_tools


def test_audit_bundle_reports_missing_sections() -> None:
    route_decision = RouteDecision(
        requested_agent=AgentKind.RESEARCH,
        resolved_agent=AgentKind.RESEARCH,
        task_type="summarize_experiment",
        artifact_profile=ArtifactProfile(),
        decision_reason="test",
    )
    bundle = AuditBundleBuilder().build(
        request_id="req-1",
        workflow_id=None,
        route_decision=route_decision,
        authorization_snapshots=[],
        artifact_bundle=None,
        tool_calls=[],
        tool_results=[],
        handoffs=[],
        response_refs=[],
        failure_events=[],
        final_response=None,
    )
    assert bundle.completeness_checks.is_complete is False
    assert "authorization_snapshots" in bundle.completeness_checks.missing_sections
    assert "final_response" in bundle.completeness_checks.missing_sections
