from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from pydantic import Field

from quant_platform.common.enums.core import AgentKind, ToolSideEffectLevel
from quant_platform.common.types.core import ArtifactRef, FrozenModel

from .base import AgentRequest, AgentResponse, GuardrailPolicy
from .tooling import ToolCall, ToolResult


class ArtifactProfile(FrozenModel):
    artifact_kinds: list[str] = Field(default_factory=list)
    total_artifacts: int = 0
    train_manifest_count: int = 0
    fit_result_count: int = 0
    prediction_count: int = 0
    backtest_result_count: int = 0
    backtest_report_count: int = 0


class WorkflowArtifactBundle(FrozenModel):
    workflow_id: str | None = None
    artifacts: list[ArtifactRef] = Field(default_factory=list)
    train_manifest_refs: list[ArtifactRef] = Field(default_factory=list)
    fit_result_refs: list[ArtifactRef] = Field(default_factory=list)
    prediction_refs: list[ArtifactRef] = Field(default_factory=list)
    backtest_result_refs: list[ArtifactRef] = Field(default_factory=list)
    backtest_report_refs: list[ArtifactRef] = Field(default_factory=list)
    metadata: dict[str, str] = Field(default_factory=dict)


class ArtifactBundleSummary(FrozenModel):
    workflow_id: str | None = None
    artifact_profile: ArtifactProfile
    artifact_refs: list[ArtifactRef] = Field(default_factory=list)


class RoutePolicy(FrozenModel):
    allow_fallback: bool = False
    enable_default_handoffs: bool = True
    allow_requested_agent_override: bool = False


class RouteDecision(FrozenModel):
    requested_agent: AgentKind
    resolved_agent: AgentKind
    task_type: str
    artifact_profile: ArtifactProfile
    decision_reason: str
    fallback_used: bool = False
    warnings: list[str] = Field(default_factory=list)
    requires_handoff: bool = False
    planned_handoff_chain: list[AgentKind] = Field(default_factory=list)
    is_allowed: bool = True
    blocked_reason: str | None = None


class ArtifactAccessScope(FrozenModel):
    allowed_artifact_kinds: list[str] = Field(default_factory=list)
    artifact_refs: list[ArtifactRef] = Field(default_factory=list)
    reason: str = ""


class AuthorizationPolicy(FrozenModel):
    require_registered_tools: bool = True
    deny_unlisted_artifact_kinds: bool = True
    max_side_effect_level: ToolSideEffectLevel = ToolSideEffectLevel.PROPOSAL_ONLY


class AuthorizationSnapshot(FrozenModel):
    request_id: str
    agent_kind: AgentKind
    allowed_tools_requested: list[str] = Field(default_factory=list)
    allowed_tools_effective: list[str] = Field(default_factory=list)
    denied_tools: list[str] = Field(default_factory=list)
    guardrail_policy: GuardrailPolicy
    artifact_access_scope: ArtifactAccessScope
    side_effect_budget: ToolSideEffectLevel
    authorization_reason: str


class HandoffRecord(FrozenModel):
    handoff_id: str
    request_id: str
    from_agent: AgentKind
    to_agent: AgentKind
    reason: str
    triggered_by: str
    input_artifacts: list[ArtifactRef] = Field(default_factory=list)
    carryover_context_refs: list[str] = Field(default_factory=list)
    carryover_allowed_tools: list[str] = Field(default_factory=list)
    upstream_response_ref: ArtifactRef | None = None
    status: Literal["success", "blocked", "skipped"] = "success"
    blocked_reason: str | None = None


class FailureEvent(FrozenModel):
    stage: str
    code: str
    message: str
    details: dict[str, str] = Field(default_factory=dict)


class AuditCompletenessChecks(FrozenModel):
    has_route_decision: bool
    has_authorization_snapshots: bool
    has_handoff_records: bool
    has_tool_trace: bool
    has_input_artifacts: bool
    has_final_response: bool
    missing_sections: list[str] = Field(default_factory=list)
    is_complete: bool


class AuditBundle(FrozenModel):
    bundle_id: str
    request_id: str
    workflow_id: str | None = None
    route_decision: RouteDecision
    authorization_snapshots: list[AuthorizationSnapshot] = Field(default_factory=list)
    artifact_bundle_summary: ArtifactBundleSummary
    tool_calls: list[ToolCall] = Field(default_factory=list)
    tool_results: list[ToolResult] = Field(default_factory=list)
    handoffs: list[HandoffRecord] = Field(default_factory=list)
    agent_response_refs: list[ArtifactRef] = Field(default_factory=list)
    failure_events: list[FailureEvent] = Field(default_factory=list)
    completeness_checks: AuditCompletenessChecks
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class OrchestrationRequest(FrozenModel):
    request: AgentRequest
    workflow_id: str | None = None
    artifact_bundle: WorkflowArtifactBundle | None = None
    route_policy: RoutePolicy = Field(default_factory=RoutePolicy)
    authorization_policy: AuthorizationPolicy | None = None


class OrchestrationResult(FrozenModel):
    route_decision: RouteDecision
    authorization_snapshot: AuthorizationSnapshot
    handoffs: list[HandoffRecord] = Field(default_factory=list)
    agent_responses: list[AgentResponse] = Field(default_factory=list)
    audit_bundle: AuditBundle
    final_response: AgentResponse
