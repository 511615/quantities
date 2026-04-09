from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import Field

from quant_platform.common.enums.core import (
    AgentKind,
    AgentResponseMode,
    AgentTaskStatus,
    ToolCallStatus,
)
from quant_platform.common.types.core import ArtifactRef, FrozenModel


class GuardrailPolicy(FrozenModel):
    allow_core_state_mutation: bool = False
    allow_raw_dataframe_inputs: bool = False
    allow_external_action_tools: bool = False


class ExecutionContextRef(FrozenModel):
    context_id: str
    context_type: str
    uri: str | None = None
    description: str | None = None


class AgentError(FrozenModel):
    code: str
    message: str
    retryable: bool = False
    details: dict[str, Any] = Field(default_factory=dict)


class ToolResultRef(FrozenModel):
    tool_name: str
    result_artifact: ArtifactRef


class InlineToolResult(FrozenModel):
    call_id: str
    tool_name: str
    status: ToolCallStatus
    payload: dict[str, Any] = Field(default_factory=dict)
    artifacts: list[ArtifactRef] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    metrics: dict[str, float] = Field(default_factory=dict)
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class AgentRequest(FrozenModel):
    request_id: str
    agent_kind: AgentKind
    task_type: str
    goal: str
    input_artifacts: list[ArtifactRef] = Field(default_factory=list)
    context_refs: list[str] = Field(default_factory=list)
    execution_contexts: list[ExecutionContextRef] = Field(default_factory=list)
    allowed_tools: list[str] = Field(default_factory=list)
    guardrail_policy: GuardrailPolicy = Field(default_factory=GuardrailPolicy)
    response_mode: AgentResponseMode = AgentResponseMode.INLINE_TOOL_RESULTS
    metadata: dict[str, Any] = Field(default_factory=dict)


class AgentResponse(FrozenModel):
    request_id: str
    agent_kind: AgentKind
    status: AgentTaskStatus
    summary: str
    observations: list[str] = Field(default_factory=list)
    tool_results: list[ToolResultRef | InlineToolResult] = Field(default_factory=list)
    output_artifacts: list[ArtifactRef] = Field(default_factory=list)
    proposed_actions: list[str] = Field(default_factory=list)
    audit_log_uri: str
    audit_bundle_ref: ArtifactRef | None = None
    error: AgentError | None = None
