from __future__ import annotations

from pydantic import Field

from quant_platform.common.enums.core import AgentResponseMode, AgentTaskStatus, AgentTaskType
from quant_platform.common.types.core import ArtifactRef, FrozenModel

from .base import GuardrailPolicy


class AgentTask(FrozenModel):
    task_id: str
    task_type: AgentTaskType
    input_refs: list[str] = Field(default_factory=list)
    allowed_tools: list[str] = Field(default_factory=list)
    guardrail_policy: GuardrailPolicy = Field(default_factory=GuardrailPolicy)
    expected_outputs: list[str] = Field(default_factory=list)
    response_mode: AgentResponseMode = AgentResponseMode.INLINE_TOOL_RESULTS


class AgentResult(FrozenModel):
    task_id: str
    status: AgentTaskStatus
    observations: list[str] = Field(default_factory=list)
    artifact_refs: list[ArtifactRef] = Field(default_factory=list)
    proposed_actions: list[str] = Field(default_factory=list)
    audit_log_uri: str
