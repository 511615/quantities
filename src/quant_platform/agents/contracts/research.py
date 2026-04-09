from __future__ import annotations

from pydantic import Field

from quant_platform.common.enums.core import AgentKind
from quant_platform.common.types.core import ArtifactRef

from .base import AgentRequest, AgentResponse


class ResearchAgentRequest(AgentRequest):
    agent_kind: AgentKind = AgentKind.RESEARCH
    experiment_refs: list[ArtifactRef] = Field(default_factory=list)
    comparison_mode: str | None = None


class ResearchAgentResponse(AgentResponse):
    agent_kind: AgentKind = AgentKind.RESEARCH
