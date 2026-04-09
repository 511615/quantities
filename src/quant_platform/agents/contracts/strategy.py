from __future__ import annotations

from pydantic import Field

from quant_platform.common.enums.core import AgentKind
from quant_platform.common.types.core import ArtifactRef

from .base import AgentRequest, AgentResponse


class StrategyAgentRequest(AgentRequest):
    agent_kind: AgentKind = AgentKind.STRATEGY
    signal_refs: list[ArtifactRef] = Field(default_factory=list)
    portfolio_constraints_ref: ArtifactRef | None = None


class StrategyAgentResponse(AgentResponse):
    agent_kind: AgentKind = AgentKind.STRATEGY
