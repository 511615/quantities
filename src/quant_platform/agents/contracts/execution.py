from __future__ import annotations

from quant_platform.common.enums.core import AgentKind
from quant_platform.common.types.core import ArtifactRef

from .base import AgentRequest, AgentResponse


class ExecutionAgentRequest(AgentRequest):
    agent_kind: AgentKind = AgentKind.EXECUTION
    order_intents_ref: ArtifactRef | None = None
    venue_constraints_ref: ArtifactRef | None = None


class ExecutionAgentResponse(AgentResponse):
    agent_kind: AgentKind = AgentKind.EXECUTION
