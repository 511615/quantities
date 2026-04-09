from __future__ import annotations

from pydantic import Field

from quant_platform.common.enums.core import AgentKind
from quant_platform.common.types.core import ArtifactRef

from .base import AgentRequest, AgentResponse


class RiskAgentRequest(AgentRequest):
    agent_kind: AgentKind = AgentKind.RISK
    risk_report_refs: list[ArtifactRef] = Field(default_factory=list)
    limit_policy_ref: ArtifactRef | None = None


class RiskAgentResponse(AgentResponse):
    agent_kind: AgentKind = AgentKind.RISK
