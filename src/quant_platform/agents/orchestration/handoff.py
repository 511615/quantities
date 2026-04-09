from __future__ import annotations

from quant_platform.agents.contracts.base import AgentRequest, AgentResponse
from quant_platform.agents.contracts.execution import ExecutionAgentRequest
from quant_platform.agents.contracts.orchestration import HandoffRecord, WorkflowArtifactBundle
from quant_platform.agents.contracts.risk import RiskAgentRequest
from quant_platform.agents.contracts.strategy import StrategyAgentRequest
from quant_platform.common.enums.core import AgentKind
from quant_platform.common.types.core import ArtifactRef


class AgentHandoff:
    _ALLOWED_TRANSITIONS = {
        AgentKind.RESEARCH: {AgentKind.STRATEGY},
        AgentKind.STRATEGY: {AgentKind.RISK},
        AgentKind.RISK: {AgentKind.EXECUTION},
    }

    def build_record(
        self,
        *,
        request_id: str,
        from_agent: AgentKind,
        to_agent: AgentKind,
        reason: str,
        triggered_by: str,
        input_artifacts: list[ArtifactRef],
        carryover_context_refs: list[str],
        carryover_allowed_tools: list[str],
        upstream_response_ref: ArtifactRef | None,
    ) -> HandoffRecord:
        if to_agent not in self._ALLOWED_TRANSITIONS.get(from_agent, set()):
            return HandoffRecord(
                handoff_id=f"{request_id}:{from_agent.value}:{to_agent.value}",
                request_id=request_id,
                from_agent=from_agent,
                to_agent=to_agent,
                reason=reason,
                triggered_by=triggered_by,
                input_artifacts=input_artifacts,
                carryover_context_refs=carryover_context_refs,
                carryover_allowed_tools=carryover_allowed_tools,
                upstream_response_ref=upstream_response_ref,
                status="blocked",
                blocked_reason="illegal handoff transition",
            )
        if upstream_response_ref is None:
            return HandoffRecord(
                handoff_id=f"{request_id}:{from_agent.value}:{to_agent.value}",
                request_id=request_id,
                from_agent=from_agent,
                to_agent=to_agent,
                reason=reason,
                triggered_by=triggered_by,
                input_artifacts=input_artifacts,
                carryover_context_refs=carryover_context_refs,
                carryover_allowed_tools=carryover_allowed_tools,
                upstream_response_ref=upstream_response_ref,
                status="blocked",
                blocked_reason="upstream response reference is required",
            )
        return HandoffRecord(
            handoff_id=f"{request_id}:{from_agent.value}:{to_agent.value}",
            request_id=request_id,
            from_agent=from_agent,
            to_agent=to_agent,
            reason=reason,
            triggered_by=triggered_by,
            input_artifacts=input_artifacts,
            carryover_context_refs=carryover_context_refs,
            carryover_allowed_tools=carryover_allowed_tools,
            upstream_response_ref=upstream_response_ref,
            status="success",
        )

    def build_next_request(
        self,
        *,
        original_request: AgentRequest,
        handoff: HandoffRecord,
        upstream_response: AgentResponse,
        artifact_bundle: WorkflowArtifactBundle | None,
    ) -> AgentRequest | None:
        if handoff.status != "success":
            return None
        if handoff.to_agent == AgentKind.STRATEGY:
            return StrategyAgentRequest(
                request_id=original_request.request_id,
                task_type=original_request.task_type,
                goal=original_request.goal,
                input_artifacts=handoff.input_artifacts,
                context_refs=handoff.carryover_context_refs,
                allowed_tools=handoff.carryover_allowed_tools,
                metadata={**original_request.metadata, "handoff_from": handoff.from_agent.value},
            )
        if handoff.to_agent == AgentKind.RISK:
            risk_artifacts = (
                artifact_bundle.backtest_result_refs
                if artifact_bundle is not None and artifact_bundle.backtest_result_refs
                else handoff.input_artifacts
            )
            return RiskAgentRequest(
                request_id=original_request.request_id,
                task_type=original_request.task_type,
                goal=original_request.goal,
                input_artifacts=risk_artifacts,
                context_refs=handoff.carryover_context_refs,
                allowed_tools=handoff.carryover_allowed_tools,
                metadata={**original_request.metadata, "handoff_from": handoff.from_agent.value},
            )
        if handoff.to_agent == AgentKind.EXECUTION:
            order_ref = next(
                (
                    artifact
                    for artifact in handoff.input_artifacts
                    if artifact.kind == "order_intents"
                ),
                None,
            )
            if order_ref is None and artifact_bundle is not None:
                order_ref = next(
                    (
                        artifact
                        for artifact in artifact_bundle.artifacts
                        if artifact.kind == "order_intents"
                    ),
                    None,
                )
            if order_ref is None:
                return None
            return ExecutionAgentRequest(
                request_id=original_request.request_id,
                task_type=original_request.task_type,
                goal=original_request.goal,
                input_artifacts=handoff.input_artifacts,
                context_refs=handoff.carryover_context_refs,
                allowed_tools=handoff.carryover_allowed_tools,
                order_intents_ref=order_ref,
                metadata={**original_request.metadata, "handoff_from": handoff.from_agent.value},
            )
        _ = upstream_response
        return None
