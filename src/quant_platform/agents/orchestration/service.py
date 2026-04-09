from __future__ import annotations

from pathlib import Path

from quant_platform.agents.contracts.base import AgentError, AgentRequest, AgentResponse
from quant_platform.agents.contracts.execution import ExecutionAgentRequest
from quant_platform.agents.contracts.orchestration import (
    AuthorizationSnapshot,
    FailureEvent,
    OrchestrationRequest,
    OrchestrationResult,
)
from quant_platform.agents.contracts.research import ResearchAgentRequest
from quant_platform.agents.contracts.risk import RiskAgentRequest
from quant_platform.agents.contracts.strategy import StrategyAgentRequest
from quant_platform.agents.orchestration.agent_router import AgentRouter
from quant_platform.agents.orchestration.audit import AuditBundleBuilder
from quant_platform.agents.orchestration.authorization import ToolAuthorizationService
from quant_platform.agents.orchestration.handoff import AgentHandoff
from quant_platform.agents.services.execution_service import ExecutionAgentService
from quant_platform.agents.services.research_service import ResearchAgentService
from quant_platform.agents.services.risk_service import RiskAgentService
from quant_platform.agents.services.strategy_service import StrategyAgentService
from quant_platform.common.enums.core import AgentKind, AgentTaskStatus
from quant_platform.common.io.files import LocalArtifactStore
from quant_platform.common.types.core import ArtifactRef


class AgentOrchestrationService:
    def __init__(
        self,
        artifact_root: Path,
        *,
        research_service: ResearchAgentService,
        strategy_service: StrategyAgentService,
        risk_service: RiskAgentService,
        execution_service: ExecutionAgentService,
        authorization_service: ToolAuthorizationService,
    ) -> None:
        self.store = LocalArtifactStore(artifact_root)
        self.router = AgentRouter()
        self.handoff = AgentHandoff()
        self.audit_builder = AuditBundleBuilder()
        self.authorization_service = authorization_service
        self.services = {
            AgentKind.RESEARCH: research_service,
            AgentKind.STRATEGY: strategy_service,
            AgentKind.RISK: risk_service,
            AgentKind.EXECUTION: execution_service,
        }

    def execute(self, request: AgentRequest) -> AgentResponse:
        return self.orchestrate(OrchestrationRequest(request=request)).final_response

    def orchestrate(self, orchestration_request: OrchestrationRequest) -> OrchestrationResult:
        request = orchestration_request.request
        route_decision = self.router.route(
            request,
            orchestration_request.artifact_bundle,
            orchestration_request.route_policy,
        )
        tool_calls = []
        tool_results = []
        handoffs = []
        failure_events: list[FailureEvent] = []
        response_refs: list[ArtifactRef] = []
        authorization_snapshots: list[AuthorizationSnapshot] = []

        auth_snapshot = self.authorization_service.authorize(
            request,
            resolved_agent=route_decision.resolved_agent,
            artifact_bundle=orchestration_request.artifact_bundle,
            policy=orchestration_request.authorization_policy,
        )
        authorization_snapshots.append(auth_snapshot)

        if not route_decision.is_allowed or (
            request.allowed_tools and not auth_snapshot.allowed_tools_effective
        ):
            reason = route_decision.blocked_reason or "request has no effective authorized tools"
            failure_events.append(
                FailureEvent(
                    stage="route" if not route_decision.is_allowed else "authorization",
                    code="route_blocked"
                    if not route_decision.is_allowed
                    else "authorization_blocked",
                    message=reason,
                )
            )
            blocked_response = AgentResponse(
                request_id=request.request_id,
                agent_kind=request.agent_kind,
                status=AgentTaskStatus.BLOCKED,
                summary="orchestration blocked request before agent execution",
                observations=[reason],
                audit_log_uri="",
                error=AgentError(code=failure_events[-1].code, message=reason),
            )
            audit_bundle = self.audit_builder.build(
                request_id=request.request_id,
                workflow_id=orchestration_request.workflow_id,
                route_decision=route_decision,
                authorization_snapshots=authorization_snapshots,
                artifact_bundle=orchestration_request.artifact_bundle,
                tool_calls=tool_calls,
                tool_results=tool_results,
                handoffs=handoffs,
                response_refs=response_refs,
                failure_events=failure_events,
                final_response=blocked_response,
            )
            audit_ref = self.store.write_model(
                f"agents/{request.request_id}/audit_bundle.json", audit_bundle
            )
            final_response = blocked_response.model_copy(
                update={
                    "audit_log_uri": audit_ref.uri,
                    "audit_bundle_ref": audit_ref,
                    "output_artifacts": [audit_ref],
                }
            )
            return OrchestrationResult(
                route_decision=route_decision,
                authorization_snapshot=auth_snapshot,
                handoffs=handoffs,
                agent_responses=[final_response],
                audit_bundle=audit_bundle,
                final_response=final_response,
            )

        current_request: AgentRequest | None = request
        agent_responses: list[AgentResponse] = []
        chain = [route_decision.resolved_agent, *route_decision.planned_handoff_chain]
        for index, agent_kind in enumerate(chain):
            if current_request is None:
                failure_events.append(
                    FailureEvent(
                        stage="handoff",
                        code="handoff_missing_request",
                        message="handoff could not build downstream request",
                    )
                )
                break
            response = self._dispatch(
                agent_kind=agent_kind,
                request=current_request,
                authorization_snapshot=authorization_snapshots[-1],
                tool_calls=tool_calls,
                tool_results=tool_results,
            )
            response_ref = self.store.write_model(
                f"agents/{request.request_id}/responses/{agent_kind.value}_{index:02d}.json",
                response,
            )
            response_refs.append(
                ArtifactRef(kind=f"{agent_kind.value}_response", uri=response_ref.uri)
            )
            agent_responses.append(response)
            if index == len(chain) - 1:
                break

            next_agent = chain[index + 1]
            handoff_record = self.handoff.build_record(
                request_id=request.request_id,
                from_agent=agent_kind,
                to_agent=next_agent,
                reason="default orchestration handoff",
                triggered_by="route_decision",
                input_artifacts=current_request.input_artifacts,
                carryover_context_refs=current_request.context_refs,
                carryover_allowed_tools=self._default_tools_for_agent(next_agent),
                upstream_response_ref=response_refs[-1],
            )
            handoffs.append(handoff_record)
            if handoff_record.status != "success":
                failure_events.append(
                    FailureEvent(
                        stage="handoff",
                        code="handoff_blocked",
                        message=handoff_record.blocked_reason or "handoff blocked",
                    )
                )
                break

            current_request = self.handoff.build_next_request(
                original_request=current_request,
                handoff=handoff_record,
                upstream_response=response,
                artifact_bundle=orchestration_request.artifact_bundle,
            )
            if current_request is None:
                handoffs[-1] = handoff_record.model_copy(
                    update={
                        "status": "blocked",
                        "blocked_reason": "handoff could not build downstream request",
                    }
                )
                failure_events.append(
                    FailureEvent(
                        stage="handoff",
                        code="handoff_missing_request",
                        message="handoff could not build downstream request",
                    )
                )
                break
            authorization_snapshots.append(
                self.authorization_service.authorize(
                    current_request,
                    resolved_agent=next_agent,
                    artifact_bundle=orchestration_request.artifact_bundle,
                    policy=orchestration_request.authorization_policy,
                )
            )

        final_response = agent_responses[-1]
        audit_bundle = self.audit_builder.build(
            request_id=request.request_id,
            workflow_id=orchestration_request.workflow_id,
            route_decision=route_decision,
            authorization_snapshots=authorization_snapshots,
            artifact_bundle=orchestration_request.artifact_bundle,
            tool_calls=tool_calls,
            tool_results=tool_results,
            handoffs=handoffs,
            response_refs=response_refs,
            failure_events=failure_events,
            final_response=final_response,
        )
        audit_ref = self.store.write_model(
            f"agents/{request.request_id}/audit_bundle.json", audit_bundle
        )
        final_response = final_response.model_copy(
            update={
                "audit_log_uri": audit_ref.uri,
                "audit_bundle_ref": audit_ref,
                "output_artifacts": [*final_response.output_artifacts, audit_ref],
            }
        )
        agent_responses[-1] = final_response
        return OrchestrationResult(
            route_decision=route_decision,
            authorization_snapshot=authorization_snapshots[0],
            handoffs=handoffs,
            agent_responses=agent_responses,
            audit_bundle=audit_bundle,
            final_response=final_response,
        )

    def _dispatch(
        self,
        *,
        agent_kind: AgentKind,
        request: AgentRequest,
        authorization_snapshot: AuthorizationSnapshot,
        tool_calls: list,
        tool_results: list,
    ) -> AgentResponse:
        service = self.services[agent_kind]
        service.activate_execution_context(authorization_snapshot, tool_calls, tool_results)
        try:
            if agent_kind == AgentKind.RESEARCH:
                return service.execute(
                    ResearchAgentRequest.model_validate(request.model_dump(mode="json"))
                )
            if agent_kind == AgentKind.STRATEGY:
                return service.execute(
                    StrategyAgentRequest.model_validate(request.model_dump(mode="json"))
                )
            if agent_kind == AgentKind.RISK:
                return service.execute(
                    RiskAgentRequest.model_validate(request.model_dump(mode="json"))
                )
            if agent_kind == AgentKind.EXECUTION:
                return service.execute(
                    ExecutionAgentRequest.model_validate(request.model_dump(mode="json"))
                )
            raise ValueError(f"unsupported agent kind '{agent_kind}'")
        finally:
            service.clear_execution_context()

    @staticmethod
    def _default_tools_for_agent(agent_kind: AgentKind) -> list[str]:
        if agent_kind == AgentKind.STRATEGY:
            return ["strategy.propose_signal_recipe", "strategy.propose_backtest_request"]
        if agent_kind == AgentKind.RISK:
            return ["risk.evaluate_backtest_risk", "risk.check_strategy_constraints"]
        if agent_kind == AgentKind.EXECUTION:
            return ["execution.prepare_execution_plan", "execution.simulate_order_route"]
        return ["research.read_experiment_summary", "research.compare_backtest_reports"]
