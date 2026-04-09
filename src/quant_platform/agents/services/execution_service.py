from __future__ import annotations

from pathlib import Path

from quant_platform.agents.contracts.execution import ExecutionAgentRequest, ExecutionAgentResponse
from quant_platform.agents.services.base import BaseAgentService
from quant_platform.agents.tool_registry.registry import ToolRegistry


class ExecutionAgentService(BaseAgentService):
    def __init__(self, artifact_root: Path, registry: ToolRegistry) -> None:
        super().__init__(artifact_root, registry)

    def execute(self, request: ExecutionAgentRequest) -> ExecutionAgentResponse:
        tool_results = []
        if request.order_intents_ref is not None:
            arguments: dict[str, object] = {
                "order_intents_ref": request.order_intents_ref.model_dump(mode="json"),
            }
            if request.venue_constraints_ref is not None:
                arguments["venue_constraints_ref"] = request.venue_constraints_ref.model_dump(
                    mode="json"
                )
            tool_results.append(
                self._run_tool(
                    request,
                    "execution.prepare_execution_plan",
                    arguments,
                )
            )
        observations = [f"{result.tool_name}:{result.status}" for result in tool_results]
        summary = "execution agent produced proposal-only execution artifacts"
        audit_artifact = self._write_audit_log(request, summary, observations, tool_results)
        return ExecutionAgentResponse(
            request_id=request.request_id,
            status=self._status_from_results(tool_results),
            summary=summary,
            observations=observations,
            tool_results=[self._to_inline_result(result) for result in tool_results],
            proposed_actions=["keep execution proposals behind manual approval"],
            audit_log_uri=audit_artifact.uri,
        )
