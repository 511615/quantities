from __future__ import annotations

from pathlib import Path

from quant_platform.agents.contracts.strategy import StrategyAgentRequest, StrategyAgentResponse
from quant_platform.agents.services.base import BaseAgentService
from quant_platform.agents.tool_registry.registry import ToolRegistry


class StrategyAgentService(BaseAgentService):
    def __init__(self, artifact_root: Path, registry: ToolRegistry) -> None:
        super().__init__(artifact_root, registry)

    def execute(self, request: StrategyAgentRequest) -> StrategyAgentResponse:
        tool_results = []
        for signal_ref in request.signal_refs:
            tool_results.append(
                self._run_tool(
                    request,
                    "strategy.propose_signal_recipe",
                    {
                        "feature_view_ref": signal_ref.model_dump(mode="json"),
                        "target_kind": request.task_type,
                        "constraints": {"goal": request.goal},
                    },
                )
            )
        observations = [f"{result.tool_name}:{result.status}" for result in tool_results]
        summary = "strategy agent returned proposal-only outputs"
        audit_artifact = self._write_audit_log(request, summary, observations, tool_results)
        return StrategyAgentResponse(
            request_id=request.request_id,
            status=self._status_from_results(tool_results),
            summary=summary,
            observations=observations,
            tool_results=[self._to_inline_result(result) for result in tool_results],
            proposed_actions=[
                "review signal assumptions",
                "approve backtest draft generation if needed",
            ],
            audit_log_uri=audit_artifact.uri,
        )
