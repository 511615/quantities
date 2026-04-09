from __future__ import annotations

from pathlib import Path

from quant_platform.agents.contracts.risk import RiskAgentRequest, RiskAgentResponse
from quant_platform.agents.services.base import BaseAgentService
from quant_platform.agents.tool_registry.registry import ToolRegistry


class RiskAgentService(BaseAgentService):
    def __init__(self, artifact_root: Path, registry: ToolRegistry) -> None:
        super().__init__(artifact_root, registry)

    def execute(self, request: RiskAgentRequest) -> RiskAgentResponse:
        tool_results = [
            self._run_tool(
                request,
                "risk.evaluate_backtest_risk",
                {"backtest_result_ref": artifact.model_dump(mode="json")},
            )
            for artifact in request.input_artifacts
        ]
        observations = [f"{result.tool_name}:{result.status}" for result in tool_results]
        summary = "risk agent summarized explicit risk artifacts"
        audit_artifact = self._write_audit_log(request, summary, observations, tool_results)
        return RiskAgentResponse(
            request_id=request.request_id,
            status=self._status_from_results(tool_results),
            summary=summary,
            observations=observations,
            tool_results=[self._to_inline_result(result) for result in tool_results],
            proposed_actions=["review suggested limits before enabling execution planning"],
            audit_log_uri=audit_artifact.uri,
        )
