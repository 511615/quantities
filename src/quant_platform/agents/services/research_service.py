from __future__ import annotations

from pathlib import Path

from quant_platform.agents.contracts.research import ResearchAgentRequest, ResearchAgentResponse
from quant_platform.agents.services.base import BaseAgentService
from quant_platform.agents.tool_registry.registry import ToolRegistry
from quant_platform.common.types.core import ArtifactRef


class ResearchAgentService(BaseAgentService):
    def __init__(self, artifact_root: Path, registry: ToolRegistry) -> None:
        super().__init__(artifact_root, registry)

    def execute(self, request: ResearchAgentRequest) -> ResearchAgentResponse:
        tool_results = []
        for experiment_ref in request.experiment_refs:
            tool_results.append(
                self._run_tool(
                    request,
                    "research.read_experiment_summary",
                    {"train_manifest_ref": experiment_ref.model_dump(mode="json")},
                )
            )
        if request.comparison_mode and request.input_artifacts:
            tool_results.append(
                self._run_tool(
                    request,
                    "research.compare_backtest_reports",
                    {
                        "report_refs": [
                            artifact.model_dump(mode="json") for artifact in request.input_artifacts
                        ]
                    },
                )
            )
        observations = [f"{result.tool_name}:{result.status}" for result in tool_results]
        summary = "research agent completed request using explicit tool contracts"
        audit_artifact = self._write_audit_log(request, summary, observations, tool_results)
        return ResearchAgentResponse(
            request_id=request.request_id,
            status=self._status_from_results(tool_results),
            summary=summary,
            observations=observations,
            tool_results=[self._to_inline_result(result) for result in tool_results],
            output_artifacts=[ArtifactRef(kind="audit_log", uri=audit_artifact.uri)],
            proposed_actions=[
                "review experiment summary",
                "decide whether to promote candidate strategy",
            ],
            audit_log_uri=audit_artifact.uri,
        )
