from __future__ import annotations

from pathlib import Path

from quant_platform.agents.contracts.agent import AgentResult, AgentTask
from quant_platform.agents.contracts.research import ResearchAgentRequest
from quant_platform.agents.services.providers import (
    BacktestQueryService,
    ExecutionProposalService,
    RiskQueryService,
    StrategyProposalService,
    TrainingQueryService,
)
from quant_platform.agents.services.research_service import (
    ResearchAgentService as RoutedResearchService,
)
from quant_platform.agents.tool_registry.adapters import register_default_tools
from quant_platform.agents.tool_registry.registry import ToolRegistry
from quant_platform.common.enums.core import AgentTaskType
from quant_platform.common.types.core import ArtifactRef


class ResearchAgentService:
    def __init__(self, artifact_root: Path) -> None:
        registry = ToolRegistry()
        register_default_tools(
            registry,
            training_queries=TrainingQueryService(artifact_root),
            backtest_queries=BacktestQueryService(artifact_root),
            risk_queries=RiskQueryService(artifact_root),
            strategy_proposals=StrategyProposalService(artifact_root),
            execution_proposals=ExecutionProposalService(artifact_root),
        )
        self.service = RoutedResearchService(artifact_root, registry)

    def execute(self, task: AgentTask) -> AgentResult:
        request = ResearchAgentRequest(
            request_id=task.task_id,
            task_type=str(task.task_type),
            goal="legacy research task adapter",
            input_artifacts=[ArtifactRef(kind="legacy_input", uri=uri) for uri in task.input_refs],
            experiment_refs=[
                ArtifactRef(kind="train_manifest", uri=uri)
                for uri in task.input_refs
                if uri.endswith("manifest.json")
            ],
            comparison_mode=(
                "report_summary"
                if task.task_type == AgentTaskType.INSPECT_BACKTEST
                or any(uri.endswith("report.json") for uri in task.input_refs)
                else None
            ),
            allowed_tools=task.allowed_tools,
            guardrail_policy=task.guardrail_policy,
            response_mode=task.response_mode,
        )
        response = self.service.execute(request)
        return AgentResult(
            task_id=response.request_id,
            status=response.status,
            observations=response.observations,
            artifact_refs=response.output_artifacts,
            proposed_actions=response.proposed_actions,
            audit_log_uri=response.audit_log_uri,
        )
