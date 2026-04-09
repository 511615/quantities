from __future__ import annotations

from pathlib import Path

import pytest

from quant_platform.agents.contracts.execution import ExecutionAgentRequest
from quant_platform.agents.contracts.research import ResearchAgentRequest
from quant_platform.agents.contracts.risk import RiskAgentRequest
from quant_platform.agents.contracts.strategy import StrategyAgentRequest
from quant_platform.agents.contracts.tooling import (
    ExecutionPlanProposal,
    ExperimentSummaryInput,
    ToolCall,
)
from quant_platform.agents.services import (
    BacktestQueryService,
    ExecutionProposalService,
    RiskQueryService,
    StrategyProposalService,
    TrainingQueryService,
)
from quant_platform.agents.tool_registry.adapters import register_default_tools
from quant_platform.agents.tool_registry.registry import ToolRegistry
from quant_platform.api.facade import QuantPlatformFacade
from quant_platform.common.enums.core import AgentKind, ToolSideEffectLevel


def test_agent_contracts_export_json_schema() -> None:
    for model in (
        ResearchAgentRequest,
        StrategyAgentRequest,
        RiskAgentRequest,
        ExecutionAgentRequest,
        ExperimentSummaryInput,
        ExecutionPlanProposal,
    ):
        schema = model.model_json_schema()
        assert "properties" in schema


def test_agent_contracts_forbid_extra_fields() -> None:
    with pytest.raises(ValueError):
        ResearchAgentRequest(
            request_id="req-1",
            task_type="summarize",
            goal="summarize",
            allowed_tools=[],
            unexpected_field="nope",
        )


def test_tool_call_requires_explicit_contract_fields() -> None:
    with pytest.raises(ValueError):
        ToolCall(
            call_id="call-1",
            tool_name="research.read_experiment_summary",
            arguments={"run_id": "run-1"},
            caller_agent=AgentKind.RESEARCH,
        )


def test_tool_registry_filters_tools_by_agent(tmp_path: Path) -> None:
    registry = ToolRegistry()
    register_default_tools(
        registry,
        training_queries=TrainingQueryService(tmp_path),
        backtest_queries=BacktestQueryService(tmp_path),
        risk_queries=RiskQueryService(tmp_path),
        strategy_proposals=StrategyProposalService(tmp_path),
        execution_proposals=ExecutionProposalService(tmp_path),
    )
    research_tools = {spec.tool_name for spec in registry.list_tools(AgentKind.RESEARCH)}
    execution_tools = {spec.tool_name for spec in registry.list_tools(AgentKind.EXECUTION)}
    assert "research.read_experiment_summary" in research_tools
    assert "execution.prepare_execution_plan" not in research_tools
    assert "execution.prepare_execution_plan" in execution_tools


def test_execution_tools_are_proposal_only(tmp_path: Path) -> None:
    registry = ToolRegistry()
    register_default_tools(
        registry,
        training_queries=TrainingQueryService(tmp_path),
        backtest_queries=BacktestQueryService(tmp_path),
        risk_queries=RiskQueryService(tmp_path),
        strategy_proposals=StrategyProposalService(tmp_path),
        execution_proposals=ExecutionProposalService(tmp_path),
    )
    spec = registry.get_spec("execution.prepare_execution_plan")
    assert spec.side_effect_level == ToolSideEffectLevel.PROPOSAL_ONLY


def test_research_agent_smoke_uses_new_contracts(tmp_path: Path) -> None:
    result = QuantPlatformFacade(tmp_path).agent_smoke()
    assert result.agent_kind == AgentKind.RESEARCH
    assert result.tool_results
    assert Path(result.audit_log_uri).exists()
