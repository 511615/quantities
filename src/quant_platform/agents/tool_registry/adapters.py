from __future__ import annotations

from datetime import datetime, timezone

from quant_platform.agents.contracts.tooling import (
    CheckStrategyConstraintsInput,
    CheckStrategyConstraintsOutput,
    CompareBacktestReportsInput,
    CompareBacktestReportsOutput,
    EvaluateBacktestRiskInput,
    EvaluateBacktestRiskOutput,
    ExperimentSummaryInput,
    ExperimentSummaryOutput,
    PrepareExecutionPlanInput,
    PrepareExecutionPlanOutput,
    ProposeBacktestRequestInput,
    ProposeBacktestRequestOutput,
    ProposeSignalRecipeInput,
    ProposeSignalRecipeOutput,
    SimulateOrderRouteInput,
    SimulateOrderRouteOutput,
    ToolCall,
    ToolInputSchema,
    ToolOutputSchema,
    ToolResult,
    ToolSpec,
)
from quant_platform.agents.services.providers import (
    BacktestQueryService,
    ExecutionProposalService,
    RiskQueryService,
    StrategyProposalService,
    TrainingQueryService,
)
from quant_platform.agents.tool_registry.registry import ToolRegistry
from quant_platform.common.enums.core import AgentKind, ToolCallStatus, ToolSideEffectLevel


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_result(call: ToolCall, payload_model: object) -> ToolResult:
    return ToolResult(
        call_id=call.call_id,
        tool_name=call.tool_name,
        status=ToolCallStatus.SUCCESS,
        payload=payload_model.model_dump(mode="json"),  # type: ignore[attr-defined]
        generated_at=_utc_now(),
    )


def build_default_tool_specs() -> list[ToolSpec]:
    return [
        ToolSpec(
            tool_name="research.read_experiment_summary",
            description="Read a training manifest and summarize experiment outputs.",
            input_schema=ToolInputSchema(
                schema_name="ExperimentSummaryInput",
                json_schema=ExperimentSummaryInput.model_json_schema(),
            ),
            output_schema=ToolOutputSchema(
                schema_name="ExperimentSummaryOutput",
                json_schema=ExperimentSummaryOutput.model_json_schema(),
            ),
            allowed_agent_kinds=[AgentKind.RESEARCH],
            side_effect_level=ToolSideEffectLevel.READ_ONLY,
        ),
        ToolSpec(
            tool_name="research.compare_backtest_reports",
            description="Compare one or more backtest reports by summary metrics.",
            input_schema=ToolInputSchema(
                schema_name="CompareBacktestReportsInput",
                json_schema=CompareBacktestReportsInput.model_json_schema(),
            ),
            output_schema=ToolOutputSchema(
                schema_name="CompareBacktestReportsOutput",
                json_schema=CompareBacktestReportsOutput.model_json_schema(),
            ),
            allowed_agent_kinds=[AgentKind.RESEARCH],
            side_effect_level=ToolSideEffectLevel.READ_ONLY,
        ),
        ToolSpec(
            tool_name="strategy.propose_signal_recipe",
            description="Turn a feature reference into a draft signal recipe.",
            input_schema=ToolInputSchema(
                schema_name="ProposeSignalRecipeInput",
                json_schema=ProposeSignalRecipeInput.model_json_schema(),
            ),
            output_schema=ToolOutputSchema(
                schema_name="ProposeSignalRecipeOutput",
                json_schema=ProposeSignalRecipeOutput.model_json_schema(),
            ),
            allowed_agent_kinds=[AgentKind.STRATEGY],
            side_effect_level=ToolSideEffectLevel.PROPOSAL_ONLY,
        ),
        ToolSpec(
            tool_name="strategy.propose_backtest_request",
            description="Create a proposal-only backtest request draft.",
            input_schema=ToolInputSchema(
                schema_name="ProposeBacktestRequestInput",
                json_schema=ProposeBacktestRequestInput.model_json_schema(),
            ),
            output_schema=ToolOutputSchema(
                schema_name="ProposeBacktestRequestOutput",
                json_schema=ProposeBacktestRequestOutput.model_json_schema(),
            ),
            allowed_agent_kinds=[AgentKind.STRATEGY],
            side_effect_level=ToolSideEffectLevel.PROPOSAL_ONLY,
        ),
        ToolSpec(
            tool_name="risk.evaluate_backtest_risk",
            description="Read a backtest risk artifact and summarize alerts.",
            input_schema=ToolInputSchema(
                schema_name="EvaluateBacktestRiskInput",
                json_schema=EvaluateBacktestRiskInput.model_json_schema(),
            ),
            output_schema=ToolOutputSchema(
                schema_name="EvaluateBacktestRiskOutput",
                json_schema=EvaluateBacktestRiskOutput.model_json_schema(),
            ),
            allowed_agent_kinds=[AgentKind.RISK],
            side_effect_level=ToolSideEffectLevel.READ_ONLY,
        ),
        ToolSpec(
            tool_name="risk.check_strategy_constraints",
            description="Check whether a strategy draft violates current limits.",
            input_schema=ToolInputSchema(
                schema_name="CheckStrategyConstraintsInput",
                json_schema=CheckStrategyConstraintsInput.model_json_schema(),
            ),
            output_schema=ToolOutputSchema(
                schema_name="CheckStrategyConstraintsOutput",
                json_schema=CheckStrategyConstraintsOutput.model_json_schema(),
            ),
            allowed_agent_kinds=[AgentKind.RISK],
            side_effect_level=ToolSideEffectLevel.READ_ONLY,
        ),
        ToolSpec(
            tool_name="execution.prepare_execution_plan",
            description="Produce a proposal-only execution plan.",
            input_schema=ToolInputSchema(
                schema_name="PrepareExecutionPlanInput",
                json_schema=PrepareExecutionPlanInput.model_json_schema(),
            ),
            output_schema=ToolOutputSchema(
                schema_name="PrepareExecutionPlanOutput",
                json_schema=PrepareExecutionPlanOutput.model_json_schema(),
            ),
            allowed_agent_kinds=[AgentKind.EXECUTION],
            side_effect_level=ToolSideEffectLevel.PROPOSAL_ONLY,
        ),
        ToolSpec(
            tool_name="execution.simulate_order_route",
            description="Simulate the route quality of a proposal-only execution plan.",
            input_schema=ToolInputSchema(
                schema_name="SimulateOrderRouteInput",
                json_schema=SimulateOrderRouteInput.model_json_schema(),
            ),
            output_schema=ToolOutputSchema(
                schema_name="SimulateOrderRouteOutput",
                json_schema=SimulateOrderRouteOutput.model_json_schema(),
            ),
            allowed_agent_kinds=[AgentKind.EXECUTION],
            side_effect_level=ToolSideEffectLevel.PROPOSAL_ONLY,
        ),
    ]


def register_default_tools(
    registry: ToolRegistry,
    training_queries: TrainingQueryService,
    backtest_queries: BacktestQueryService,
    risk_queries: RiskQueryService,
    strategy_proposals: StrategyProposalService,
    execution_proposals: ExecutionProposalService,
) -> None:
    for spec in build_default_tool_specs():
        if spec.tool_name == "research.read_experiment_summary":
            registry.register(
                spec,
                lambda call: _build_result(
                    call,
                    training_queries.read_experiment_summary(
                        ExperimentSummaryInput.model_validate(call.arguments)
                    ),
                ),
            )
        elif spec.tool_name == "research.compare_backtest_reports":
            registry.register(
                spec,
                lambda call: _build_result(
                    call,
                    backtest_queries.compare_backtest_reports(
                        CompareBacktestReportsInput.model_validate(call.arguments)
                    ),
                ),
            )
        elif spec.tool_name == "strategy.propose_signal_recipe":
            registry.register(
                spec,
                lambda call: _build_result(
                    call,
                    strategy_proposals.propose_signal_recipe(
                        ProposeSignalRecipeInput.model_validate(call.arguments)
                    ),
                ),
            )
        elif spec.tool_name == "strategy.propose_backtest_request":
            registry.register(
                spec,
                lambda call: _build_result(
                    call,
                    strategy_proposals.propose_backtest_request(
                        ProposeBacktestRequestInput.model_validate(call.arguments)
                    ),
                ),
            )
        elif spec.tool_name == "risk.evaluate_backtest_risk":
            registry.register(
                spec,
                lambda call: _build_result(
                    call,
                    risk_queries.evaluate_backtest_risk(
                        EvaluateBacktestRiskInput.model_validate(call.arguments)
                    ),
                ),
            )
        elif spec.tool_name == "risk.check_strategy_constraints":
            registry.register(
                spec,
                lambda call: _build_result(
                    call,
                    risk_queries.check_strategy_constraints(
                        CheckStrategyConstraintsInput.model_validate(call.arguments)
                    ),
                ),
            )
        elif spec.tool_name == "execution.prepare_execution_plan":
            registry.register(
                spec,
                lambda call: _build_result(
                    call,
                    execution_proposals.prepare_execution_plan(
                        PrepareExecutionPlanInput.model_validate(call.arguments)
                    ),
                ),
            )
        elif spec.tool_name == "execution.simulate_order_route":
            registry.register(
                spec,
                lambda call: _build_result(
                    call,
                    execution_proposals.simulate_order_route(
                        SimulateOrderRouteInput.model_validate(call.arguments)
                    ),
                ),
            )
