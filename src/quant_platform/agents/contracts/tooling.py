from __future__ import annotations

from typing import Any

from pydantic import Field, model_validator

from quant_platform.backtest.contracts.backtest import (
    BenchmarkSpec,
    CalendarSpec,
    CostModel,
    PortfolioConfig,
    StrategyConfig,
)
from quant_platform.common.enums.core import AgentKind, ToolCallStatus, ToolSideEffectLevel
from quant_platform.common.types.core import ArtifactRef, FrozenModel


class ToolInputSchema(FrozenModel):
    schema_name: str
    json_schema: dict[str, Any]


class ToolOutputSchema(FrozenModel):
    schema_name: str
    json_schema: dict[str, Any]


class ToolError(FrozenModel):
    code: str
    message: str
    retryable: bool = False


class ToolSpec(FrozenModel):
    tool_name: str
    description: str
    input_schema: ToolInputSchema
    output_schema: ToolOutputSchema
    allowed_agent_kinds: list[AgentKind]
    side_effect_level: ToolSideEffectLevel = ToolSideEffectLevel.READ_ONLY


class ToolCall(FrozenModel):
    call_id: str
    tool_name: str
    arguments: dict[str, Any] = Field(default_factory=dict)
    caller_agent: AgentKind
    request_id: str


class ToolResult(FrozenModel):
    call_id: str
    tool_name: str
    status: ToolCallStatus
    payload: dict[str, Any] = Field(default_factory=dict)
    artifacts: list[ArtifactRef] = Field(default_factory=list)
    errors: list[ToolError] = Field(default_factory=list)
    metrics: dict[str, float] = Field(default_factory=dict)
    generated_at: str


class ExperimentSummaryInput(FrozenModel):
    run_id: str | None = None
    train_manifest_ref: ArtifactRef | None = None

    @model_validator(mode="after")
    def validate_locator(self) -> "ExperimentSummaryInput":
        if self.run_id is None and self.train_manifest_ref is None:
            raise ValueError("either run_id or train_manifest_ref must be provided")
        return self


class ExperimentSummaryOutput(FrozenModel):
    run_id: str
    metrics: dict[str, float]
    repro_digest: str | None = None
    dataset_ref_uri: str | None = None
    model_artifact_uri: str | None = None
    manifest_uri: str | None = None
    artifact_uris: list[str] = Field(default_factory=list)


class CompareBacktestReportsInput(FrozenModel):
    report_refs: list[ArtifactRef] = Field(min_length=1)


class CompareBacktestReportsOutput(FrozenModel):
    compared_reports: list[str]
    comparison_metrics: dict[str, dict[str, float | int | str]]


class ProposeSignalRecipeInput(FrozenModel):
    feature_view_ref: ArtifactRef
    target_kind: str
    constraints: dict[str, str] = Field(default_factory=dict)


class ProposeSignalRecipeOutput(FrozenModel):
    recipe_name: str
    rationale: str
    dependent_features: list[str]
    validation_checks: list[str]


class BacktestRequestDraft(FrozenModel):
    strategy_config: StrategyConfig
    portfolio_config: PortfolioConfig
    cost_model: CostModel
    benchmark_spec: BenchmarkSpec
    calendar_spec: CalendarSpec
    notes: list[str] = Field(default_factory=list)


class ProposeBacktestRequestInput(FrozenModel):
    signal_recipe_ref: ArtifactRef | None = None
    portfolio_constraints: PortfolioConfig
    cost_model: CostModel
    benchmark_spec: BenchmarkSpec
    calendar_spec: CalendarSpec


class ProposeBacktestRequestOutput(FrozenModel):
    draft: BacktestRequestDraft
    rationale: list[str]


class EvaluateBacktestRiskInput(FrozenModel):
    backtest_result_ref: ArtifactRef


class EvaluateBacktestRiskOutput(FrozenModel):
    risk_summary: dict[str, float]
    alerts: list[str]
    suggested_limits: list[str]


class CheckStrategyConstraintsInput(FrozenModel):
    strategy_draft_ref: ArtifactRef
    risk_policy_ref: ArtifactRef | None = None


class CheckStrategyConstraintsOutput(FrozenModel):
    passed: bool
    checks: dict[str, str]
    violations: list[str]


class PrepareExecutionPlanInput(FrozenModel):
    order_intents_ref: ArtifactRef
    venue_constraints_ref: ArtifactRef | None = None
    risk_limits_ref: ArtifactRef | None = None


class ExecutionPlanProposal(FrozenModel):
    plan_id: str
    execution_style: str
    venue_sequence: list[str]
    slicing_policy: str
    guardrails: list[str]


class PrepareExecutionPlanOutput(FrozenModel):
    proposal: ExecutionPlanProposal
    assumptions: list[str]


class SimulateOrderRouteInput(FrozenModel):
    execution_plan_ref: ArtifactRef


class SimulateOrderRouteOutput(FrozenModel):
    estimated_slippage_bps: float
    fill_probability: float = Field(ge=0.0, le=1.0)
    failure_reasons: list[str]
