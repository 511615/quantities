from __future__ import annotations

from pathlib import Path

from quant_platform.agents.contracts.tooling import (
    BacktestRequestDraft,
    CheckStrategyConstraintsInput,
    CheckStrategyConstraintsOutput,
    CompareBacktestReportsInput,
    CompareBacktestReportsOutput,
    EvaluateBacktestRiskInput,
    EvaluateBacktestRiskOutput,
    ExecutionPlanProposal,
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
)
from quant_platform.backtest.contracts.backtest import StrategyConfig
from quant_platform.common.hashing.digest import stable_digest
from quant_platform.common.io.files import LocalArtifactStore
from quant_platform.experiment.manifests.run_manifest import RunManifest


class TrainingQueryService:
    def __init__(self, artifact_root: Path) -> None:
        self.store = LocalArtifactStore(artifact_root)

    def read_experiment_summary(self, request: ExperimentSummaryInput) -> ExperimentSummaryOutput:
        manifest_uri = (
            request.train_manifest_ref.uri if request.train_manifest_ref is not None else None
        )
        if manifest_uri is None and request.run_id is not None:
            manifest_uri = str(self.store.root / "models" / request.run_id / "manifest.json")
        if manifest_uri is None:
            raise ValueError("missing train manifest reference")
        manifest = self.store.read_model(manifest_uri, RunManifest)
        return ExperimentSummaryOutput(
            run_id=manifest.run_id,
            metrics=manifest.metrics,
            dataset_ref_uri=manifest.dataset_ref_uri,
            model_artifact_uri=manifest.model_artifact.uri,
            manifest_uri=manifest_uri,
            artifact_uris=[manifest.model_artifact.uri, manifest_uri],
        )


class BacktestQueryService:
    def __init__(self, artifact_root: Path) -> None:
        self.store = LocalArtifactStore(artifact_root)

    def compare_backtest_reports(
        self, request: CompareBacktestReportsInput
    ) -> CompareBacktestReportsOutput:
        comparison: dict[str, dict[str, float | int | str]] = {}
        for report_ref in request.report_refs:
            payload = self.store.read_json(report_ref.uri)
            comparison[report_ref.uri] = {
                "summary": str(payload.get("summary", "")),
                "position_count": int(payload.get("position_count", 0)),
            }
        return CompareBacktestReportsOutput(
            compared_reports=[ref.uri for ref in request.report_refs],
            comparison_metrics=comparison,
        )


class RiskQueryService:
    def __init__(self, artifact_root: Path) -> None:
        self.store = LocalArtifactStore(artifact_root)

    def evaluate_backtest_risk(
        self, request: EvaluateBacktestRiskInput
    ) -> EvaluateBacktestRiskOutput:
        payload = self.store.read_json(request.backtest_result_ref.uri)
        gross_exposure = float(payload.get("gross_exposure", 0.0))
        alerts = [] if gross_exposure <= 3.0 else ["gross exposure exceeds smoke threshold"]
        return EvaluateBacktestRiskOutput(
            risk_summary={"gross_exposure": gross_exposure},
            alerts=alerts,
            suggested_limits=["cap gross exposure at 2.0x", "review turnover before promotion"],
        )

    def check_strategy_constraints(
        self, request: CheckStrategyConstraintsInput
    ) -> CheckStrategyConstraintsOutput:
        payload = self.store.read_json(request.strategy_draft_ref.uri)
        max_weight = float(payload.get("portfolio_config", {}).get("max_position_weight", 1.0))
        violations = [] if max_weight <= 1.0 else ["max_position_weight exceeds 1.0"]
        return CheckStrategyConstraintsOutput(
            passed=not violations,
            checks={"max_position_weight": f"{max_weight:.2f}"},
            violations=violations,
        )


class StrategyProposalService:
    def __init__(self, artifact_root: Path) -> None:
        self.store = LocalArtifactStore(artifact_root)

    def propose_signal_recipe(self, request: ProposeSignalRecipeInput) -> ProposeSignalRecipeOutput:
        feature_payload = self.store.read_json(request.feature_view_ref.uri)
        dependent_features = sorted(str(key) for key in feature_payload.keys())
        return ProposeSignalRecipeOutput(
            recipe_name=f"{request.target_kind}_baseline_recipe",
            rationale="combine stable market features into a low-complexity candidate signal",
            dependent_features=dependent_features,
            validation_checks=["verify feature freshness", "compare against naive baseline"],
        )

    def propose_backtest_request(
        self, request: ProposeBacktestRequestInput
    ) -> ProposeBacktestRequestOutput:
        draft = BacktestRequestDraft(
            strategy_config=StrategyConfig(name="agent_strategy_proposal"),
            portfolio_config=request.portfolio_constraints,
            cost_model=request.cost_model,
            benchmark_spec=request.benchmark_spec,
            calendar_spec=request.calendar_spec,
            notes=[
                "generated by StrategyProposalService",
                "confirm signal definition before execution",
            ],
        )
        return ProposeBacktestRequestOutput(
            draft=draft,
            rationale=[
                "preserve explicit portfolio and cost constraints",
                "stay in proposal-only mode",
            ],
        )


class ExecutionProposalService:
    def __init__(self, artifact_root: Path) -> None:
        self.store = LocalArtifactStore(artifact_root)

    def prepare_execution_plan(
        self, request: PrepareExecutionPlanInput
    ) -> PrepareExecutionPlanOutput:
        order_payload = self.store.read_json(request.order_intents_ref.uri)
        venue_sequence = ["primary", "secondary"] if request.venue_constraints_ref else ["primary"]
        proposal = ExecutionPlanProposal(
            plan_id=stable_digest({"order_payload": order_payload, "venues": venue_sequence}),
            execution_style="twap_proposal",
            venue_sequence=venue_sequence,
            slicing_policy="equal_time_slices",
            guardrails=["proposal_only", "manual approval required before routing"],
        )
        return PrepareExecutionPlanOutput(
            proposal=proposal,
            assumptions=[
                "no live venue connectivity",
                "simulation uses static liquidity assumptions",
            ],
        )

    def simulate_order_route(self, request: SimulateOrderRouteInput) -> SimulateOrderRouteOutput:
        _ = self.store.read_json(request.execution_plan_ref.uri)
        return SimulateOrderRouteOutput(
            estimated_slippage_bps=4.5,
            fill_probability=0.82,
            failure_reasons=[
                "venue liquidity snapshot unavailable",
                "risk hold may reject oversized child order",
            ],
        )
