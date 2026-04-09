from __future__ import annotations

from quant_platform.backtest.contracts.report import (
    BacktestSummaryArtifact,
    BenchmarkSummaryArtifact,
    PromotionDecisionArtifact,
)
from quant_platform.common.hashing.digest import stable_digest

DEFAULT_PROMOTION_TOP_K = 2
DEFAULT_MAX_DRAWDOWN = 0.35
DEFAULT_MAX_TURNOVER = 24.0
DEFAULT_MAX_STRESS_FAILS = 0
DEFAULT_MIN_WORST_SCENARIO_RETURN_DELTA = -0.25
DEFAULT_MAX_SIMULATION_OUTPERFORMANCE = 0.05


def build_promotion_decisions(
    *,
    benchmark_summary: BenchmarkSummaryArtifact,
    backtest_summary: BacktestSummaryArtifact,
    top_k: int = DEFAULT_PROMOTION_TOP_K,
) -> list[PromotionDecisionArtifact]:
    ranked = {row.model_name: row.rank for row in benchmark_summary.leaderboard}
    decisions: list[PromotionDecisionArtifact] = []
    for row in backtest_summary.rows:
        hard_failures: list[str] = []
        soft_warnings = list(backtest_summary.comparison_warnings)
        admission_gate = benchmark_summary.admission_gates.get(row.model_name, {})
        benchmark_gate_passed = (
            row.model_name in ranked
            and ranked[row.model_name] <= top_k
            and (
                benchmark_summary.official_benchmark
                or bool(benchmark_summary.reference_consistency)
            )
        )
        if not benchmark_gate_passed:
            soft_warnings.append("benchmark gate not passed")
        default_gate_passed = bool(admission_gate.get("default_gate_passed", True))
        if not default_gate_passed:
            soft_warnings.append("advanced default admission gate not passed")
        for engine_name, metrics in (
            ("research", row.research_metrics),
            ("simulation", row.simulation_metrics),
        ):
            if metrics.get("max_drawdown", 0.0) > DEFAULT_MAX_DRAWDOWN:
                hard_failures.append(f"{engine_name} max_drawdown exceeds limit")
            if metrics.get("turnover_total", 0.0) > DEFAULT_MAX_TURNOVER:
                hard_failures.append(f"{engine_name} turnover_total exceeds limit")
            if metrics.get("risk_trigger_count", 0.0) > 0.0:
                hard_failures.append(f"{engine_name} risk_trigger_count must be zero")
        if (
            row.scenario_metrics.get("worst_scenario_return_delta", 0.0)
            < DEFAULT_MIN_WORST_SCENARIO_RETURN_DELTA
        ):
            hard_failures.append("worst_scenario_return_delta breaches stress threshold")
        if row.scenario_metrics.get("stress_fail_count", 0.0) > DEFAULT_MAX_STRESS_FAILS:
            hard_failures.append("stress_fail_count exceeds allowed maximum")
        if row.divergence_metrics.get("simulation_minus_research_shortfall", 0.0) < 0.0:
            hard_failures.append("simulation shortfall is unexpectedly better than research")
        if (
            row.divergence_metrics.get("simulation_minus_research_cumulative_return", 0.0)
            > DEFAULT_MAX_SIMULATION_OUTPERFORMANCE
        ):
            hard_failures.append("simulation materially outperforms research")
        backtest_gate_passed = not hard_failures and row.passed_consistency_checks
        if hard_failures:
            decision = "REJECT"
        elif benchmark_gate_passed and backtest_gate_passed:
            decision = "PROMOTE"
        else:
            decision = "HOLD"
        decisions.append(
            PromotionDecisionArtifact(
                decision_id=stable_digest(
                    {
                        "dataset_id": backtest_summary.dataset_id,
                        "model_name": row.model_name,
                        "run_id": row.run_id,
                        "decision": decision,
                    }
                ),
                dataset_id=backtest_summary.dataset_id,
                model_name=row.model_name,
                run_id=row.run_id,
                decision=decision,
                benchmark_gate_passed=benchmark_gate_passed,
                backtest_gate_passed=backtest_gate_passed,
                default_gate_passed=default_gate_passed,
                hard_failures=hard_failures,
                soft_warnings=soft_warnings,
                supporting_artifacts={
                    "benchmark_summary": benchmark_summary.benchmark_name,
                    "research_result_uri": row.research_result_uri,
                    "simulation_result_uri": row.simulation_result_uri,
                },
            )
        )
    return decisions
