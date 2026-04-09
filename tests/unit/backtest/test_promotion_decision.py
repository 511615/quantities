from __future__ import annotations

from quant_platform.backtest.contracts.report import (
    BacktestSummaryArtifact,
    BacktestSummaryRow,
    BenchmarkSummaryArtifact,
    BenchmarkSummaryRow,
)
from quant_platform.backtest.metrics.promotion import build_promotion_decisions


def test_promotion_decision_promotes_when_benchmark_and_backtest_gates_pass() -> None:
    decisions = build_promotion_decisions(
        benchmark_summary=_benchmark_summary(rank=1),
        backtest_summary=_backtest_summary(
            decision_case="promote",
            passed_consistency_checks=True,
            stress_fail_count=0.0,
        ),
    )

    assert decisions[0].decision == "PROMOTE"
    assert decisions[0].benchmark_gate_passed is True
    assert decisions[0].backtest_gate_passed is True


def test_promotion_decision_holds_when_benchmark_gate_is_not_met() -> None:
    decisions = build_promotion_decisions(
        benchmark_summary=_benchmark_summary(rank=3),
        backtest_summary=_backtest_summary(
            decision_case="hold",
            passed_consistency_checks=True,
            stress_fail_count=0.0,
        ),
    )

    assert decisions[0].decision == "HOLD"
    assert decisions[0].benchmark_gate_passed is False
    assert "benchmark gate not passed" in decisions[0].soft_warnings


def test_promotion_decision_rejects_when_stress_or_divergence_fails() -> None:
    decisions = build_promotion_decisions(
        benchmark_summary=_benchmark_summary(rank=1),
        backtest_summary=_backtest_summary(
            decision_case="reject",
            passed_consistency_checks=False,
            stress_fail_count=2.0,
        ),
    )

    assert decisions[0].decision == "REJECT"
    assert decisions[0].backtest_gate_passed is False
    assert any("stress_fail_count" in failure for failure in decisions[0].hard_failures)
    assert any(
        "simulation materially outperforms research" in failure
        for failure in decisions[0].hard_failures
    )


def test_promotion_decision_marks_default_gate_for_advanced_model() -> None:
    decisions = build_promotion_decisions(
        benchmark_summary=BenchmarkSummaryArtifact(
            benchmark_name="official-benchmark",
            dataset_id="dataset-1",
            data_source="real",
            window_count=4,
            ranking_metric="mean_test_mae",
            leaderboard=[
                BenchmarkSummaryRow(
                    rank=1,
                    model_name="transformer_reference",
                    family="deep",
                    advanced_kind="transformer",
                    backend="native",
                    mean_valid_mae=0.1,
                    mean_test_mae=0.2,
                    artifact_uri="artifact://benchmark",
                )
            ],
            reference_consistency={"top_model_consistent": True},
            selected_top_k=2,
            official_benchmark=True,
            baseline_model_names=["elastic_net"],
            admission_gates={"transformer_reference": {"default_gate_passed": False}},
        ),
        backtest_summary=BacktestSummaryArtifact(
            summary_id="summary-1",
            dataset_id="dataset-1",
            prediction_scope="full",
            data_source="real",
            benchmark_name="official-benchmark",
            request_digest="digest-1",
            rows=[
                BacktestSummaryRow(
                    model_name="transformer_reference",
                    run_id="run-1",
                    prediction_frame_uri="artifact://prediction",
                    research_result_uri="artifact://research",
                    simulation_result_uri="artifact://simulation",
                    research_metrics={
                        "max_drawdown": 0.10,
                        "turnover_total": 1.0,
                        "risk_trigger_count": 0.0,
                    },
                    simulation_metrics={
                        "max_drawdown": 0.12,
                        "turnover_total": 1.2,
                        "risk_trigger_count": 0.0,
                    },
                    divergence_metrics={
                        "simulation_minus_research_alpha_pnl": -5.0,
                        "simulation_minus_research_cumulative_return": -0.01,
                        "simulation_minus_research_drawdown": 0.02,
                        "simulation_minus_research_shortfall": 0.01,
                    },
                    scenario_metrics={
                        "cost_x2_return_delta": -0.02,
                        "cost_x5_return_delta": -0.05,
                        "latency_shock_return_delta": -0.03,
                        "liquidity_drought_return_delta": -0.04,
                        "worst_scenario_return_delta": -0.10,
                        "stress_fail_count": 0.0,
                    },
                    passed_consistency_checks=True,
                )
            ],
            comparison_warnings=[],
        ),
    )

    assert decisions[0].decision == "PROMOTE"
    assert decisions[0].default_gate_passed is False
    assert "advanced default admission gate not passed" in decisions[0].soft_warnings


def _benchmark_summary(rank: int) -> BenchmarkSummaryArtifact:
    return BenchmarkSummaryArtifact(
        benchmark_name="baseline_family_walk_forward",
        dataset_id="dataset-1",
        data_source="real",
        window_count=4,
        ranking_metric="mean_test_mae",
        leaderboard=[
            BenchmarkSummaryRow(
                rank=rank,
                model_name="elastic_net",
                family="linear",
                advanced_kind="baseline",
                backend="native",
                mean_valid_mae=0.1,
                mean_test_mae=0.2,
                artifact_uri="artifact://benchmark",
            )
        ],
        reference_consistency={"top_model_consistent": True},
        selected_top_k=2,
        official_benchmark=True,
        baseline_model_names=["elastic_net"],
        admission_gates={"elastic_net": {"default_gate_passed": True}},
    )


def _backtest_summary(
    *,
    decision_case: str,
    passed_consistency_checks: bool,
    stress_fail_count: float,
) -> BacktestSummaryArtifact:
    if decision_case == "reject":
        divergence = {
            "simulation_minus_research_alpha_pnl": 100.0,
            "simulation_minus_research_cumulative_return": 0.08,
            "simulation_minus_research_drawdown": 0.01,
            "simulation_minus_research_shortfall": 0.0,
        }
        scenario_metrics = {
            "cost_x2_return_delta": -0.02,
            "cost_x5_return_delta": -0.05,
            "latency_shock_return_delta": -0.03,
            "liquidity_drought_return_delta": -0.04,
            "worst_scenario_return_delta": -0.30,
            "stress_fail_count": stress_fail_count,
        }
    else:
        divergence = {
            "simulation_minus_research_alpha_pnl": -10.0,
            "simulation_minus_research_cumulative_return": -0.01,
            "simulation_minus_research_drawdown": 0.01,
            "simulation_minus_research_shortfall": 0.02,
        }
        scenario_metrics = {
            "cost_x2_return_delta": -0.02,
            "cost_x5_return_delta": -0.05,
            "latency_shock_return_delta": -0.03,
            "liquidity_drought_return_delta": -0.04,
            "worst_scenario_return_delta": -0.10,
            "stress_fail_count": stress_fail_count,
        }
    row = BacktestSummaryRow(
        model_name="elastic_net",
        run_id="run-1",
        prediction_frame_uri="artifact://prediction",
        research_result_uri="artifact://research",
        simulation_result_uri="artifact://simulation",
        research_metrics={
            "max_drawdown": 0.20,
            "turnover_total": 2.0,
            "risk_trigger_count": 0.0,
        },
        simulation_metrics={
            "max_drawdown": 0.22,
            "turnover_total": 2.5,
            "risk_trigger_count": 0.0,
        },
        divergence_metrics=divergence,
        scenario_metrics=scenario_metrics,
        passed_consistency_checks=passed_consistency_checks,
    )
    return BacktestSummaryArtifact(
        summary_id="summary-1",
        dataset_id="dataset-1",
        prediction_scope="full",
        data_source="real",
        benchmark_name="baseline_family_walk_forward",
        request_digest="digest-1",
        rows=[row],
        comparison_warnings=[],
    )
