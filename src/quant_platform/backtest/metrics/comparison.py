from __future__ import annotations

from typing import Any

from quant_platform.backtest.contracts.backtest import BacktestResult
from quant_platform.backtest.contracts.report import (
    BacktestSummaryArtifact,
    BacktestSummaryRow,
    BenchmarkSummaryArtifact,
    BenchmarkSummaryRow,
)
from quant_platform.backtest.scenarios.presets import build_standard_scenarios
from quant_platform.common.hashing.digest import stable_digest
from quant_platform.common.io.files import LocalArtifactStore

PROMOTION_SCENARIO_NAMES = (
    "BASELINE",
    "COST_X2",
    "COST_X5",
    "LATENCY_SHOCK",
    "LIQUIDITY_DROUGHT",
    "LONG_ONLY_FALLBACK",
)


def protocol_scenarios() -> tuple[str, ...]:
    known = {scenario.name for scenario in build_standard_scenarios()}
    return tuple(name for name in PROMOTION_SCENARIO_NAMES if name in known)


def build_benchmark_summary_artifact(
    *,
    benchmark_name: str,
    dataset_id: str,
    data_source: str | None,
    rows: list[dict[str, Any]],
    validation_summary: dict[str, object],
    ranking_metric: str = "mean_test_mae",
    selected_top_k: int = 2,
) -> BenchmarkSummaryArtifact:
    leaderboard = [
        BenchmarkSummaryRow(
            rank=int(row["rank"]),
            model_name=str(row["model_name"]),
            family=str(row["family"]),
            advanced_kind=str(row.get("advanced_kind", "baseline")),
            backend=str(row["backend"]),
            mean_valid_mae=float(row["mean_valid_mae"]),
            mean_test_mae=float(row["mean_test_mae"]),
            artifact_uri=str(row["artifact_uri"]),
        )
        for row in rows
    ]
    baseline_rows = [row for row in leaderboard if row.advanced_kind == "baseline"]
    baseline_model_names = [row.model_name for row in baseline_rows]
    admission_gates: dict[str, dict[str, object]] = {}
    for row in leaderboard:
        baseline_models_beaten = [
            baseline.model_name
            for baseline in baseline_rows
            if row.mean_test_mae < baseline.mean_test_mae
        ]
        is_advanced = row.advanced_kind != "baseline"
        default_gate_passed = (not is_advanced) or bool(baseline_models_beaten)
        admission_gates[row.model_name] = {
            "is_advanced": is_advanced,
            "baseline_models_beaten": baseline_models_beaten,
            "baseline_count_beaten": len(baseline_models_beaten),
            "beats_at_least_one_baseline": bool(baseline_models_beaten),
            "default_gate_passed": default_gate_passed,
        }
    return BenchmarkSummaryArtifact(
        benchmark_name=benchmark_name,
        dataset_id=dataset_id,
        data_source=data_source,
        window_count=int(rows[0]["window_count"]) if rows else 0,
        ranking_metric=ranking_metric,
        leaderboard=leaderboard,
        reference_consistency=validation_summary,
        selected_top_k=selected_top_k,
        official_benchmark=True,
        baseline_model_names=baseline_model_names,
        admission_gates=admission_gates,
    )


def build_backtest_summary_row(
    *,
    store: LocalArtifactStore,
    model_name: str,
    run_id: str,
    prediction_frame_uri: str,
    research_result_uri: str,
    research_result: BacktestResult,
    simulation_result_uri: str,
    simulation_result: BacktestResult,
) -> tuple[BacktestSummaryRow, list[str]]:
    research_metrics = load_protocol_metrics(store, research_result)
    simulation_metrics = load_protocol_metrics(store, simulation_result)
    divergence_metrics = {
        "simulation_minus_research_alpha_pnl": simulation_metrics["alpha_pnl"]
        - research_metrics["alpha_pnl"],
        "simulation_minus_research_cumulative_return": simulation_metrics["cumulative_return"]
        - research_metrics["cumulative_return"],
        "simulation_minus_research_drawdown": simulation_metrics["max_drawdown"]
        - research_metrics["max_drawdown"],
        "simulation_minus_research_shortfall": simulation_metrics["implementation_shortfall"]
        - research_metrics["implementation_shortfall"],
    }
    scenario_metrics = {
        "cost_x2_return_delta": simulation_metrics["cost_x2_return_delta"],
        "cost_x5_return_delta": simulation_metrics["cost_x5_return_delta"],
        "latency_shock_return_delta": simulation_metrics["latency_shock_return_delta"],
        "liquidity_drought_return_delta": simulation_metrics["liquidity_drought_return_delta"],
        "worst_scenario_return_delta": simulation_metrics["worst_scenario_return_delta"],
        "stress_fail_count": simulation_metrics["stress_fail_count"],
    }
    warnings = comparison_warnings(research_metrics, simulation_metrics, divergence_metrics)
    return (
        BacktestSummaryRow(
            model_name=model_name,
            run_id=run_id,
            prediction_frame_uri=prediction_frame_uri,
            research_result_uri=research_result_uri,
            simulation_result_uri=simulation_result_uri,
            research_metrics=research_metrics,
            simulation_metrics=simulation_metrics,
            divergence_metrics=divergence_metrics,
            scenario_metrics=scenario_metrics,
            passed_consistency_checks=not warnings,
        ),
        warnings,
    )


def build_backtest_summary_artifact(
    *,
    dataset_id: str,
    prediction_scope: str,
    data_source: str | None,
    benchmark_name: str | None,
    request_digest: str,
    rows: list[BacktestSummaryRow],
    comparison_warnings: list[str],
) -> BacktestSummaryArtifact:
    return BacktestSummaryArtifact(
        summary_id=stable_digest(
            {
                "dataset_id": dataset_id,
                "prediction_scope": prediction_scope,
                "request_digest": request_digest,
                "rows": [row.model_dump(mode="json") for row in rows],
            }
        ),
        dataset_id=dataset_id,
        prediction_scope=prediction_scope,
        data_source=data_source,
        benchmark_name=benchmark_name,
        request_digest=request_digest,
        rows=rows,
        comparison_warnings=comparison_warnings,
    )


def leaderboard_rows(rows: list[BacktestSummaryRow]) -> list[BacktestSummaryRow]:
    return sorted(
        rows,
        key=lambda row: (
            -row.simulation_metrics.get("annual_return", 0.0),
            row.simulation_metrics.get("max_drawdown", 0.0),
            row.divergence_metrics.get("simulation_minus_research_shortfall", 0.0),
        ),
    )


def load_protocol_metrics(
    store: LocalArtifactStore,
    result: BacktestResult,
) -> dict[str, float]:
    diagnostics = (
        store.read_json(result.diagnostics_uri) if result.diagnostics_uri is not None else {}
    )
    pnl = store.read_json(result.pnl_uri)
    scenarios = (
        store.read_json(result.scenario_summary_uri)
        if result.scenario_summary_uri is not None
        else {"scenarios": []}
    )
    performance = diagnostics.get("performance_metrics", {})
    execution = diagnostics.get("execution_metrics", {})
    risk = diagnostics.get("risk_metrics", {})
    scenario_rows = {row["scenario_name"]: row for row in scenarios.get("scenarios", [])}
    selected = {
        name: float(
            scenario_rows.get(name, {}).get("metrics_delta", {}).get("cumulative_return", 0.0)
        )
        for name in protocol_scenarios()
    }
    scenario_fail_count = sum(1 for value in selected.values() if value < -0.25)
    worst_scenario = min(selected.values()) if selected else 0.0
    return {
        "cumulative_return": _metric(performance, "cumulative_return"),
        "annual_return": _metric(performance, "annual_return"),
        "max_drawdown": _metric(performance, "max_drawdown"),
        "information_ratio": _metric(performance, "information_ratio"),
        "alpha": _metric(performance, "alpha"),
        "beta": _metric(performance, "beta"),
        "turnover_total": _metric(execution, "turnover_total"),
        "average_fee_bps": _metric(execution, "average_fee_bps"),
        "average_slippage_bps": _metric(execution, "average_slippage_bps"),
        "implementation_shortfall": _metric(execution, "implementation_shortfall"),
        "fill_rate": _metric(execution, "fill_rate"),
        "partial_fill_rate": _metric(execution, "partial_fill_rate"),
        "gross_leverage": _metric(risk, "gross_leverage"),
        "concentration_hhi": _metric(risk, "concentration_hhi"),
        "risk_trigger_count": _metric(risk, "risk_trigger_count"),
        "alpha_pnl": float(pnl.get("alpha_pnl", 0.0)),
        "cost_x2_return_delta": selected.get("COST_X2", 0.0),
        "cost_x5_return_delta": selected.get("COST_X5", 0.0),
        "latency_shock_return_delta": selected.get("LATENCY_SHOCK", 0.0),
        "liquidity_drought_return_delta": selected.get("LIQUIDITY_DROUGHT", 0.0),
        "long_only_fallback_return_delta": selected.get("LONG_ONLY_FALLBACK", 0.0),
        "worst_scenario_return_delta": worst_scenario,
        "stress_fail_count": float(scenario_fail_count),
    }


def comparison_warnings(
    research_metrics: dict[str, float],
    simulation_metrics: dict[str, float],
    divergence_metrics: dict[str, float],
) -> list[str]:
    warnings: list[str] = []
    if divergence_metrics["simulation_minus_research_cumulative_return"] > 0.05:
        warnings.append("simulation return is materially better than research return")
    if divergence_metrics["simulation_minus_research_shortfall"] < 0.0:
        warnings.append("simulation shortfall is better than research shortfall")
    for name in (
        "cost_x2_return_delta",
        "cost_x5_return_delta",
        "latency_shock_return_delta",
        "liquidity_drought_return_delta",
    ):
        if simulation_metrics[name] > research_metrics[name] + 1e-9:
            warnings.append(f"simulation friction delta is unexpectedly better for {name}")
    return warnings


def _metric(container: dict[str, object], key: str) -> float:
    value = container.get(key, 0.0)
    return float(value) if isinstance(value, (int, float)) else 0.0
