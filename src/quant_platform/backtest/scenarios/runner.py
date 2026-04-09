from __future__ import annotations

from quant_platform.backtest.contracts.scenario import ScenarioResult, ScenarioSpec


def summarize_scenarios(
    scenario_specs: list[ScenarioSpec],
    baseline_metrics: dict[str, float],
    scenario_metrics: dict[str, dict[str, float]],
    risk_trigger_counts: dict[str, int],
) -> list[ScenarioResult]:
    results: list[ScenarioResult] = []
    for scenario in scenario_specs:
        current = scenario_metrics.get(scenario.name, {})
        metric_delta = {
            key: current.get(key, 0.0) - baseline_metrics.get(key, 0.0)
            for key in set(baseline_metrics) | set(current)
        }
        results.append(
            ScenarioResult(
                scenario_name=scenario.name,
                metrics_delta=metric_delta,
                execution_delta={},
                risk_trigger_count=risk_trigger_counts.get(scenario.name, 0),
                pnl_delta=current.get("cumulative_return", 0.0)
                - baseline_metrics.get("cumulative_return", 0.0),
                failure_summary=None,
            )
        )
    return results
