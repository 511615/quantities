from __future__ import annotations

from quant_platform.backtest.contracts.backtest import BacktestResult
from quant_platform.backtest.metrics.comparison import build_backtest_summary_row
from quant_platform.common.io.files import LocalArtifactStore


def test_cost_aware_divergence_warns_when_simulation_is_better_than_research(tmp_path) -> None:
    store = LocalArtifactStore(tmp_path)
    research = _write_result(
        store=store,
        prefix="research",
        cumulative_return=0.10,
        annual_return=0.20,
        max_drawdown=0.08,
        turnover_total=3.0,
        implementation_shortfall=8.0,
        alpha_pnl=1200.0,
        scenario_deltas={
            "BASELINE": 0.0,
            "COST_X2": -0.03,
            "COST_X5": -0.08,
            "LATENCY_SHOCK": -0.02,
            "LIQUIDITY_DROUGHT": -0.04,
            "LONG_ONLY_FALLBACK": -0.01,
        },
    )
    simulation = _write_result(
        store=store,
        prefix="simulation",
        cumulative_return=0.18,
        annual_return=0.26,
        max_drawdown=0.06,
        turnover_total=3.0,
        implementation_shortfall=6.0,
        alpha_pnl=1500.0,
        scenario_deltas={
            "BASELINE": 0.0,
            "COST_X2": -0.01,
            "COST_X5": -0.03,
            "LATENCY_SHOCK": -0.01,
            "LIQUIDITY_DROUGHT": -0.02,
            "LONG_ONLY_FALLBACK": -0.01,
        },
    )

    row, warnings = build_backtest_summary_row(
        store=store,
        model_name="elastic_net",
        run_id="run-1",
        prediction_frame_uri=str(tmp_path / "prediction.json"),
        research_result_uri=str(tmp_path / "research_result.json"),
        research_result=research,
        simulation_result_uri=str(tmp_path / "simulation_result.json"),
        simulation_result=simulation,
    )

    assert not row.passed_consistency_checks
    assert "simulation return is materially better than research return" in warnings
    assert "simulation shortfall is better than research shortfall" in warnings
    assert any("cost_x2_return_delta" in warning for warning in warnings)


def _write_result(
    *,
    store: LocalArtifactStore,
    prefix: str,
    cumulative_return: float,
    annual_return: float,
    max_drawdown: float,
    turnover_total: float,
    implementation_shortfall: float,
    alpha_pnl: float,
    scenario_deltas: dict[str, float],
) -> BacktestResult:
    diagnostics = store.write_json(
        f"{prefix}/diagnostics.json",
        {
            "performance_metrics": {
                "cumulative_return": cumulative_return,
                "annual_return": annual_return,
                "max_drawdown": max_drawdown,
                "information_ratio": 1.0,
                "alpha": 0.03,
                "beta": 0.1,
            },
            "execution_metrics": {
                "turnover_total": turnover_total,
                "average_fee_bps": 5.0,
                "average_slippage_bps": 3.0,
                "implementation_shortfall": implementation_shortfall,
                "fill_rate": 1.0,
                "partial_fill_rate": 0.0,
            },
            "risk_metrics": {
                "gross_leverage": 1.0,
                "concentration_hhi": 0.3,
                "risk_trigger_count": 0.0,
            },
            "signal_metrics": {},
            "warnings": [],
        },
    )
    pnl = store.write_json(
        f"{prefix}/pnl.json",
        {"alpha_pnl": alpha_pnl},
    )
    scenarios = store.write_json(
        f"{prefix}/scenarios.json",
        {
            "scenarios": [
                {
                    "scenario_name": name,
                    "metrics_delta": {"cumulative_return": delta},
                    "execution_delta": {},
                    "risk_trigger_count": 0,
                    "pnl_delta": delta,
                    "failure_summary": None,
                }
                for name, delta in scenario_deltas.items()
            ]
        },
    )
    return BacktestResult(
        backtest_id=prefix,
        engine_type="research" if prefix == "research" else "simulation",
        orders_uri=str(store.root / prefix / "orders.json"),
        fills_uri=str(store.root / prefix / "fills.json"),
        positions_uri=str(store.root / prefix / "positions.json"),
        pnl_uri=pnl.uri,
        risk_metrics={"gross_exposure": 1.0},
        report_uri=str(store.root / prefix / "report.json"),
        diagnostics_uri=diagnostics.uri,
        scenario_summary_uri=scenarios.uri,
    )
