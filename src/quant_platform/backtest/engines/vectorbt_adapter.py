from __future__ import annotations

import importlib
from collections import defaultdict
from pathlib import Path

from quant_platform.backtest.contracts.backtest import BacktestRequest, BacktestResult
from quant_platform.backtest.contracts.order import ChildOrder, ParentOrder
from quant_platform.backtest.contracts.portfolio import PortfolioSnapshot, PositionSnapshot
from quant_platform.backtest.contracts.report import BacktestDiagnostics, BacktestReport
from quant_platform.backtest.contracts.scenario import ScenarioSpec
from quant_platform.backtest.contracts.signal import SignalFrame
from quant_platform.backtest.engines.core import (
    apply_scenario,
    benchmark_returns,
    build_backtest_id,
    leakage_audit,
    persist_backtest_artifacts,
    prepare_targets,
    realized_signal_returns,
    resolve_market_events,
    resolve_signal_frame,
)
from quant_platform.backtest.metrics.attribution import compute_pnl_attribution
from quant_platform.backtest.metrics.diagnostics import (
    compute_execution_metrics,
    compute_signal_metrics,
)
from quant_platform.backtest.metrics.performance import compute_performance_metrics
from quant_platform.backtest.metrics.risk import compute_risk_metrics
from quant_platform.backtest.scenarios.runner import summarize_scenarios
from quant_platform.common.io.files import LocalArtifactStore
from quant_platform.data.contracts.market import NormalizedMarketBar
from quant_platform.training.contracts.training import PredictionFrame


class VectorbtResearchAdapter:
    def __init__(self, artifact_root: Path) -> None:
        self.store = LocalArtifactStore(artifact_root)

    def run(
        self,
        request: BacktestRequest,
        prediction_frame: PredictionFrame | None = None,
        market_bars: list[NormalizedMarketBar] | None = None,
        signal_frame: SignalFrame | None = None,
    ) -> BacktestResult:
        effective_request = request.model_copy(
            update={"engine_type": "research", "research_backend": "vectorbt"}
        )
        normalized_signal_frame = resolve_signal_frame(
            effective_request, prediction_frame, signal_frame
        )
        baseline = ScenarioSpec(name="BASELINE", description="default backtest configuration")
        scenarios = [baseline]
        scenarios.extend(
            scenario for scenario in effective_request.scenario_specs if scenario.name != "BASELINE"
        )
        scenario_metrics: dict[str, dict[str, float]] = {}
        risk_counts: dict[str, int] = {}
        baseline_payload: dict[str, object] | None = None
        for scenario in scenarios:
            payload = self._run_single_scenario(
                request=effective_request,
                signal_frame=normalized_signal_frame,
                market_bars=market_bars,
                scenario=scenario,
            )
            scenario_metrics[scenario.name] = (
                payload["performance_metrics"] | payload["risk_metrics"]
            )
            risk_counts[scenario.name] = len(payload["risk_events"])
            if scenario.name == "BASELINE":
                baseline_payload = payload
        if baseline_payload is None:
            raise ValueError("baseline vectorbt scenario execution failed")
        backtest_id = build_backtest_id(effective_request, "research_vectorbt")
        scenario_results = summarize_scenarios(
            scenario_specs=scenarios,
            baseline_metrics=scenario_metrics["BASELINE"],
            scenario_metrics=scenario_metrics,
            risk_trigger_counts=risk_counts,
        )
        diagnostics = BacktestDiagnostics(
            performance_metrics=baseline_payload["performance_metrics"],
            execution_metrics=baseline_payload["execution_metrics"],
            risk_metrics=baseline_payload["risk_metrics"],
            signal_metrics=baseline_payload["signal_metrics"],
            warnings=baseline_payload["warnings"],
        )
        report = BacktestReport(
            summary="vectorbt research backtest completed",
            diagnostics=diagnostics,
            artifact_uris={},
        )
        leakage_payload = leakage_audit(
            signal_frame=baseline_payload["signal_frame"],
            fills=baseline_payload["fills"],
            request=effective_request,
        )
        artifacts = persist_backtest_artifacts(
            store=self.store,
            backtest_id=backtest_id,
            orders=[*baseline_payload["parent_orders"], *baseline_payload["child_orders"]],
            fills=baseline_payload["fills"],
            snapshots=baseline_payload["snapshots"],
            pnl_payload=baseline_payload["pnl_payload"],
            report_payload=report.model_dump(mode="json"),
            diagnostics_payload=diagnostics.model_dump(mode="json"),
            leakage_payload=leakage_payload,
            scenario_payload={
                "scenarios": [item.model_dump(mode="json") for item in scenario_results]
            },
        )
        report = report.model_copy(update={"artifact_uris": artifacts})
        self.store.write_json(
            f"backtests/{backtest_id}/report.json",
            report.model_dump(mode="json"),
        )
        return BacktestResult(
            backtest_id=backtest_id,
            engine_type="research",
            orders_uri=artifacts["orders_uri"],
            fills_uri=artifacts["fills_uri"],
            positions_uri=artifacts["positions_uri"],
            pnl_uri=artifacts["pnl_uri"],
            risk_metrics=baseline_payload["risk_metrics"],
            report_uri=artifacts["report_uri"],
            diagnostics_uri=artifacts["diagnostics_uri"],
            leakage_audit_uri=artifacts["leakage_audit_uri"],
            scenario_summary_uri=artifacts["scenario_summary_uri"],
        )

    def _run_single_scenario(
        self,
        *,
        request: BacktestRequest,
        signal_frame: SignalFrame,
        market_bars: list[NormalizedMarketBar] | None,
        scenario: ScenarioSpec,
    ) -> dict[str, object]:
        pd, vectorbt = _load_vectorbt_dependencies()
        strategy_config, _, _, risk_constraints = apply_scenario(request, scenario)
        scenario_signal_frame = signal_frame.model_copy(
            update={
                "rows": [
                    row.model_copy(update={"direction_mode": strategy_config.direction_mode})
                    for row in signal_frame.rows
                ]
            }
        )
        events = resolve_market_events(market_bars, scenario, scenario_signal_frame)
        targets = prepare_targets(
            signal_frame=scenario_signal_frame,
            request=request.model_copy(update={"strategy_config": strategy_config}),
            strategy_config=strategy_config,
            risk_constraints=risk_constraints,
            market_bars=market_bars,
        )
        close_frame, weight_frame = self._build_market_frames(
            pd=pd,
            events=events,
            targets=targets,
        )
        portfolio = vectorbt.Portfolio.from_orders(
            close=close_frame,
            size=weight_frame,
            size_type="targetpercent",
            init_cash=request.portfolio_config.initial_cash,
            fees=request.cost_model.fee_bps / 10000.0,
            slippage=request.cost_model.slippage_bps / 10000.0,
            cash_sharing=True,
            direction="both",
        )
        snapshots = self._build_snapshots(
            nav_series=portfolio.value(),
            close_frame=close_frame,
            weight_frame=weight_frame,
            initial_cash=request.portfolio_config.initial_cash,
        )
        parent_orders = [
            ParentOrder(
                parent_order_id=f"vectorbt-parent-{index}",
                created_time=target.timestamp,
                instrument=target.instrument,
                target_type=target.target_type,
                target_value=target.target_value,
                current_value=0.0,
                delta_value=target.target_value,
                signal_ref=target.signal_ref,
                reason_code=target.reason_code,
            )
            for index, (target, _) in enumerate(targets)
        ]
        child_orders: list[ChildOrder] = []
        fills = []
        warnings = [f"research backend=vectorbt scenario={scenario.name}"]
        performance_metrics = compute_performance_metrics(
            snapshots,
            benchmark_returns(events, request.benchmark_spec.symbol),
        )
        execution_metrics = compute_execution_metrics(child_orders, fills, snapshots)
        risk_metrics = compute_risk_metrics(snapshots, 0)
        signal_metrics = compute_signal_metrics(
            scenario_signal_frame,
            realized_signal_returns(scenario_signal_frame, events),
        )
        pnl_payload = compute_pnl_attribution(
            snapshots,
            benchmark_returns(events, request.benchmark_spec.symbol),
        )
        return {
            "parent_orders": parent_orders,
            "child_orders": child_orders,
            "fills": fills,
            "snapshots": snapshots,
            "risk_events": [],
            "warnings": warnings,
            "performance_metrics": performance_metrics,
            "execution_metrics": execution_metrics,
            "risk_metrics": risk_metrics,
            "signal_metrics": signal_metrics,
            "pnl_payload": pnl_payload,
            "signal_frame": scenario_signal_frame,
        }

    def _build_market_frames(self, *, pd, events, targets):
        price_rows: dict[object, dict[str, float]] = defaultdict(dict)
        symbols: list[str] = []
        for event in events:
            price_rows[event.event_time][event.instrument] = event.close
            if event.instrument not in symbols:
                symbols.append(event.instrument)
        ordered_times = sorted(price_rows)
        close_frame = pd.DataFrame(
            [
                {symbol: price_rows[bar_time].get(symbol) for symbol in symbols}
                for bar_time in ordered_times
            ],
            index=ordered_times,
        ).ffill().dropna(how="all")
        weight_rows: dict[object, dict[str, float]] = {}
        current_weights = {symbol: 0.0 for symbol in symbols}
        targets_by_time: dict[object, list[ParentOrder | object]] = defaultdict(list)
        for target, eligible_time in targets:
            targets_by_time[eligible_time].append(target)
        for bar_time in close_frame.index:
            for target in targets_by_time.get(bar_time, []):
                current_weights[target.instrument] = target.target_value
            weight_rows[bar_time] = dict(current_weights)
        weight_frame = pd.DataFrame.from_dict(weight_rows, orient="index").reindex(close_frame.index).ffill().fillna(0.0)
        weight_frame = weight_frame.reindex(columns=close_frame.columns).fillna(0.0)
        return close_frame, weight_frame

    def _build_snapshots(
        self,
        *,
        nav_series,
        close_frame,
        weight_frame,
        initial_cash: float,
    ) -> list[PortfolioSnapshot]:
        snapshots: list[PortfolioSnapshot] = []
        peak_nav = float(initial_cash)
        previous_nav = float(initial_cash)
        for timestamp, nav_value in nav_series.items():
            nav = float(nav_value)
            peak_nav = max(peak_nav, nav)
            positions: list[PositionSnapshot] = []
            gross_exposure = 0.0
            net_exposure = 0.0
            long_exposure = 0.0
            short_exposure = 0.0
            for symbol in close_frame.columns:
                weight = float(weight_frame.loc[timestamp, symbol])
                price = float(close_frame.loc[timestamp, symbol])
                if abs(weight) <= 1e-12 or price <= 0.0:
                    continue
                market_value = nav * weight
                notional = abs(market_value)
                quantity = market_value / price
                gross_exposure += notional
                net_exposure += market_value
                long_exposure += max(market_value, 0.0)
                short_exposure += abs(min(market_value, 0.0))
                positions.append(
                    PositionSnapshot(
                        instrument=str(symbol),
                        quantity=quantity,
                        side="long" if quantity > 0 else "short",
                        avg_entry_price=price,
                        mark_price=price,
                        market_value=market_value,
                        notional=notional,
                        unrealized_pnl=0.0,
                        realized_pnl_cum=0.0,
                        last_fill_time=None,
                        weight=weight,
                        initial_margin=0.0,
                        maintenance_margin=0.0,
                    )
                )
            snapshots.append(
                PortfolioSnapshot(
                    timestamp=timestamp,
                    nav=nav,
                    equity=nav,
                    cash_free=nav - net_exposure,
                    cash_locked=0.0,
                    realized_pnl=nav - initial_cash,
                    unrealized_pnl=0.0,
                    fees_paid=0.0,
                    slippage_cost=0.0,
                    funding_pnl=0.0,
                    borrow_cost=0.0,
                    gross_exposure=gross_exposure,
                    net_exposure=net_exposure,
                    long_exposure=long_exposure,
                    short_exposure=short_exposure,
                    gross_leverage=(gross_exposure / nav) if abs(nav) > 1e-9 else 0.0,
                    net_leverage=(net_exposure / nav) if abs(nav) > 1e-9 else 0.0,
                    turnover_1d=abs(nav - previous_nav),
                    margin_used=0.0,
                    maintenance_margin=0.0,
                    liquidation_buffer=max(nav, 0.0),
                    drawdown=(peak_nav - nav) / peak_nav if peak_nav > 0 else 0.0,
                    positions=positions,
                )
            )
            previous_nav = nav
        return snapshots


def _load_vectorbt_dependencies():
    try:
        pd = importlib.import_module("pandas")
        vectorbt = importlib.import_module("vectorbt")
    except ModuleNotFoundError as exc:
        raise ValueError(
            "vectorbt research backend requires the optional 'research_backends' dependencies"
        ) from exc
    return pd, vectorbt
