from __future__ import annotations

from pathlib import Path

from quant_platform.backtest.contracts.backtest import BacktestRequest, BacktestResult
from quant_platform.backtest.contracts.order import ChildOrder, ParentOrder
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
from quant_platform.backtest.engines.vectorized import ResearchBacktestEngine
from quant_platform.backtest.execution.sim_broker import SimulatedBroker
from quant_platform.backtest.metrics.attribution import compute_pnl_attribution
from quant_platform.backtest.metrics.diagnostics import (
    compute_execution_metrics,
    compute_signal_metrics,
    summarize_block_reasons,
)
from quant_platform.backtest.metrics.performance import compute_performance_metrics
from quant_platform.backtest.metrics.risk import compute_risk_metrics
from quant_platform.backtest.portfolio.ledger import PortfolioLedger
from quant_platform.backtest.portfolio.risk_checks import clip_target
from quant_platform.backtest.scenarios.runner import summarize_scenarios
from quant_platform.common.hashing.digest import stable_digest
from quant_platform.common.io.files import LocalArtifactStore
from quant_platform.data.contracts.market import NormalizedMarketBar
from quant_platform.training.contracts.training import PredictionFrame


class EventDrivenSimulationEngine(ResearchBacktestEngine):
    """Layer 2 event-driven engine with order lifecycle simulation."""

    def __init__(self, artifact_root: Path) -> None:
        self.store = LocalArtifactStore(artifact_root)

    def run(
        self,
        request: BacktestRequest,
        prediction_frame: PredictionFrame | None = None,
        market_bars: list[NormalizedMarketBar] | None = None,
        signal_frame: SignalFrame | None = None,
    ) -> BacktestResult:
        effective_request = request.model_copy(update={"engine_type": "simulation"})
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
            raise ValueError("baseline scenario execution failed")
        backtest_id = build_backtest_id(effective_request, "simulation")
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
            block_reasons=baseline_payload["block_reasons"],
            warnings=baseline_payload["warnings"],
        )
        report = BacktestReport(
            summary="event-driven simulation completed",
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
            engine_type="simulation",
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
        request: BacktestRequest,
        signal_frame: SignalFrame,
        market_bars: list[NormalizedMarketBar] | None,
        scenario: ScenarioSpec,
    ) -> dict[str, object]:
        strategy_config, cost_model, execution_config, risk_constraints = apply_scenario(
            request,
            scenario,
        )
        scenario_signal_frame = signal_frame.model_copy(
            update={
                "rows": [
                    row.model_copy(update={"direction_mode": strategy_config.direction_mode})
                    for row in signal_frame.rows
                ]
            }
        )
        events = resolve_market_events(market_bars, scenario, scenario_signal_frame)
        ledger = PortfolioLedger(
            initial_cash=request.portfolio_config.initial_cash,
            max_gross_leverage=request.portfolio_config.max_gross_leverage,
        )
        broker = SimulatedBroker(execution_config=execution_config, cost_model=cost_model)
        targets = prepare_targets(
            signal_frame=scenario_signal_frame,
            request=request.model_copy(
                update={"execution_config": execution_config, "strategy_config": strategy_config}
            ),
            strategy_config=strategy_config,
            risk_constraints=risk_constraints,
            market_bars=market_bars,
        )
        targets.sort(key=lambda item: item[0].timestamp)
        parent_orders: list[ParentOrder] = []
        final_order_states: dict[str, ChildOrder] = {}
        risk_events = []
        warnings: list[str] = []
        block_reasons: list[str] = []
        fills = []
        active_orders: dict[str, ChildOrder] = {}
        next_target_index = 0
        snapshots = []
        eligible_order_count = 0
        blocked_order_count = 0
        for event in events:
            ledger.mark(event)
            while (
                next_target_index < len(targets)
                and event.event_time > targets[next_target_index][0].timestamp
            ):
                target, eligible_time = targets[next_target_index]
                next_target_index += 1
                clipped_target, target_risk_events = clip_target(target, risk_constraints)
                risk_events.extend(target_risk_events)
                snapshot_before = ledger.snapshot(event.event_time)
                current_quantity = (
                    ledger.positions.get(target.instrument).quantity
                    if target.instrument in ledger.positions
                    else 0.0
                )
                desired_quantity = self._target_quantity(
                    target_value=clipped_target.target_value,
                    event_price=event.open,
                    equity=max(snapshot_before.equity, 1e-9),
                    target_type=clipped_target.target_type,
                    allow_fractional_qty=risk_constraints.allow_fractional_qty,
                )
                delta_quantity = desired_quantity - current_quantity
                order_notional = abs(delta_quantity * event.open)
                if abs(delta_quantity) <= 1e-12:
                    blocked_order_count += 1
                    if abs(clipped_target.target_value) <= 1e-12 and abs(current_quantity) <= 1e-12:
                        block_reasons.append("all signals resolved to zero target value")
                    elif abs(clipped_target.target_value) <= 1e-12:
                        block_reasons.append("risk constraints clipped target to zero")
                    else:
                        block_reasons.append("target already matched current position")
                    continue
                eligible_order_count += 1
                parent_order = ParentOrder(
                    parent_order_id=stable_digest(
                        {
                            "signal_id": clipped_target.signal_ref,
                            "instrument": clipped_target.instrument,
                            "eligible_time": str(eligible_time),
                        }
                    ),
                    created_time=clipped_target.timestamp,
                    instrument=clipped_target.instrument,
                    target_type=clipped_target.target_type,
                    target_value=clipped_target.target_value,
                    current_value=current_quantity,
                    delta_value=delta_quantity,
                    signal_ref=clipped_target.signal_ref,
                    reason_code=clipped_target.reason_code,
                )
                parent_orders.append(parent_order)
                child_order = self._build_child_order(
                    parent_order=parent_order,
                    created_time=clipped_target.timestamp,
                    eligible_time=eligible_time,
                    delta_quantity=delta_quantity,
                    order_notional=order_notional,
                    execution_config=execution_config,
                    risk_constraints=risk_constraints,
                )
                if child_order.status == "REJECTED":
                    blocked_order_count += 1
                    final_order_states[child_order.order_id] = child_order
                    rejection_reason = child_order.rejection_reason or "rejected"
                    warnings.append(rejection_reason)
                    block_reasons.append(rejection_reason)
                    continue
                active_orders[child_order.order_id] = child_order
                final_order_states[child_order.order_id] = child_order
            completed_order_ids: list[str] = []
            for order_id, order in list(active_orders.items()):
                updated_order, new_fills = broker.process_event(order, event)
                active_orders[order_id] = updated_order
                if new_fills:
                    for fill in new_fills:
                        signed_quantity = fill.quantity if fill.side == "BUY" else -fill.quantity
                        ledger.apply_fill(
                            instrument=fill.instrument,
                            signed_quantity=signed_quantity,
                            price=fill.price,
                            fee=fill.fee,
                            slippage_cost=fill.slippage_cost,
                            fill_time=fill.fill_time,
                        )
                        fills.append(fill)
                if updated_order.status in {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}:
                    completed_order_ids.append(order_id)
                final_order_states[order_id] = updated_order
            for order_id in completed_order_ids:
                active_orders.pop(order_id, None)
            snapshots.append(ledger.snapshot(event.event_time))
        child_orders = list(final_order_states.values())
        position_open_count = len(
            {
                position.instrument
                for snapshot in snapshots
                for position in snapshot.positions
                if abs(position.quantity) > 1e-12
            }
        )
        performance_metrics = compute_performance_metrics(
            snapshots,
            benchmark_returns(events, request.benchmark_spec.symbol),
        )
        execution_metrics = compute_execution_metrics(
            child_orders,
            fills,
            snapshots,
            initial_cash=request.portfolio_config.initial_cash,
            eligible_order_count=eligible_order_count,
            blocked_order_count=blocked_order_count,
            position_open_count=position_open_count,
        )
        risk_metrics = compute_risk_metrics(snapshots, len(risk_events))
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
            "risk_events": risk_events,
            "warnings": warnings,
            "block_reasons": summarize_block_reasons(block_reasons),
            "performance_metrics": performance_metrics,
            "execution_metrics": execution_metrics,
            "risk_metrics": risk_metrics,
            "signal_metrics": signal_metrics,
            "pnl_payload": pnl_payload,
            "signal_frame": scenario_signal_frame,
        }
