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
from quant_platform.backtest.execution.costs import fee_cost
from quant_platform.backtest.execution.slippage import execution_price, slippage_bps_for_order
from quant_platform.backtest.metrics.attribution import compute_pnl_attribution
from quant_platform.backtest.metrics.diagnostics import (
    compute_execution_metrics,
    compute_signal_metrics,
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


class ResearchBacktestEngine:
    """Layer 1 research backtest engine for fast batch comparisons."""

    def __init__(self, artifact_root: Path) -> None:
        self.store = LocalArtifactStore(artifact_root)

    def run(
        self,
        request: BacktestRequest,
        prediction_frame: PredictionFrame | None = None,
        market_bars: list[NormalizedMarketBar] | None = None,
        signal_frame: SignalFrame | None = None,
    ) -> BacktestResult:
        effective_request = request.model_copy(update={"engine_type": "research"})
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
        backtest_id = build_backtest_id(effective_request, "research")
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
            summary="research backtest completed",
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
        targets = prepare_targets(
            signal_frame=scenario_signal_frame,
            request=request.model_copy(
                update={"execution_config": execution_config, "strategy_config": strategy_config}
            ),
            strategy_config=strategy_config,
            risk_constraints=risk_constraints,
            market_bars=market_bars,
        )
        targets.sort(key=lambda item: item[1])
        parent_orders: list[ParentOrder] = []
        child_orders: list[ChildOrder] = []
        fills = []
        risk_events = []
        warnings: list[str] = []
        next_target_index = 0
        snapshots = []
        for event in events:
            ledger.mark(event)
            while (
                next_target_index < len(targets)
                and event.event_time > targets[next_target_index][1]
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
                    continue
                order_id_seed = {
                    "signal_id": clipped_target.signal_ref,
                    "instrument": clipped_target.instrument,
                    "eligible_time": str(eligible_time),
                }
                parent_order = ParentOrder(
                    parent_order_id=stable_digest(order_id_seed),
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
                    child_orders.append(child_order)
                    warnings.append(child_order.rejection_reason or "rejected")
                    continue
                participation = min(
                    child_order.participation_cap,
                    risk_constraints.max_participation_rate,
                )
                slippage_bps = min(
                    child_order.max_slippage_bps,
                    slippage_bps_for_order(cost_model, clipped_target.urgency, participation),
                )
                fill_price = execution_price(event.open, child_order.side, slippage_bps)
                fee = fee_cost(abs(delta_quantity * fill_price), cost_model)
                ledger.apply_fill(
                    instrument=child_order.instrument,
                    signed_quantity=delta_quantity,
                    price=fill_price,
                    fee=fee,
                    slippage_cost=abs(fill_price - event.open) * abs(delta_quantity),
                    fill_time=event.event_time,
                )
                filled_order = child_order.model_copy(update={"status": "FILLED"})
                child_orders.append(filled_order)
                fills.append(
                    self._fill_from_order(
                        order=filled_order,
                        fill_time=event.event_time,
                        quantity=abs(delta_quantity),
                        fill_price=fill_price,
                        fee=fee,
                        base_price=event.open,
                    )
                )
            snapshots.append(ledger.snapshot(event.event_time))
        performance_metrics = compute_performance_metrics(
            snapshots,
            benchmark_returns(events, request.benchmark_spec.symbol),
        )
        execution_metrics = compute_execution_metrics(child_orders, fills, snapshots)
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
            "performance_metrics": performance_metrics,
            "execution_metrics": execution_metrics,
            "risk_metrics": risk_metrics,
            "signal_metrics": signal_metrics,
            "pnl_payload": pnl_payload,
            "signal_frame": scenario_signal_frame,
        }

    def _build_child_order(
        self,
        parent_order: ParentOrder,
        created_time: object,
        eligible_time: object,
        delta_quantity: float,
        order_notional: float,
        execution_config,
        risk_constraints,
    ) -> ChildOrder:
        side = "BUY" if delta_quantity >= 0 else "SELL"
        rejection_reason: str | None = None
        status = "NEW"
        if not risk_constraints.allow_short and side == "SELL" and parent_order.target_value < 0:
            status = "REJECTED"
            rejection_reason = "short orders are disabled"
        if order_notional > risk_constraints.max_order_notional:
            status = "REJECTED"
            rejection_reason = "order exceeds max_order_notional"
        return ChildOrder(
            order_id=stable_digest(
                {
                    "parent_order_id": parent_order.parent_order_id,
                    "instrument": parent_order.instrument,
                }
            ),
            parent_order_id=parent_order.parent_order_id,
            created_time=created_time,
            eligible_time=eligible_time,
            instrument=parent_order.instrument,
            side=side,
            order_type="MARKET",
            time_in_force="IOC",
            quantity=abs(delta_quantity),
            max_slippage_bps=execution_config.max_slippage_bps,
            participation_cap=min(
                execution_config.participation_cap,
                risk_constraints.max_participation_rate,
            ),
            reduce_only=False,
            status=status,
            rejection_reason=rejection_reason,
        )

    def _fill_from_order(
        self,
        order: ChildOrder,
        fill_time: object,
        quantity: float,
        fill_price: float,
        fee: float,
        base_price: float,
    ):
        from quant_platform.backtest.contracts.order import FillEvent

        return FillEvent(
            fill_id=stable_digest({"order_id": order.order_id, "fill_time": str(fill_time)}),
            order_id=order.order_id,
            instrument=order.instrument,
            side=order.side,
            fill_time=fill_time,
            quantity=quantity,
            price=fill_price,
            notional=abs(quantity * fill_price),
            fee=fee,
            slippage_cost=abs(fill_price - base_price) * quantity,
            liquidity_flag="taker",
        )

    def _target_quantity(
        self,
        target_value: float,
        event_price: float,
        equity: float,
        target_type: str,
        allow_fractional_qty: bool,
    ) -> float:
        if target_type == "quantity":
            quantity = target_value
        elif target_type == "notional":
            quantity = target_value / max(event_price, 1e-9)
        else:
            quantity = target_value * equity / max(event_price, 1e-9)
        if allow_fractional_qty:
            return quantity
        return float(int(quantity))


class VectorizedBacktestEngine(ResearchBacktestEngine):
    """Compatibility alias for the legacy research backtest name."""
