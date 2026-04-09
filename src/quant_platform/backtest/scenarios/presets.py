from __future__ import annotations

from quant_platform.backtest.contracts.scenario import ScenarioSpec


def build_standard_scenarios() -> list[ScenarioSpec]:
    return [
        ScenarioSpec(name="BASELINE", description="default backtest configuration"),
        ScenarioSpec(
            name="COST_X2",
            description="double fees and slippage",
            fee_multiplier=2.0,
            slippage_multiplier=2.0,
        ),
        ScenarioSpec(
            name="COST_X5",
            description="five times fees and slippage",
            fee_multiplier=5.0,
            slippage_multiplier=5.0,
        ),
        ScenarioSpec(
            name="LATENCY_SHOCK",
            description="higher signal and order delay",
            latency_multiplier=3.0,
        ),
        ScenarioSpec(
            name="SPREAD_WIDENING",
            description="wider spreads and impact",
            slippage_multiplier=1.5,
        ),
        ScenarioSpec(
            name="LIQUIDITY_DROUGHT",
            description="reduced executable volume",
            participation_multiplier=0.5,
            volume_multiplier=0.5,
        ),
        ScenarioSpec(
            name="FUNDING_SPIKE",
            description="higher funding and borrow costs",
        ),
        ScenarioSpec(
            name="GAP_OPEN",
            description="higher realized volatility",
            volatility_multiplier=1.5,
        ),
        ScenarioSpec(
            name="VOLATILITY_CLUSTER",
            description="clustered volatile execution prices",
            volatility_multiplier=2.0,
        ),
        ScenarioSpec(
            name="DELIST_OR_TRADING_HALT",
            description="trading unavailable in later events",
            volume_multiplier=0.2,
        ),
        ScenarioSpec(
            name="LEVERAGE_CUT",
            description="force lower effective leverage",
            participation_multiplier=0.7,
        ),
        ScenarioSpec(
            name="LONG_ONLY_FALLBACK",
            description="rerun in long_only mode",
            direction_mode_override="long_only",
        ),
        ScenarioSpec(
            name="UNIVERSE_CHURN",
            description="reduced tradable capacity",
            participation_multiplier=0.6,
        ),
        ScenarioSpec(
            name="STALE_SIGNAL",
            description="signals become slower",
            latency_multiplier=2.0,
        ),
        ScenarioSpec(
            name="BROKEN_DATA_GUARD",
            description="degraded market data availability",
            volume_multiplier=0.3,
        ),
    ]
