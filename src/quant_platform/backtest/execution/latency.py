from __future__ import annotations

from datetime import timedelta

from quant_platform.backtest.contracts.backtest import ExecutionConfig


def order_eligible_time(signal_time: object, execution_config: ExecutionConfig) -> object:
    return signal_time + timedelta(seconds=execution_config.latency_config.order_delay_seconds)


def ack_time(eligible_time: object, execution_config: ExecutionConfig) -> object:
    return eligible_time + timedelta(seconds=execution_config.latency_config.ack_delay_seconds)
