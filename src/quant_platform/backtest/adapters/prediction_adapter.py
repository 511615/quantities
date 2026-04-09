from __future__ import annotations

from datetime import timedelta

from quant_platform.backtest.contracts.backtest import LatencyConfig, StrategyConfig
from quant_platform.backtest.contracts.signal import SignalFrame, SignalRecord
from quant_platform.common.hashing.digest import stable_digest
from quant_platform.training.contracts.training import PredictionFrame


class PredictionToSignalAdapter:
    """Normalize model predictions into a stable signal contract."""

    def adapt(
        self,
        prediction_frame: PredictionFrame,
        strategy_config: StrategyConfig,
        latency_config: LatencyConfig,
        source_prediction_uri: str | None = None,
    ) -> SignalFrame:
        rows: list[SignalRecord] = []
        inference_latency = (
            timedelta(milliseconds=prediction_frame.metadata.inference_latency_ms)
            if prediction_frame.metadata
            else timedelta(0)
        )
        signal_delay = timedelta(seconds=latency_config.signal_delay_seconds)
        for prediction in prediction_frame.rows:
            signal_time = prediction.timestamp
            available_time = prediction.feature_available_time or prediction.timestamp
            if signal_time < available_time:
                raise ValueError("prediction_time >= max(feature_available_time) is required")
            tradable_from = available_time + inference_latency + signal_delay
            if tradable_from < signal_time:
                raise ValueError("tradable_from must be later than or equal to prediction_time")
            rows.append(
                SignalRecord(
                    signal_id=stable_digest(
                        {
                            "model_run_id": prediction.model_run_id,
                            "instrument": prediction.entity_keys.get(
                                "instrument",
                                prediction.entity_keys.get("symbol", "unknown"),
                            ),
                            "timestamp": prediction.timestamp,
                        }
                    ),
                    model_run_id=prediction.model_run_id,
                    instrument=prediction.entity_keys.get(
                        "instrument",
                        prediction.entity_keys.get("symbol", "unknown"),
                    ),
                    venue=str(prediction.entity_keys.get("venue", "unknown")),
                    signal_time=signal_time,
                    available_time=available_time,
                    tradable_from=tradable_from,
                    horizon_end=None,
                    signal_type=strategy_config.signal_type,
                    raw_value=prediction.prediction,
                    normalized_value=None,
                    confidence=prediction.confidence,
                    direction_mode=strategy_config.direction_mode,
                    meta={},
                )
            )
        return SignalFrame(
            rows=rows,
            source_prediction_uri=source_prediction_uri,
            source_model_run_id=rows[0].model_run_id if rows else None,
        )
