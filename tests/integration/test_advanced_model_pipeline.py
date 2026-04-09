from __future__ import annotations

import pytest

from quant_platform.backtest.contracts.backtest import (
    BacktestRequest,
    BenchmarkSpec,
    CalendarSpec,
    CostModel,
    PortfolioConfig,
    StrategyConfig,
)
from quant_platform.common.enums.core import ModelFamily
from quant_platform.common.types.core import SchemaField
from quant_platform.models.contracts.model_spec import ModelSpec
from quant_platform.training.contracts.training import (
    FitRequest,
    PredictionScope,
    PredictRequest,
    TrackingContext,
    TrainerConfig,
)


@pytest.mark.parametrize(
    ("model_name", "family", "hyperparams"),
    [
        ("transformer_reference", ModelFamily.DEEP, {"lookback": 3}),
        ("temporal_fusion_reference", ModelFamily.DEEP, {"lookback": 3}),
        ("patch_mixer_reference", ModelFamily.DEEP, {"lookback": 4, "patch_size": 2}),
        (
            "multimodal_reference",
            ModelFamily.DEEP,
            {"lookback": 3, "text_feature_prefixes": ["text_", "sentiment_", "news_"]},
        ),
    ],
)
def test_advanced_model_families_fit_predict_and_backtest(
    facade,
    built_dataset,
    model_name: str,
    family: ModelFamily,
    hyperparams: dict[str, object],
) -> None:
    dataset_ref, samples = built_dataset
    facade.dataset_store[dataset_ref.dataset_id] = samples
    model_spec = ModelSpec(
        model_name=model_name,
        family=family,
        version="0.1.0",
        input_schema=[
            SchemaField(name="lag_return_1", dtype="float"),
            SchemaField(name="volume_zscore", dtype="float"),
        ],
        output_schema=[SchemaField(name="prediction", dtype="float")],
        hyperparams=hyperparams,
    )
    fit_result = facade.training_runner.fit(
        FitRequest(
            run_id=f"{model_name}-fit",
            dataset_ref=dataset_ref,
            model_spec=model_spec,
            trainer_config=TrainerConfig(
                runner="local", epochs=1, batch_size=8, deterministic=True
            ),
            seed=7,
            tracking_context=TrackingContext(
                backend="file", experiment_name="advanced-model-tests"
            ),
        )
    )
    frame = facade.prediction_runner.predict(
        PredictRequest(
            model_artifact_uri=fit_result.model_artifact_uri,
            dataset_ref=dataset_ref,
            prediction_scope=PredictionScope(
                scope_name="full", as_of_time=dataset_ref.feature_view_ref.as_of_time
            ),
        )
    )
    assert fit_result.metrics["sample_count"] == float(len(samples))
    assert fit_result.metrics["best_epoch"] >= 1.0
    assert fit_result.metrics["trained_steps"] >= 1.0
    assert frame.sample_count == len(samples)
    backtest_result = facade.backtest_engine.run(
        BacktestRequest(
            prediction_frame_uri=str(facade.artifact_root / "backtests" / f"{model_name}.json"),
            strategy_config=StrategyConfig(name="sign_strategy"),
            portfolio_config=PortfolioConfig(
                initial_cash=100000.0,
                max_gross_leverage=1.0,
                max_position_weight=1.0,
            ),
            cost_model=CostModel(fee_bps=1.0, slippage_bps=1.0),
            benchmark_spec=BenchmarkSpec(name="buy_and_hold", symbol="BTCUSDT"),
            calendar_spec=CalendarSpec(timezone="UTC", frequency="1h"),
        ),
        frame,
    )
    assert backtest_result.risk_metrics["position_count"] >= 1.0
    assert backtest_result.report_uri


@pytest.mark.parametrize(
    ("model_name", "family", "hyperparams"),
    [
        ("transformer_reference", ModelFamily.DEEP, {"lookback": 3}),
        ("temporal_fusion_reference", ModelFamily.DEEP, {"lookback": 3}),
        ("patch_mixer_reference", ModelFamily.DEEP, {"lookback": 4, "patch_size": 2}),
        (
            "multimodal_reference",
            ModelFamily.DEEP,
            {"lookback": 3, "text_feature_prefixes": ["text_", "sentiment_", "news_"]},
        ),
    ],
)
def test_advanced_model_artifact_reload_preserves_prediction_count(
    facade,
    built_dataset,
    model_name: str,
    family: ModelFamily,
    hyperparams: dict[str, object],
) -> None:
    dataset_ref, samples = built_dataset
    facade.dataset_store[dataset_ref.dataset_id] = samples
    model_spec = ModelSpec(
        model_name=model_name,
        family=family,
        version="0.1.0",
        input_schema=[
            SchemaField(name="lag_return_1", dtype="float"),
            SchemaField(name="volume_zscore", dtype="float"),
        ],
        output_schema=[SchemaField(name="prediction", dtype="float")],
        hyperparams=hyperparams,
    )
    fit_result = facade.training_runner.fit(
        FitRequest(
            run_id=f"{model_name}-reload",
            dataset_ref=dataset_ref,
            model_spec=model_spec,
            trainer_config=TrainerConfig(
                runner="local", epochs=4, batch_size=4, deterministic=True
            ),
            seed=7,
            tracking_context=TrackingContext(
                backend="file", experiment_name="advanced-model-tests"
            ),
        )
    )
    model, meta = facade.model_registry.load_from_artifact(fit_result.model_artifact_uri)
    runtime = facade.model_registry.resolve_runtime(model_name)
    predict_input = runtime.input_adapter.build_predict_input(
        samples,
        dataset_ref,
        meta.model_spec,
        runtime.registration,
    )
    raw_outputs = model.predict(predict_input)
    assert len(raw_outputs.predictions) == len(samples)
    assert meta.best_epoch is not None
    assert meta.trained_steps is not None
    assert meta.prediction_metadata["confidence_source"] in {
        "validation_residual",
        "fallback_residual",
    }
