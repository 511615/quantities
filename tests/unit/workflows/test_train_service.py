from __future__ import annotations

from pathlib import Path

from quant_platform.common.enums.core import ModelFamily
from quant_platform.models.contracts.model_spec import ModelSpec
from quant_platform.training.contracts.training import TrackingContext, TrainerConfig
from quant_platform.workflows.contracts.requests import TrainWorkflowRequest
from quant_platform.workflows.services.prepare import PrepareWorkflowService
from quant_platform.workflows.services.train import TrainWorkflowService


def test_train_service_returns_ranked_fit_results(workflow_runtime) -> None:
    prepare_service = PrepareWorkflowService(workflow_runtime)
    train_service = TrainWorkflowService(workflow_runtime)
    prepare_result = prepare_service.prepare(prepare_service.build_smoke_request())

    result = train_service.train(
        TrainWorkflowRequest(
            dataset_ref=prepare_result.dataset_ref,
            model_specs=[prepare_service.build_smoke_model_spec()],
            trainer_config=TrainerConfig(
                runner="local",
                epochs=1,
                batch_size=32,
                deterministic=True,
            ),
            tracking_context=TrackingContext(
                backend="file",
                experiment_name="workflow-train-unit",
                tracking_uri=str(workflow_runtime.artifact_root / "tracking"),
            ),
            seed=7,
            run_id_prefix="unit-workflow-train",
        )
    )

    assert result.dataset_ref.dataset_id == prepare_result.dataset_ref.dataset_id
    assert len(result.items) == 1
    assert result.leaderboard[0].rank == 1
    assert result.leaderboard[0].model_name == "elastic_net"
    assert Path(result.items[0].fit_result_uri).exists()
    assert Path(result.items[0].fit_result.train_manifest_uri).exists()


def test_train_service_supports_lstm_with_rolling_window_spec(workflow_runtime) -> None:
    prepare_service = PrepareWorkflowService(workflow_runtime)
    train_service = TrainWorkflowService(workflow_runtime)
    prepare_result = prepare_service.prepare(prepare_service.build_smoke_request())

    result = train_service.train(
        TrainWorkflowRequest(
            dataset_ref=prepare_result.dataset_ref,
            model_specs=[
                ModelSpec(
                    model_name="lstm",
                    family=ModelFamily.SEQUENCE,
                    version="0.1.0",
                    input_schema=prepare_service.build_smoke_model_spec().input_schema,
                    output_schema=prepare_service.build_smoke_model_spec().output_schema,
                    hyperparams={
                        "lookback": 3,
                        "forecast_horizon": 1,
                        "stride": 1,
                        "subsequence_length": 2,
                        "subsequence_stride": 1,
                        "force_backend": "fallback",
                            "rolling_window_spec": {
                                "train_size": 3,
                                "valid_size": 1,
                                "test_size": 0,
                                "step_size": 1,
                                "min_train_size": 3,
                                "embargo": 0,
                            "purge_gap": 0,
                            "expanding_train": True,
                        },
                    },
                )
            ],
            trainer_config=TrainerConfig(
                runner="local",
                epochs=1,
                batch_size=4,
                deterministic=True,
            ),
            tracking_context=TrackingContext(
                backend="file",
                experiment_name="workflow-train-lstm-rolling",
                tracking_uri=str(workflow_runtime.artifact_root / "tracking"),
            ),
            seed=7,
            run_id_prefix="unit-workflow-train-lstm",
        )
    )

    assert len(result.items) == 1
    assert result.items[0].model_name == "lstm"
    assert result.leaderboard[0].model_name == "lstm"
    assert Path(result.items[0].fit_result.train_manifest_uri).exists()
