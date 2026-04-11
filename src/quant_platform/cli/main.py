from __future__ import annotations

import json
from pathlib import Path

import typer

from quant_platform.api.facade import QuantPlatformFacade
from quant_platform.backtest.contracts.backtest import (
    BacktestRequest,
    BenchmarkSpec,
    CalendarSpec,
    CostModel,
    PortfolioConfig,
    StrategyConfig,
)
from quant_platform.common.config.loader import load_app_config
from quant_platform.common.enums.core import AgentTaskType
from quant_platform.common.logging.setup import configure_logging
from quant_platform.common.types.core import ArtifactRef
from quant_platform.datasets.contracts.dataset import DatasetRef
from quant_platform.training.contracts.training import (
    PredictionScope,
    TrackingContext,
    TrainerConfig,
)
from quant_platform.webapi.repositories.artifacts import ArtifactRepository
from quant_platform.webapi.services.model_cleanup import ModelCleanupService
from quant_platform.workflows.contracts.requests import (
    BacktestWorkflowRequest,
    PredictWorkflowRequest,
    PredictionInputRef,
    ReviewWorkflowRequest,
    TrainWorkflowRequest,
    WorkflowRunRequest,
)
from quant_platform.workflows.contracts.state import WorkflowStageName

app = typer.Typer(help="Quant platform CLI")
data_app = typer.Typer(help="Data commands")
train_app = typer.Typer(help="Training commands")
backtest_app = typer.Typer(help="Backtest commands")
agent_app = typer.Typer(help="Agent commands")
workflow_app = typer.Typer(help="Workflow commands")

app.add_typer(data_app, name="data")
app.add_typer(train_app, name="train")
app.add_typer(backtest_app, name="backtest")
app.add_typer(agent_app, name="agent")
app.add_typer(workflow_app, name="workflow")


def _build_facade() -> QuantPlatformFacade:
    config = load_app_config()
    return QuantPlatformFacade(Path(config.env.artifact_root))


def _echo_json(payload: object) -> None:
    if hasattr(payload, "model_dump_json"):
        typer.echo(payload.model_dump_json(indent=2))
        return
    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))


def _smoke_dataset_ref(facade: QuantPlatformFacade) -> DatasetRef:
    return facade.store.read_model(
        str(facade.artifact_root / "datasets" / "smoke_dataset_dataset_ref.json"),
        DatasetRef,
    )


def _build_smoke_train_request(
    facade: QuantPlatformFacade,
    dataset_ref: DatasetRef | None,
    *,
    run_id_prefix: str = "smoke-train-run",
) -> TrainWorkflowRequest:
    return TrainWorkflowRequest(
        dataset_ref=dataset_ref,
        model_specs=[facade.build_smoke_model_spec()],
        trainer_config=TrainerConfig(
            runner="local",
            epochs=1,
            batch_size=32,
            deterministic=True,
        ),
        tracking_context=TrackingContext(
            backend="file",
            experiment_name="quant-platform-smoke",
            tracking_uri=str(facade.artifact_root / "tracking"),
        ),
        seed=7,
        run_id_prefix=run_id_prefix,
    )


def _build_smoke_backtest_template(prediction_frame_uri: str) -> BacktestRequest:
    return BacktestRequest(
        prediction_frame_uri=prediction_frame_uri,
        strategy_config=StrategyConfig(name="sign_strategy"),
        portfolio_config=PortfolioConfig(
            initial_cash=100000.0,
            max_gross_leverage=1.0,
            max_position_weight=1.0,
        ),
        cost_model=CostModel(fee_bps=5.0, slippage_bps=2.0),
        benchmark_spec=BenchmarkSpec(name="buy_and_hold", symbol="BTCUSDT"),
        calendar_spec=CalendarSpec(timezone="UTC", frequency="1h"),
    )


def _build_smoke_review_request(
    *,
    request_id: str,
    fit_result_uri: str,
    train_manifest_uri: str,
    report_uri: str,
) -> ReviewWorkflowRequest:
    return ReviewWorkflowRequest(
        request_id=request_id,
        goal="Summarize smoke-train experiment and compare smoke backtest outputs",
        task_type=AgentTaskType.SUMMARIZE_EXPERIMENT.value,
        input_artifacts=[ArtifactRef(kind="backtest_report", uri=report_uri)],
        experiment_refs=[
            ArtifactRef(kind="fit_result", uri=fit_result_uri),
            ArtifactRef(kind="train_manifest", uri=train_manifest_uri),
        ],
        comparison_mode="report_summary",
        allowed_tools=[
            "research.read_experiment_summary",
            "research.compare_backtest_reports",
        ],
    )


def _parse_stages(stage_names: str) -> list[WorkflowStageName]:
    return [WorkflowStageName(stage.strip()) for stage in stage_names.split(",") if stage.strip()]


@app.callback()
def main() -> None:
    configure_logging()


@app.command()
def info() -> None:
    config = load_app_config()
    typer.echo(f"project={config.project.name}")
    typer.echo(f"env={config.env.name}")
    typer.echo(f"default_model={config.model.default_model}")
    typer.echo(f"tracking_backend={config.env.tracking_backend}")


@data_app.command("smoke")
def data_smoke() -> None:
    facade = _build_facade()
    _echo_json(facade.data_smoke())


@train_app.command("smoke")
def train_smoke() -> None:
    facade = _build_facade()
    _echo_json(facade.train_smoke())


@train_app.command("predict-smoke")
def predict_smoke() -> None:
    facade = _build_facade()
    fit_result = facade.train_smoke()
    _echo_json(facade.build_prediction_frame(fit_result))


@train_app.command("benchmark-baselines")
def benchmark_baselines() -> None:
    facade = _build_facade()
    _echo_json(facade.run_baseline_benchmark())


@train_app.command("normalize-artifacts")
def normalize_artifacts(
    delete_irreparable: bool = typer.Option(
        True,
        help="Hard delete runs that cannot be repaired into the new artifact format.",
    ),
) -> None:
    facade = _build_facade()
    service = ModelCleanupService(
        repository=ArtifactRepository(facade.artifact_root),
        facade=facade,
    )
    _echo_json(service.normalize_repository(delete_irreparable=delete_irreparable))


@backtest_app.command("smoke")
def backtest_smoke() -> None:
    facade = _build_facade()
    _echo_json(facade.backtest_smoke())


@agent_app.command("smoke")
def agent_smoke() -> None:
    facade = _build_facade()
    _echo_json(facade.agent_smoke())


@workflow_app.command("prepare")
def workflow_prepare(
    preset: str = typer.Option("smoke", help="Currently supports smoke preset only"),
) -> None:
    facade = _build_facade()
    if preset != "smoke":
        raise typer.BadParameter("workflow prepare currently supports --preset smoke only")
    _echo_json(facade.prepare_workflow.prepare(facade.prepare_workflow.build_smoke_request()))


@workflow_app.command("train")
def workflow_train(
    preset: str = typer.Option("smoke", help="Currently supports smoke preset only"),
) -> None:
    facade = _build_facade()
    if preset != "smoke":
        raise typer.BadParameter("workflow train currently supports --preset smoke only")
    prepare_result = facade.prepare_workflow.prepare(facade.prepare_workflow.build_smoke_request())
    _echo_json(
        facade.train_workflow.train(_build_smoke_train_request(facade, prepare_result.dataset_ref))
    )


@workflow_app.command("predict")
def workflow_predict(
    preset: str = typer.Option("smoke", help="Currently supports smoke preset only"),
) -> None:
    facade = _build_facade()
    if preset != "smoke":
        raise typer.BadParameter("workflow predict currently supports --preset smoke only")
    prepare_result = facade.prepare_workflow.prepare(facade.prepare_workflow.build_smoke_request())
    train_result = facade.train_workflow.train(
        _build_smoke_train_request(facade, prepare_result.dataset_ref)
    )
    _echo_json(
        facade.predict_workflow.predict(
            PredictWorkflowRequest(
                dataset_ref=prepare_result.dataset_ref,
                fit_results=[item.fit_result for item in train_result.items],
                prediction_scope=PredictionScope(
                    scope_name="full",
                    as_of_time=prepare_result.dataset_ref.feature_view_ref.as_of_time,
                ),
            )
        )
    )


@workflow_app.command("benchmark")
def workflow_benchmark(
    preset: str = typer.Option("baseline", help="Currently supports baseline preset only"),
) -> None:
    facade = _build_facade()
    if preset != "baseline":
        raise typer.BadParameter("workflow benchmark currently supports --preset baseline only")
    _echo_json(facade.benchmark_workflow.run_baseline_benchmark())


@workflow_app.command("backtest")
def workflow_backtest(
    preset: str = typer.Option("smoke", help="Currently supports smoke preset only"),
) -> None:
    facade = _build_facade()
    if preset != "smoke":
        raise typer.BadParameter("workflow backtest currently supports --preset smoke only")
    prepare_result = facade.prepare_workflow.prepare(facade.prepare_workflow.build_smoke_request())
    train_result = facade.train_workflow.train(
        _build_smoke_train_request(facade, prepare_result.dataset_ref)
    )
    predict_result = facade.predict_workflow.predict(
        PredictWorkflowRequest(
            dataset_ref=prepare_result.dataset_ref,
            fit_results=[item.fit_result for item in train_result.items],
            prediction_scope=PredictionScope(
                scope_name="full",
                as_of_time=prepare_result.dataset_ref.feature_view_ref.as_of_time,
            ),
        )
    )
    _echo_json(
        facade.backtest_workflow.backtest(
            BacktestWorkflowRequest(
                prediction_inputs=[
                    PredictionInputRef(
                        model_name=item.model_name,
                        run_id=item.run_id,
                        prediction_frame_uri=item.prediction_frame_uri,
                    )
                    for item in predict_result.items
                ],
                backtest_request_template=_build_smoke_backtest_template(
                    predict_result.items[0].prediction_frame_uri
                ),
            )
        )
    )


@workflow_app.command("review")
def workflow_review(
    preset: str = typer.Option("smoke", help="Currently supports smoke preset only"),
) -> None:
    facade = _build_facade()
    if preset != "smoke":
        raise typer.BadParameter("workflow review currently supports --preset smoke only")
    prepare_result = facade.prepare_workflow.prepare(facade.prepare_workflow.build_smoke_request())
    train_result = facade.train_workflow.train(
        _build_smoke_train_request(facade, prepare_result.dataset_ref)
    )
    predict_result = facade.predict_workflow.predict(
        PredictWorkflowRequest(
            dataset_ref=prepare_result.dataset_ref,
            fit_results=[item.fit_result for item in train_result.items],
            prediction_scope=PredictionScope(
                scope_name="full",
                as_of_time=prepare_result.dataset_ref.feature_view_ref.as_of_time,
            ),
        )
    )
    backtest_result = facade.backtest_workflow.backtest(
        BacktestWorkflowRequest(
            prediction_inputs=[
                PredictionInputRef(
                    model_name=item.model_name,
                    run_id=item.run_id,
                    prediction_frame_uri=item.prediction_frame_uri,
                )
                for item in predict_result.items
            ],
            backtest_request_template=_build_smoke_backtest_template(
                predict_result.items[0].prediction_frame_uri
            ),
        )
    )
    _echo_json(
        facade.review_workflow.review(
            _build_smoke_review_request(
                request_id="workflow-review-smoke",
                fit_result_uri=train_result.items[0].fit_result_uri,
                train_manifest_uri=train_result.items[0].fit_result.train_manifest_uri,
                report_uri=backtest_result.items[0].backtest_result.report_uri,
            )
        )
    )


@workflow_app.command("run")
def workflow_run(
    preset: str = typer.Option("smoke", help="Currently supports smoke preset only"),
    stages: str = typer.Option(
        "prepare,train,predict,backtest,review",
        help="Comma-separated workflow stages",
    ),
) -> None:
    facade = _build_facade()
    if preset != "smoke":
        raise typer.BadParameter("workflow run currently supports --preset smoke only")
    prepare_request = facade.prepare_workflow.build_smoke_request()
    _echo_json(
        facade.pipeline_workflow.run(
            WorkflowRunRequest(
                workflow_id="workflow-run-smoke",
                stages=_parse_stages(stages),
                prepare=prepare_request,
                train=_build_smoke_train_request(
                    facade,
                    None,
                    run_id_prefix="workflow-run-smoke",
                ),
                predict=PredictWorkflowRequest(
                    dataset_ref=None,
                    prediction_scope=PredictionScope(
                        scope_name="full",
                        as_of_time=prepare_request.as_of_time,
                    ),
                ),
                backtest=BacktestWorkflowRequest(
                    backtest_request_template=_build_smoke_backtest_template(
                        str(
                            facade.artifact_root
                            / "predictions"
                            / "workflow-run-smoke"
                            / "full.json"
                        )
                    )
                ),
                review=ReviewWorkflowRequest(
                    request_id="workflow-run-smoke-review",
                    goal="Summarize smoke-train experiment and compare smoke backtest outputs",
                    task_type=AgentTaskType.SUMMARIZE_EXPERIMENT.value,
                    comparison_mode="report_summary",
                    allowed_tools=[
                        "research.read_experiment_summary",
                        "research.compare_backtest_reports",
                    ],
                ),
            )
        )
    )


if __name__ == "__main__":
    app()


def run() -> None:
    app()
