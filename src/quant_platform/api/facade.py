from __future__ import annotations

import csv
import json
from urllib.error import URLError
from urllib.request import urlopen
from datetime import UTC, datetime
from math import cos, sin
from pathlib import Path

from quant_platform.agents.contracts.agent import GuardrailPolicy
from quant_platform.agents.contracts.research import ResearchAgentRequest
from quant_platform.agents.services.providers import (
    BacktestQueryService,
    ExecutionProposalService,
    RiskQueryService,
    StrategyProposalService,
    TrainingQueryService,
)
from quant_platform.agents.services.research_service import ResearchAgentService
from quant_platform.agents.tool_registry.adapters import register_default_tools
from quant_platform.agents.tool_registry.registry import ToolRegistry
from quant_platform.backtest.contracts.backtest import (
    BacktestRequest,
    BacktestResult,
    BenchmarkSpec,
    CalendarSpec,
    CostModel,
    PortfolioConfig,
    StrategyConfig,
)
from quant_platform.backtest.engines.event_driven import EventDrivenSimulationEngine
from quant_platform.backtest.engines.vectorized import VectorizedBacktestEngine
from quant_platform.backtest.facade import BacktestFacade
from quant_platform.common.config.loader import load_app_config
from quant_platform.common.config.models import ModelConfig
from quant_platform.common.enums.core import AgentTaskType, LabelKind, ModelFamily
from quant_platform.common.io.files import LocalArtifactStore
from quant_platform.common.types.core import ArtifactRef, SchemaField
from quant_platform.data.catalog.catalog import DataCatalog
from quant_platform.data.contracts.market import NormalizedMarketBar
from quant_platform.datasets.builders.dataset_builder import DatasetBuilder
from quant_platform.datasets.contracts.dataset import (
    DatasetRef,
    DatasetSample,
    LabelSpec,
    SamplePolicy,
)
from quant_platform.datasets.labeling.forward_return import ForwardReturnLabeler
from quant_platform.datasets.splits.time_series import RollingWindowSpec, TimeSeriesSplitPlanner
from quant_platform.features.transforms.market_features import MarketFeatureBuilder
from quant_platform.models.contracts.model_spec import ModelSpec
from quant_platform.models.registry.default_models import register_default_models
from quant_platform.models.registry.model_registry import ModelRegistry
from quant_platform.training.contracts.training import (
    FitRequest,
    FitResult,
    PredictionFrame,
    PredictionScope,
    PredictRequest,
    TrackingContext,
    TrainerConfig,
)
from quant_platform.training.runners import LocalTrainingRunner, PredictionRunner
from quant_platform.workflows.contracts.requests import (
    BacktestWorkflowRequest,
    BenchmarkWorkflowRequest,
    PredictWorkflowRequest,
    PredictionInputRef,
    ReviewWorkflowRequest,
    RollingWindowWorkflowSpec,
    TrainWorkflowRequest,
)
from quant_platform.workflows.runtime import WorkflowRuntime
from quant_platform.workflows.services import (
    BacktestWorkflowService,
    BenchmarkWorkflowService,
    EvaluationWorkflowService,
    PredictWorkflowService,
    PrepareWorkflowService,
    ReviewWorkflowService,
    TrainWorkflowService,
    WorkflowPipelineService,
)


class QuantPlatformFacade:
    """Minimal service facade wiring contracts together for smoke runs."""

    def __init__(self, artifact_root: Path, model_config: ModelConfig | None = None) -> None:
        self.artifact_root = artifact_root
        self.runtime = WorkflowRuntime.build(artifact_root, model_config=model_config)
        self.dataset_store = self.runtime.dataset_store
        self.model_registry = self.runtime.model_registry
        self.training_runner = self.runtime.training_runner
        self.prediction_runner = self.runtime.prediction_runner
        self.backtest_facade = self.runtime.backtest_facade
        self.backtest_engine = self.runtime.backtest_facade.research_engine
        self.simulation_backtest_engine = self.runtime.backtest_facade.simulation_engine
        self.store = self.runtime.store
        self.data_catalog = self.runtime.data_catalog
        self.feature_builder = self.runtime.feature_builder
        self.labeler = self.runtime.labeler
        self.tool_registry = self.runtime.tool_registry
        self.agent_service = self.runtime.agent_service
        self.prepare_workflow = PrepareWorkflowService(self.runtime)
        self.train_workflow = TrainWorkflowService(self.runtime)
        self.predict_workflow = PredictWorkflowService(self.runtime)
        self.benchmark_workflow = BenchmarkWorkflowService(self.runtime)
        self.backtest_workflow = BacktestWorkflowService(self.runtime)
        self.evaluation_workflow = EvaluationWorkflowService(self.runtime)
        self.review_workflow = ReviewWorkflowService(self.runtime)
        self.pipeline_workflow = WorkflowPipelineService(self.runtime)

    def build_smoke_market_bars(self) -> list[NormalizedMarketBar]:
        return self.prepare_workflow.build_smoke_market_bars()

    def build_smoke_dataset(self) -> DatasetRef:
        return self.prepare_workflow.prepare(
            self.prepare_workflow.build_smoke_request()
        ).dataset_ref

    def build_smoke_model_spec(self) -> ModelSpec:
        return self.prepare_workflow.build_smoke_model_spec()

    def train_smoke(self) -> FitResult:
        prepare_result = self.prepare_workflow.prepare(self.prepare_workflow.build_smoke_request())
        train_result = self.train_workflow.train(
            TrainWorkflowRequest(
                dataset_ref=prepare_result.dataset_ref,
                model_specs=[self.build_smoke_model_spec()],
                trainer_config=TrainerConfig(
                    runner="local",
                    epochs=1,
                    batch_size=32,
                    deterministic=True,
                ),
                tracking_context=TrackingContext(
                    backend="file",
                    experiment_name="quant-platform-smoke",
                    tracking_uri=str(self.artifact_root / "tracking"),
                ),
                seed=7,
                run_id_prefix="smoke-train-run",
            )
        )
        return train_result.items[0].fit_result

    def build_prediction_frame(self, fit_result: FitResult) -> PredictionFrame:
        dataset_ref = self.store.read_model(
            str(self.artifact_root / "datasets" / "smoke_dataset_dataset_ref.json"),
            DatasetRef,
        )
        predict_result = self.predict_workflow.predict(
            PredictWorkflowRequest(
                dataset_ref=dataset_ref,
                fit_results=[fit_result],
                prediction_scope=PredictionScope(
                    scope_name="full",
                    as_of_time=dataset_ref.feature_view_ref.as_of_time,
                ),
            )
        )
        return predict_result.items[0].prediction_frame

    def predict(self, request: PredictRequest) -> PredictionFrame:
        frame = self.prediction_runner.predict(request)
        model_meta_dir = Path(request.model_artifact_uri).parent
        output_name = f"{request.prediction_scope.scope_name}.json"
        self.store.write_model(f"predictions/{model_meta_dir.name}/{output_name}", frame)
        return frame

    def build_benchmark_market_bars(self, bar_count: int = 40) -> list[NormalizedMarketBar]:
        return self.prepare_workflow.build_benchmark_market_bars(bar_count=bar_count)

    def build_benchmark_dataset(self) -> DatasetRef:
        return self.prepare_workflow.prepare(
            self.prepare_workflow.build_synthetic_reference_request().model_copy(
                update={"dataset_id": "baseline_benchmark_dataset"}
            )
        ).dataset_ref

    def build_baseline_model_specs(self) -> list[ModelSpec]:
        return self.benchmark_workflow.build_baseline_model_specs()

    def run_baseline_benchmark(self) -> dict[str, object]:
        return self.benchmark_workflow.run_baseline_benchmark().model_dump(mode="json")

    def build_promotion_decision(
        self,
        benchmark_result,
        backtest_result,
    ) -> dict[str, object]:
        decisions, artifacts = self.evaluation_workflow.build_promotion_decision(
            benchmark_result,
            backtest_result,
        )
        return {
            "decisions": [decision.model_dump(mode="json") for decision in decisions],
            "artifacts": [artifact.model_dump(mode="json") for artifact in artifacts],
        }

    def build_real_benchmark_dataset(self) -> tuple[DatasetRef, list[DatasetSample], str]:
        request, data_source = self.prepare_workflow.build_real_benchmark_request()
        result = self.prepare_workflow.prepare(request)
        return result.dataset_ref, self.dataset_store[result.dataset_ref.dataset_id], data_source

    def load_real_market_bars(self) -> tuple[list[NormalizedMarketBar], str]:
        return self.prepare_workflow.load_real_market_bars()

    def _build_deep_backend_comparison(
        self,
        *,
        dataset_ref: DatasetRef,
        samples: list[DatasetSample],
        windows: list,
        baseline_specs: list[ModelSpec],
    ) -> list[dict[str, object]]:
        comparisons: list[dict[str, object]] = []
        for model_name in ("mlp", "gru"):
            base_spec = next(spec for spec in baseline_specs if spec.model_name == model_name)
            fallback_metrics = self._evaluate_benchmark_spec(
                dataset_ref=dataset_ref,
                samples=samples,
                windows=windows,
                model_spec=base_spec.model_copy(
                    update={"hyperparams": {**base_spec.hyperparams, "force_backend": "fallback"}}
                ),
            )
            torch_metrics = self._evaluate_benchmark_spec(
                dataset_ref=dataset_ref,
                samples=samples,
                windows=windows,
                model_spec=base_spec.model_copy(
                    update={"hyperparams": {**base_spec.hyperparams, "force_backend": "torch"}}
                ),
            )
            comparisons.append(
                {
                    "model_name": model_name,
                    "fallback_mean_test_mae": fallback_metrics["mean_test_mae"],
                    "torch_mean_test_mae": torch_metrics["mean_test_mae"],
                    "torch_backend": torch_metrics["backend"],
                    "delta_test_mae": torch_metrics["mean_test_mae"]
                    - fallback_metrics["mean_test_mae"],
                }
            )
        return comparisons

    def _build_reference_summary(
        self,
        *,
        dataset_ref: DatasetRef,
        samples: list[DatasetSample],
        windows: list,
        baseline_specs: list[ModelSpec],
    ) -> dict[str, object]:
        rows = [
            self._evaluate_benchmark_spec(
                dataset_ref=dataset_ref,
                samples=samples,
                windows=windows,
                model_spec=spec,
            )
            for spec in baseline_specs
        ]
        ranked = sorted(rows, key=lambda item: float(item["mean_test_mae"]))
        for index, row in enumerate(ranked, start=1):
            row["rank"] = index
        return {
            "dataset_id": dataset_ref.dataset_id,
            "window_count": len(windows),
            "leaderboard": ranked,
        }

    def _build_validation_summary(
        self,
        real_results: list[dict[str, object]],
        synthetic_reference: dict[str, object],
    ) -> dict[str, object]:
        real_top = real_results[0]["model_name"]
        synthetic_top = synthetic_reference["leaderboard"][0]["model_name"]
        return {
            "real_top_model": real_top,
            "synthetic_top_model": synthetic_top,
            "top_model_consistent": real_top == synthetic_top,
            "real_gru_rank": next(
                row["rank"] for row in real_results if row["model_name"] == "gru"
            ),
            "synthetic_gru_rank": next(
                row["rank"]
                for row in synthetic_reference["leaderboard"]
                if row["model_name"] == "gru"
            ),
        }

    def _evaluate_benchmark_spec(
        self,
        *,
        dataset_ref: DatasetRef,
        samples: list[DatasetSample],
        windows: list,
        model_spec: ModelSpec,
    ) -> dict[str, object]:
        runtime = self.model_registry.resolve_runtime(model_spec.model_name)
        valid_mae_values: list[float] = []
        test_mae_values: list[float] = []
        backend_name = "native"
        for window in windows:
            train_samples = samples[window.train_start : window.train_end]
            valid_context = samples[window.train_start : window.valid_end]
            test_context = samples[window.train_start : window.test_end]
            valid_samples = samples[window.valid_start : window.valid_end]
            test_samples = samples[window.test_start : window.test_end]
            plugin = runtime.model_cls(model_spec)
            fit_valid_input = runtime.input_adapter.build_predict_input(
                valid_context,
                dataset_ref,
                model_spec,
                runtime.registration,
            )
            plugin.fit(train_samples, fit_valid_input)
            backend_name = getattr(plugin, "_backend", "native")
            valid_input = runtime.input_adapter.build_predict_input(
                valid_context,
                dataset_ref,
                model_spec,
                runtime.registration,
            )
            valid_frame = runtime.prediction_adapter.build_prediction_frame(
                plugin.predict(valid_input),
                valid_input,
                model_run_id=f"benchmark-{model_spec.model_name}",
            )
            valid_tail = valid_frame.rows[-len(valid_samples) :]
            valid_mae_values.append(
                sum(
                    abs(row.prediction - sample.target)
                    for row, sample in zip(valid_tail, valid_samples, strict=False)
                )
                / max(1, len(valid_samples))
            )
            test_input = runtime.input_adapter.build_predict_input(
                test_context,
                dataset_ref,
                model_spec,
                runtime.registration,
            )
            test_frame = runtime.prediction_adapter.build_prediction_frame(
                plugin.predict(test_input),
                test_input,
                model_run_id=f"benchmark-{model_spec.model_name}",
            )
            test_tail = test_frame.rows[-len(test_samples) :]
            test_mae_values.append(
                sum(
                    abs(row.prediction - sample.target)
                    for row, sample in zip(test_tail, test_samples, strict=False)
                )
                / max(1, len(test_samples))
            )
        return {
            "model_name": model_spec.model_name,
            "backend": backend_name,
            "mean_valid_mae": sum(valid_mae_values) / max(1, len(valid_mae_values)),
            "mean_test_mae": sum(test_mae_values) / max(1, len(test_mae_values)),
        }

    def _write_benchmark_markdown(self, payload: dict[str, object]) -> None:
        lines = [
            "# Baseline Benchmark Summary",
            "",
            f"- Dataset: `{payload['dataset_id']}`",
            f"- Data source: `{payload['data_source']}`",
            f"- Windows: `{payload['window_count']}`",
            "",
            "| Rank | Model | Family | Backend | Valid MAE | Test MAE |",
            "| --- | --- | --- | --- | ---: | ---: |",
        ]
        for row in payload["results"]:
            lines.append(
                f"| {row['rank']} | {row['model_name']} | {row['family']} | {row['backend']} | "
                f"{row['mean_valid_mae']:.6f} | {row['mean_test_mae']:.6f} |"
            )
        lines.extend(
            [
                "",
                "## Deep Backend Comparison",
                "",
                "| Model | Torch Backend | Fallback Test MAE | Torch Test MAE | Delta |",
                "| --- | --- | ---: | ---: | ---: |",
            ]
        )
        for row in payload["deep_backend_comparison"]:
            lines.append(
                f"| {row['model_name']} | {row['torch_backend']} | "
                f"{row['fallback_mean_test_mae']:.6f} | "
                f"{row['torch_mean_test_mae']:.6f} | {row['delta_test_mae']:.6f} |"
            )
        lines.extend(
            [
                "",
                "## Synthetic Reference",
                "",
                "| Rank | Model | Valid MAE | Test MAE |",
                "| --- | --- | ---: | ---: |",
            ]
        )
        for row in payload["synthetic_reference"]["leaderboard"]:
            lines.append(
                f"| {row['rank']} | {row['model_name']} | "
                f"{row['mean_valid_mae']:.6f} | {row['mean_test_mae']:.6f} |"
            )
        best_row = payload["results"][0]
        default_recommendation = self._recommend_default_baseline(payload)
        lines.extend(
            [
                "",
                "## Conclusion",
                "",
                (
                    f"当前 benchmark 中，`{best_row['model_name']}` 排名第一，"
                    f"`mean_valid_mae={best_row['mean_valid_mae']:.6f}`，"
                    f"`mean_test_mae={best_row['mean_test_mae']:.6f}`，"
                    f"backend=`{best_row['backend']}`。"
                ),
                (
                    "这组结果应解读为对当前合成数字资产 regime 数据的相对比较，"
                    "并已额外用真实历史样本或其缓存做过交叉验证，不应直接外推到真实生产收益。"
                ),
                (
                    f"Synthetic 参考的第一名是 `{payload['validation_summary']['synthetic_top_model']}`，"
                    f"真实样本第一名是 `{payload['validation_summary']['real_top_model']}`，"
                    f"top1 一致性=`{payload['validation_summary']['top_model_consistent']}`。"
                ),
                "",
                "## Recommended Default Baseline",
                "",
                default_recommendation,
            ]
        )
        target = self.artifact_root / "benchmarks" / "baseline_family_walk_forward.md"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("\n".join(lines), encoding="utf-8")

    def _write_benchmark_csv(self, payload: dict[str, object]) -> None:
        target = self.artifact_root / "benchmarks" / "baseline_family_walk_forward.csv"
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "dataset_id",
                    "data_source",
                    "rank",
                    "model_name",
                    "family",
                    "backend",
                    "mean_valid_mae",
                    "mean_test_mae",
                    "artifact_uri",
                ],
            )
            writer.writeheader()
            for row in payload["results"]:
                writer.writerow(
                    {
                        "dataset_id": payload["dataset_id"],
                        "data_source": payload["data_source"],
                        "rank": row["rank"],
                        "model_name": row["model_name"],
                        "family": row["family"],
                        "backend": row["backend"],
                        "mean_valid_mae": row["mean_valid_mae"],
                        "mean_test_mae": row["mean_test_mae"],
                        "artifact_uri": row["artifact_uri"],
                    }
                )

    def _recommend_default_baseline(self, payload: dict[str, object]) -> str:
        best_row = payload["results"][0]
        if best_row["backend"] == "torch" and best_row["model_name"] in {"gru", "mlp"}:
            return (
                f"推荐默认基线使用 `{best_row['model_name']}`，因为它在当前 benchmark 上取得最优 test MAE，"
                "并且真实深度训练链路已经可用。与此同时，建议保留 `elastic_net` 作为稳健对照组。"
            )
        return (
            f"推荐默认基线使用 `{best_row['model_name']}` 作为主基线，"
            "同时保留 `gru` 作为深度时序对照，持续观察在更多 regime 和真实样本上的稳定性。"
        )

    def _write_market_bars_csv(
        self,
        target: Path,
        bars: list[NormalizedMarketBar],
    ) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "event_time",
                    "available_time",
                    "symbol",
                    "venue",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                ],
            )
            writer.writeheader()
            for bar in bars:
                writer.writerow(
                    {
                        "event_time": bar.event_time.isoformat(),
                        "available_time": bar.available_time.isoformat(),
                        "symbol": bar.symbol,
                        "venue": bar.venue,
                        "open": bar.open,
                        "high": bar.high,
                        "low": bar.low,
                        "close": bar.close,
                        "volume": bar.volume,
                    }
                )

    def _read_market_bars_csv(self, path: Path) -> list[NormalizedMarketBar]:
        bars: list[NormalizedMarketBar] = []
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                bars.append(
                    NormalizedMarketBar(
                        event_time=datetime.fromisoformat(str(row["event_time"])),
                        available_time=datetime.fromisoformat(str(row["available_time"])),
                        symbol=str(row["symbol"]),
                        venue=str(row["venue"]),
                        open=float(row["open"]),
                        high=float(row["high"]),
                        low=float(row["low"]),
                        close=float(row["close"]),
                        volume=float(row["volume"]),
                    )
                )
        return bars

    def data_smoke(self) -> dict[str, object]:
        request = self.prepare_workflow.build_smoke_request()
        result = self.prepare_workflow.prepare(request)
        return {
            "asset_id": result.data_asset_ref.asset_id,
            "row_count": len(request.market_bars),
            "storage_uri": result.data_asset_ref.storage_uri,
            "event_end": request.market_bars[-1].event_time.isoformat(),
            "dataset_id": result.dataset_ref.dataset_id,
            "dataset_manifest_uri": result.dataset_manifest_uri,
            "quality_report_uri": result.quality_report_uri,
        }

    def backtest_smoke(self) -> BacktestResult:
        fit_result = self.train_smoke()
        dataset_ref = self.store.read_model(
            str(self.artifact_root / "datasets" / "smoke_dataset_dataset_ref.json"),
            DatasetRef,
        )
        predict_result = self.predict_workflow.predict(
            PredictWorkflowRequest(
                dataset_ref=dataset_ref,
                fit_results=[fit_result],
                prediction_scope=PredictionScope(
                    scope_name="full",
                    as_of_time=dataset_ref.feature_view_ref.as_of_time,
                ),
            )
        )
        backtest_result = self.backtest_workflow.backtest(
            BacktestWorkflowRequest(
                prediction_inputs=[
                    PredictionInputRef(
                        model_name=fit_result.model_name,
                        run_id=fit_result.run_id,
                        prediction_frame_uri=predict_result.items[0].prediction_frame_uri,
                    )
                ],
                backtest_request_template=BacktestRequest(
                    prediction_frame_uri=predict_result.items[0].prediction_frame_uri,
                    strategy_config=StrategyConfig(name="sign_strategy"),
                    portfolio_config=PortfolioConfig(
                        initial_cash=100000.0,
                        max_gross_leverage=1.0,
                        max_position_weight=1.0,
                    ),
                    cost_model=CostModel(fee_bps=5.0, slippage_bps=2.0),
                    benchmark_spec=BenchmarkSpec(name="buy_and_hold", symbol="BTCUSDT"),
                    calendar_spec=CalendarSpec(timezone="UTC", frequency="1h"),
                ),
            )
        )
        return backtest_result.items[0].backtest_result

    def agent_smoke(self) -> object:
        backtest_result = self.backtest_smoke()
        fit_result = self.train_smoke()
        review_result = self.review_workflow.review(
            ReviewWorkflowRequest(
                request_id="agent-smoke-task",
                goal="Summarize smoke-train experiment and compare smoke backtest outputs",
                task_type=AgentTaskType.SUMMARIZE_EXPERIMENT.value,
                input_artifacts=[
                    ArtifactRef(kind="backtest_report", uri=backtest_result.report_uri)
                ],
                experiment_refs=[
                    ArtifactRef(kind="train_manifest", uri=fit_result.train_manifest_uri)
                ],
                comparison_mode="report_summary",
                allowed_tools=[
                    "research.read_experiment_summary",
                    "research.compare_backtest_reports",
                ],
                guardrail_policy=GuardrailPolicy(),
            )
        )
        return review_result.response
