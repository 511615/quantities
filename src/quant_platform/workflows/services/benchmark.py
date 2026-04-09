from __future__ import annotations

import csv
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

from quant_platform.backtest.contracts.report import BenchmarkSummaryArtifact
from quant_platform.backtest.metrics.comparison import build_benchmark_summary_artifact
from quant_platform.common.enums.core import ModelFamily
from quant_platform.common.hashing.digest import stable_digest
from quant_platform.common.types.core import ArtifactRef, SchemaField
from quant_platform.datasets.contracts.dataset import DatasetRef, DatasetSample
from quant_platform.datasets.splits.time_series import RollingWindowSpec, TimeSeriesSplitPlanner
from quant_platform.models.contracts.model_spec import ModelSpec
from quant_platform.models.support import merge_training_hyperparams
from quant_platform.training.contracts.training import (
    PredictionScope,
    TrackingContext,
    TrainerConfig,
)
from quant_platform.workflows.contracts.requests import BenchmarkWorkflowRequest
from quant_platform.workflows.contracts.results import (
    BenchmarkResultRow,
    BenchmarkWorkflowResult,
)
from quant_platform.workflows.contracts.state import (
    WorkflowStageName,
    WorkflowStageResult,
    WorkflowStageStatus,
)
from quant_platform.workflows.runtime import WorkflowRuntime


class BenchmarkWorkflowService:
    def __init__(self, runtime: WorkflowRuntime) -> None:
        self.runtime = runtime

    def benchmark(self, request: BenchmarkWorkflowRequest) -> BenchmarkWorkflowResult:
        started_at = datetime.now(UTC)
        if request.dataset_ref is None:
            raise ValueError("benchmark workflow requires dataset_ref")
        samples = self._samples_for_dataset(request.dataset_ref)
        windows = self._resolve_windows(len(samples), request)
        rows = [
            self._evaluate_benchmark_spec(
                dataset_ref=request.dataset_ref,
                samples=samples,
                windows=windows,
                model_spec=spec,
                trainer_config=request.trainer_config,
                seed=request.seed,
            )
            for spec in request.model_specs
        ]
        ranked_rows = [
            row.model_copy(update={"rank": index + 1})
            for index, row in enumerate(sorted(rows, key=lambda item: item.mean_test_mae))
        ]
        benchmark_summary = build_benchmark_summary_artifact(
            benchmark_name=request.benchmark_name,
            dataset_id=request.dataset_ref.dataset_id,
            data_source=request.data_source,
            rows=[row.model_dump(mode="json") for row in ranked_rows],
            validation_summary={},
        )
        summary_artifacts = self._write_standard_summary_bundle(
            benchmark_name=request.benchmark_name,
            dataset_ref=request.dataset_ref,
            data_source=request.data_source or "unknown",
            rows=ranked_rows,
            benchmark_summary=benchmark_summary,
        )
        stage_result = WorkflowStageResult(
            stage=WorkflowStageName.BENCHMARK,
            status=WorkflowStageStatus.SUCCESS,
            request_digest=stable_digest(request),
            started_at=started_at,
            finished_at=datetime.now(UTC),
            artifacts=summary_artifacts,
            summary=f"benchmarked {len(ranked_rows)} model(s) across {len(windows)} windows",
        )
        return BenchmarkWorkflowResult(
            stage_result=stage_result,
            dataset_ref=request.dataset_ref,
            benchmark_name=request.benchmark_name,
            data_source=request.data_source,
            results=ranked_rows,
            leaderboard=ranked_rows,
            benchmark_summary=benchmark_summary,
            summary_artifacts=summary_artifacts,
        )

    def build_baseline_model_specs(self) -> list[ModelSpec]:
        schema = [
            SchemaField(name=feature_name, dtype="float")
            for feature_name in self.runtime.feature_builder.FEATURE_NAMES
        ]
        output_schema = [SchemaField(name="prediction", dtype="float")]
        return [
            ModelSpec(
                model_name="mean_baseline",
                family=ModelFamily.BASELINE,
                version="0.1.0",
                input_schema=schema,
                output_schema=output_schema,
                hyperparams={},
            ),
            ModelSpec(
                model_name="elastic_net",
                family=ModelFamily.LINEAR,
                version="0.1.0",
                input_schema=schema,
                output_schema=output_schema,
                hyperparams={"alpha": 0.001, "l1_ratio": 0.5},
            ),
            ModelSpec(
                model_name="lightgbm",
                family=ModelFamily.TREE,
                version="0.1.0",
                input_schema=schema,
                output_schema=output_schema,
                hyperparams={"n_estimators": 32},
            ),
            ModelSpec(
                model_name="mlp",
                family=ModelFamily.DEEP,
                version="0.1.0",
                input_schema=schema,
                output_schema=output_schema,
                hyperparams={
                    "hidden_layers": [16, 8],
                    "epochs": 120,
                    "learning_rate": 0.001,
                    "weight_decay": 0.01,
                    "dropout": 0.0,
                    "patience": 15,
                    "min_delta": 1e-5,
                },
            ),
            ModelSpec(
                model_name="gru",
                family=ModelFamily.SEQUENCE,
                version="0.1.0",
                input_schema=schema,
                output_schema=output_schema,
                hyperparams={
                    "lookback": 6,
                    "forecast_horizon": 1,
                    "hidden_size": 16,
                    "dropout": 0.1,
                    "epochs": 90,
                    "patience": 8,
                    "learning_rate": 0.01,
                    "num_layers": 1,
                    "bidirectional": False,
                    "layer_norm": True,
                },
            ),
        ]

    def run_baseline_benchmark(self) -> BenchmarkWorkflowResult:
        from quant_platform.workflows.services.prepare import PrepareWorkflowService

        prepare_service = PrepareWorkflowService(self.runtime)
        real_prepare_request, data_source = prepare_service.build_real_benchmark_request()
        real_prepare_result = prepare_service.prepare(real_prepare_request)
        synthetic_prepare_result = prepare_service.prepare(
            prepare_service.build_synthetic_reference_request()
        )
        benchmark_request = BenchmarkWorkflowRequest(
            dataset_ref=real_prepare_result.dataset_ref,
            model_specs=self.build_baseline_model_specs(),
            trainer_config=TrainerConfig(
                runner="local",
                epochs=1,
                batch_size=32,
                deterministic=True,
            ),
            tracking_context=TrackingContext(backend="file", experiment_name="baseline-benchmark"),
            seed=7,
            prediction_scope=PredictionScope(
                scope_name="test",
                as_of_time=real_prepare_result.dataset_ref.feature_view_ref.as_of_time,
            ),
            rolling_window_spec=self._default_window_spec(
                len(self._samples_for_dataset(real_prepare_result.dataset_ref))
            ),
            benchmark_name="baseline_family_walk_forward",
            data_source=data_source,
        )
        real_result = self.benchmark(benchmark_request)
        synthetic_request = benchmark_request.model_copy(
            update={
                "dataset_ref": synthetic_prepare_result.dataset_ref,
                "benchmark_name": "baseline_family_reference",
                "data_source": "synthetic_reference",
                "rolling_window_spec": self._default_window_spec(
                    len(self._samples_for_dataset(synthetic_prepare_result.dataset_ref))
                ),
            }
        )
        synthetic_result = self.benchmark(synthetic_request)
        deep_backend_comparison = self._build_deep_backend_comparison(real_result, synthetic_result)
        validation_summary = self._build_validation_summary(real_result, synthetic_result)
        benchmark_summary = build_benchmark_summary_artifact(
            benchmark_name=benchmark_request.benchmark_name,
            dataset_id=real_result.dataset_ref.dataset_id,
            data_source=data_source,
            rows=[row.model_dump(mode="json") for row in real_result.results],
            validation_summary=validation_summary,
        )
        summary_artifacts = self._write_baseline_summary_bundle(
            real_result=real_result,
            synthetic_result=synthetic_result,
            data_source=data_source,
            deep_backend_comparison=deep_backend_comparison,
            validation_summary=validation_summary,
            benchmark_summary=benchmark_summary,
        )
        stage_result = real_result.stage_result.model_copy(
            update={
                "artifacts": summary_artifacts,
                "summary": (
                    "baseline benchmark completed with real and synthetic reference comparisons"
                ),
            }
        )
        return real_result.model_copy(
            update={
                "stage_result": stage_result,
                "benchmark_type": "real_with_synthetic_reference",
                "deep_backend_comparison": deep_backend_comparison,
                "validation_summary": validation_summary,
                "benchmark_summary": benchmark_summary,
                "summary_artifacts": summary_artifacts,
            }
        )

    def _samples_for_dataset(self, dataset_ref: DatasetRef) -> list[DatasetSample]:
        if dataset_ref.dataset_id not in self.runtime.dataset_store:
            raise KeyError(
                f"dataset '{dataset_ref.dataset_id}' is not available in workflow runtime"
            )
        return self.runtime.dataset_store[dataset_ref.dataset_id]

    def _resolve_windows(self, sample_count: int, request: BenchmarkWorkflowRequest):
        spec = request.rolling_window_spec or self._default_window_spec(sample_count)
        return TimeSeriesSplitPlanner.rolling_windows(
            sample_count,
            RollingWindowSpec(
                train_size=spec.train_size,
                valid_size=spec.valid_size,
                test_size=spec.test_size,
                step_size=spec.step_size,
                embargo=spec.embargo,
                purge_gap=spec.purge_gap,
                expanding_train=spec.expanding_train,
            ),
        )

    def _default_window_spec(self, sample_count: int):
        from quant_platform.workflows.contracts.requests import RollingWindowWorkflowSpec

        train_size = max(8, sample_count // 2)
        valid_size = max(4, sample_count // 6)
        test_size = max(2, sample_count // 10)
        step_size = max(2, test_size)
        if train_size + valid_size + test_size > sample_count:
            train_size = max(4, sample_count // 2)
            valid_size = max(2, sample_count // 4)
            test_size = max(2, sample_count - train_size - valid_size)
            step_size = max(1, test_size)
        return RollingWindowWorkflowSpec(
            train_size=train_size,
            valid_size=valid_size,
            test_size=test_size,
            step_size=step_size,
        )

    def _evaluate_benchmark_spec(
        self,
        *,
        dataset_ref: DatasetRef,
        samples: list[DatasetSample],
        windows: list,
        model_spec: ModelSpec,
        trainer_config: TrainerConfig,
        seed: int,
    ) -> BenchmarkResultRow:
        runtime = self.runtime.model_registry.resolve_runtime(model_spec.model_name)
        effective_spec = model_spec.model_copy(
            update={
                "hyperparams": {
                    **runtime.registration.default_hyperparams,
                    **model_spec.hyperparams,
                }
            }
        )
        effective_spec = merge_training_hyperparams(
            effective_spec,
            trainer_config,
            seed=seed,
        )
        valid_mae_values: list[float] = []
        test_mae_values: list[float] = []
        fit_metrics_history: list[dict[str, float]] = []
        confidence_sources: set[str] = set()
        backend_name = "native"
        for window in windows:
            train_samples = samples[window.train_start : window.train_end]
            valid_context = samples[window.train_start : window.valid_end]
            test_context = samples[window.train_start : window.test_end]
            valid_samples = samples[window.valid_start : window.valid_end]
            test_samples = samples[window.test_start : window.test_end]
            plugin = runtime.model_cls(effective_spec)
            train_input = runtime.input_adapter.build_train_input(
                train_samples,
                dataset_ref,
                effective_spec,
                runtime.registration,
            )
            self.runtime.model_registry.capability_validator.validate(
                runtime.registration,
                dataset_ref,
                train_input,
            )
            fit_valid_input = runtime.input_adapter.build_predict_input(
                valid_context,
                dataset_ref,
                effective_spec,
                runtime.registration,
            )
            fit_valid_input = replace(
                fit_valid_input,
                metadata={
                    **fit_valid_input.metadata,
                    "target_start_index": len(valid_context) - len(valid_samples),
                },
            )
            self.runtime.model_registry.capability_validator.validate(
                runtime.registration,
                dataset_ref,
                fit_valid_input,
            )
            fit_metrics = plugin.fit(train_input, fit_valid_input)
            fit_metrics_history.append(fit_metrics)
            backend_name = getattr(plugin, "_backend", "native")
            valid_input = runtime.input_adapter.build_predict_input(
                valid_context,
                dataset_ref,
                effective_spec,
                runtime.registration,
            )
            valid_input = replace(
                valid_input,
                metadata={
                    **valid_input.metadata,
                    "target_start_index": len(valid_context) - len(valid_samples),
                },
            )
            self.runtime.model_registry.capability_validator.validate(
                runtime.registration,
                dataset_ref,
                valid_input,
            )
            valid_outputs = plugin.predict(valid_input)
            confidence_sources.add(
                str(getattr(valid_outputs, "metadata", {}).get("confidence_source", "unknown"))
            )
            valid_frame = runtime.prediction_adapter.build_prediction_frame(
                valid_outputs,
                valid_input,
                model_run_id=f"benchmark-{effective_spec.model_name}",
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
                effective_spec,
                runtime.registration,
            )
            self.runtime.model_registry.capability_validator.validate(
                runtime.registration,
                dataset_ref,
                test_input,
            )
            test_outputs = plugin.predict(test_input)
            confidence_sources.add(
                str(getattr(test_outputs, "metadata", {}).get("confidence_source", "unknown"))
            )
            test_frame = runtime.prediction_adapter.build_prediction_frame(
                test_outputs,
                test_input,
                model_run_id=f"benchmark-{effective_spec.model_name}",
            )
            test_tail = test_frame.rows[-len(test_samples) :]
            test_mae_values.append(
                sum(
                    abs(row.prediction - sample.target)
                    for row, sample in zip(test_tail, test_samples, strict=False)
                )
                / max(1, len(test_samples))
            )
        detail_artifact = self.runtime.store.write_json(
            f"benchmarks/{model_spec.model_name}.json",
            {
                "model_name": model_spec.model_name,
                "family": model_spec.family.value,
                "advanced_kind": runtime.registration.advanced_kind.value,
                "backend": backend_name,
                "window_count": len(windows),
                "mean_valid_mae": sum(valid_mae_values) / max(1, len(valid_mae_values)),
                "mean_test_mae": sum(test_mae_values) / max(1, len(test_mae_values)),
                "fit_metrics_history": fit_metrics_history,
                "confidence_sources": sorted(confidence_sources),
                "best_epoch": min(
                    (
                        int(metrics.get("best_epoch", 0))
                        for metrics in fit_metrics_history
                        if metrics.get("best_epoch") is not None
                    ),
                    default=0,
                ),
                "trained_steps": sum(
                    int(metrics.get("trained_steps", 0)) for metrics in fit_metrics_history
                ),
            },
        )
        return BenchmarkResultRow(
            model_name=model_spec.model_name,
            family=model_spec.family.value,
            advanced_kind=runtime.registration.advanced_kind.value,
            backend=backend_name,
            window_count=len(windows),
            mean_valid_mae=sum(valid_mae_values) / max(1, len(valid_mae_values)),
            mean_test_mae=sum(test_mae_values) / max(1, len(test_mae_values)),
            artifact_uri=detail_artifact.uri,
        )

    def _build_deep_backend_comparison(
        self,
        real_result: BenchmarkWorkflowResult,
        synthetic_result: BenchmarkWorkflowResult,
    ) -> list[dict[str, object]]:
        synthetic_by_name = {row.model_name: row for row in synthetic_result.results}
        comparisons: list[dict[str, object]] = []
        for row in real_result.results:
            if row.model_name not in {"mlp", "gru"}:
                continue
            reference = synthetic_by_name[row.model_name]
            comparisons.append(
                {
                    "model_name": row.model_name,
                    "torch_backend": row.backend,
                    "fallback_mean_test_mae": reference.mean_test_mae,
                    "torch_mean_test_mae": row.mean_test_mae,
                    "delta_test_mae": row.mean_test_mae - reference.mean_test_mae,
                }
            )
        return comparisons

    def _build_validation_summary(
        self,
        real_result: BenchmarkWorkflowResult,
        synthetic_result: BenchmarkWorkflowResult,
    ) -> dict[str, object]:
        return {
            "real_top_model": real_result.leaderboard[0].model_name,
            "synthetic_top_model": synthetic_result.leaderboard[0].model_name,
            "top_model_consistent": real_result.leaderboard[0].model_name
            == synthetic_result.leaderboard[0].model_name,
        }

    def _write_standard_summary_bundle(
        self,
        *,
        benchmark_name: str,
        dataset_ref: DatasetRef,
        data_source: str,
        rows: list[BenchmarkResultRow],
        benchmark_summary: BenchmarkSummaryArtifact,
    ) -> list[ArtifactRef]:
        payload = benchmark_summary.model_dump(mode="json")
        payload.update(
            {
                "results": [row.model_dump(mode="json") for row in rows],
                "leaderboard": [row.model_dump(mode="json") for row in rows],
            }
        )
        json_artifact = self.runtime.store.write_json(f"benchmarks/{benchmark_name}.json", payload)
        markdown_path = self.runtime.artifact_root / "benchmarks" / f"{benchmark_name}.md"
        csv_path = self.runtime.artifact_root / "benchmarks" / f"{benchmark_name}.csv"
        self._write_simple_markdown(
            target=markdown_path,
            title=benchmark_name,
            dataset_id=dataset_ref.dataset_id,
            data_source=data_source,
            rows=rows,
        )
        self._write_csv(
            target=csv_path,
            dataset_id=dataset_ref.dataset_id,
            data_source=data_source,
            rows=rows,
        )
        return self._summary_artifact_refs(json_artifact.uri, markdown_path, csv_path)

    def _write_baseline_summary_bundle(
        self,
        *,
        real_result: BenchmarkWorkflowResult,
        synthetic_result: BenchmarkWorkflowResult,
        data_source: str,
        deep_backend_comparison: list[dict[str, object]],
        validation_summary: dict[str, object],
        benchmark_summary: BenchmarkSummaryArtifact,
    ) -> list[ArtifactRef]:
        payload = benchmark_summary.model_dump(mode="json")
        payload.update(
            {
                "benchmark_type": "real_with_synthetic_reference",
                "results": [row.model_dump(mode="json") for row in real_result.results],
                "leaderboard": [row.model_dump(mode="json") for row in real_result.leaderboard],
                "deep_backend_comparison": deep_backend_comparison,
                "validation_summary": validation_summary,
                "synthetic_reference": {
                    "leaderboard": [
                        row.model_dump(mode="json") for row in synthetic_result.leaderboard
                    ]
                },
            }
        )
        json_artifact = self.runtime.store.write_json(
            "benchmarks/baseline_family_walk_forward.json",
            payload,
        )
        markdown_path = (
            self.runtime.artifact_root / "benchmarks" / "baseline_family_walk_forward.md"
        )
        csv_path = self.runtime.artifact_root / "benchmarks" / "baseline_family_walk_forward.csv"
        self._write_baseline_markdown(
            target=markdown_path,
            dataset_id=real_result.dataset_ref.dataset_id,
            data_source=data_source,
            rows=real_result.results,
            deep_backend_comparison=deep_backend_comparison,
            synthetic_rows=synthetic_result.leaderboard,
            validation_summary=validation_summary,
        )
        self._write_csv(
            target=csv_path,
            dataset_id=real_result.dataset_ref.dataset_id,
            data_source=data_source,
            rows=real_result.results,
        )
        return self._summary_artifact_refs(json_artifact.uri, markdown_path, csv_path)

    def _write_simple_markdown(
        self,
        *,
        target: Path,
        title: str,
        dataset_id: str,
        data_source: str,
        rows: list[BenchmarkResultRow],
    ) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        lines = [
            f"# {title}",
            "",
            f"- Dataset: `{dataset_id}`",
            f"- Data source: `{data_source}`",
            "",
            "| Rank | Model | Family | Backend | Valid MAE | Test MAE |",
            "| --- | --- | --- | --- | ---: | ---: |",
        ]
        for row in rows:
            lines.append(
                f"| {row.rank} | {row.model_name} | {row.family} | {row.backend} | "
                f"{row.mean_valid_mae:.6f} | {row.mean_test_mae:.6f} |"
            )
        target.write_text("\n".join(lines), encoding="utf-8")

    def _write_baseline_markdown(
        self,
        *,
        target: Path,
        dataset_id: str,
        data_source: str,
        rows: list[BenchmarkResultRow],
        deep_backend_comparison: list[dict[str, object]],
        synthetic_rows: list[BenchmarkResultRow],
        validation_summary: dict[str, object],
    ) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        lines = [
            "# Baseline Benchmark Summary",
            "",
            f"- Dataset: `{dataset_id}`",
            f"- Data source: `{data_source}`",
            "",
            "| Rank | Model | Family | Backend | Valid MAE | Test MAE |",
            "| --- | --- | --- | --- | ---: | ---: |",
        ]
        for row in rows:
            lines.append(
                f"| {row.rank} | {row.model_name} | {row.family} | {row.backend} | "
                f"{row.mean_valid_mae:.6f} | {row.mean_test_mae:.6f} |"
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
        for row in deep_backend_comparison:
            lines.append(
                f"| {row['model_name']} | {row['torch_backend']} | "
                f"{float(row['fallback_mean_test_mae']):.6f} | "
                f"{float(row['torch_mean_test_mae']):.6f} | "
                f"{float(row['delta_test_mae']):.6f} |"
            )
        lines.extend(
            [
                "",
                "## Synthetic Reference",
                "",
                "| Rank | Model | Family | Backend | Valid MAE | Test MAE |",
                "| --- | --- | --- | --- | ---: | ---: |",
            ]
        )
        for row in synthetic_rows:
            lines.append(
                f"| {row.rank} | {row.model_name} | {row.family} | {row.backend} | "
                f"{row.mean_valid_mae:.6f} | {row.mean_test_mae:.6f} |"
            )
        best_row = rows[0]
        lines.extend(
            [
                "",
                "## Conclusion",
                "",
                (
                    f"`{best_row.model_name}` is currently ranked first in the real-sample "
                    f"walk-forward benchmark with `mean_valid_mae={best_row.mean_valid_mae:.6f}`, "
                    f"`mean_test_mae={best_row.mean_test_mae:.6f}`, and "
                    f"backend=`{best_row.backend}`."
                ),
                (
                    f"Real-sample top1=`{validation_summary['real_top_model']}`, "
                    f"synthetic-reference top1=`{validation_summary['synthetic_top_model']}`, "
                    f"top1 consistency=`{validation_summary['top_model_consistent']}`."
                ),
                "",
                "## Recommended Default Baseline",
                "",
                self._recommend_default_baseline(best_row),
            ]
        )
        target.write_text("\n".join(lines), encoding="utf-8")

    def _write_csv(
        self,
        *,
        target: Path,
        dataset_id: str,
        data_source: str,
        rows: list[BenchmarkResultRow],
    ) -> None:
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
                    "window_count",
                    "mean_valid_mae",
                    "mean_test_mae",
                    "artifact_uri",
                ],
            )
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {
                        "dataset_id": dataset_id,
                        "data_source": data_source,
                        "rank": row.rank,
                        "model_name": row.model_name,
                        "family": row.family,
                        "backend": row.backend,
                        "window_count": row.window_count,
                        "mean_valid_mae": row.mean_valid_mae,
                        "mean_test_mae": row.mean_test_mae,
                        "artifact_uri": row.artifact_uri,
                    }
                )

    def _summary_artifact_refs(
        self,
        json_uri: str,
        markdown_path: Path,
        csv_path: Path,
    ) -> list[ArtifactRef]:
        return [
            ArtifactRef(kind="benchmark_summary_json", uri=json_uri),
            ArtifactRef(kind="benchmark_summary_markdown", uri=str(markdown_path)),
            ArtifactRef(kind="benchmark_summary_csv", uri=str(csv_path)),
            ArtifactRef(kind="benchmark_json", uri=json_uri),
            ArtifactRef(kind="benchmark_markdown", uri=str(markdown_path)),
            ArtifactRef(kind="benchmark_csv", uri=str(csv_path)),
        ]

    def _recommend_default_baseline(self, best_row: BenchmarkResultRow) -> str:
        if best_row.backend == "torch" and best_row.model_name in {"gru", "mlp"}:
            return (
                f"Recommend `{best_row.model_name}` as the default baseline while keeping "
                "`elastic_net` as the stable linear control."
            )
        return (
            f"Recommend `{best_row.model_name}` as the default baseline while keeping `gru` "
            "as the sequence-model control."
        )
