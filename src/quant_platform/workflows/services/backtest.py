from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from pathlib import Path

from quant_platform.backtest.contracts.backtest import BacktestRequest
from quant_platform.backtest.metrics.comparison import (
    build_backtest_summary_artifact,
    build_backtest_summary_row,
    leaderboard_rows,
    protocol_scenarios,
)
from quant_platform.backtest.scenarios.presets import build_standard_scenarios
from quant_platform.common.hashing.digest import stable_digest
from quant_platform.common.types.core import ArtifactRef
from quant_platform.training.contracts.training import PredictionFrame
from quant_platform.workflows.contracts.requests import BacktestWorkflowRequest
from quant_platform.workflows.contracts.results import (
    BacktestWorkflowItem,
    BacktestWorkflowResult,
)
from quant_platform.workflows.contracts.state import (
    WorkflowStageName,
    WorkflowStageResult,
    WorkflowStageStatus,
)
from quant_platform.workflows.runtime import WorkflowRuntime


class BacktestWorkflowService:
    def __init__(self, runtime: WorkflowRuntime) -> None:
        self.runtime = runtime

    def backtest(self, request: BacktestWorkflowRequest) -> BacktestWorkflowResult:
        started_at = datetime.now(UTC)
        if not request.prediction_inputs:
            raise ValueError("backtest workflow requires at least one prediction input")
        items: list[BacktestWorkflowItem] = []
        artifacts: list[ArtifactRef] = []
        summary_rows = []
        comparison_warnings: list[str] = []
        scope_names: set[str] = set()
        request_digest = self._summary_request_digest(request.backtest_request_template)
        for index, prediction_input in enumerate(request.prediction_inputs):
            prediction_frame, prediction_uri = self._resolve_prediction_input(
                prediction_input, index
            )
            scope_names.add(self._prediction_scope_name(prediction_uri))
            effective_request = self._effective_backtest_request(
                request.backtest_request_template,
                prediction_uri,
            )
            research_result = self.runtime.backtest_facade.run_research(
                request=effective_request.model_copy(update={"engine_type": "research"}),
                prediction_frame=prediction_frame,
                market_bars=request.market_bars or None,
            )
            simulation_result = self.runtime.backtest_facade.run_simulation(
                request=effective_request.model_copy(update={"engine_type": "simulation"}),
                prediction_frame=prediction_frame,
                market_bars=request.market_bars or None,
            )
            research_artifact = self.runtime.store.write_model(
                f"workflows/backtest/{research_result.backtest_id}_research.json",
                research_result,
            )
            simulation_artifact = self.runtime.store.write_model(
                f"workflows/backtest/{simulation_result.backtest_id}_simulation.json",
                simulation_result,
            )
            summary_row, row_warnings = build_backtest_summary_row(
                store=self.runtime.store,
                model_name=prediction_input.model_name or f"prediction_{index:02d}",
                run_id=prediction_input.run_id or f"prediction_{index:02d}",
                prediction_frame_uri=prediction_uri,
                research_result_uri=research_artifact.uri,
                research_result=research_result,
                simulation_result_uri=simulation_artifact.uri,
                simulation_result=simulation_result,
            )
            summary_rows.append(summary_row)
            comparison_warnings.extend(row_warnings)
            items.append(
                BacktestWorkflowItem(
                    model_name=summary_row.model_name,
                    run_id=summary_row.run_id,
                    prediction_frame_uri=prediction_uri,
                    backtest_result_uri=research_artifact.uri,
                    backtest_result=research_result,
                    research_backtest_result_uri=research_artifact.uri,
                    research_backtest_result=research_result,
                    simulation_backtest_result_uri=simulation_artifact.uri,
                    simulation_backtest_result=simulation_result,
                    summary_row=summary_row,
                )
            )
            artifacts.extend(
                [
                    ArtifactRef(kind="backtest_result", uri=research_artifact.uri),
                    ArtifactRef(kind="backtest_report", uri=research_result.report_uri),
                    ArtifactRef(kind="research_backtest_result", uri=research_artifact.uri),
                    ArtifactRef(kind="research_backtest_report", uri=research_result.report_uri),
                    ArtifactRef(kind="simulation_backtest_result", uri=simulation_artifact.uri),
                    ArtifactRef(
                        kind="simulation_backtest_report", uri=simulation_result.report_uri
                    ),
                ]
            )
        ordered_rows = leaderboard_rows(summary_rows)
        backtest_summary = build_backtest_summary_artifact(
            dataset_id=request.dataset_ref.dataset_id
            if request.dataset_ref is not None
            else "unknown_dataset",
            prediction_scope=self._resolve_prediction_scope(scope_names),
            data_source=request.data_source,
            benchmark_name=request.benchmark_name,
            request_digest=request_digest,
            rows=ordered_rows,
            comparison_warnings=comparison_warnings,
        )
        summary_artifacts = self._write_backtest_summary_artifacts(backtest_summary)
        artifacts.extend(summary_artifacts)
        stage_result = WorkflowStageResult(
            stage=WorkflowStageName.BACKTEST,
            status=WorkflowStageStatus.SUCCESS,
            request_digest=stable_digest(request),
            started_at=started_at,
            finished_at=datetime.now(UTC),
            artifacts=artifacts,
            summary=f"executed {len(items)} backtest run(s) across research and simulation",
        )
        return BacktestWorkflowResult(
            stage_result=stage_result,
            items=items,
            leaderboard=ordered_rows,
            backtest_summary=backtest_summary,
            summary_artifacts=summary_artifacts,
        )

    def _resolve_prediction_input(
        self, prediction_input, index: int
    ) -> tuple[PredictionFrame, str]:
        if prediction_input.prediction_frame_uri is not None:
            return (
                self.runtime.store.read_model(
                    prediction_input.prediction_frame_uri, PredictionFrame
                ),
                prediction_input.prediction_frame_uri,
            )
        if prediction_input.prediction_frame is None:
            raise ValueError("prediction input has no usable frame")
        artifact = self.runtime.store.write_model(
            f"workflows/backtest/input_prediction_{index:02d}.json",
            prediction_input.prediction_frame,
        )
        return prediction_input.prediction_frame, artifact.uri

    def _effective_backtest_request(
        self,
        template: BacktestRequest,
        prediction_uri: str,
    ) -> BacktestRequest:
        return template.model_copy(
            update={
                "input_ref": prediction_uri,
                "prediction_frame_uri": prediction_uri,
                "input_type": "prediction_frame",
                "scenario_specs": self._protocol_scenarios(template),
            }
        )

    def _protocol_scenarios(self, template: BacktestRequest):
        if template.scenario_specs:
            return template.scenario_specs
        standard_scenarios = {scenario.name: scenario for scenario in build_standard_scenarios()}
        return [
            standard_scenarios[name] for name in protocol_scenarios() if name in standard_scenarios
        ]

    def _summary_request_digest(self, template: BacktestRequest) -> str:
        digest_template = template.model_copy(
            update={
                "input_ref": None,
                "prediction_frame_uri": None,
                "scenario_specs": self._protocol_scenarios(template),
            }
        )
        return stable_digest(digest_template)

    def _prediction_scope_name(self, prediction_uri: str) -> str:
        return Path(prediction_uri).stem or "unknown_scope"

    def _resolve_prediction_scope(self, scope_names: set[str]) -> str:
        if not scope_names:
            return "unknown_scope"
        if len(scope_names) == 1:
            return next(iter(scope_names))
        return "mixed"

    def _write_backtest_summary_artifacts(self, backtest_summary) -> list[ArtifactRef]:
        json_artifact = self.runtime.store.write_json(
            "workflows/backtest/backtest_summary.json",
            backtest_summary.model_dump(mode="json"),
        )
        markdown_path = (
            self.runtime.artifact_root / "workflows" / "backtest" / "backtest_summary.md"
        )
        csv_path = self.runtime.artifact_root / "workflows" / "backtest" / "backtest_summary.csv"
        self._write_backtest_summary_markdown(markdown_path, backtest_summary)
        self._write_backtest_summary_csv(csv_path, backtest_summary)
        return [
            ArtifactRef(kind="backtest_summary_json", uri=json_artifact.uri),
            ArtifactRef(kind="backtest_summary_markdown", uri=str(markdown_path)),
            ArtifactRef(kind="backtest_summary_csv", uri=str(csv_path)),
        ]

    def _write_backtest_summary_markdown(self, target: Path, backtest_summary) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        lines = [
            "# Backtest Summary",
            "",
            f"- Dataset: `{backtest_summary.dataset_id}`",
            f"- Prediction scope: `{backtest_summary.prediction_scope}`",
            f"- Benchmark: `{backtest_summary.benchmark_name or 'n/a'}`",
            f"- Request digest: `{backtest_summary.request_digest}`",
            "",
            (
                "| Model | Run ID | Sim Annual Return | Sim Max Drawdown | "
                "Sim Turnover | Return Delta vs Research | Shortfall Delta | "
                "Stress Fail Count |"
            ),
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
        for row in backtest_summary.rows:
            return_delta = row.divergence_metrics.get(
                "simulation_minus_research_cumulative_return",
                0.0,
            )
            shortfall_delta = row.divergence_metrics.get(
                "simulation_minus_research_shortfall",
                0.0,
            )
            lines.append(
                f"| {row.model_name} | {row.run_id} | "
                f"{row.simulation_metrics.get('annual_return', 0.0):.6f} | "
                f"{row.simulation_metrics.get('max_drawdown', 0.0):.6f} | "
                f"{row.simulation_metrics.get('turnover_total', 0.0):.6f} | "
                f"{return_delta:.6f} | "
                f"{shortfall_delta:.6f} | "
                f"{row.scenario_metrics.get('stress_fail_count', 0.0):.0f} |"
            )
        if backtest_summary.comparison_warnings:
            lines.extend(["", "## Comparison Warnings", ""])
            for warning in backtest_summary.comparison_warnings:
                lines.append(f"- {warning}")
        target.write_text("\n".join(lines), encoding="utf-8")

    def _write_backtest_summary_csv(self, target: Path, backtest_summary) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "model_name",
                    "run_id",
                    "prediction_frame_uri",
                    "research_result_uri",
                    "simulation_result_uri",
                    "research_metrics",
                    "simulation_metrics",
                    "divergence_metrics",
                    "scenario_metrics",
                    "passed_consistency_checks",
                ],
            )
            writer.writeheader()
            for row in backtest_summary.rows:
                writer.writerow(
                    {
                        "model_name": row.model_name,
                        "run_id": row.run_id,
                        "prediction_frame_uri": row.prediction_frame_uri,
                        "research_result_uri": row.research_result_uri,
                        "simulation_result_uri": row.simulation_result_uri,
                        "research_metrics": json.dumps(row.research_metrics, sort_keys=True),
                        "simulation_metrics": json.dumps(row.simulation_metrics, sort_keys=True),
                        "divergence_metrics": json.dumps(row.divergence_metrics, sort_keys=True),
                        "scenario_metrics": json.dumps(row.scenario_metrics, sort_keys=True),
                        "passed_consistency_checks": row.passed_consistency_checks,
                    }
                )
