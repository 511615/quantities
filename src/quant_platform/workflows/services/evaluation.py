from __future__ import annotations

from pathlib import Path

from quant_platform.backtest.contracts.report import PromotionDecisionArtifact
from quant_platform.backtest.metrics.promotion import build_promotion_decisions
from quant_platform.common.types.core import ArtifactRef
from quant_platform.workflows.contracts.results import (
    BacktestWorkflowResult,
    BenchmarkWorkflowResult,
)
from quant_platform.workflows.runtime import WorkflowRuntime


class EvaluationWorkflowService:
    def __init__(self, runtime: WorkflowRuntime) -> None:
        self.runtime = runtime

    def build_promotion_decision(
        self,
        benchmark_result: BenchmarkWorkflowResult,
        backtest_result: BacktestWorkflowResult,
    ) -> tuple[list[PromotionDecisionArtifact], list[ArtifactRef]]:
        if benchmark_result.benchmark_summary is None:
            raise ValueError("benchmark_result is missing benchmark_summary")
        if backtest_result.backtest_summary is None:
            raise ValueError("backtest_result is missing backtest_summary")
        decisions = build_promotion_decisions(
            benchmark_summary=benchmark_result.benchmark_summary,
            backtest_summary=backtest_result.backtest_summary,
        )
        payload = {
            "dataset_id": backtest_result.backtest_summary.dataset_id,
            "benchmark_name": benchmark_result.benchmark_name,
            "decision_count": len(decisions),
            "decisions": [decision.model_dump(mode="json") for decision in decisions],
        }
        json_artifact = self.runtime.store.write_json(
            "evaluation/promotion_decision.json",
            payload,
        )
        markdown_path = self.runtime.artifact_root / "evaluation" / "promotion_decision.md"
        self._write_markdown(markdown_path, payload)
        return decisions, [
            ArtifactRef(kind="promotion_decision_json", uri=json_artifact.uri),
            ArtifactRef(kind="promotion_decision_markdown", uri=str(markdown_path)),
        ]

    def _write_markdown(self, target: Path, payload: dict[str, object]) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        lines = [
            "# Promotion Decision",
            "",
            f"- Dataset: `{payload['dataset_id']}`",
            f"- Benchmark: `{payload['benchmark_name']}`",
            "",
            (
                "| Model | Run ID | Decision | Benchmark Gate | Backtest Gate | "
                "Default Gate | Hard Failures | Soft Warnings |"
            ),
            "| --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
        for row in payload["decisions"]:
            lines.append(
                f"| {row['model_name']} | {row['run_id']} | {row['decision']} | "
                f"{row['benchmark_gate_passed']} | {row['backtest_gate_passed']} | "
                f"{row.get('default_gate_passed', 'n/a')} | "
                f"{'; '.join(row['hard_failures']) or 'none'} | "
                f"{'; '.join(row['soft_warnings']) or 'none'} |"
            )
        target.write_text("\n".join(lines), encoding="utf-8")
