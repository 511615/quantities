from __future__ import annotations

from pathlib import Path

from quant_platform.workflows.services.benchmark import BenchmarkWorkflowService


def test_workflow_benchmark_produces_leaderboard_and_summary_artifacts(workflow_runtime) -> None:
    service = BenchmarkWorkflowService(workflow_runtime)

    result = service.run_baseline_benchmark()

    assert result.benchmark_type == "real_with_synthetic_reference"
    assert {row.model_name for row in result.results} == {
        "mean_baseline",
        "elastic_net",
        "lightgbm",
        "mlp",
        "gru",
    }
    assert result.leaderboard[0].rank == 1
    assert {row["model_name"] for row in result.deep_backend_comparison} == {"mlp", "gru"}
    assert result.validation_summary["real_top_model"] in {row.model_name for row in result.results}
    assert result.validation_summary["synthetic_top_model"] in {
        row.model_name for row in result.results
    }
    assert result.benchmark_summary is not None
    assert result.benchmark_summary.reference_consistency == result.validation_summary
    assert result.benchmark_summary.official_benchmark is True
    assert set(result.benchmark_summary.baseline_model_names) == {
        row.model_name for row in result.results
    }
    summary_artifacts = {artifact.kind: artifact.uri for artifact in result.summary_artifacts}
    assert Path(summary_artifacts["benchmark_summary_json"]).exists()
    assert Path(summary_artifacts["benchmark_summary_markdown"]).exists()
    assert Path(summary_artifacts["benchmark_summary_csv"]).exists()
    assert Path(summary_artifacts["benchmark_json"]).exists()
    assert Path(summary_artifacts["benchmark_markdown"]).exists()
    assert Path(summary_artifacts["benchmark_csv"]).exists()
