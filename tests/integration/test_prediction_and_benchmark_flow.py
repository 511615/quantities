from __future__ import annotations

from pathlib import Path


def test_training_exports_feature_importance_and_predict_artifact(facade) -> None:
    fit_result = facade.train_smoke()
    assert fit_result.feature_importance_uri is not None
    assert Path(fit_result.feature_importance_uri).exists()

    prediction_frame = facade.build_prediction_frame(fit_result)
    prediction_artifact = facade.artifact_root / "predictions" / fit_result.run_id / "full.json"
    assert prediction_frame.sample_count >= 1
    assert prediction_artifact.exists()


def test_baseline_benchmark_runs_all_models_and_writes_artifact(facade) -> None:
    result = facade.run_baseline_benchmark()
    model_names = {row["model_name"] for row in result["results"]}
    assert model_names == {"mean_baseline", "elastic_net", "lightgbm", "mlp", "gru"}
    assert result["benchmark_type"] == "real_with_synthetic_reference"
    assert result["benchmark_summary"]["reference_consistency"] == result["validation_summary"]
    assert result["benchmark_summary"]["official_benchmark"] is True
    assert set(result["benchmark_summary"]["baseline_model_names"]) == model_names
    assert "data_source" in result
    assert all("mean_valid_mae" in row for row in result["results"])
    assert all("advanced_kind" in row for row in result["results"])
    assert all("rank" in row for row in result["results"])
    assert result["leaderboard"][0]["rank"] == 1
    assert {row["model_name"] for row in result["deep_backend_comparison"]} == {"mlp", "gru"}
    assert result["validation_summary"]["real_top_model"] in model_names
    assert result["validation_summary"]["synthetic_top_model"] in model_names
    for row in result["results"]:
        assert Path(row["artifact_uri"]).exists()
    assert (facade.artifact_root / "benchmarks" / "baseline_family_walk_forward.json").exists()
    assert (facade.artifact_root / "benchmarks" / "baseline_family_walk_forward.md").exists()
    assert (facade.artifact_root / "benchmarks" / "baseline_family_walk_forward.csv").exists()
    markdown = (facade.artifact_root / "benchmarks" / "baseline_family_walk_forward.md").read_text(
        encoding="utf-8"
    )
    assert "## Conclusion" in markdown
    assert "## Recommended Default Baseline" in markdown
