from __future__ import annotations

from pathlib import Path

import pytest

from quant_platform.models.baselines.mean_baseline import build_mean_baseline
from tests.fixtures.dataset_samples import build_dataset_samples
from tests.fixtures.model_specs import build_model_spec


def test_mean_baseline_fits_and_serializes_state() -> None:
    model = build_mean_baseline(build_model_spec())
    metrics = model.fit(build_dataset_samples())
    assert metrics["sample_count"] == 3.0
    assert "mae" in metrics


def test_mean_baseline_predict_save_and_load(tmp_path: Path) -> None:
    samples = build_dataset_samples()
    model = build_mean_baseline(build_model_spec())
    model.fit(samples)
    meta = model.save(tmp_path / "mean")
    restored = type(model).load(model.spec, tmp_path / "mean")
    frame = restored.predict(samples, model_run_id="mean-run")
    assert Path(meta.artifact_uri).exists()
    assert frame.sample_count == len(samples)
    assert all(row.model_run_id == "mean-run" for row in frame.rows)


def test_mean_baseline_rejects_empty_samples() -> None:
    model = build_mean_baseline(build_model_spec())
    with pytest.raises(ValueError, match="cannot be empty"):
        model.fit([])
