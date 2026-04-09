from __future__ import annotations

from pathlib import Path

import pytest

from quant_platform.common.enums.core import ModelFamily
from quant_platform.models.baselines.elastic_net import ElasticNetModel
from quant_platform.models.baselines.gru import GRUSequenceModel
from quant_platform.models.baselines.lightgbm import LightGBMModel
from quant_platform.models.baselines.mlp import MLPModel
from tests.fixtures.dataset_samples import build_dataset_samples
from tests.fixtures.model_specs import build_model_spec


@pytest.mark.parametrize(
    ("model_cls", "model_name", "family", "lookback"),
    [
        (ElasticNetModel, "elastic_net", ModelFamily.LINEAR, None),
        (LightGBMModel, "lightgbm", ModelFamily.TREE, None),
        (MLPModel, "mlp", ModelFamily.DEEP, None),
        (GRUSequenceModel, "gru", ModelFamily.SEQUENCE, 2),
    ],
)
def test_baseline_family_supports_fit_save_load_predict(
    tmp_path: Path,
    model_cls: type,
    model_name: str,
    family: ModelFamily,
    lookback: int | None,
) -> None:
    samples = build_dataset_samples()
    spec = build_model_spec(
        model_name,
        family=family,
        lookback=lookback,
        hyperparams={"random_state": 7},
    )
    model = model_cls(spec)
    metrics = model.fit(samples)
    meta = model.save(tmp_path / model_name)
    restored = model_cls.load(spec, tmp_path / model_name)
    frame = restored.predict(samples, model_run_id=f"{model_name}-run")

    assert Path(meta.artifact_uri).exists()
    assert metrics["sample_count"] >= 2.0
    assert frame.sample_count >= 2
    assert all(row.model_run_id == f"{model_name}-run" for row in frame.rows)


def test_gru_predictions_align_to_window_tail() -> None:
    samples = build_dataset_samples()
    spec = build_model_spec(
        "gru",
        family=ModelFamily.SEQUENCE,
        lookback=2,
        hyperparams={"random_state": 7},
    )
    model = GRUSequenceModel(spec)
    model.fit(samples)
    frame = model.predict(samples, model_run_id="gru-run")
    assert frame.sample_count == 2
    assert frame.rows[0].timestamp == samples[1].timestamp
    assert frame.rows[1].timestamp == samples[2].timestamp
