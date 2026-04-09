from __future__ import annotations

from quant_platform.models.serialization.artifact import read_artifact_meta, read_artifact_state


def test_registry_wires_model_into_runner(facade) -> None:
    fit_result = facade.train_smoke()
    meta = read_artifact_meta(fit_result.model_artifact_uri)
    state = read_artifact_state(fit_result.model_artifact_uri)
    assert meta.model_name == "elastic_net"
    assert "estimator" in state
    assert fit_result.metrics["sample_count"] == 4.0
