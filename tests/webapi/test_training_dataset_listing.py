from fastapi.testclient import TestClient

from quant_platform.webapi.app import create_app


def test_training_dataset_listing_skips_official_nlp_gate(monkeypatch) -> None:
    app = create_app()
    workbench = app.state.services.workbench
    called = False

    def fail_if_called(*args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal called
        called = True
        raise AssertionError("training dataset listing should not evaluate the official NLP gate")

    monkeypatch.setattr(workbench, "_dataset_official_nlp_gate", fail_if_called)

    client = TestClient(app)
    response = client.get("/api/datasets/training")

    assert response.status_code == 200
    assert isinstance(response.json()["items"], list)
    assert called is False
