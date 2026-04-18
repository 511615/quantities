from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from quant_platform.webapi.app import ENV_TEST_ARTIFACT_ROOT, create_app
from quant_platform.webapi.services.catalog import ResearchWorkbenchService


def test_create_app_starts_with_isolated_test_artifact_root(monkeypatch, tmp_path: Path) -> None:
    artifact_root = tmp_path / "playwright-artifacts"
    monkeypatch.setenv(ENV_TEST_ARTIFACT_ROOT, str(artifact_root))
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.delenv("QUANT_PLATFORM_OFFLINE_BENCHMARK", raising=False)
    monkeypatch.setattr(
        ResearchWorkbenchService,
        "ensure_official_multimodal_benchmark",
        lambda self: None,
    )

    client = TestClient(create_app())

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_runs_endpoint_is_available_when_only_test_artifact_root_is_set(
    monkeypatch,
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "playwright-artifacts"
    monkeypatch.setenv(ENV_TEST_ARTIFACT_ROOT, str(artifact_root))
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.delenv("QUANT_PLATFORM_OFFLINE_BENCHMARK", raising=False)
    monkeypatch.setattr(
        ResearchWorkbenchService,
        "ensure_official_multimodal_benchmark",
        lambda self: None,
    )

    client = TestClient(create_app())
    response = client.get("/api/runs?page=1&per_page=20")

    assert response.status_code == 200
    payload = response.json()
    run_ids = [item["run_id"] for item in payload["items"]]
    assert "multimodal-compose-20260413144642-80a31c" in run_ids
