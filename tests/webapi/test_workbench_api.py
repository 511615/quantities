from __future__ import annotations

import copy
import json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from quant_platform.common.io.files import LocalArtifactStore
from quant_platform.data.contracts.ingestion import DataConnectorError
from quant_platform.data.contracts.market import NormalizedMarketBar
from quant_platform.data.contracts.series import NormalizedSeriesPoint
from quant_platform.datasets.contracts.dataset import DatasetRef
from quant_platform.datasets.manifests.dataset_manifest import DatasetBuildManifest
from quant_platform.webapi.app import create_app


def _wait_for_job(
    client: TestClient,
    job_id: str,
    timeout_seconds: float = 20.0,
) -> dict[str, object]:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            response = client.get(f"/api/jobs/{job_id}")
        except PermissionError:
            time.sleep(0.05)
            continue
        response.raise_for_status()
        payload = response.json()
        if payload["status"] in {"success", "failed"}:
            return payload
        time.sleep(0.1)
    raise AssertionError(f"Job '{job_id}' did not finish within {timeout_seconds} seconds.")


def _series_point(
    *,
    event_time: datetime,
    identifier: str,
    data_domain: str,
    vendor: str,
    frequency: str,
    value: float,
    metric_name: str = "value",
) -> NormalizedSeriesPoint:
    return NormalizedSeriesPoint(
        event_time=event_time,
        available_time=event_time,
        series_key=f"{data_domain}:{vendor}:{identifier}:{metric_name}",
        entity_key=identifier,
        domain=data_domain,
        vendor=vendor,
        metric_name=metric_name,
        frequency=frequency,
        value=value,
        dimensions={"identifier": identifier},
    )


def _inject_failed_official_nlp_gate(app, dataset_id: str = "smoke_dataset") -> None:
    artifact_root = app.state.services.jobs.artifact_root
    store = LocalArtifactStore(artifact_root)
    manifest_path = artifact_root / "datasets" / f"{dataset_id}_dataset_manifest.json"
    dataset_manifest = store.read_model(str(manifest_path), DatasetBuildManifest)
    acquisition_profile = dict(dataset_manifest.acquisition_profile or {})
    dataset_manifest = dataset_manifest.model_copy(
        update={
            "acquisition_profile": {
                **acquisition_profile,
                "data_domain": "market",
                "data_domains": ["market", "sentiment_events"],
                "source_specs": [
                    {
                        "data_domain": "sentiment_events",
                        "source_vendor": "gnews",
                        "frequency": "1h",
                        "identifier": "btc_news_failed_gate",
                    }
                ],
            }
        }
    )
    store.write_model(f"datasets/{dataset_id}_dataset_manifest.json", dataset_manifest)
    points = [
        NormalizedSeriesPoint(
            event_time=datetime(2024, 1, 1, 0, tzinfo=UTC),
            available_time=datetime(2024, 1, 1, 0, tzinfo=UTC),
            series_key="sentiment:gnews:btc_news_failed_gate:event_count",
            entity_key="BTCUSDT",
            domain="sentiment_events",
            vendor="gnews",
            metric_name="event_count",
            frequency="1h",
            value=1.0,
            dimensions={
                "identifier": "btc_news_failed_gate",
                "symbol": "BTCUSDT",
                "preview_events_json": json.dumps(
                    [
                        {
                            "event_id": "gnews-failed-gate-1",
                            "symbol": "BTCUSDT",
                            "title": "archival gate failure probe",
                            "event_time": "2024-01-01T00:00:00Z",
                            "available_time": "2024-01-01T00:00:00Z",
                        }
                    ]
                ),
            },
        ),
        NormalizedSeriesPoint(
            event_time=datetime(2024, 1, 1, 0, tzinfo=UTC),
            available_time=datetime(2024, 1, 1, 0, tzinfo=UTC),
            series_key="sentiment:gnews:btc_news_failed_gate:sentiment_score",
            entity_key="BTCUSDT",
            domain="sentiment_events",
            vendor="gnews",
            metric_name="sentiment_score",
            frequency="1h",
            value=0.25,
            dimensions={"identifier": "btc_news_failed_gate", "symbol": "BTCUSDT"},
        ),
    ]
    store.write_json(
        f"datasets/{dataset_id}_sentiment_points.json",
        {"rows": [point.model_dump(mode="json") for point in points]},
    )


def _inject_zero_event_official_gate_probe(app, dataset_id: str = "smoke_dataset") -> None:
    artifact_root = app.state.services.jobs.artifact_root
    store = LocalArtifactStore(artifact_root)
    manifest_path = artifact_root / "datasets" / f"{dataset_id}_dataset_manifest.json"
    dataset_manifest = store.read_model(str(manifest_path), DatasetBuildManifest)
    acquisition_profile = dict(dataset_manifest.acquisition_profile or {})
    dataset_manifest = dataset_manifest.model_copy(
        update={
            "acquisition_profile": {
                **acquisition_profile,
                "data_domain": "market",
                "data_domains": ["market", "sentiment_events"],
                "source_specs": [
                    {
                        "data_domain": "sentiment_events",
                        "source_vendor": "gdelt",
                        "frequency": "1h",
                        "identifier": "btc_zero_gate_probe",
                    }
                ],
            }
        }
    )
    store.write_model(f"datasets/{dataset_id}_dataset_manifest.json", dataset_manifest)
    points = [
        NormalizedSeriesPoint(
            event_time=datetime(2024, 1, 1, 3, tzinfo=UTC),
            available_time=datetime(2024, 1, 1, 3, tzinfo=UTC),
            series_key="sentiment:gdelt:btc_zero_gate_probe:event_count",
            entity_key="BTCUSDT",
            domain="sentiment_events",
            vendor="gdelt",
            metric_name="event_count",
            frequency="1h",
            value=0.0,
            dimensions={
                "identifier": "btc_zero_gate_probe",
                "symbol": "BTCUSDT",
                "preview_events_json": "[]",
            },
        ),
        NormalizedSeriesPoint(
            event_time=datetime(2024, 1, 1, 3, tzinfo=UTC),
            available_time=datetime(2024, 1, 1, 3, tzinfo=UTC),
            series_key="sentiment:gdelt:btc_zero_gate_probe:sentiment_score",
            entity_key="BTCUSDT",
            domain="sentiment_events",
            vendor="gdelt",
            metric_name="sentiment_score",
            frequency="1h",
            value=0.0,
            dimensions={"identifier": "btc_zero_gate_probe", "symbol": "BTCUSDT"},
        ),
    ]
    store.write_json(
        f"datasets/{dataset_id}_sentiment_points.json",
        {"rows": [point.model_dump(mode="json") for point in points]},
    )


def _append_run_input_feature(
    app,
    *,
    run_id: str,
    feature_name: str,
) -> tuple[Path, str]:
    artifact_root = app.state.services.jobs.artifact_root
    metadata_path = artifact_root / "models" / run_id / "metadata.json"
    original_text = metadata_path.read_text(encoding="utf-8")
    payload = json.loads(original_text)
    feature_names = list(payload.get("feature_names") or [])
    if feature_name not in feature_names:
        feature_names.append(feature_name)
    payload["feature_names"] = feature_names
    model_spec = dict(payload.get("model_spec") or {})
    input_schema = list(model_spec.get("input_schema") or [])
    if feature_name not in {
        item.get("name")
        for item in input_schema
        if isinstance(item, dict)
    }:
        input_schema.append(
            {
                "name": feature_name,
                "dtype": "float",
                "nullable": False,
                "description": "Injected unsupported feature for official preflight test.",
            }
        )
    model_spec["input_schema"] = input_schema
    payload["model_spec"] = model_spec
    metadata_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return metadata_path, original_text


def _mark_official_multimodal_benchmark_stale(app) -> None:
    artifact_root = app.state.services.jobs.artifact_root
    store = LocalArtifactStore(artifact_root)
    manifest_path = artifact_root / "datasets" / "official_reddit_pullpush_multimodal_v2_fusion_dataset_manifest.json"
    dataset_manifest = store.read_model(str(manifest_path), DatasetBuildManifest)
    acquisition_profile = dict(dataset_manifest.acquisition_profile or {})
    dataset_manifest = dataset_manifest.model_copy(
        update={
            "acquisition_profile": {
                **acquisition_profile,
                "market_snapshot_version": "stale-market-snapshot",
                "sentiment_snapshot_version": "stale-sentiment-snapshot",
            }
        }
    )
    store.write_model("datasets/official_reddit_pullpush_multimodal_v2_fusion_dataset_manifest.json", dataset_manifest)


def _market_dataset_request_payload(
    request_name: str,
    *,
    dataset_type: str = "training_panel",
) -> dict[str, object]:
    return {
        "request_name": request_name,
        "data_domain": "market",
        "dataset_type": dataset_type,
        "asset_mode": "single_asset",
        "time_window": {
            "start_time": "2024-01-01T00:00:00Z",
            "end_time": "2024-01-05T00:00:00Z",
        },
        "symbol_selector": {
            "symbol_type": "spot",
            "selection_mode": "explicit",
            "symbols": ["BTCUSDT"],
            "symbol_count": 1,
            "tags": [],
        },
        "selection_mode": "explicit",
        "source_vendor": "internal_smoke",
        "exchange": "binance",
        "frequency": "1h",
        "filters": {},
        "build_config": {
            "feature_set_id": "baseline_market_features",
            "label_horizon": 1,
            "label_kind": "regression",
            "split_strategy": "time_series",
            "sample_policy_name": "training_panel_strict",
            "alignment_policy_name": "event_time_inner",
            "missing_feature_policy_name": "drop_if_missing",
            "sample_policy": {},
            "alignment_policy": {},
            "missing_feature_policy": {},
        },
    }


def _sentiment_dataset_request_payload(
    request_name: str,
    *,
    identifier: str = "btc_news",
    dataset_type: str = "training_panel",
    start_time: str = "2026-04-09T00:00:00Z",
    end_time: str = "2026-04-09T12:00:00Z",
) -> dict[str, object]:
    return {
        "request_name": request_name,
        "data_domain": "sentiment_events",
        "dataset_type": dataset_type,
        "asset_mode": "single_asset",
        "time_window": {
            "start_time": start_time,
            "end_time": end_time,
        },
        "selection_mode": "explicit",
        "source_vendor": "news_archive",
        "frequency": "1h",
        "filters": {},
        "sources": [
            {
                "data_domain": "sentiment_events",
                "source_vendor": "news_archive",
                "frequency": "1h",
                "identifier": identifier,
                "filters": {},
            }
        ],
        "build_config": {
            "feature_set_id": "baseline_market_features",
            "label_horizon": 1,
            "label_kind": "regression",
            "split_strategy": "time_series",
            "sample_policy_name": "training_panel_strict",
            "alignment_policy_name": "event_time_inner",
            "missing_feature_policy_name": "drop_if_missing",
            "sample_policy": {},
            "alignment_policy": {},
            "missing_feature_policy": {},
        },
    }


def _launch_train_and_wait(
    client: TestClient,
    *,
    dataset_id: str | None = None,
    dataset_preset: str | None = None,
    run_id_prefix: str,
    timeout_seconds: float = 20.0,
) -> str:
    payload: dict[str, object] = {
        "template_id": "registry::elastic_net",
        "trainer_preset": "fast",
        "experiment_name": run_id_prefix,
        "run_id_prefix": run_id_prefix,
    }
    if dataset_id is not None:
        payload["dataset_id"] = dataset_id
        payload["dataset_preset"] = dataset_preset or "smoke"
    elif dataset_preset is not None:
        payload["dataset_preset"] = dataset_preset
    response = client.post("/api/launch/train", json=payload)
    assert response.status_code == 200
    job_payload = _wait_for_job(client, response.json()["job_id"], timeout_seconds=timeout_seconds)
    assert job_payload["status"] == "success"
    return job_payload["result"]["run_ids"][0]


def _materialize_stub_run(
    app,
    *,
    run_id: str,
    dataset_id: str,
    feature_names: list[str],
    model_name: str = "elastic_net",
) -> None:
    artifact_root = app.state.services.jobs.artifact_root
    workbench = app.state.services.workbench
    dataset_detail = workbench.get_dataset_detail(dataset_id)
    dataset_readiness = workbench.get_dataset_readiness(dataset_id)
    assert dataset_detail is not None
    assert dataset_readiness is not None

    created_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    model_dir = artifact_root / "models" / run_id
    prediction_dir = artifact_root / "predictions" / run_id
    model_dir.mkdir(parents=True, exist_ok=True)
    prediction_dir.mkdir(parents=True, exist_ok=True)

    train_manifest = {
        "run_id": run_id,
        "created_at": created_at,
        "dataset_ref_uri": f"dataset://{dataset_id}",
        "dataset_id": dataset_id,
        "dataset_manifest_uri": str(
            (artifact_root / "datasets" / f"{dataset_id}_dataset_manifest.json").resolve()
        ),
        "dataset_type": dataset_detail.dataset.dataset_type,
        "data_domain": dataset_detail.dataset.data_domain,
        "data_domains": list(dataset_detail.dataset.data_domains),
        "snapshot_version": dataset_detail.dataset.snapshot_version,
        "entity_scope": dataset_detail.dataset.entity_scope,
        "entity_count": dataset_detail.dataset.entity_count,
        "feature_schema_hash": dataset_readiness.feature_schema_hash,
        "dataset_readiness_status": dataset_readiness.readiness_status,
        "dataset_readiness_warnings": list(dataset_readiness.warnings),
        "metrics": {},
    }
    metadata = {
        "run_id": run_id,
        "model_name": model_name,
        "model_family": "linear",
        "advanced_kind": "baseline",
        "model_spec": {
            "model_name": model_name,
            "family": "linear",
            "version": "0.1.0",
            "input_schema": [
                {"name": name, "dtype": "float", "nullable": False, "description": None}
                for name in feature_names
            ],
            "output_schema": [{"name": "prediction", "dtype": "float", "nullable": False}],
            "task_type": "regression",
            "lookback": None,
            "target_horizon": dataset_detail.dataset.label_horizon or 1,
            "prediction_type": "return",
            "hyperparams": {},
        },
        "registration": {
            "model_name": model_name,
            "family": "linear",
            "advanced_kind": "baseline",
            "input_adapter_key": "tabular_default",
            "prediction_adapter_key": "standard_prediction",
            "artifact_adapter_key": "json_manifest",
            "capabilities": [],
            "benchmark_eligible": True,
            "default_eligible": True,
            "enabled": True,
        },
        "artifact_uri": str((model_dir / "metadata.json").resolve()),
        "artifact_dir": str(model_dir.resolve()),
        "state_uri": None,
        "backend": "json_manifest",
        "training_sample_count": dataset_detail.dataset.sample_count or 0,
        "feature_names": list(feature_names),
        "training_config": {},
        "training_metrics": {},
        "best_epoch": None,
        "trained_steps": None,
        "checkpoint_tag": None,
        "input_metadata": {},
        "prediction_metadata": {},
    }
    evaluation_summary = {
        "run_id": run_id,
        "dataset_id": dataset_id,
        "task_type": "regression",
        "selected_scope": "full",
        "sample_count": dataset_detail.dataset.sample_count or 0,
        "regression_metrics": {},
        "split_metrics": {},
        "coverage": {
            "available_scopes": ["full"],
            "sample_count": dataset_detail.dataset.sample_count or 0,
        },
        "series": {},
    }
    tracking = {
        "created_at": created_at,
        "metrics": {},
        "params": {
            "dataset_id": dataset_id,
            "model_name": model_name,
        },
        "run_id": run_id,
    }
    prediction_stub = {
        "rows": [],
        "metadata": {
            "feature_view_ref": None,
            "prediction_time": created_at,
            "inference_latency_ms": 0,
            "target_horizon": dataset_detail.dataset.label_horizon or 1,
        },
    }

    (artifact_root / "tracking").mkdir(parents=True, exist_ok=True)
    (artifact_root / "tracking" / f"{run_id}.json").write_text(
        json.dumps(tracking, indent=2),
        encoding="utf-8",
    )
    (model_dir / "train_manifest.json").write_text(json.dumps(train_manifest, indent=2), encoding="utf-8")
    (model_dir / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    (model_dir / "evaluation_summary.json").write_text(
        json.dumps(evaluation_summary, indent=2),
        encoding="utf-8",
    )
    (prediction_dir / "full.json").write_text(json.dumps(prediction_stub, indent=2), encoding="utf-8")


def _isoformat_z(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _frequency_to_timedelta(frequency: str) -> timedelta:
    normalized = frequency.lower()
    if normalized.endswith("h"):
        return timedelta(hours=int(normalized[:-1]))
    if normalized.endswith("d"):
        return timedelta(days=int(normalized[:-1]))
    raise ValueError(f"Unsupported frequency '{frequency}'")


def _time_range(start_time: datetime, end_time: datetime, step: timedelta) -> list[datetime]:
    points: list[datetime] = []
    current = start_time
    while current < end_time:
        points.append(current)
        current += step
    return points


def _aligned_market_bars(
    start_time: datetime,
    end_time: datetime,
    *,
    symbol: str,
    venue: str,
    frequency: str,
) -> list[NormalizedMarketBar]:
    bars: list[NormalizedMarketBar] = []
    step = _frequency_to_timedelta(frequency)
    price = 100.0
    for event_time in _time_range(start_time, end_time, step):
        bars.append(
            NormalizedMarketBar(
                event_time=event_time,
                available_time=event_time,
                symbol=symbol.upper(),
                venue=venue,
                open=price,
                high=price + 1.0,
                low=price - 1.0,
                close=price + 0.5,
                volume=100.0,
            )
        )
        price += 1.0
    return bars


def _aligned_series_points(
    start_time: datetime,
    end_time: datetime,
    *,
    data_domain: str,
    vendor: str,
    identifier: str,
    frequency: str,
    offset: timedelta = timedelta(0),
    metric_name: str = "value",
    value_offset: float = 1.0,
) -> list[NormalizedSeriesPoint]:
    step = _frequency_to_timedelta(frequency)
    points = []
    for idx, event_time in enumerate(_time_range(start_time, end_time, step)):
        timestamp = event_time + offset
        points.append(
            _series_point(
                event_time=timestamp,
                identifier=identifier,
                data_domain=data_domain,
                vendor=vendor,
                frequency=frequency,
                value=value_offset + idx,
                metric_name=metric_name,
            )
        )
    return points


def _multi_domain_request_payload(
    request_name: str,
    *,
    start_time: datetime,
    end_time: datetime,
    dataset_type: str = "training_panel",
    merge_policy_name: str = "available_time_safe_asof",
) -> dict[str, object]:
    payload = copy.deepcopy(
        _market_dataset_request_payload(request_name, dataset_type=dataset_type)
    )
    payload["time_window"] = {
        "start_time": _isoformat_z(start_time),
        "end_time": _isoformat_z(end_time),
    }
    symbol_selector = {
        "symbol_type": "spot",
        "selection_mode": "explicit",
        "symbols": ["BTCUSDT"],
        "symbol_count": 1,
        "tags": [],
    }
    payload["symbol_selector"] = copy.deepcopy(symbol_selector)
    payload["sources"] = [
        {
            "data_domain": "market",
            "vendor": "internal_smoke",
            "exchange": "binance",
            "frequency": "1h",
            "symbol_selector": copy.deepcopy(symbol_selector),
            "filters": {},
        },
        {
            "data_domain": "macro",
            "vendor": "fred",
            "identifier": "DFF",
            "frequency": "1h",
            "filters": {},
        },
        {
            "data_domain": "on_chain",
            "vendor": "defillama",
            "identifier": "ethereum",
            "frequency": "1h",
            "filters": {},
        },
    ]
    payload["merge_policy_name"] = merge_policy_name
    for deprecated in ("data_domain", "source_vendor", "exchange", "frequency"):
        payload.pop(deprecated, None)
    return payload


def _patch_multi_domain_ingestion(
    app,
    *,
    macro_offset: timedelta = timedelta(0),
    on_chain_offset: timedelta = timedelta(0),
    macro_error: Exception | None = None,
    on_chain_error: Exception | None = None,
    macro_empty: bool = False,
    on_chain_empty: bool = False,
):
    ingestion = app.state.services.workbench.facade.runtime.ingestion_service

    def fake_fetch_market_bars(
        *,
        symbol: str,
        vendor: str,
        exchange: str,
        frequency: str,
        start_time: datetime,
        end_time: datetime,
    ) -> tuple[list[NormalizedMarketBar], str]:
        bars = _aligned_market_bars(
            start_time,
            end_time,
            symbol=symbol,
            venue=exchange,
            frequency=frequency,
        )
        return bars, "live_fetch"

    def _raise_empty(domain: str, vendor: str, identifier: str) -> None:
        raise DataConnectorError(
            data_domain=domain,
            vendor=vendor,
            identifier=identifier,
            message="Connector returned no rows and no synthetic fallback is allowed.",
            retryable=False,
            code="empty_result",
        )

    def fake_fetch_series_points(
        *,
        data_domain: str,
        identifier: str,
        vendor: str,
        frequency: str,
        start_time: datetime,
        end_time: datetime,
        options: dict[str, object] | None = None,
    ) -> tuple[list[NormalizedSeriesPoint], str]:
        del options
        if data_domain == "macro":
            if macro_error:
                raise macro_error
            if macro_empty:
                _raise_empty(data_domain, vendor, identifier)
            points = _aligned_series_points(
                start_time,
                end_time,
                data_domain="macro",
                vendor=vendor,
                identifier=identifier,
                frequency=frequency,
                offset=macro_offset,
                metric_name="macro_dff_value",
                value_offset=2.0,
            )
            return points, "live_fetch"
        if data_domain == "on_chain":
            if on_chain_error:
                raise on_chain_error
            if on_chain_empty:
                _raise_empty(data_domain, vendor, identifier)
            points = _aligned_series_points(
                start_time,
                end_time,
                data_domain="on_chain",
                vendor=vendor,
                identifier=identifier,
                frequency=frequency,
                offset=on_chain_offset,
                metric_name="on_chain_value",
                value_offset=500.0,
            )
            return points, "live_fetch"
        raise AssertionError(f"Unexpected series domain '{data_domain}'")

    ingestion.fetch_market_bars = fake_fetch_market_bars
    ingestion.fetch_series_points = fake_fetch_series_points


def test_workbench_overview_endpoint_returns_required_sections() -> None:
    client = TestClient(create_app())

    response = client.get("/api/workbench/overview")

    assert response.status_code == 200
    payload = response.json()
    assert "recent_runs" in payload
    assert "recent_benchmarks" in payload
    assert "recent_jobs" in payload
    assert "datasets" in payload
    assert "recommended_actions" in payload


def test_runs_and_run_detail_endpoints_work() -> None:
    client = TestClient(create_app())

    list_response = client.get("/api/runs")
    assert list_response.status_code == 200
    list_payload = list_response.json()
    assert list_payload["total"] >= 1

    detail_response = client.get("/api/runs/smoke-train-run")
    assert detail_response.status_code == 200
    detail_payload = detail_response.json()
    assert detail_payload["run_id"] == "smoke-train-run"
    assert "summary" in detail_payload
    assert "artifacts" in detail_payload
    assert isinstance(detail_payload["glossary_hints"], list)


def test_runs_endpoint_does_not_depend_on_heavy_dataset_detail_loading() -> None:
    app = create_app()

    def _unexpected_dataset_detail(_: str):
        raise AssertionError("list_runs should not call get_dataset_detail")

    app.state.services.workbench.get_dataset_detail = _unexpected_dataset_detail  # type: ignore[method-assign]
    client = TestClient(app)

    response = client.get("/api/runs")
    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] >= 1
    assert any(item["dataset_id"] for item in payload["items"])


def test_benchmark_detail_endpoint_contains_summary() -> None:
    client = TestClient(create_app())

    response = client.get("/api/benchmarks/baseline_family_walk_forward")

    assert response.status_code == 200
    payload = response.json()
    assert payload["leaderboard"]
    assert "summary" in payload


def test_benchmark_list_excludes_model_detail_artifacts() -> None:
    client = TestClient(create_app())

    detail_response = client.get("/api/benchmarks/baseline_family_walk_forward")
    assert detail_response.status_code == 200

    list_response = client.get("/api/benchmarks")
    assert list_response.status_code == 200
    benchmark_names = {item["benchmark_name"] for item in list_response.json()}

    assert "baseline_family_walk_forward" in benchmark_names
    assert "elastic_net" not in benchmark_names
    assert "gru" not in benchmark_names

    model_detail_response = client.get("/api/benchmarks/elastic_net")
    assert model_detail_response.status_code == 404


def test_train_options_include_template_and_registry_source() -> None:
    client = TestClient(create_app())

    response = client.get("/api/launch/train/options")

    assert response.status_code == 200
    payload = response.json()
    assert payload["template_options"]
    assert payload["constraints"]["model_options_source"] == "registry"


def test_launch_train_supports_template_id_and_returns_deeplinks() -> None:
    client = TestClient(create_app())

    launch_response = client.post(
        "/api/launch/train",
        json={
            "dataset_preset": "smoke",
            "template_id": "registry::elastic_net",
            "template_overrides": {"alpha": 0.002},
            "trainer_preset": "fast",
            "seed": 7,
            "experiment_name": "api-test-template-train",
            "run_id_prefix": "api-test-template-train",
        },
    )

    assert launch_response.status_code == 200
    launch_payload = launch_response.json()
    assert launch_payload["job_id"]
    assert launch_payload["job_api_path"]

    job_payload = _wait_for_job(client, launch_payload["job_id"])
    assert job_payload["status"] == "success"
    assert job_payload["result"]["run_ids"]
    assert job_payload["result"]["result_links"]
    assert "run_detail" in job_payload["result"]["deeplinks"]


def test_model_templates_crud_endpoints() -> None:
    client = TestClient(create_app())

    list_response = client.get("/api/models/templates")
    assert list_response.status_code == 200
    list_payload = list_response.json()
    assert list_payload["model_options_source"] == "registry"
    assert list_payload["items"]

    create_response = client.post(
        "/api/models/templates",
        json={
            "name": "custom elastic net",
            "model_name": "elastic_net",
            "description": "for webapi test",
            "hyperparams": {"alpha": 0.0015},
            "trainer_preset": "fast",
            "dataset_preset": "smoke",
        },
    )
    assert create_response.status_code == 200
    created = create_response.json()
    template_id = created["template_id"]

    get_response = client.get(f"/api/models/templates/{template_id}")
    assert get_response.status_code == 200
    assert get_response.json()["name"] == "custom elastic net"

    patch_response = client.patch(
        f"/api/models/templates/{template_id}",
        json={"description": "updated description"},
    )
    assert patch_response.status_code == 200
    assert patch_response.json()["description"] == "updated description"

    delete_response = client.delete(f"/api/models/templates/{template_id}")
    assert delete_response.status_code == 204


def test_trained_models_endpoints() -> None:
    client = TestClient(create_app())

    list_response = client.get("/api/models/trained")
    assert list_response.status_code == 200
    list_payload = list_response.json()
    assert "items" in list_payload

    detail_response = client.get("/api/models/trained/smoke-train-run")
    assert detail_response.status_code == 200
    assert detail_response.json()["run_id"] == "smoke-train-run"

    note_response = client.patch(
        "/api/models/trained/smoke-train-run/note",
        json={"note": "keep as baseline"},
    )
    assert note_response.status_code == 200
    assert note_response.json()["note"] == "keep as baseline"


def test_dataset_and_ohlcv_endpoints() -> None:
    client = TestClient(create_app())

    list_response = client.get("/api/datasets", params={"page": 1, "per_page": 200})
    assert list_response.status_code == 200
    list_payload = list_response.json()
    assert list_payload["items"]
    assert list_payload["items"][0]["display_name"]
    assert "row_count" in list_payload["items"][0]
    assert "feature_count" in list_payload["items"][0]

    dataset_id = list_payload["items"][0]["dataset_id"]
    detail_response = client.get(f"/api/datasets/{dataset_id}")
    assert detail_response.status_code == 200
    detail_payload = detail_response.json()
    assert detail_payload["dataset"]["dataset_id"] == dataset_id
    assert detail_payload["display_name"]
    assert "feature_groups" in detail_payload
    assert "quality_summary" in detail_payload
    assert isinstance(detail_payload["glossary_hints"], list)

    bars_response = client.get(f"/api/datasets/{dataset_id}/ohlcv")
    assert bars_response.status_code == 200
    bars_payload = bars_response.json()
    assert "items" in bars_payload


def test_dataset_registry_endpoints_expose_facets_slices_series_and_dependencies() -> None:
    client = TestClient(create_app())

    list_response = client.get("/api/datasets", params={"page": 1, "per_page": 200})
    assert list_response.status_code == 200
    dataset_id = list_response.json()["items"][0]["dataset_id"]

    facets_response = client.get("/api/datasets/facets")
    assert facets_response.status_code == 200
    facets_payload = facets_response.json()
    assert facets_payload["domains"]
    assert facets_payload["dataset_types"]

    slices_response = client.get(f"/api/datasets/{dataset_id}/slices")
    assert slices_response.status_code == 200
    slices_payload = slices_response.json()
    assert slices_payload["dataset_id"] == dataset_id
    assert any(item["slice_id"] == "full_dataset" for item in slices_payload["items"])

    series_response = client.get(f"/api/datasets/{dataset_id}/series")
    assert series_response.status_code == 200
    series_payload = series_response.json()
    assert series_payload["dataset_id"] == dataset_id
    assert series_payload["items"]

    dependencies_response = client.get(f"/api/datasets/{dataset_id}/dependencies")
    assert dependencies_response.status_code == 200
    dependencies_payload = dependencies_response.json()
    assert dependencies_payload["dataset_id"] == dataset_id
    assert isinstance(dependencies_payload["items"], list)


def test_system_recommended_dataset_delete_is_blocked() -> None:
    app = create_app()
    client = TestClient(app)

    response = client.delete("/api/datasets/smoke_dataset")

    assert response.status_code == 409
    payload = response.json()
    assert "系统推荐数据集" in payload["detail"]

    dependencies_response = client.get("/api/datasets/smoke_dataset/dependencies")
    assert dependencies_response.status_code == 200
    dependencies_payload = dependencies_response.json()
    assert dependencies_payload["can_delete"] is False
    assert "系统推荐数据集" in dependencies_payload["deletion_reason"]


def test_backtest_delete_removes_detail_and_list_entry_but_keeps_run() -> None:
    app = create_app()
    client = TestClient(app)

    launch_response = client.post(
        "/api/launch/backtest",
        json={
            "run_id": "smoke-train-run",
            "dataset_preset": "smoke",
            "prediction_scope": "test",
            "strategy_preset": "sign",
            "portfolio_preset": "research_default",
            "cost_preset": "standard",
            "benchmark_symbol": "BTCUSDT",
        },
    )
    assert launch_response.status_code == 200
    job_payload = _wait_for_job(client, launch_response.json()["job_id"])
    assert job_payload["status"] == "success"

    list_response = client.get("/api/backtests", params={"page": 1, "per_page": 50})
    assert list_response.status_code == 200
    items = list_response.json()["items"]
    assert items

    backtest_id = items[0]["backtest_id"]
    detail_response = client.get(f"/api/backtests/{backtest_id}")
    assert detail_response.status_code == 200
    detail_payload = detail_response.json()
    run_id = detail_payload.get("run_id")

    delete_response = client.delete(f"/api/backtests/{backtest_id}")
    assert delete_response.status_code == 200
    delete_payload = delete_response.json()
    assert delete_payload["status"] == "deleted"

    missing_detail_response = client.get(f"/api/backtests/{backtest_id}")
    assert missing_detail_response.status_code == 404

    refreshed_list_response = client.get("/api/backtests", params={"page": 1, "per_page": 50})
    assert refreshed_list_response.status_code == 200
    listed_ids = {item["backtest_id"] for item in refreshed_list_response.json()["items"]}
    assert backtest_id not in listed_ids

    if run_id:
        run_detail_response = client.get(f"/api/runs/{run_id}")
        assert run_detail_response.status_code == 200
        related_ids = {item["backtest_id"] for item in run_detail_response.json()["related_backtests"]}
        assert backtest_id not in related_ids


def test_dataset_delete_physically_removes_unreferenced_dataset_artifacts() -> None:
    app = create_app()
    client = TestClient(app)

    request_response = client.post(
        "/api/datasets/requests",
        json={
            "request_name": "Delete Candidate Dataset",
            "data_domain": "market",
            "asset_mode": "single_asset",
            "time_window": {
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-05T00:00:00Z",
            },
            "symbol_selector": {
                "symbol_type": "spot",
                "selection_mode": "explicit",
                "symbols": ["BTCUSDT"],
                "symbol_count": 1,
                "tags": [],
            },
            "source_vendor": "internal_smoke",
            "exchange": "binance",
            "frequency": "1h",
            "filters": {},
            "build_config": {
                "feature_set_id": "baseline_market_features",
                "label_horizon": 1,
                "label_kind": "regression",
                "split_strategy": "time_series",
                "sample_policy": {},
                "alignment_policy": {},
                "missing_feature_policy": {},
            },
        },
    )
    assert request_response.status_code == 200
    request_job_payload = _wait_for_job(client, request_response.json()["job_id"])
    assert request_job_payload["status"] == "success"

    dataset_id = "delete_candidate_dataset"
    artifact_root = app.state.services.jobs.artifact_root
    dataset_ref_path = artifact_root / "datasets" / f"{dataset_id}_dataset_ref.json"
    assert dataset_ref_path.exists()

    delete_response = client.delete(f"/api/datasets/{dataset_id}")
    assert delete_response.status_code == 200
    delete_payload = delete_response.json()
    assert delete_payload["status"] == "deleted"
    assert delete_payload["deleted_files"]
    assert not dataset_ref_path.exists()

    detail_response = client.get(f"/api/datasets/{dataset_id}")
    assert detail_response.status_code == 404


def test_dataset_request_uses_existing_job_system() -> None:
    client = TestClient(create_app())

    options_response = client.get("/api/datasets/request-options")
    assert options_response.status_code == 200
    options_payload = options_response.json()
    assert options_payload["constraints"]["request_flow"] == "api_datasets_requests_via_jobs"
    assert options_payload["selection_modes"]
    assert options_payload["sample_policies"]
    assert options_payload["alignment_policies"]
    assert options_payload["missing_feature_policies"]
    assert set(options_payload["constraints"]["current_supported_domains"]) >= {
        "market",
        "macro",
        "on_chain",
    }

    launch_response = client.post(
        "/api/datasets/requests",
        json={
            "request_name": "Smoke request from datasets page",
            "data_domain": "market",
            "dataset_type": "training_panel",
            "asset_mode": "single_asset",
            "time_window": {
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-05T00:00:00Z",
            },
            "symbol_selector": {
                "symbol_type": "spot",
                "selection_mode": "explicit",
                "symbols": ["BTCUSDT"],
                "symbol_count": 1,
                "tags": [],
            },
            "selection_mode": "explicit",
            "source_vendor": "internal_smoke",
            "exchange": "binance",
            "frequency": "1h",
            "filters": {},
            "build_config": {
                "feature_set_id": "baseline_market_features",
                "label_horizon": 1,
                "label_kind": "regression",
                "split_strategy": "time_series",
                "sample_policy_name": "training_panel_strict",
                "alignment_policy_name": "event_time_inner",
                "missing_feature_policy_name": "drop_if_missing",
                "sample_policy": {},
                "alignment_policy": {},
                "missing_feature_policy": {},
            },
        },
    )

    assert launch_response.status_code == 200
    launch_payload = launch_response.json()
    assert launch_payload["job_id"]
    assert launch_payload["job_api_path"]

    job_payload = _wait_for_job(client, launch_payload["job_id"])
    assert job_payload["status"] == "success"
    assert job_payload["result"]["dataset_id"] == "smoke_request_from_datasets_page"
    assert any(
        item["kind"] == "dataset_detail" for item in job_payload["result"]["result_links"]
    )

    readiness_response = client.get("/api/datasets/smoke_request_from_datasets_page/readiness")
    assert readiness_response.status_code == 200
    readiness_payload = readiness_response.json()
    assert readiness_payload["dataset_id"] == "smoke_request_from_datasets_page"
    assert readiness_payload["readiness_status"] in {"ready", "warning"}

    training_response = client.get("/api/datasets/training")
    assert training_response.status_code == 200
    training_payload = training_response.json()
    assert training_payload["total"] >= 1
    assert any(
        item["dataset_id"] == "smoke_request_from_datasets_page"
        for item in training_payload["items"]
    )


def test_sentiment_dataset_request_materializes_training_dataset_and_supports_train_launch() -> None:
    client = TestClient(create_app())
    suffix = str(int(time.time() * 1000))
    dataset_id = f"btc_sentiment_smoke_{suffix}"

    options_response = client.get("/api/datasets/request-options")
    assert options_response.status_code == 200
    sentiment_capability = options_response.json()["domain_capabilities"]["sentiment_events"]
    assert sentiment_capability["supports_real_ingestion"] is True
    assert {"gnews", "reddit_archive", "news_archive"} <= set(
        sentiment_capability["supported_vendors"]
    )
    assert sentiment_capability["supported_frequencies"] == ["1h"]

    request_response = client.post(
        "/api/datasets/requests",
        json=_sentiment_dataset_request_payload(dataset_id),
    )
    assert request_response.status_code == 200

    request_job_payload = _wait_for_job(client, request_response.json()["job_id"])
    assert request_job_payload["status"] == "success"
    assert request_job_payload["result"]["dataset_id"] == dataset_id

    readiness_response = client.get(f"/api/datasets/{dataset_id}/readiness")
    assert readiness_response.status_code == 200
    readiness_payload = readiness_response.json()
    assert readiness_payload["dataset_id"] == dataset_id
    assert readiness_payload["readiness_status"] == "ready"
    assert readiness_payload["blocking_issues"] == []

    detail_response = client.get(f"/api/datasets/{dataset_id}")
    assert detail_response.status_code == 200
    detail_payload = detail_response.json()
    assert detail_payload["dataset"]["data_domain"] == "sentiment_events"
    assert detail_payload["dataset"]["source_vendor"] == "news_archive"
    assert detail_payload["dataset"]["dataset_type"] == "training_panel"

    browser_response = client.get(
        "/api/datasets",
        params={"data_domain": "sentiment_events", "page": 1, "per_page": 50},
    )
    assert browser_response.status_code == 200
    assert any(item["dataset_id"] == dataset_id for item in browser_response.json()["items"])

    training_response = client.get("/api/datasets/training")
    assert training_response.status_code == 200
    assert any(item["dataset_id"] == dataset_id for item in training_response.json()["items"])

    train_response = client.post(
        "/api/launch/train",
        json={
            "dataset_id": dataset_id,
            "dataset_preset": "smoke",
            "template_id": "registry::elastic_net",
            "trainer_preset": "fast",
            "seed": 7,
            "experiment_name": f"sentiment-train-{suffix}",
            "run_id_prefix": f"sentiment-train-{suffix}",
        },
    )
    assert train_response.status_code == 200
    train_job_payload = _wait_for_job(client, train_response.json()["job_id"])
    assert train_job_payload["status"] == "success"
    assert train_job_payload["result"]["dataset_id"] == dataset_id
    assert train_job_payload["result"]["run_ids"]


def test_dataset_fusion_builds_materialized_training_panel_and_blocks_base_delete() -> None:
    app = create_app()
    client = TestClient(app)
    suffix = str(int(time.time() * 1000))
    base_dataset_id = f"fusion_base_dataset_{suffix}"
    fusion_request_name = f"market_macro_onchain_{suffix}"

    def fake_fetch_series_points(
        *,
        data_domain: str,
        identifier: str,
        vendor: str,
        frequency: str,
        start_time: datetime,
        end_time: datetime,
        options: dict[str, object] | None = None,
    ) -> tuple[list[NormalizedSeriesPoint], str]:
        del options
        if data_domain == "macro":
            return (
                [
                    _series_point(
                        event_time=start_time,
                        identifier=identifier,
                        data_domain=data_domain,
                        vendor=vendor,
                        frequency=frequency,
                        value=5.25,
                    ),
                    _series_point(
                        event_time=start_time + timedelta(days=2),
                        identifier=identifier,
                        data_domain=data_domain,
                        vendor=vendor,
                        frequency=frequency,
                        value=5.0,
                    ),
                ],
                "live_fetch",
            )
        if data_domain == "on_chain":
            return (
                [
                    _series_point(
                        event_time=start_time,
                        identifier=identifier,
                        data_domain=data_domain,
                        vendor=vendor,
                        frequency=frequency,
                        value=1_000_000.0,
                        metric_name="tvl",
                    ),
                    _series_point(
                        event_time=end_time - timedelta(hours=1),
                        identifier=identifier,
                        data_domain=data_domain,
                        vendor=vendor,
                        frequency=frequency,
                        value=1_250_000.0,
                        metric_name="tvl",
                    ),
                ],
                "live_fetch",
            )
        raise AssertionError(f"Unexpected fusion source request: {data_domain}/{vendor}/{identifier}")

    app.state.services.workbench.facade.runtime.ingestion_service.fetch_series_points = fake_fetch_series_points

    request_response = client.post(
        "/api/datasets/requests",
        json={
            "request_name": base_dataset_id,
            "data_domain": "market",
            "dataset_type": "training_panel",
            "asset_mode": "single_asset",
            "time_window": {
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-05T00:00:00Z",
            },
            "symbol_selector": {
                "symbol_type": "spot",
                "selection_mode": "explicit",
                "symbols": ["BTCUSDT"],
                "symbol_count": 1,
                "tags": [],
            },
            "selection_mode": "explicit",
            "source_vendor": "internal_smoke",
            "exchange": "binance",
            "frequency": "1h",
            "filters": {},
            "build_config": {
                "feature_set_id": "baseline_market_features",
                "label_horizon": 1,
                "label_kind": "regression",
                "split_strategy": "time_series",
                "sample_policy_name": "training_panel_strict",
                "alignment_policy_name": "event_time_inner",
                "missing_feature_policy_name": "drop_if_missing",
                "sample_policy": {},
                "alignment_policy": {},
                "missing_feature_policy": {},
            },
        },
    )
    assert request_response.status_code == 200
    request_job_payload = _wait_for_job(client, request_response.json()["job_id"])
    assert request_job_payload["status"] == "success"

    fusion_response = client.post(
        "/api/datasets/fusions",
        json={
            "request_name": fusion_request_name,
            "base_dataset_id": base_dataset_id,
            "alignment_policy_name": "available_time_safe_asof",
            "missing_feature_policy_name": "drop_if_missing",
            "sources": [
                {
                    "data_domain": "macro",
                    "vendor": "fred",
                    "identifier": "DFF",
                    "frequency": "1d",
                    "feature_name": "macro_dff_value",
                },
                {
                    "data_domain": "on_chain",
                    "vendor": "defillama",
                    "identifier": "ethereum",
                    "frequency": "1d",
                    "feature_name": "on_chain_ethereum_tvl",
                    "metric_name": "tvl",
                },
            ],
        },
    )

    assert fusion_response.status_code == 200
    fusion_payload = fusion_response.json()
    assert fusion_payload["status"] == "created"
    fusion_dataset_id = fusion_payload["dataset_id"]
    assert fusion_dataset_id == f"{fusion_request_name}_fusion"
    assert fusion_payload["readiness"]["readiness_status"] in {"ready", "warning"}
    assert fusion_payload["training_summary"]["dataset_type"] == "fusion_training_panel"

    detail_response = client.get(f"/api/datasets/{fusion_dataset_id}")
    assert detail_response.status_code == 200
    detail_payload = detail_response.json()
    assert detail_payload["dataset"]["dataset_type"] == "fusion_training_panel"
    assert detail_payload["dataset"]["data_domain"] == "market"

    series_response = client.get(f"/api/datasets/{fusion_dataset_id}/series")
    assert series_response.status_code == 200
    series_payload = series_response.json()
    assert any(item["series_kind"] == "fusion_input_series" for item in series_payload["items"])
    assert {item["data_domain"] for item in series_payload["items"]} >= {"macro", "on_chain"}

    training_response = client.get("/api/datasets/training")
    assert training_response.status_code == 200
    training_payload = training_response.json()
    fusion_training = next(
        item for item in training_payload["items"] if item["dataset_id"] == fusion_dataset_id
    )
    assert fusion_training["dataset_type"] == "fusion_training_panel"

    delete_response = client.delete(f"/api/datasets/{base_dataset_id}")
    assert delete_response.status_code == 200
    delete_payload = delete_response.json()
    assert delete_payload["status"] == "deleted"
    assert any(
        item["dependency_kind"] == "fusion_dataset"
        and item["dependency_id"] == fusion_dataset_id
        for item in delete_payload["blocking_items"]
    )


def test_multi_domain_dataset_request_materializes_merged_dataset() -> None:
    app = create_app()
    client = TestClient(app)
    suffix = str(int(time.time() * 1000))
    request_name = f"multi_domain_request_{suffix}"
    start_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    end_time = start_time + timedelta(hours=3)

    _patch_multi_domain_ingestion(app)

    request_response = client.post(
        "/api/datasets/requests",
        json=_multi_domain_request_payload(
            request_name,
            start_time=start_time,
            end_time=end_time,
            merge_policy_name="strict_timestamp_inner",
        ),
    )
    assert request_response.status_code == 200
    job_payload = _wait_for_job(client, request_response.json()["job_id"])
    assert job_payload["status"] == "success"

    dataset_id = job_payload["result"]["dataset_id"]
    detail_response = client.get(f"/api/datasets/{dataset_id}")
    assert detail_response.status_code == 200
    detail_payload = detail_response.json()
    assert detail_payload["dataset"]["dataset_type"] == "training_panel"
    assert detail_payload["dataset"]["data_domain"] == "market"
    assert set(detail_payload["dataset"]["data_domains"]) == {
        "market",
        "macro",
        "on_chain",
    }

    readiness_response = client.get(f"/api/datasets/{dataset_id}/readiness")
    assert readiness_response.status_code == 200
    readiness_payload = readiness_response.json()
    assert readiness_payload["dataset_id"] == dataset_id
    assert set(readiness_payload["data_domains"]) == {
        "market",
        "macro",
        "on_chain",
    }
    assert readiness_payload["readiness_status"] in {"ready", "warning"}

    training_response = client.get("/api/datasets/training")
    assert training_response.status_code == 200
    training_payload = training_response.json()
    assert any(
        item["dataset_id"] == dataset_id for item in training_payload["items"]
    )

    list_response = client.get("/api/datasets", params={"page": 1, "per_page": 200})
    assert list_response.status_code == 200
    assert any(
        item["dataset_id"] == dataset_id for item in list_response.json()["items"]
    )


def test_multi_domain_request_without_market_source_fails() -> None:
    app = create_app()
    client = TestClient(app)
    suffix = str(int(time.time() * 1000))
    request_name = f"multi_domain_request_no_market_{suffix}"
    start_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    end_time = start_time + timedelta(hours=3)

    _patch_multi_domain_ingestion(app)
    payload = _multi_domain_request_payload(
        request_name,
        start_time=start_time,
        end_time=end_time,
    )
    payload["sources"] = [
        source for source in payload["sources"] if source["data_domain"] != "market"
    ]

    request_response = client.post("/api/datasets/requests", json=payload)
    assert request_response.status_code == 200
    job_payload = _wait_for_job(client, request_response.json()["job_id"])
    assert job_payload["status"] == "failed"
    assert "market" in (job_payload["error_message"] or "").lower()


def test_multi_domain_request_with_frequency_mismatch_fails() -> None:
    app = create_app()
    client = TestClient(app)
    suffix = str(int(time.time() * 1000))
    request_name = f"multi_domain_request_freq_mismatch_{suffix}"
    start_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    end_time = start_time + timedelta(hours=3)

    _patch_multi_domain_ingestion(app)
    payload = _multi_domain_request_payload(
        request_name,
        start_time=start_time,
        end_time=end_time,
    )
    for source in payload["sources"]:
        if source["data_domain"] == "macro":
            source["frequency"] = "1d"

    request_response = client.post("/api/datasets/requests", json=payload)
    assert request_response.status_code == 200
    job_payload = _wait_for_job(client, request_response.json()["job_id"])
    assert job_payload["status"] == "failed"
    assert "frequency" in (job_payload["error_message"] or "").lower()


def test_multi_domain_request_with_timestamp_mismatch_fails() -> None:
    app = create_app()
    client = TestClient(app)
    suffix = str(int(time.time() * 1000))
    request_name = f"multi_domain_request_ts_mismatch_{suffix}"
    start_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    end_time = start_time + timedelta(hours=3)

    _patch_multi_domain_ingestion(app, macro_offset=timedelta(minutes=30))
    request_response = client.post(
        "/api/datasets/requests",
        json=_multi_domain_request_payload(
            request_name,
            start_time=start_time,
            end_time=end_time,
            merge_policy_name="strict_timestamp_inner",
        ),
    )
    assert request_response.status_code == 200
    job_payload = _wait_for_job(client, request_response.json()["job_id"])
    assert job_payload["status"] == "failed"
    assert "timestamp" in (job_payload["error_message"] or "").lower()


def test_multi_domain_request_with_asof_alignment_succeeds_on_timestamp_mismatch() -> None:
    app = create_app()
    client = TestClient(app)
    suffix = str(int(time.time() * 1000))
    request_name = f"multi_domain_request_asof_{suffix}"
    start_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    end_time = start_time + timedelta(hours=3)

    _patch_multi_domain_ingestion(
        app,
        macro_offset=timedelta(minutes=-30),
        on_chain_offset=timedelta(minutes=-30),
    )
    request_response = client.post(
        "/api/datasets/requests",
        json=_multi_domain_request_payload(
            request_name,
            start_time=start_time,
            end_time=end_time,
            merge_policy_name="available_time_safe_asof",
        ),
    )
    assert request_response.status_code == 200
    job_payload = _wait_for_job(client, request_response.json()["job_id"])
    assert job_payload["status"] == "success"
    assert job_payload["result"]["dataset_id"]


def test_multi_domain_request_connector_error_fails() -> None:
    app = create_app()
    client = TestClient(app)
    suffix = str(int(time.time() * 1000))
    request_name = f"multi_domain_request_conn_error_{suffix}"
    start_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    end_time = start_time + timedelta(hours=3)

    macro_error = DataConnectorError(
        data_domain="macro",
        vendor="fred",
        identifier="DFF",
        message="FRED_API_KEY is not configured.",
        retryable=False,
        code="credentials_missing",
    )
    _patch_multi_domain_ingestion(app, macro_error=macro_error)

    request_response = client.post(
        "/api/datasets/requests",
        json=_multi_domain_request_payload(
            request_name,
            start_time=start_time,
            end_time=end_time,
        ),
    )
    assert request_response.status_code == 200
    job_payload = _wait_for_job(client, request_response.json()["job_id"])
    assert job_payload["status"] == "failed"
    assert "fred" in (job_payload["error_message"] or "").lower()


def test_multi_domain_request_empty_connector_result_fails() -> None:
    app = create_app()
    client = TestClient(app)
    suffix = str(int(time.time() * 1000))
    request_name = f"multi_domain_request_empty_result_{suffix}"
    start_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    end_time = start_time + timedelta(hours=3)

    _patch_multi_domain_ingestion(app, macro_empty=True)
    request_response = client.post(
        "/api/datasets/requests",
        json=_multi_domain_request_payload(
            request_name,
            start_time=start_time,
            end_time=end_time,
        ),
    )
    assert request_response.status_code == 200
    job_payload = _wait_for_job(client, request_response.json()["job_id"])
    assert job_payload["status"] == "failed"
    assert "empty" in (job_payload["error_message"] or "").lower()


def test_merged_dataset_can_be_trained_and_backtested() -> None:
    app = create_app()
    client = TestClient(app)
    suffix = str(int(time.time() * 1000))
    request_name = f"multi_domain_train_{suffix}"
    start_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    end_time = start_time + timedelta(hours=3)

    _patch_multi_domain_ingestion(app)
    request_response = client.post(
        "/api/datasets/requests",
        json=_multi_domain_request_payload(
            request_name,
            start_time=start_time,
            end_time=end_time,
        ),
    )
    assert request_response.status_code == 200
    dataset_job = _wait_for_job(client, request_response.json()["job_id"])
    assert dataset_job["status"] == "success"

    dataset_id = dataset_job["result"]["dataset_id"]
    train_response = client.post(
        "/api/launch/train",
        json={
            "dataset_id": dataset_id,
            "template_id": "registry::elastic_net",
            "trainer_preset": "fast",
            "experiment_name": f"multi_domain_train_launch_{suffix}",
            "run_id_prefix": f"multi_domain_train_launch_{suffix}",
        },
    )
    assert train_response.status_code == 200
    train_job = _wait_for_job(client, train_response.json()["job_id"], timeout_seconds=120.0)
    assert train_job["status"] == "success"
    assert train_job["result"]["dataset_id"] == dataset_id
    run_id = train_job["result"]["run_ids"][0]

    run_detail_response = client.get(f"/api/runs/{run_id}")
    assert run_detail_response.status_code == 200
    run_detail = run_detail_response.json()
    assert run_detail["dataset_id"] == dataset_id

    backtest_response = client.post(
        "/api/launch/backtest",
        json={
            "run_id": run_id,
            "dataset_id": dataset_id,
            "prediction_scope": "full",
            "strategy_preset": "sign",
            "portfolio_preset": "research_default",
            "cost_preset": "standard",
        },
    )
    assert backtest_response.status_code == 200
    backtest_job = _wait_for_job(client, backtest_response.json()["job_id"])
    assert backtest_job["status"] == "success"
    assert backtest_job["result"]["dataset_id"] == dataset_id
    backtest_id = backtest_job["result"]["backtest_ids"][0]

    backtest_detail_response = client.get(f"/api/backtests/{backtest_id}")
    assert backtest_detail_response.status_code == 200
    backtest_detail = backtest_detail_response.json()
    assert backtest_detail["run_id"] == run_id
    assert backtest_detail["passed_consistency_checks"] is not None

    backtests_response = client.get("/api/backtests", params={"page": 1, "per_page": 50})
    assert backtests_response.status_code == 200
    backtests_payload = backtests_response.json()
    listed = next(item for item in backtests_payload["items"] if item["backtest_id"] == backtest_id)
    assert listed["status"] == "success"
    assert listed["passed_consistency_checks"] == backtest_detail["passed_consistency_checks"]


def test_backtest_schema_mismatch_fails() -> None:
    app = create_app()
    client = TestClient(app)
    suffix = str(int(time.time() * 1000))
    request_name = f"multi_domain_backtest_mismatch_{suffix}"
    start_time = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    end_time = start_time + timedelta(hours=3)

    _patch_multi_domain_ingestion(app)
    request_response = client.post(
        "/api/datasets/requests",
        json=_multi_domain_request_payload(
            request_name,
            start_time=start_time,
            end_time=end_time,
        ),
    )
    assert request_response.status_code == 200
    dataset_job = _wait_for_job(client, request_response.json()["job_id"])
    assert dataset_job["status"] == "success"

    dataset_id = dataset_job["result"]["dataset_id"]
    train_response = client.post(
        "/api/launch/train",
        json={
            "dataset_id": dataset_id,
            "template_id": "registry::elastic_net",
            "trainer_preset": "fast",
            "experiment_name": f"multi_domain_mismatch_train_{suffix}",
            "run_id_prefix": f"multi_domain_mismatch_train_{suffix}",
        },
    )
    assert train_response.status_code == 200
    train_job = _wait_for_job(client, train_response.json()["job_id"], timeout_seconds=120.0)
    assert train_job["status"] == "success"
    run_id = train_job["result"]["run_ids"][0]

    backtest_response = client.post(
        "/api/launch/backtest",
        json={
            "run_id": run_id,
            "dataset_id": "smoke_dataset",
            "prediction_scope": "full",
            "strategy_preset": "sign",
            "portfolio_preset": "research_default",
            "cost_preset": "standard",
        },
    )
    assert backtest_response.status_code == 200
    backtest_job = _wait_for_job(client, backtest_response.json()["job_id"])
    assert backtest_job["status"] == "failed"
    assert "schema" in (backtest_job["error_message"] or "").lower()


def test_backtest_history_preserves_multiple_records_for_one_run() -> None:
    client = TestClient(create_app())
    suffix = str(int(time.time() * 1000))

    train_response = client.post(
        "/api/launch/train",
        json={
            "dataset_preset": "smoke",
            "template_id": "registry::elastic_net",
            "trainer_preset": "fast",
            "experiment_name": f"multi-backtest-history-{suffix}",
            "run_id_prefix": f"multi-backtest-history-{suffix}",
        },
    )
    assert train_response.status_code == 200
    train_job = _wait_for_job(client, train_response.json()["job_id"], timeout_seconds=120.0)
    assert train_job["status"] == "success"
    run_id = train_job["result"]["run_ids"][0]

    backtest_ids: list[str] = []
    for prediction_scope in ("full", "test"):
        backtest_response = client.post(
            "/api/launch/backtest",
            json={
                "run_id": run_id,
                "prediction_scope": prediction_scope,
                "strategy_preset": "sign",
                "portfolio_preset": "research_default",
                "cost_preset": "standard",
            },
        )
        assert backtest_response.status_code == 200
        backtest_job = _wait_for_job(client, backtest_response.json()["job_id"])
        assert backtest_job["status"] == "success"
        assert backtest_job["result"]["dataset_id"] == "smoke_dataset"
        backtest_ids.append(backtest_job["result"]["backtest_ids"][0])

    assert len(set(backtest_ids)) == 2

    backtests_response = client.get(
        "/api/backtests",
        params={"page": 1, "per_page": 100},
    )
    assert backtests_response.status_code == 200
    listed_ids = {
        item["backtest_id"]
        for item in backtests_response.json()["items"]
        if item.get("run_id") == run_id
    }
    assert set(backtest_ids) <= listed_ids

    for backtest_id in backtest_ids:
        detail_response = client.get(f"/api/backtests/{backtest_id}")
        assert detail_response.status_code == 200
        detail_payload = detail_response.json()
        assert detail_payload["run_id"] == run_id
        assert detail_payload["research"] is not None
        assert detail_payload["simulation"] is not None

    run_detail_response = client.get(f"/api/runs/{run_id}")
    assert run_detail_response.status_code == 200
    related_ids = {
        item["backtest_id"] for item in run_detail_response.json()["related_backtests"]
    }
    assert set(backtest_ids) <= related_ids

    dependencies_response = client.get("/api/datasets/smoke_dataset/dependencies")
    assert dependencies_response.status_code == 200
    dependency_ids = {
        item["dependency_id"]
        for item in dependencies_response.json()["items"]
        if item["dependency_kind"] == "backtest" and item["metadata"].get("run_id") == run_id
    }
    assert set(backtest_ids) <= dependency_ids


def test_launch_backtest_dataset_preset_is_only_used_as_fallback() -> None:
    client = TestClient(create_app())

    options_response = client.get("/api/launch/backtest/options")
    assert options_response.status_code == 200
    options_payload = options_response.json()
    assert options_payload["default_mode"] == "official"
    assert options_payload["official_template_id"] == "system::official_backtest_protocol_v1"
    assert options_payload["template_options"][0]["official"] is True
    assert options_payload["template_options"][0]["read_only"] is True
    assert options_payload["template_options"][0]["fixed_prediction_scope"] == "test"
    assert {item["value"] for item in options_payload["dataset_presets"]} == {
        "smoke",
        "real_benchmark",
    }
    assert (
        options_payload["constraints"]["dataset_selector"]["priority"]
        == "dataset_id_gt_run_manifest_gt_dataset_preset"
    )
    assert options_payload["default_official_window_days"] == 180
    assert [int(item["value"]) for item in options_payload["official_window_options"]] == [
        30,
        90,
        180,
        365,
    ]

    suffix = str(int(time.time() * 1000))
    train_response = client.post(
        "/api/launch/train",
        json={
            "dataset_preset": "smoke",
            "template_id": "registry::elastic_net",
            "trainer_preset": "fast",
            "experiment_name": f"backtest-preset-fallback-{suffix}",
            "run_id_prefix": f"backtest-preset-fallback-{suffix}",
        },
    )
    assert train_response.status_code == 200
    train_job = _wait_for_job(client, train_response.json()["job_id"], timeout_seconds=120.0)
    assert train_job["status"] == "success"
    run_id = train_job["result"]["run_ids"][0]

    backtest_response = client.post(
        "/api/launch/backtest",
        json={
            "run_id": run_id,
            "mode": "custom",
            "dataset_preset": "real_benchmark",
            "prediction_scope": "full",
            "strategy_preset": "sign",
            "portfolio_preset": "research_default",
            "cost_preset": "standard",
        },
    )
    assert backtest_response.status_code == 200
    backtest_job = _wait_for_job(client, backtest_response.json()["job_id"])
    assert backtest_job["status"] == "success"
    assert backtest_job["result"]["dataset_id"] == "smoke_dataset"


def test_dataset_readiness_and_nlp_inspection_expose_official_nlp_gate_fields() -> None:
    app = create_app()
    client = TestClient(app)
    _inject_failed_official_nlp_gate(app, "smoke_dataset")

    readiness_response = client.get("/api/datasets/smoke_dataset/readiness")
    assert readiness_response.status_code == 200
    readiness_payload = readiness_response.json()
    assert readiness_payload["official_template_eligible"] is True
    assert readiness_payload["official_nlp_gate_status"] == "failed"
    assert readiness_payload["archival_nlp_source_only"] is False
    assert readiness_payload["nlp_requested_start_time"] == readiness_payload["market_window_start_time"]
    assert readiness_payload["nlp_requested_end_time"] == readiness_payload["market_window_end_time"]
    assert readiness_payload["official_nlp_gate_reasons"]
    assert readiness_payload["nlp_test_coverage_ratio"] is not None

    inspection_response = client.get("/api/datasets/smoke_dataset/nlp-inspection")
    assert inspection_response.status_code == 200
    inspection_payload = inspection_response.json()
    assert inspection_payload["contains_nlp"] is True
    assert inspection_payload["official_template_gate_status"] == "failed"
    assert inspection_payload["archival_source_only"] is False
    assert inspection_payload["market_window_start_time"]
    assert inspection_payload["official_backtest_start_time"]
    assert inspection_payload["actual_start_time"]
    assert inspection_payload["source_vendors"] == ["gnews"]


def test_launch_official_backtest_blocks_failed_official_nlp_gate() -> None:
    app = create_app()
    client = TestClient(app)
    suffix = str(int(time.time() * 1000))
    train_response = client.post(
        "/api/launch/train",
        json={
            "dataset_id": "official_reddit_pullpush_multimodal_v2_fusion",
            "template_id": "registry::elastic_net",
            "trainer_preset": "fast",
            "experiment_name": f"official-nlp-gate-{suffix}",
            "run_id_prefix": f"official-nlp-gate-{suffix}",
        },
    )
    assert train_response.status_code == 200
    train_job = _wait_for_job(client, train_response.json()["job_id"], timeout_seconds=120.0)
    assert train_job["status"] == "success"
    run_id = train_job["result"]["run_ids"][0]

    _inject_failed_official_nlp_gate(app, "official_reddit_pullpush_multimodal_v2_fusion")

    backtest_response = client.post(
        "/api/launch/backtest",
        json={
            "run_id": run_id,
            "mode": "official",
            "strategy_preset": "sign",
            "portfolio_preset": "research_default",
            "cost_preset": "standard",
        },
    )
    assert backtest_response.status_code == 200
    backtest_job = _wait_for_job(client, backtest_response.json()["job_id"], timeout_seconds=120.0)
    assert backtest_job["status"] == "failed"
    assert "NLP quality gate" in (backtest_job["error_message"] or "")


def test_launch_official_backtest_uses_official_rolling_benchmarks() -> None:
    app = create_app()
    client = TestClient(app)
    suffix = str(int(time.time() * 1000))
    run_id = _launch_train_and_wait(
        client,
        dataset_id="baseline_real_benchmark_dataset",
        run_id_prefix=f"official-rolling-{suffix}",
    )

    backtest_response = client.post(
        "/api/launch/backtest",
        json={
            "run_id": run_id,
            "mode": "official",
            "official_window_days": 30,
            "strategy_preset": "sign",
            "portfolio_preset": "research_default",
            "cost_preset": "standard",
        },
    )
    assert backtest_response.status_code == 200
    backtest_job = _wait_for_job(client, backtest_response.json()["job_id"], timeout_seconds=300.0)
    assert backtest_job["status"] == "success"
    assert backtest_job["result"]["dataset_id"] == "baseline_real_benchmark_dataset"
    assert backtest_job["result"]["dataset_ids"] == ["baseline_real_benchmark_dataset"]

    backtest_id = backtest_job["result"]["backtest_ids"][0]
    backtest_detail_response = client.get(f"/api/backtests/{backtest_id}")
    assert backtest_detail_response.status_code == 200
    backtest_detail_payload = backtest_detail_response.json()
    assert backtest_detail_payload["dataset_id"].endswith("__official_30d")
    assert "baseline_real_benchmark_dataset" in backtest_detail_payload["dataset_ids"]
    assert any(item.endswith("__official_30d") for item in backtest_detail_payload["dataset_ids"])
    assert backtest_detail_payload["protocol"]["official_benchmark_version"]
    assert backtest_detail_payload["protocol"]["official_window_days"] == 30
    assert (
        backtest_detail_payload["protocol"]["official_market_dataset_id"]
        == "baseline_real_benchmark_dataset"
    )
    assert backtest_detail_payload["protocol"]["official_multimodal_dataset_id"] is None
    assert backtest_detail_payload["protocol"]["slice_coverage"][0].startswith(
        "official_reddit_pullpush_multimodal_v2_fusion:"
    )


def test_official_multimodal_readiness_uses_materialized_archival_sources() -> None:
    app = create_app()
    client = TestClient(app)

    readiness_response = client.get("/api/datasets/official_reddit_pullpush_multimodal_v2_fusion/readiness")

    assert readiness_response.status_code == 200
    readiness_payload = readiness_response.json()
    assert readiness_payload["archival_nlp_source_only"] is True
    assert readiness_payload["official_nlp_gate_status"] == "passed"


def test_official_preflight_rematerializes_stale_multimodal_benchmark() -> None:
    app = create_app()
    _mark_official_multimodal_benchmark_stale(app)
    assert (
        app.state.services.workbench.ensure_official_multimodal_benchmark()
        == "official_reddit_pullpush_multimodal_v2_fusion"
    )

    artifact_root = app.state.services.jobs.artifact_root
    store = LocalArtifactStore(artifact_root)
    official_manifest = store.read_model(
        str(artifact_root / "datasets" / "official_reddit_pullpush_multimodal_v2_fusion_dataset_manifest.json"),
        DatasetBuildManifest,
    )
    market_manifest = store.read_model(
        str(artifact_root / "datasets" / "baseline_real_benchmark_dataset_dataset_manifest.json"),
        DatasetBuildManifest,
    )
    acquisition_profile = dict(official_manifest.acquisition_profile or {})
    assert acquisition_profile.get("market_snapshot_version") == market_manifest.snapshot_version
    assert acquisition_profile.get("sentiment_snapshot_version") not in {
        None,
        "stale-sentiment-snapshot",
    }


def test_backtest_preflight_allows_current_multimodal_run_with_platform_compatible_schema() -> None:
    app = create_app()
    client = TestClient(app)

    response = client.post(
        "/api/launch/backtest/preflight",
        json={
            "run_id": "multimodal-compose-20260413144642-80a31c",
            "mode": "official",
            "template_id": "system::official_backtest_protocol_v1",
            "official_window_days": 30,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["compatible"] is True
    assert payload["official_multimodal_dataset_id"] == "official_reddit_pullpush_multimodal_v2_fusion"
    assert payload["official_market_dataset_id"] == "baseline_real_benchmark_dataset"
    assert payload["missing_official_feature_names"] == []
    assert payload["official_window_start_time"]
    assert payload["official_window_end_time"]
    assert payload["blocking_reasons"] == []


def test_backtest_preflight_reports_missing_nonstandard_nlp_feature() -> None:
    app = create_app()
    client = TestClient(app)
    metadata_path, original_text = _append_run_input_feature(
        app,
        run_id="workbench-train-20260413112342",
        feature_name="text_reddit_embedding_768",
    )

    try:
        response = client.post(
            "/api/launch/backtest/preflight",
            json={
                "run_id": "workbench-train-20260413112342",
                "mode": "official",
                "template_id": "system::official_backtest_protocol_v1",
                "official_window_days": 30,
            },
        )
    finally:
        metadata_path.write_text(original_text, encoding="utf-8")

    assert response.status_code == 200
    payload = response.json()
    assert payload["compatible"] is False
    assert payload["requires_text_features"] is True
    assert "text_reddit_embedding_768" in payload["required_feature_names"]
    assert payload["missing_official_feature_names"] == ["text_reddit_embedding_768"]
    assert any(
        "text_reddit_embedding_768" in reason for reason in payload["blocking_reasons"]
    )


def test_launch_official_backtest_reuses_preflight_failure_reason_for_missing_schema() -> None:
    app = create_app()
    client = TestClient(app)
    metadata_path, original_text = _append_run_input_feature(
        app,
        run_id="workbench-train-20260413112342",
        feature_name="text_reddit_embedding_768",
    )

    try:
        preflight_response = client.post(
            "/api/launch/backtest/preflight",
            json={
                "run_id": "workbench-train-20260413112342",
                "mode": "official",
                "template_id": "system::official_backtest_protocol_v1",
                "official_window_days": 30,
            },
        )
        assert preflight_response.status_code == 200
        preflight_payload = preflight_response.json()

        launch_response = client.post(
            "/api/launch/backtest",
            json={
                "run_id": "workbench-train-20260413112342",
                "mode": "official",
                "official_window_days": 30,
                "strategy_preset": "sign",
                "portfolio_preset": "research_default",
                "cost_preset": "standard",
            },
        )
        assert launch_response.status_code == 200
        launch_job = _wait_for_job(client, launch_response.json()["job_id"], timeout_seconds=120.0)
    finally:
        metadata_path.write_text(original_text, encoding="utf-8")

    assert preflight_payload["compatible"] is False
    assert preflight_payload["blocking_reasons"]
    assert launch_job["status"] == "failed"
    assert preflight_payload["blocking_reasons"][0] in (launch_job["error_message"] or "")


def test_launch_official_backtest_reports_missing_model_artifact_after_compatibility_checks_pass() -> None:
    app = create_app()
    client = TestClient(app)
    preflight_response = client.post(
        "/api/launch/backtest/preflight",
        json={
            "run_id": "multimodal-compose-20260413144642-80a31c",
            "mode": "official",
            "template_id": "system::official_backtest_protocol_v1",
            "official_window_days": 30,
        },
    )
    assert preflight_response.status_code == 200
    preflight_payload = preflight_response.json()
    assert preflight_payload["compatible"] is True

    launch_response = client.post(
        "/api/launch/backtest",
        json={
            "run_id": "multimodal-compose-20260413144642-80a31c",
            "mode": "official",
            "official_window_days": 30,
            "strategy_preset": "sign",
            "portfolio_preset": "research_default",
            "cost_preset": "standard",
        },
    )

    assert launch_response.status_code == 200
    launch_job = _wait_for_job(client, launch_response.json()["job_id"], timeout_seconds=180.0)
    assert launch_job["status"] == "failed"
    assert "Unable to resolve model artifact" in (launch_job["error_message"] or "")

def test_official_nlp_gate_ignores_zero_event_bars_for_coverage() -> None:
    app = create_app()
    client = TestClient(app)
    _inject_zero_event_official_gate_probe(app, "smoke_dataset")

    readiness_response = client.get("/api/datasets/smoke_dataset/readiness")
    assert readiness_response.status_code == 200
    readiness_payload = readiness_response.json()

    assert readiness_payload["official_nlp_gate_status"] == "failed"
    assert readiness_payload["nlp_test_coverage_ratio"] == 0.0


def test_dataset_download_returns_zip_archive() -> None:
    app = create_app()
    client = TestClient(app)

    response = client.get("/api/datasets/smoke_dataset/download")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/zip")
    assert "smoke_dataset.zip" in response.headers["content-disposition"]

    import io
    import zipfile

    archive = zipfile.ZipFile(io.BytesIO(response.content))
    assert any(name.endswith("smoke_dataset_dataset_ref.json") for name in archive.namelist())


def test_model_composition_allows_platform_compatible_sources() -> None:
    app = create_app()
    client = TestClient(app)
    suffix = str(int(time.time() * 1000))
    sentiment_dataset_name = f"platform_compat_sentiment_{suffix}"
    sentiment_response = client.post(
        "/api/datasets/requests",
        json=_sentiment_dataset_request_payload(sentiment_dataset_name),
    )
    assert sentiment_response.status_code == 200
    sentiment_job = _wait_for_job(client, sentiment_response.json()["job_id"], timeout_seconds=120.0)
    assert sentiment_job["status"] == "success"
    sentiment_dataset_id = sentiment_job["result"]["dataset_id"]

    market_run_id = f"platform-compat-market-{suffix}"
    nlp_run_id = f"platform-compat-nlp-{suffix}"
    _materialize_stub_run(
        app,
        run_id=market_run_id,
        dataset_id="smoke_dataset",
        feature_names=[
            "lag_return_1",
            "lag_return_2",
            "momentum_3",
            "realized_vol_3",
            "close_to_open",
            "range_frac",
            "volume_zscore",
            "volume_ratio_3",
        ],
    )
    _materialize_stub_run(
        app,
        run_id=nlp_run_id,
        dataset_id=sentiment_dataset_id,
        feature_names=[
            "news_event_count",
            "text_reddit_attention_zscore_24h",
            "text_reddit_body_len_mean_1h",
            "text_reddit_comment_count_1h",
            "text_reddit_controversiality_ratio_1h",
            "text_reddit_core_subreddit_ratio_1h",
            "text_reddit_negative_ratio_1h",
            "text_reddit_positive_ratio_1h",
            "text_reddit_score_mean_1h",
            "text_reddit_score_sum_1h",
            "text_reddit_sentiment_mean_1h",
            "text_reddit_sentiment_std_1h",
            "text_reddit_unique_author_count_1h",
            "sentiment_score",
        ],
    )
    composition_response = client.post(
        "/api/launch/model-composition",
        json={
            "source_run_ids": [market_run_id, nlp_run_id],
            "composition_name": "blocked_multimodal_contract",
            "fusion_strategy": "late_score_blend",
            "weights": {
                market_run_id: 0.6,
                nlp_run_id: 0.4,
            },
        },
    )
    assert composition_response.status_code == 200
    composition_job = _wait_for_job(client, composition_response.json()["job_id"], timeout_seconds=120.0)
    assert composition_job["status"] == "success"
    composed_run_id = composition_job["result"]["run_ids"][0]
    run_detail_response = client.get(f"/api/runs/{composed_run_id}")
    assert run_detail_response.status_code == 200
    run_detail_payload = run_detail_response.json()
    assert run_detail_payload["official_template_eligible"] is True
    assert run_detail_payload["official_blocking_reasons"] == []


def test_model_composition_marks_official_eligible_runs_for_preflight() -> None:
    app = create_app()
    client = TestClient(app)
    suffix = str(int(time.time() * 1000))
    market_run_id = f"official-market-compose-{suffix}"
    nlp_run_id = f"official-nlp-compose-{suffix}"

    _materialize_stub_run(
        app,
        run_id=market_run_id,
        dataset_id="baseline_real_benchmark_dataset",
        feature_names=[
            "lag_return_1",
            "lag_return_2",
            "momentum_3",
            "realized_vol_3",
            "close_to_open",
            "range_frac",
            "volume_zscore",
            "volume_ratio_3",
        ],
    )
    _materialize_stub_run(
        app,
        run_id=nlp_run_id,
        dataset_id="official_reddit_pullpush_multimodal_v2_fusion",
        feature_names=[
            "news_event_count",
            "text_reddit_attention_zscore_24h",
            "text_reddit_body_len_mean_1h",
            "text_reddit_comment_count_1h",
            "text_reddit_controversiality_ratio_1h",
            "text_reddit_core_subreddit_ratio_1h",
            "text_reddit_negative_ratio_1h",
            "text_reddit_positive_ratio_1h",
            "text_reddit_score_mean_1h",
            "text_reddit_score_sum_1h",
            "text_reddit_sentiment_mean_1h",
            "text_reddit_sentiment_std_1h",
            "text_reddit_unique_author_count_1h",
            "sentiment_score",
        ],
    )

    composition_response = client.post(
        "/api/launch/model-composition",
        json={
            "source_run_ids": [market_run_id, nlp_run_id],
            "composition_name": f"official_multimodal_{suffix}",
            "fusion_strategy": "late_score_blend",
            "weights": {
                market_run_id: 0.6,
                nlp_run_id: 0.4,
            },
        },
    )
    assert composition_response.status_code == 200
    composition_job = _wait_for_job(client, composition_response.json()["job_id"], timeout_seconds=120.0)
    assert composition_job["status"] == "success"

    composed_run_id = composition_job["result"]["run_ids"][0]
    run_detail_response = client.get(f"/api/runs/{composed_run_id}")
    assert run_detail_response.status_code == 200
    run_detail_payload = run_detail_response.json()
    assert run_detail_payload["official_template_eligible"] is True
    assert run_detail_payload["official_blocking_reasons"] == []
    assert run_detail_payload["dataset_ids"] == [
        "baseline_real_benchmark_dataset",
        "official_reddit_pullpush_multimodal_v2_fusion",
    ]

    preflight_response = client.post(
        "/api/launch/backtest/preflight",
        json={
            "run_id": composed_run_id,
            "mode": "official",
            "template_id": "system::official_backtest_protocol_v1",
            "official_window_days": 30,
        },
    )
    assert preflight_response.status_code == 200
    preflight_payload = preflight_response.json()
    assert preflight_payload["compatible"] is True
    assert preflight_payload["blocking_reasons"] == []


def test_dataset_fusion_surfaces_connector_errors_as_explicit_400() -> None:
    app = create_app()
    client = TestClient(app)
    base_dataset_id = f"fusion_error_base_{int(time.time() * 1000)}"

    request_response = client.post(
        "/api/datasets/requests",
        json={
            "request_name": base_dataset_id,
            "data_domain": "market",
            "dataset_type": "training_panel",
            "asset_mode": "single_asset",
            "time_window": {
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-05T00:00:00Z",
            },
            "symbol_selector": {
                "symbol_type": "spot",
                "selection_mode": "explicit",
                "symbols": ["BTCUSDT"],
                "symbol_count": 1,
                "tags": [],
            },
            "selection_mode": "explicit",
            "source_vendor": "internal_smoke",
            "exchange": "binance",
            "frequency": "1h",
            "filters": {},
            "build_config": {
                "feature_set_id": "baseline_market_features",
                "label_horizon": 1,
                "label_kind": "regression",
                "split_strategy": "time_series",
                "sample_policy_name": "training_panel_strict",
                "alignment_policy_name": "event_time_inner",
                "missing_feature_policy_name": "drop_if_missing",
                "sample_policy": {},
                "alignment_policy": {},
                "missing_feature_policy": {},
            },
        },
    )
    assert request_response.status_code == 200
    request_job_payload = _wait_for_job(client, request_response.json()["job_id"])
    assert request_job_payload["status"] == "success"

    def failing_fetch_series_points(**kwargs):  # type: ignore[no-untyped-def]
        raise DataConnectorError(
            data_domain="macro",
            vendor="fred",
            identifier=str(kwargs["identifier"]),
            message="FRED_API_KEY is not configured for FRED ingestion.",
            retryable=False,
            code="credentials_missing",
        )

    app.state.services.workbench.facade.runtime.ingestion_service.fetch_series_points = (
        failing_fetch_series_points
    )

    fusion_response = client.post(
        "/api/datasets/fusions",
        json={
            "request_name": "fusion_connector_error_case",
            "base_dataset_id": base_dataset_id,
            "alignment_policy_name": "available_time_safe_asof",
            "missing_feature_policy_name": "drop_if_missing",
            "sources": [
                {
                    "data_domain": "macro",
                    "vendor": "fred",
                    "identifier": "DFF",
                    "frequency": "1d",
                    "feature_name": "macro_dff_value",
                }
            ],
        },
    )

    assert fusion_response.status_code == 400
    payload = fusion_response.json()
    assert payload["detail"]["code"] == "credentials_missing"
    assert payload["detail"]["data_domain"] == "macro"
    assert payload["detail"]["vendor"] == "fred"


def test_launch_train_supports_dataset_id_after_dataset_request() -> None:
    client = TestClient(create_app())

    request_response = client.post(
        "/api/datasets/requests",
        json={
            "request_name": "Dataset id train request",
            "data_domain": "market",
            "asset_mode": "single_asset",
            "time_window": {
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-05T00:00:00Z",
            },
            "symbol_selector": {
                "symbol_type": "spot",
                "selection_mode": "explicit",
                "symbols": ["BTCUSDT"],
                "symbol_count": 1,
                "tags": [],
            },
            "source_vendor": "internal_smoke",
            "exchange": "binance",
            "frequency": "1h",
            "filters": {},
            "build_config": {
                "feature_set_id": "baseline_market_features",
                "label_horizon": 1,
                "label_kind": "regression",
                "split_strategy": "time_series",
                "sample_policy": {},
                "alignment_policy": {},
                "missing_feature_policy": {},
            },
        },
    )
    assert request_response.status_code == 200
    request_job_payload = _wait_for_job(client, request_response.json()["job_id"])
    assert request_job_payload["status"] == "success"

    train_response = client.post(
        "/api/launch/train",
        json={
            "dataset_id": "dataset_id_train_request",
            "dataset_preset": "smoke",
            "template_id": "registry::elastic_net",
            "trainer_preset": "fast",
            "seed": 7,
            "experiment_name": "api-train-dataset-id",
            "run_id_prefix": "api-train-dataset-id",
        },
    )

    assert train_response.status_code == 200
    train_job_payload = _wait_for_job(client, train_response.json()["job_id"])
    assert train_job_payload["status"] == "success"
    assert train_job_payload["result"]["dataset_id"] == "dataset_id_train_request"
    assert train_job_payload["result"]["run_ids"]


def test_launch_train_rejects_dataset_id_when_readiness_is_not_ready() -> None:
    app = create_app()
    client = TestClient(app)

    request_response = client.post(
        "/api/datasets/requests",
        json={
            "request_name": "Blocked train request",
            "data_domain": "market",
            "asset_mode": "single_asset",
            "time_window": {
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-05T00:00:00Z",
            },
            "symbol_selector": {
                "symbol_type": "spot",
                "selection_mode": "explicit",
                "symbols": ["BTCUSDT"],
                "symbol_count": 1,
                "tags": [],
            },
            "source_vendor": "internal_smoke",
            "exchange": "binance",
            "frequency": "1h",
            "filters": {},
            "build_config": {
                "feature_set_id": "baseline_market_features",
                "label_horizon": 1,
                "label_kind": "regression",
                "split_strategy": "time_series",
                "sample_policy": {},
                "alignment_policy": {},
                "missing_feature_policy": {},
            },
        },
    )
    assert request_response.status_code == 200
    request_job_payload = _wait_for_job(client, request_response.json()["job_id"])
    assert request_job_payload["status"] == "success"

    artifact_root = app.state.services.jobs.artifact_root
    store = LocalArtifactStore(artifact_root)
    dataset_id = "blocked_train_request"
    dataset_ref_path = artifact_root / "datasets" / f"{dataset_id}_dataset_ref.json"
    dataset_manifest_path = artifact_root / "datasets" / f"{dataset_id}_dataset_manifest.json"
    dataset_ref = store.read_model(str(dataset_ref_path), DatasetRef).model_copy(
        update={"readiness_status": "not_ready"}
    )
    dataset_manifest = store.read_model(str(dataset_manifest_path), DatasetBuildManifest).model_copy(
        update={
            "readiness_status": "not_ready",
            "label_alignment_status": "failed",
        }
    )
    store.write_model(f"datasets/{dataset_id}_dataset_ref.json", dataset_ref)
    store.write_model(f"datasets/{dataset_id}_dataset_manifest.json", dataset_manifest)

    train_response = client.post(
        "/api/launch/train",
        json={
            "dataset_id": dataset_id,
            "dataset_preset": "smoke",
            "template_id": "registry::elastic_net",
            "trainer_preset": "fast",
            "seed": 7,
            "experiment_name": "api-train-dataset-not-ready",
            "run_id_prefix": "api-train-dataset-not-ready",
        },
    )

    assert train_response.status_code == 200
    train_job_payload = _wait_for_job(client, train_response.json()["job_id"])
    assert train_job_payload["status"] == "failed"
    assert "not ready for training" in (train_job_payload["error_message"] or "")


def test_multi_asset_dataset_request_exposes_aggregated_metadata() -> None:
    client = TestClient(create_app())
    request_name = f"Multi Asset Request {int(time.time() * 1000)}"

    request_response = client.post(
        "/api/datasets/requests",
        json={
            "request_name": request_name,
            "data_domain": "market",
            "asset_mode": "multi_asset",
            "time_window": {
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-05T00:00:00Z",
            },
            "symbol_selector": {
                "symbol_type": "spot",
                "selection_mode": "explicit",
                "symbols": ["BTCUSDT", "ETHUSDT"],
                "symbol_count": 2,
                "tags": ["multi"],
            },
            "source_vendor": "internal_smoke",
            "exchange": "binance",
            "frequency": "1h",
            "filters": {},
            "build_config": {
                "feature_set_id": "baseline_market_features",
                "label_horizon": 1,
                "label_kind": "regression",
                "split_strategy": "time_series",
                "sample_policy": {},
                "alignment_policy": {},
                "missing_feature_policy": {},
            },
        },
    )

    assert request_response.status_code == 200
    request_job_payload = _wait_for_job(client, request_response.json()["job_id"])
    assert request_job_payload["status"] == "success"

    dataset_id = str(request_job_payload["result"]["dataset_id"])
    list_response = client.get("/api/datasets", params={"page": 1, "per_page": 200})
    assert list_response.status_code == 200
    items = {item["dataset_id"]: item for item in list_response.json()["items"]}
    assert dataset_id in items
    assert items[dataset_id]["entity_scope"] == "multi_asset"
    assert items[dataset_id]["entity_count"] >= 2
    assert set(items[dataset_id]["symbols_preview"]) >= {"BTCUSDT", "ETHUSDT"}

    detail_response = client.get(f"/api/datasets/{dataset_id}")
    assert detail_response.status_code == 200
    detail_payload = detail_response.json()
    assert detail_payload["dataset"]["entity_scope"] == "multi_asset"
    assert detail_payload["acquisition_profile"]["symbols"] == ["BTCUSDT", "ETHUSDT"]
    assert detail_payload["training_profile"]["entity_scope"] == "multi_asset"

    readiness_response = client.get(f"/api/datasets/{dataset_id}/readiness")
    assert readiness_response.status_code == 200
    readiness_payload = readiness_response.json()
    assert readiness_payload["entity_scope"] == "multi_asset"
    assert readiness_payload["entity_count"] >= 2
    assert readiness_payload["readiness_status"] in {"ready", "warning"}


def test_launch_train_supports_fusion_dataset_and_persists_fusion_manifest_metadata() -> None:
    app = create_app()
    client = TestClient(app)
    suffix = str(int(time.time() * 1000))
    base_dataset_id = f"fusion_base_request_{suffix}"
    fusion_request_name = f"macro_onchain_fusion_{suffix}"

    def fake_fetch_series_points(
        *,
        data_domain: str,
        identifier: str,
        vendor: str,
        frequency: str,
        start_time: datetime,
        end_time: datetime,
        options: dict[str, object] | None = None,
    ) -> tuple[list[NormalizedSeriesPoint], str]:
        del options
        if data_domain == "macro":
            return (
                [
                    _series_point(
                        event_time=start_time,
                        identifier=identifier,
                        data_domain=data_domain,
                        vendor=vendor,
                        frequency=frequency,
                        value=300.0,
                    ),
                    _series_point(
                        event_time=start_time + timedelta(days=2),
                        identifier=identifier,
                        data_domain=data_domain,
                        vendor=vendor,
                        frequency=frequency,
                        value=301.5,
                    ),
                ],
                "live_fetch",
            )
        if data_domain == "on_chain":
            return (
                [
                    _series_point(
                        event_time=start_time,
                        identifier=identifier,
                        data_domain=data_domain,
                        vendor=vendor,
                        frequency=frequency,
                        value=1_000_000.0,
                    ),
                    _series_point(
                        event_time=end_time - timedelta(hours=1),
                        identifier=identifier,
                        data_domain=data_domain,
                        vendor=vendor,
                        frequency=frequency,
                        value=1_250_000.0,
                    ),
                ],
                "live_fetch",
            )
        raise AssertionError(f"Unexpected fusion source request: {data_domain}/{vendor}/{identifier}")

    app.state.services.workbench.facade.runtime.ingestion_service.fetch_series_points = fake_fetch_series_points

    base_request_response = client.post(
        "/api/datasets/requests",
        json={
            "request_name": base_dataset_id,
            "data_domain": "market",
            "asset_mode": "single_asset",
            "time_window": {
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-05T00:00:00Z",
            },
            "symbol_selector": {
                "symbol_type": "spot",
                "selection_mode": "explicit",
                "symbols": ["BTCUSDT"],
                "symbol_count": 1,
                "tags": [],
            },
            "source_vendor": "internal_smoke",
            "exchange": "binance",
            "frequency": "1h",
            "filters": {},
            "build_config": {
                "feature_set_id": "baseline_market_features",
                "label_horizon": 1,
                "label_kind": "regression",
                "split_strategy": "time_series",
                "sample_policy": {},
                "alignment_policy": {},
                "missing_feature_policy": {},
            },
        },
    )
    assert base_request_response.status_code == 200
    base_request_job = _wait_for_job(client, base_request_response.json()["job_id"])
    assert base_request_job["status"] == "success"

    fusion_response = client.post(
        "/api/datasets/fusions",
        json={
            "request_name": fusion_request_name,
            "base_dataset_id": base_dataset_id,
            "dataset_type": "fusion_training_panel",
            "sample_policy_name": "fusion_training_panel_strict",
            "alignment_policy_name": "available_time_safe_asof",
            "missing_feature_policy_name": "drop_if_missing",
            "sample_policy": {},
            "alignment_policy": {},
            "missing_feature_policy": {
                "min_feature_coverage_ratio": 0.5,
            },
            "sources": [
                {
                    "data_domain": "macro",
                    "vendor": "fred",
                    "identifier": "CPIAUCSL",
                    "frequency": "1d",
                    "metric_name": "value",
                    "feature_name": "macro_cpi_value",
                    "options": {},
                },
                {
                    "data_domain": "on_chain",
                    "vendor": "glassnode",
                    "identifier": "active_addresses",
                    "frequency": "1d",
                    "metric_name": "value",
                    "feature_name": "onchain_active_addresses",
                    "options": {},
                },
            ],
        },
    )
    assert fusion_response.status_code == 200
    fusion_payload = fusion_response.json()
    dataset_id = fusion_payload["dataset_id"]
    assert fusion_payload["training_summary"]["dataset_type"] == "fusion_training_panel"
    assert fusion_payload["readiness"]["readiness_status"] in {"ready", "warning"}

    train_response = client.post(
        "/api/launch/train",
        json={
            "dataset_id": dataset_id,
            "dataset_preset": "smoke",
            "template_id": "registry::elastic_net",
            "trainer_preset": "fast",
            "seed": 7,
            "experiment_name": "api-train-fusion-dataset",
            "run_id_prefix": "api-train-fusion-dataset",
        },
    )
    assert train_response.status_code == 200
    train_job_payload = _wait_for_job(client, train_response.json()["job_id"])
    assert train_job_payload["status"] == "success"
    assert train_job_payload["result"]["dataset_id"] == dataset_id
    assert train_job_payload["result"]["run_ids"]

    run_id = train_job_payload["result"]["run_ids"][0]
    manifest_path = (
        Path(app.state.services.jobs.artifact_root) / "models" / run_id / "train_manifest.json"
    )
    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_payload["dataset_type"] == "fusion_training_panel"
    assert manifest_payload["data_domain"] == "market"
    assert manifest_payload["dataset_readiness_status"] in {"ready", "warning"}
    assert manifest_payload["source_dataset_ids"] == [base_dataset_id]
    assert set(manifest_payload["fusion_domains"]) >= {"market", "macro", "on_chain"}


def test_dataset_pipeline_base_only_runs_request_stages_in_one_job() -> None:
    client = TestClient(create_app())

    launch_response = client.post(
        "/api/datasets/pipelines",
        json={
            "base_request": _market_dataset_request_payload("pipeline_base_only"),
            "fusion": {"enabled": False},
            "training": {"enabled": False},
        },
    )

    assert launch_response.status_code == 200
    launch_payload = launch_response.json()
    assert launch_payload["requested_stages"] == [
        "acquire_base",
        "prepare_base",
        "readiness_base",
    ]
    assert launch_payload["final_stage"] == "readiness_base"

    job_payload = _wait_for_job(client, launch_payload["job_id"])
    assert job_payload["status"] == "success"
    assert job_payload["result"]["base_dataset_id"] == "pipeline_base_only"
    assert job_payload["result"]["dataset_id"] == "pipeline_base_only"
    assert job_payload["result"]["fusion_dataset_id"] is None
    assert job_payload["result"]["run_ids"] == []
    assert job_payload["result"]["pipeline_summary"]["requested_stages"] == [
        "acquire_base",
        "prepare_base",
        "readiness_base",
    ]
    stage_status = {
        item["stage"]: item["status"] for item in job_payload["result"]["pipeline_summary"]["stages"]
    }
    assert stage_status["acquire_base"] == "success"
    assert stage_status["prepare_base"] == "success"
    assert stage_status["readiness_base"] == "success"


def test_dataset_pipeline_supports_base_plus_fusion_without_training() -> None:
    app = create_app()
    client = TestClient(app)
    suffix = str(int(time.time() * 1000))
    base_dataset_id = f"pipeline_fusion_base_{suffix}"

    def fake_fetch_series_points(
        *,
        data_domain: str,
        identifier: str,
        vendor: str,
        frequency: str,
        start_time: datetime,
        end_time: datetime,
        options: dict[str, object] | None = None,
    ) -> tuple[list[NormalizedSeriesPoint], str]:
        del options
        if data_domain == "macro":
            return (
                [
                    _series_point(
                        event_time=start_time,
                        identifier=identifier,
                        data_domain=data_domain,
                        vendor=vendor,
                        frequency=frequency,
                        value=4.75,
                    ),
                    _series_point(
                        event_time=start_time + timedelta(days=2),
                        identifier=identifier,
                        data_domain=data_domain,
                        vendor=vendor,
                        frequency=frequency,
                        value=4.5,
                    ),
                ],
                "live_fetch",
            )
        return (
            [
                _series_point(
                    event_time=start_time,
                    identifier=identifier,
                    data_domain=data_domain,
                    vendor=vendor,
                    frequency=frequency,
                    value=900_000.0,
                ),
                _series_point(
                    event_time=end_time - timedelta(hours=1),
                    identifier=identifier,
                    data_domain=data_domain,
                    vendor=vendor,
                    frequency=frequency,
                    value=1_050_000.0,
                ),
            ],
            "live_fetch",
        )

    app.state.services.workbench.facade.runtime.ingestion_service.fetch_series_points = fake_fetch_series_points

    launch_response = client.post(
        "/api/datasets/pipelines",
        json={
            "base_request": _market_dataset_request_payload(base_dataset_id),
            "fusion": {
                "enabled": True,
                "request_name": f"pipeline_fusion_only_{suffix}",
                "alignment_policy_name": "available_time_safe_asof",
                "missing_feature_policy_name": "drop_if_missing",
                "sources": [
                    {
                        "data_domain": "macro",
                        "vendor": "fred",
                        "identifier": "DFF",
                        "frequency": "1d",
                        "feature_name": "macro_dff_value",
                    },
                    {
                        "data_domain": "on_chain",
                        "vendor": "defillama",
                        "identifier": "tvl",
                        "frequency": "1d",
                        "feature_name": "onchain_tvl_value",
                    },
                ],
            },
            "training": {"enabled": False},
        },
    )

    assert launch_response.status_code == 200
    job_payload = _wait_for_job(client, launch_response.json()["job_id"])
    assert job_payload["status"] == "success"
    assert job_payload["result"]["base_dataset_id"] == base_dataset_id
    assert job_payload["result"]["fusion_dataset_id"]
    assert job_payload["result"]["dataset_id"] == job_payload["result"]["fusion_dataset_id"]
    assert job_payload["result"]["run_ids"] == []
    assert {
        item["kind"] for item in job_payload["result"]["result_links"]
    } >= {"dataset_detail", "base_dataset_detail", "fusion_dataset_detail"}


def test_dataset_pipeline_runs_request_fusion_and_train_in_one_job() -> None:
    app = create_app()
    client = TestClient(app)
    suffix = str(int(time.time() * 1000))
    base_dataset_id = f"pipeline_train_base_{suffix}"

    def fake_fetch_series_points(
        *,
        data_domain: str,
        identifier: str,
        vendor: str,
        frequency: str,
        start_time: datetime,
        end_time: datetime,
        options: dict[str, object] | None = None,
    ) -> tuple[list[NormalizedSeriesPoint], str]:
        del options
        if data_domain == "macro":
            return (
                [
                    _series_point(
                        event_time=start_time,
                        identifier=identifier,
                        data_domain=data_domain,
                        vendor=vendor,
                        frequency=frequency,
                        value=305.0,
                    ),
                    _series_point(
                        event_time=start_time + timedelta(days=2),
                        identifier=identifier,
                        data_domain=data_domain,
                        vendor=vendor,
                        frequency=frequency,
                        value=306.0,
                    ),
                ],
                "live_fetch",
            )
        return (
            [
                _series_point(
                    event_time=start_time,
                    identifier=identifier,
                    data_domain=data_domain,
                    vendor=vendor,
                    frequency=frequency,
                    value=1_100_000.0,
                ),
                _series_point(
                    event_time=end_time - timedelta(hours=1),
                    identifier=identifier,
                    data_domain=data_domain,
                    vendor=vendor,
                    frequency=frequency,
                    value=1_250_000.0,
                ),
            ],
            "live_fetch",
        )

    app.state.services.workbench.facade.runtime.ingestion_service.fetch_series_points = fake_fetch_series_points

    launch_response = client.post(
        "/api/datasets/pipelines",
        json={
            "base_request": _market_dataset_request_payload(base_dataset_id),
            "fusion": {
                "enabled": True,
                "request_name": f"pipeline_train_fusion_{suffix}",
                "alignment_policy_name": "available_time_safe_asof",
                "missing_feature_policy_name": "drop_if_missing",
                "sources": [
                    {
                        "data_domain": "macro",
                        "vendor": "fred",
                        "identifier": "CPIAUCSL",
                        "frequency": "1d",
                        "feature_name": "macro_cpi_value",
                    },
                    {
                        "data_domain": "on_chain",
                        "vendor": "defillama",
                        "identifier": "tvl",
                        "frequency": "1d",
                        "feature_name": "onchain_tvl_value",
                    },
                ],
            },
            "training": {
                "enabled": True,
                "template_id": "registry::elastic_net",
                "trainer_preset": "fast",
                "seed": 7,
                "experiment_name": f"api-pipeline-train-{suffix}",
                "run_id_prefix": f"api-pipeline-train-{suffix}",
            },
        },
    )

    assert launch_response.status_code == 200
    launch_payload = launch_response.json()
    assert launch_payload["requested_stages"] == [
        "acquire_base",
        "prepare_base",
        "readiness_base",
        "build_fusion",
        "readiness_fusion",
        "train",
    ]

    job_payload = _wait_for_job(client, launch_payload["job_id"])
    assert job_payload["status"] == "success"
    assert job_payload["result"]["base_dataset_id"] == base_dataset_id
    assert job_payload["result"]["fusion_dataset_id"]
    assert job_payload["result"]["run_ids"]
    assert {
        item["kind"] for item in job_payload["result"]["result_links"]
    } >= {"dataset_detail", "base_dataset_detail", "fusion_dataset_detail", "run_detail"}
    assert set(job_payload["result"]["pipeline_summary"]["completed_stages"]) >= {
        "acquire_base",
        "prepare_base",
        "readiness_base",
        "build_fusion",
        "readiness_fusion",
        "train",
    }


def test_dataset_pipeline_blocks_train_when_fusion_readiness_is_not_ready() -> None:
    app = create_app()
    client = TestClient(app)
    suffix = str(int(time.time() * 1000))
    base_dataset_id = f"pipeline_not_ready_base_{suffix}"

    def sparse_fetch_series_points(
        *,
        data_domain: str,
        identifier: str,
        vendor: str,
        frequency: str,
        start_time: datetime,
        end_time: datetime,
        options: dict[str, object] | None = None,
    ) -> tuple[list[NormalizedSeriesPoint], str]:
        del options
        return (
            [
                _series_point(
                    event_time=end_time - timedelta(hours=2),
                    identifier=identifier,
                    data_domain=data_domain,
                    vendor=vendor,
                    frequency=frequency,
                    value=1.0,
                ),
                _series_point(
                    event_time=end_time - timedelta(hours=1),
                    identifier=identifier,
                    data_domain=data_domain,
                    vendor=vendor,
                    frequency=frequency,
                    value=1.1,
                )
            ],
            "live_fetch",
        )

    app.state.services.workbench.facade.runtime.ingestion_service.fetch_series_points = (
        sparse_fetch_series_points
    )

    launch_response = client.post(
        "/api/datasets/pipelines",
        json={
            "base_request": _market_dataset_request_payload(base_dataset_id),
            "fusion": {
                "enabled": True,
                "request_name": f"pipeline_not_ready_fusion_{suffix}",
                "alignment_policy_name": "available_time_safe_asof",
                "missing_feature_policy_name": "drop_if_missing",
                "min_feature_coverage_ratio": 0.95,
                "sources": [
                    {
                        "data_domain": "macro",
                        "vendor": "fred",
                        "identifier": "DFF",
                        "frequency": "1d",
                        "feature_name": "macro_dff_value",
                    }
                ],
            },
            "training": {
                "enabled": True,
                "template_id": "registry::elastic_net",
                "trainer_preset": "fast",
            },
        },
    )

    assert launch_response.status_code == 200
    job_payload = _wait_for_job(client, launch_response.json()["job_id"])
    assert job_payload["status"] == "failed"
    assert job_payload["result"]["base_dataset_id"] == base_dataset_id
    assert job_payload["result"]["fusion_dataset_id"]
    assert job_payload["result"]["run_ids"] == []
    assert "not ready for pipeline continuation" in (job_payload["error_message"] or "")
    stage_status = {
        item["stage"]: item["status"] for item in job_payload["result"]["pipeline_summary"]["stages"]
    }
    assert stage_status["readiness_fusion"] == "failed"
    assert stage_status["train"] == "blocked"


def test_dataset_pipeline_surfaces_connector_errors_in_job_status() -> None:
    app = create_app()
    client = TestClient(app)
    suffix = str(int(time.time() * 1000))
    base_dataset_id = f"pipeline_error_base_{suffix}"

    def failing_fetch_series_points(**kwargs):  # type: ignore[no-untyped-def]
        raise DataConnectorError(
            data_domain="macro",
            vendor="fred",
            identifier=str(kwargs["identifier"]),
            message="FRED_API_KEY is not configured for FRED ingestion.",
            retryable=False,
            code="credentials_missing",
        )

    app.state.services.workbench.facade.runtime.ingestion_service.fetch_series_points = (
        failing_fetch_series_points
    )

    launch_response = client.post(
        "/api/datasets/pipelines",
        json={
            "base_request": _market_dataset_request_payload(base_dataset_id),
            "fusion": {
                "enabled": True,
                "request_name": f"pipeline_error_fusion_{suffix}",
                "sources": [
                    {
                        "data_domain": "macro",
                        "vendor": "fred",
                        "identifier": "DFF",
                        "frequency": "1d",
                        "feature_name": "macro_dff_value",
                    }
                ],
            },
            "training": {"enabled": False},
        },
    )

    assert launch_response.status_code == 200
    job_payload = _wait_for_job(client, launch_response.json()["job_id"])
    assert job_payload["status"] == "failed"
    assert "FRED_API_KEY is not configured" in (job_payload["error_message"] or "")
    stage_status = {
        item["stage"]: item["status"] for item in job_payload["result"]["pipeline_summary"]["stages"]
    }
    assert stage_status["build_fusion"] == "failed"


def test_workbench_overview_tolerates_legacy_job_payloads() -> None:
    app = create_app()
    jobs_root = Path(app.state.services.jobs.jobs_root)
    legacy_path = jobs_root / "legacy-job-payload.json"
    legacy_path.write_text(
        json.dumps(
            {
                "job_id": "legacy-job-payload",
                "job_type": "train",
                "status": "success",
                "created_at": datetime.now(UTC).isoformat(),
                "updated_at": datetime.now(UTC).isoformat(),
                "stages": [],
                "result": {
                    "dataset_id": "smoke_dataset",
                    "run_ids": ["legacy-run"],
                    "backtest_ids": [],
                    "fit_result_uris": [],
                    "summary_artifacts": [],
                    "benchmark_names": [],
                    "prediction_scope": None,
                    "summary": {"headline": "legacy"},
                    "pipeline_summary": {"status": "success", "stages": []},
                    "result_links": [],
                    "review_summary": None,
                },
                "error_message": None,
            }
        ),
        encoding="utf-8",
    )

    try:
        client = TestClient(app)
        jobs_response = client.get("/api/jobs")
        overview_response = client.get("/api/workbench/overview")

        assert jobs_response.status_code == 200
        assert overview_response.status_code == 200
        payload = jobs_response.json()
        assert any(item["job_id"] == "legacy-job-payload" for item in payload["items"])
    finally:
        legacy_path.unlink(missing_ok=True)
