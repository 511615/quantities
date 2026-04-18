from __future__ import annotations

from pathlib import Path

from quant_platform.common.io.files import LocalArtifactStore
from quant_platform.webapi.repositories.artifacts import ArtifactRepository
from quant_platform.webapi.repositories.dataset_registry import DatasetRegistryRepository
from quant_platform.webapi.schemas.launch import LaunchBacktestRequest
from quant_platform.webapi.schemas.views import JobResultView
from quant_platform.webapi.services.backtest_protocol import (
    DEFAULT_BACKTEST_MAX_POSITION_WEIGHT,
    DEFAULT_BACKTEST_MAX_TURNOVER_PER_REBALANCE,
    build_official_backtest_request,
)
from quant_platform.webapi.services.catalog import ResearchWorkbenchService
from quant_platform.webapi.services.jobs import JobService


def _build_services(tmp_path: Path) -> tuple[ResearchWorkbenchService, JobService]:
    artifact_root = tmp_path / "artifacts"
    repository = ArtifactRepository(artifact_root)
    registry = DatasetRegistryRepository(artifact_root, repository)
    store = LocalArtifactStore(artifact_root)
    workbench = ResearchWorkbenchService(
        repository=repository,
        dataset_registry=registry,
        store=store,
        model_families={},
    )
    jobs = JobService(
        artifact_root=artifact_root,
        workbench=workbench,
        facade=object(),  # type: ignore[arg-type]
    )
    return workbench, jobs


def test_dataset_request_options_expose_ccxt_vendor_and_okx_exchange(tmp_path: Path) -> None:
    workbench, _jobs = _build_services(tmp_path)

    options = workbench.get_dataset_request_options()

    assert "ccxt" in {item.value for item in options.source_vendors}
    assert "okx" in {item.value for item in options.exchanges}


def test_backtest_options_expose_optional_backend_and_portfolio_methods(tmp_path: Path) -> None:
    _workbench, jobs = _build_services(tmp_path)

    options = jobs.get_backtest_options()

    assert [item.value for item in options.research_backends] == ["native", "vectorbt"]
    assert [item.value for item in options.portfolio_methods] == [
        "proportional",
        "skfolio_mean_risk",
    ]
    assert options.constraints["research_backend"]["default"] == "native"
    assert options.constraints["portfolio_method"]["default"] == "proportional"


def test_launch_backtest_request_accepts_optional_backend_and_portfolio_fields() -> None:
    request = LaunchBacktestRequest(
        run_id="run-123",
        research_backend="vectorbt",
        portfolio_method="skfolio_mean_risk",
    )

    assert request.research_backend == "vectorbt"
    assert request.portfolio_method == "skfolio_mean_risk"


def test_backtest_job_result_and_catalog_metadata_carry_backend_choices(tmp_path: Path) -> None:
    workbench, _jobs = _build_services(tmp_path)
    result = JobResultView(
        dataset_id="dataset-1",
        dataset_ids=["dataset-1"],
        research_backend="vectorbt",
        portfolio_method="skfolio_mean_risk",
    )

    metadata = workbench._protocol_metadata_from_job_result(result.model_dump(mode="json"))  # noqa: SLF001

    assert result.research_backend == "vectorbt"
    assert result.portfolio_method == "skfolio_mean_risk"
    assert metadata["research_backend"] == "vectorbt"
    assert metadata["portfolio_method"] == "skfolio_mean_risk"


def test_official_backtest_request_uses_conservative_position_and_turnover_defaults() -> None:
    request = build_official_backtest_request(
        prediction_frame_uri="memory://predictions/frame.json",
        benchmark_symbol="BTCUSDT",
    )

    assert request.portfolio_config.max_position_weight == DEFAULT_BACKTEST_MAX_POSITION_WEIGHT
    assert (
        request.portfolio_config.max_turnover_per_rebalance
        == DEFAULT_BACKTEST_MAX_TURNOVER_PER_REBALANCE
    )


def test_dynamic_composition_weights_shrink_toward_equal_weight(tmp_path: Path) -> None:
    _workbench, jobs = _build_services(tmp_path)

    weighted = jobs._apply_dynamic_composition_weights(  # noqa: SLF001
        [
            {
                "run_id": "market-run",
                "weight": 1.0,
                "combination_quality": {"valid_mae": 0.01, "sign_hit_rate": 0.56},
            },
            {
                "run_id": "macro-run",
                "weight": 1.0,
                "combination_quality": {"valid_mae": 0.20, "sign_hit_rate": 0.50},
            },
        ]
    )

    market = next(item for item in weighted if item["run_id"] == "market-run")
    macro = next(item for item in weighted if item["run_id"] == "macro-run")

    assert market["weight"] > macro["weight"]
    assert market["weight"] < 1.0
    assert macro["weight"] > 0.0
    assert market["weight_source"] == "dynamic_valid_mae_shrinkage"
