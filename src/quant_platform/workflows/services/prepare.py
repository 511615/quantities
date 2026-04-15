from __future__ import annotations

import csv
from datetime import UTC, datetime
from math import cos, sin
from pathlib import Path

from quant_platform.common.enums.core import LabelKind, ModelFamily, SplitStrategy
from quant_platform.common.hashing.digest import stable_digest
from quant_platform.common.types.core import ArtifactRef, SchemaField
from quant_platform.data.contracts.data_asset import DataAssetRef
from quant_platform.data.contracts.market import NormalizedMarketBar
from quant_platform.datasets.builders.dataset_builder import DatasetBuilder
from quant_platform.datasets.contracts.dataset import LabelSpec, SamplePolicy
from quant_platform.datasets.splits.time_series import TimeSeriesSplitPlanner
from quant_platform.features.contracts.feature_view import (
    FeatureRow,
    FeatureViewBuildResult,
    FeatureViewRef,
)
from quant_platform.models.contracts.model_spec import ModelSpec
from quant_platform.workflows.contracts.requests import (
    DatasetAcquisitionRequest,
    PrepareWorkflowRequest,
)
from quant_platform.workflows.contracts.results import PrepareWorkflowResult
from quant_platform.workflows.contracts.state import (
    WorkflowStageName,
    WorkflowStageResult,
    WorkflowStageStatus,
)
from quant_platform.workflows.runtime import WorkflowRuntime
from quant_platform.workflows.services.market_acquisition import MarketAcquisitionHandler


class PrepareWorkflowService:
    def __init__(self, runtime: WorkflowRuntime) -> None:
        self.runtime = runtime
        self.market_acquisition = MarketAcquisitionHandler(runtime)

    def prepare(self, request: PrepareWorkflowRequest) -> PrepareWorkflowResult:
        started_at = datetime.now(UTC)
        if not request.market_bars:
            raise ValueError("prepare workflow currently requires explicit normalized market bars")
        data_refs, quality_manifests, feature_result = self._build_feature_view(request)
        primary_data_ref = data_refs[0]
        closes_by_timestamp = {
            (bar.symbol, bar.event_time): bar.close for bar in request.market_bars
        }
        labels = self.runtime.labeler.build(
            feature_result.rows,
            closes_by_timestamp,
            horizon=request.label_spec.horizon,
        )
        split_manifest = request.split_manifest
        if split_manifest is None:
            if request.split_strategy != SplitStrategy.TIME_SERIES:
                raise ValueError(
                    "prepare workflow currently supports explicit single time_series split"
                )
            timestamps = sorted({row.timestamp for row in feature_result.rows})
            train_end_index = request.train_end_index or max(1, len(timestamps) // 2)
            valid_end_index = request.valid_end_index or max(
                train_end_index + 1,
                int(len(timestamps) * 0.75),
            )
            split_manifest = TimeSeriesSplitPlanner.single_split(
                timestamps=timestamps,
                train_end_index=train_end_index,
                valid_end_index=valid_end_index,
            )
        dataset_ref, samples, dataset_manifest = DatasetBuilder.build_dataset(
            dataset_id=request.dataset_id,
            feature_result=feature_result,
            labels=labels,
            label_spec=request.label_spec,
            split_manifest=split_manifest,
            sample_policy=request.sample_policy,
        )
        dataset_samples_artifact = self.runtime.store.write_json(
            f"datasets/{request.dataset_id}_dataset_samples.json",
            {"samples": [sample.model_dump(mode="json") for sample in samples]},
        )
        quality_report_uri = quality_manifests[0].quality_report_uri if quality_manifests else None
        dataset_manifest = dataset_manifest.model_copy(
            update={
                "build_config": {
                    "alignment_policy": request.alignment_policy,
                    "missing_feature_policy": request.missing_feature_policy,
                    "normalization_profile": request.normalization_profile,
                    "dataset_tags": request.dataset_tags,
                },
                "acquisition_profile": request.acquisition_profile,
                "quality_status": "healthy",
                "freshness_status": self._freshness_status(
                    request.as_of_time,
                    request.market_bars[-1].available_time,
                ),
                "readiness_status": self._readiness_status(samples, request, dataset_manifest),
            }
        )
        dataset_ref = dataset_ref.model_copy(
            update={
                "dataset_samples_uri": dataset_samples_artifact.uri,
                "entity_scope": request.entity_scope,
                "entity_count": len({sample.entity_key for sample in samples}),
                "readiness_status": dataset_manifest.readiness_status,
            }
        )
        self.runtime.dataset_store[dataset_ref.dataset_id] = samples
        feature_view_artifact = self.runtime.store.write_model(
            f"datasets/{request.dataset_id}_feature_view_ref.json",
            feature_result.feature_view_ref,
        )
        dataset_ref_artifact = self.runtime.store.write_model(
            f"datasets/{request.dataset_id}_dataset_ref.json",
            dataset_ref,
        )
        dataset_manifest_artifact = self.runtime.store.write_model(
            f"datasets/{request.dataset_id}_dataset_manifest.json",
            dataset_manifest,
        )
        dataset_ref = dataset_ref.model_copy(
            update={
                "dataset_manifest_uri": dataset_manifest_artifact.uri,
                "dataset_samples_uri": dataset_samples_artifact.uri,
            }
        )
        dataset_ref_artifact = self.runtime.store.write_model(
            f"datasets/{request.dataset_id}_dataset_ref.json",
            dataset_ref,
        )
        stage_result = WorkflowStageResult(
            stage=WorkflowStageName.PREPARE,
            status=WorkflowStageStatus.SUCCESS,
            request_digest=stable_digest(request),
            started_at=started_at,
            finished_at=datetime.now(UTC),
            artifacts=[
                ArtifactRef(kind="feature_view_ref", uri=feature_view_artifact.uri),
                ArtifactRef(kind="dataset_ref", uri=dataset_ref_artifact.uri),
                ArtifactRef(kind="dataset_manifest", uri=dataset_manifest_artifact.uri),
                ArtifactRef(kind="dataset_samples", uri=dataset_samples_artifact.uri),
                ArtifactRef(kind="quality_report", uri=quality_report_uri or ""),
            ],
            summary=f"prepared dataset '{request.dataset_id}' with {len(samples)} samples",
        )
        return PrepareWorkflowResult(
            stage_result=stage_result,
            data_asset_ref=primary_data_ref,
            feature_view_ref=feature_result.feature_view_ref,
            dataset_ref=dataset_ref,
            dataset_manifest_uri=dataset_manifest_artifact.uri,
            quality_report_uri=quality_report_uri,
        )

    def prepare_from_dataset_request(self, request: DatasetAcquisitionRequest) -> PrepareWorkflowResult:
        prepare_request = self.build_prepare_request_from_dataset_request(request)
        return self.prepare(prepare_request)

    def build_smoke_market_bars(self) -> list[NormalizedMarketBar]:
        base = datetime(2024, 1, 1, tzinfo=UTC)
        closes = [100.0, 101.0, 100.5, 102.0, 103.5, 102.5]
        volumes = [10.0, 11.0, 9.5, 15.0, 16.0, 12.0]
        bars: list[NormalizedMarketBar] = []
        for index, close in enumerate(closes):
            open_price = closes[index - 1] if index > 0 else close
            bars.append(
                NormalizedMarketBar(
                    event_time=base.replace(hour=index),
                    available_time=base.replace(hour=index),
                    symbol="BTCUSDT",
                    venue="binance",
                    open=open_price,
                    high=max(open_price, close) + 0.5,
                    low=min(open_price, close) - 0.5,
                    close=close,
                    volume=volumes[index],
                )
            )
        return bars

    def build_smoke_request(self) -> PrepareWorkflowRequest:
        return PrepareWorkflowRequest(
            data_source="internal",
            asset_id="market_ohlcv_btcusdt_1h",
            frequency="1h",
            dataset_id="smoke_dataset",
            feature_set_id="baseline_market_features",
            label_spec=LabelSpec(
                target_column="future_return_1",
                horizon=1,
                kind=LabelKind.REGRESSION,
            ),
            sample_policy=SamplePolicy(min_history_bars=10),
            as_of_time=datetime(2024, 1, 5, tzinfo=UTC),
            market_bars=self.build_smoke_market_bars(),
            train_end_index=1,
            valid_end_index=2,
        )

    def build_smoke_model_spec(self) -> ModelSpec:
        return ModelSpec(
            model_name="elastic_net",
            family=ModelFamily.LINEAR,
            version="0.1.0",
            input_schema=[
                SchemaField(name=feature_name, dtype="float")
                for feature_name in self.runtime.feature_builder.FEATURE_NAMES
            ],
            output_schema=[SchemaField(name="prediction", dtype="float")],
            hyperparams={"alpha": 0.001, "l1_ratio": 0.5, "random_state": 7},
        )

    def build_benchmark_market_bars(self, bar_count: int = 40) -> list[NormalizedMarketBar]:
        base = datetime(2024, 2, 1, tzinfo=UTC)
        bars: list[NormalizedMarketBar] = []
        previous_close = 100.0
        for index in range(bar_count):
            regime = index // 10
            if regime == 0:
                drift = 0.42
                vol_scale = 0.18
            elif regime == 1:
                drift = -0.35
                vol_scale = 0.32
            elif regime == 2:
                drift = 0.08
                vol_scale = 0.55
            else:
                drift = 0.28 if index % 6 < 4 else -0.22
                vol_scale = 0.4
            micro_cycle = sin(index * 0.65) * vol_scale
            momentum = sin(index * 0.18) * 0.14
            volatility_cluster = abs(cos(index * 0.31)) * vol_scale
            jump = 0.0
            if index in {9, 19, 27, 34}:
                jump = [-1.6, 1.2, -2.1, 1.8][[9, 19, 27, 34].index(index)]
            close = max(35.0, previous_close + drift + micro_cycle + momentum + jump)
            event_time = base.replace(day=base.day + index // 24, hour=index % 24)
            bars.append(
                NormalizedMarketBar(
                    event_time=event_time,
                    available_time=event_time,
                    symbol="BTCUSDT",
                    venue="binance",
                    open=previous_close,
                    high=max(previous_close, close) + 0.35 + volatility_cluster,
                    low=min(previous_close, close) - 0.35 - volatility_cluster,
                    close=close,
                    volume=120.0
                    + float((regime + 1) * 30)
                    + abs(jump) * 35
                    + volatility_cluster * 40,
                )
            )
            previous_close = close
        return bars

    def load_real_market_bars(self) -> tuple[list[NormalizedMarketBar], str]:
        start_time = datetime(2024, 1, 1, tzinfo=UTC)
        end_time = datetime.now(UTC)
        return self.runtime.ingestion_service.fetch_market_bars(
            symbol="BTCUSDT",
            vendor="binance",
            exchange="binance",
            frequency="1h",
            start_time=start_time,
            end_time=end_time,
        )

    def build_real_benchmark_request(self) -> tuple[PrepareWorkflowRequest, str]:
        bars, data_source = self.load_real_market_bars()
        return (
            PrepareWorkflowRequest(
                data_source=data_source,
                asset_id="market_ohlcv_btcusdt_1h_real_benchmark",
                frequency="1h",
                dataset_id="baseline_real_benchmark_dataset",
                feature_set_id="baseline_market_features",
                label_spec=LabelSpec(
                    target_column="future_return_1",
                    horizon=1,
                    kind=LabelKind.REGRESSION,
                ),
                sample_policy=SamplePolicy(min_history_bars=10),
                as_of_time=bars[-1].available_time,
                market_bars=bars,
                train_end_index=max(10, len(bars) // 2),
                valid_end_index=max(20, int(len(bars) * 0.75)),
                entity_scope="single_asset",
                acquisition_profile={
                    "source_vendor": "binance",
                    "exchange": "binance",
                    "request_origin": data_source,
                    "asset_mode": "single_asset",
                    "fallback_used": False,
                    "symbols": ["BTCUSDT"],
                },
            ),
            data_source,
        )

    def build_synthetic_reference_request(self) -> PrepareWorkflowRequest:
        bars = self.build_benchmark_market_bars(bar_count=120)
        return PrepareWorkflowRequest(
            data_source="synthetic_reference",
            asset_id="market_ohlcv_btcusdt_1h_reference",
            frequency="1h",
            dataset_id="baseline_reference_benchmark_dataset",
            feature_set_id="baseline_market_features",
            label_spec=LabelSpec(
                target_column="future_return_1",
                horizon=1,
                kind=LabelKind.REGRESSION,
            ),
            sample_policy=SamplePolicy(min_history_bars=10),
            as_of_time=bars[-1].available_time,
            market_bars=bars,
            train_end_index=max(10, len(bars) // 2),
            valid_end_index=max(20, int(len(bars) * 0.75)),
            entity_scope="single_asset",
            acquisition_profile={
                "source_vendor": "synthetic_reference",
                "exchange": "binance",
                "request_origin": "synthetic_reference",
                "asset_mode": "single_asset",
                "symbols": ["BTCUSDT"],
            },
        )

    def build_prepare_request_from_dataset_request(
        self,
        request: DatasetAcquisitionRequest,
    ) -> PrepareWorkflowRequest:
        sources = request.normalized_sources()
        if len(sources) != 1 or sources[0].data_domain != "market":
            raise ValueError(
                "prepare workflow currently materializes market anchor datasets directly; "
                "multi-domain requests must be orchestrated by the dataset request job."
            )
        source = sources[0]
        symbol_selector = source.symbol_selector or request.symbol_selector
        if symbol_selector is None:
            raise ValueError("market dataset request requires a symbol selector")
        symbols = list(symbol_selector.symbols)
        if not symbols:
            symbols = ["BTCUSDT"]
        if symbol_selector.symbol_count:
            symbols = symbols[: symbol_selector.symbol_count]
        acquisition = self.market_acquisition.build_market_panel(
            request=request,
            source=source,
            symbols=symbols,
        )
        bars = acquisition.bars
        request_origin = acquisition.request_origin
        label_kind = request.build_config.label_kind
        sample_policy_payload = dict(request.build_config.sample_policy)
        sample_policy = SamplePolicy(
            min_history_bars=int(sample_policy_payload.get("min_history_bars", 10) or 10),
            drop_missing_targets=bool(sample_policy_payload.get("drop_missing_targets", True)),
            universe=("multi_asset" if request.asset_mode == "multi_asset" else "single_asset"),
            recommended_training_use=request.dataset_type,
        )
        dataset_id = self._slugify(request.request_name or "requested-dataset")
        return PrepareWorkflowRequest(
            data_source=source.vendor,
            asset_id=f"market_ohlcv_{dataset_id}",
            frequency=source.frequency,
            dataset_id=dataset_id,
            feature_set_id=request.build_config.feature_set_id,
            label_spec=LabelSpec(
                target_column=f"future_return_{request.build_config.label_horizon}",
                horizon=request.build_config.label_horizon,
                kind=label_kind,
            ),
            sample_policy=sample_policy,
            as_of_time=max(bar.available_time for bar in bars),
            market_bars=bars,
            split_strategy=SplitStrategy.TIME_SERIES,
            train_end_index=self._train_end_index(bars),
            valid_end_index=self._valid_end_index(bars),
            entity_scope=("multi_asset" if request.asset_mode == "multi_asset" else "single_asset"),
            alignment_policy={
                "calendar_mode": "shared_market_calendar",
                "join_key": ["entity_key", "timestamp"],
                "drop_unaligned_rows": True,
                "min_entity_coverage_ratio": float(
                    request.build_config.alignment_policy.get("min_entity_coverage_ratio", 0.8)
                    or 0.8
                ),
                **request.build_config.alignment_policy,
            },
            missing_feature_policy={
                "strategy": request.build_config.missing_feature_policy.get("strategy", "drop"),
                "max_missing_ratio": float(
                    request.build_config.missing_feature_policy.get("max_missing_ratio", 0.1)
                    or 0.1
                ),
                "required_features": list(
                    request.build_config.missing_feature_policy.get("required_features", [])
                ),
                **request.build_config.missing_feature_policy,
            },
            normalization_profile={"profile": "market_v1"},
            dataset_tags=list(request.symbol_selector.tags),
            acquisition_profile={
                "request_name": request.request_name,
                "data_domain": "market",
                "data_domains": ["market"],
                "dataset_type": request.dataset_type,
                "asset_mode": request.asset_mode,
                "selection_mode": request.selection_mode,
                "time_window": request.time_window.model_dump(mode="json"),
                "source_vendor": source.vendor,
                "exchange": source.exchange,
                "frequency": source.frequency,
                "symbols": symbols,
                "request_origin": request_origin,
                "fallback_used": False,
                "filters": source.filters or request.filters,
                "contract_names": {
                    "sample_policy_name": request.build_config.sample_policy_name,
                    "alignment_policy_name": request.build_config.alignment_policy_name,
                    "missing_feature_policy_name": request.build_config.missing_feature_policy_name,
                },
            },
        )

    def _write_market_bars_csv(self, target: Path, bars: list[NormalizedMarketBar]) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "event_time",
                    "available_time",
                    "symbol",
                    "venue",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                ],
            )
            writer.writeheader()
            for bar in bars:
                writer.writerow(
                    {
                        "event_time": bar.event_time.isoformat(),
                        "available_time": bar.available_time.isoformat(),
                        "symbol": bar.symbol,
                        "venue": bar.venue,
                        "open": bar.open,
                        "high": bar.high,
                        "low": bar.low,
                        "close": bar.close,
                        "volume": bar.volume,
                    }
                )

    def _read_market_bars_csv(self, path: Path) -> list[NormalizedMarketBar]:
        bars: list[NormalizedMarketBar] = []
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                bars.append(
                    NormalizedMarketBar(
                        event_time=datetime.fromisoformat(str(row["event_time"])),
                        available_time=datetime.fromisoformat(str(row["available_time"])),
                        symbol=str(row["symbol"]),
                        venue=str(row["venue"]),
                        open=float(row["open"]),
                        high=float(row["high"]),
                        low=float(row["low"]),
                        close=float(row["close"]),
                        volume=float(row["volume"]),
                    )
                )
        return bars

    def _build_feature_view(
        self,
        request: PrepareWorkflowRequest,
    ) -> tuple[list[DataAssetRef], list, FeatureViewBuildResult]:
        rows_by_symbol: dict[str, list[NormalizedMarketBar]] = {}
        for bar in request.market_bars:
            rows_by_symbol.setdefault(bar.symbol, []).append(bar)
        data_refs: list[DataAssetRef] = []
        manifests: list = []
        feature_rows: list[FeatureRow] = []
        feature_schema = None
        single_symbol = len(rows_by_symbol) == 1
        for symbol, bars in rows_by_symbol.items():
            asset_id = request.asset_id if single_symbol else f"{request.asset_id}_{self._slugify(symbol.lower())}"
            data_ref, manifest = self.runtime.data_catalog.register_market_asset(
                asset_id=asset_id,
                source=request.data_source,
                frequency=request.frequency,
                rows=bars,
                tags=request.dataset_tags,
                request_origin=str(request.acquisition_profile.get("request_origin", request.data_source)),
                fallback_used=self._is_fallback_request(request),
            )
            data_refs.append(data_ref)
            manifests.append(manifest)
            feature_result = self.runtime.feature_builder.build(
                feature_set_id=request.feature_set_id,
                data_ref=data_ref,
                bars=bars,
                as_of_time=request.as_of_time,
            )
            feature_rows.extend(feature_result.rows)
            feature_schema = feature_result.feature_view_ref.feature_schema
        feature_rows.sort(key=lambda row: (row.timestamp, row.entity_key))
        if feature_schema is None:
            raise ValueError("no feature schema was built")
        feature_view_ref = FeatureViewRef(
            feature_set_id=request.feature_set_id,
            input_data_refs=data_refs,
            as_of_time=request.as_of_time,
            feature_schema=feature_schema,
            build_config_hash=stable_digest(
                {
                    "feature_set_id": request.feature_set_id,
                    "asset_ids": [ref.asset_id for ref in data_refs],
                    "as_of_time": request.as_of_time,
                    "entity_scope": request.entity_scope,
                }
            ),
            storage_uri=f"artifact://datasets/{request.dataset_id}_feature_rows.json",
        )
        self.runtime.store.write_json(
            f"datasets/{request.dataset_id}_feature_rows.json",
            {"rows": [row.model_dump(mode="json") for row in feature_rows]},
        )
        return data_refs, manifests, FeatureViewBuildResult(feature_view_ref=feature_view_ref, rows=feature_rows)

    @staticmethod
    def _slugify(value: str) -> str:
        cleaned = [ch.lower() if ch.isalnum() else "_" for ch in value]
        slug = "".join(cleaned).strip("_")
        while "__" in slug:
            slug = slug.replace("__", "_")
        return slug or "dataset"

    @staticmethod
    def _freshness_status(as_of_time: datetime, data_end_time: datetime) -> str:
        lag = max((as_of_time - data_end_time).total_seconds(), 0.0)
        if lag <= 3600:
            return "fresh"
        if lag <= 86400:
            return "warning"
        return "stale"

    @staticmethod
    def _is_fallback_request(request: PrepareWorkflowRequest) -> bool:
        return bool(request.acquisition_profile.get("fallback_used"))

    @staticmethod
    def _readiness_status(samples: list, request: PrepareWorkflowRequest, manifest) -> str:
        if not samples:
            return "not_ready"
        if (
            request.entity_scope == "multi_asset"
            and len({sample.entity_key for sample in samples}) <= 1
        ):
            return "not_ready"
        max_missing_ratio = float(
            request.missing_feature_policy.get("max_missing_ratio", 0.1) or 0.1
        )
        if max_missing_ratio < 0.01:
            return "warning"
        return "ready"

    @staticmethod
    def _train_end_index(bars: list[NormalizedMarketBar]) -> int:
        unique_timestamps = len({bar.event_time for bar in bars})
        if unique_timestamps < 4:
            return 1
        return max(1, min(unique_timestamps - 4, unique_timestamps // 2))

    @staticmethod
    def _valid_end_index(bars: list[NormalizedMarketBar]) -> int:
        unique_timestamps = len({bar.event_time for bar in bars})
        train_end = PrepareWorkflowService._train_end_index(bars)
        if unique_timestamps < 5:
            return max(train_end + 1, min(unique_timestamps - 3, train_end + 1))
        return max(train_end + 1, min(unique_timestamps - 3, int(unique_timestamps * 0.75)))
