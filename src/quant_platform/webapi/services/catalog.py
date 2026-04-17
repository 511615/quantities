from __future__ import annotations

from bisect import bisect_right
import json
import os
import shutil
import uuid
import zipfile
from datetime import UTC, datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Any

from quant_platform.backtest.contracts.backtest import BacktestResult
from quant_platform.backtest.metrics.comparison import build_backtest_summary_row
from quant_platform.common.enums.core import LabelKind
from quant_platform.common.hashing.digest import stable_digest
from quant_platform.common.io.files import LocalArtifactStore
from quant_platform.common.types.core import FeatureField, TimeRange
from quant_platform.data.connectors.sentiment import RedditArchiveSentimentConnector
from quant_platform.data.contracts.ingestion import IngestionRequest
from quant_platform.data.contracts.data_asset import DataAssetRef
from quant_platform.data.contracts.market import NormalizedMarketBar
from quant_platform.data.contracts.series import NormalizedSeriesPoint
from quant_platform.datasets.builders.dataset_builder import DatasetBuilder
from quant_platform.datasets.contracts.dataset import (
    DatasetRef,
    DatasetSample,
    LabelSpec,
    SamplePolicy,
)
from quant_platform.datasets.labeling.forward_return import ForwardReturnLabeler
from quant_platform.datasets.manifests.dataset_manifest import DatasetBuildManifest
from quant_platform.datasets.splits.time_series import TimeSeriesSplitPlanner
from quant_platform.features.contracts.feature_view import (
    FeatureRow,
    FeatureViewBuildResult,
    FeatureViewRef,
)
from quant_platform.features.transforms.market_features import MarketFeatureBuilder
from quant_platform.webapi.repositories.artifacts import ArtifactRepository
from quant_platform.webapi.repositories.dataset_registry import (
    DatasetDependencyEntry,
    DatasetRegistryEntry,
    DatasetRegistryRepository,
)
from quant_platform.webapi.schemas.views import (
    ArtifactPreviewResponse,
    ArtifactView,
    BacktestAlignmentView,
    BacktestDeleteResponse,
    BacktestEngineView,
    BacktestListItemView,
    BacktestReportView,
    BacktestsResponse,
    BenchmarkDetailView,
    BenchmarkListItemView,
    BenchmarkRowView,
    ComparisonRowView,
    DataFreshnessView,
    DatasetAcquisitionRequest,
    DatasetDeleteResponse,
    DatasetAcquisitionSourceRequest,
    DatasetAcquisitionTimeWindow,
    DatasetDependenciesResponse,
    DatasetDependencyView,
    DatasetDetailView,
    DatasetFacetBucketView,
    DatasetFacetsView,
    DatasetFieldGroupView,
    DatasetFreshnessView,
    DatasetFusionBuildResponse,
    DatasetFusionRequest,
    DatasetFusionSourceRequest,
    DatasetBuildConfigView,
    DatasetLinkView,
    DatasetReadinessSummaryView,
    DatasetRequestOptionsView,
    DatasetRequestOptionView,
    DatasetSeriesResponse,
    DatasetSeriesView,
    DatasetSliceView,
    DatasetSlicesResponse,
    DatasetListResponse,
    DatasetNlpEventPreviewView,
    DatasetNlpInspectionView,
    DatasetNlpKeywordView,
    DatasetNlpSourceBreakdownView,
    DatasetNlpTimelinePointView,
    DatasetQualitySummaryView,
    DatasetSummaryView,
    DeepLinkView,
    ExperimentListItem,
    ExperimentsResponse,
    GlossaryHintView,
    JobStatusView,
    ModelComparisonRequest,
    ModelComparisonView,
    ModelTemplateCreateRequest,
    ModelTemplateListResponse,
    ModelTemplateUpdateRequest,
    ModelTemplateView,
    OhlcvBarView,
    OhlcvBarsResponse,
    PredictionArtifactView,
    RecentJobView,
    RecommendedActionView,
    RelatedBacktestView,
    ReviewSummaryView,
    RunDetailView,
    RunCompositionSourceView,
    RunCompositionView,
    ScenarioDeltaView,
    StableSummaryView,
    DatasetSymbolSelectorView,
    TimeValuePoint,
    TrainingDatasetSummaryView,
    TrainingDatasetsResponse,
    TrainedModelDetailView,
    TrainedModelListResponse,
    TrainedModelSummaryView,
    WarningSummaryView,
    WorkbenchOverviewView,
)
from quant_platform.webapi.services.backtest_protocol import (
    OFFICIAL_MARKET_BENCHMARK_DATASET_ID,
    OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID,
    OFFICIAL_NLP_ARCHIVAL_VENDORS,
    OFFICIAL_NLP_MAX_DUPLICATE_RATIO,
    OFFICIAL_NLP_MAX_TEST_EMPTY_BARS,
    OFFICIAL_NLP_MIN_ENTITY_LINK_COVERAGE_RATIO,
    OFFICIAL_NLP_MIN_TEST_COVERAGE_RATIO,
    compute_protocol_result,
    custom_backtest_template,
)

_SENTIMENT_VENDOR_CANONICAL_MAP: dict[str, str] = {
    "reddit_history_csv": "reddit_archive",
    "reddit_pullpush": "reddit_archive",
    "reddit_public": "reddit_archive",
}

OFFICIAL_MARKET_STANDARD_FEATURES_V1: list[str] = [
    "lag_return_1",
    "lag_return_2",
    "momentum_3",
    "realized_vol_3",
    "close_to_open",
    "range_frac",
    "volume_zscore",
    "volume_ratio_3",
]

OFFICIAL_MULTIMODAL_STANDARD_NLP_FEATURES_V1: list[str] = [
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
]

OFFICIAL_MULTIMODAL_STANDARD_AUX_FEATURES_V1: list[str] = [
    "macro_dff_value",
    "on_chain_ethereum_tvl",
    "derivatives_funding_rate",
    "derivatives_open_interest",
    "derivatives_global_long_short_ratio",
    "derivatives_taker_buy_sell_ratio",
]

OFFICIAL_MULTIMODAL_STANDARD_SCHEMA_VERSION = "official_multimodal_standard_v3"


class ResearchWorkbenchService:
    def __init__(
        self,
        repository: ArtifactRepository,
        dataset_registry: DatasetRegistryRepository,
        store: LocalArtifactStore,
        model_families: dict[str, str],
        model_registry_entries: dict[str, Any] | None = None,
        facade: Any | None = None,
    ) -> None:
        self.repository = repository
        self.dataset_registry = dataset_registry
        self.store = store
        self.model_families = model_families
        self.model_registry_entries = model_registry_entries or {}
        self.facade = facade
        self.templates_root = self.repository.artifact_root / "webapi" / "model_templates"
        self.trained_root = self.repository.artifact_root / "webapi" / "trained_models"
        self.templates_root.mkdir(parents=True, exist_ok=True)
        self.trained_root.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _canonical_sentiment_vendor(vendor: str | None) -> str | None:
        if not vendor:
            return vendor
        return _SENTIMENT_VENDOR_CANONICAL_MAP.get(vendor, vendor)

    @staticmethod
    def _prefer_offline_official_sources() -> bool:
        return bool(os.getenv("PYTEST_CURRENT_TEST") or os.getenv("QUANT_PLATFORM_OFFLINE_BENCHMARK"))

    @staticmethod
    def official_multimodal_feature_names_v1() -> list[str]:
        return [
            *OFFICIAL_MARKET_STANDARD_FEATURES_V1,
            *OFFICIAL_MULTIMODAL_STANDARD_AUX_FEATURES_V1,
            *OFFICIAL_MULTIMODAL_STANDARD_NLP_FEATURES_V1,
        ]

    @staticmethod
    def _official_nlp_feature_aliases(feature_name: str) -> list[str]:
        alias_map = {
            "news_event_count": [
                "news_event_count",
                "text_social_event_count",
                "text_reddit_event_count",
                "text_event_count",
            ],
            "text_reddit_attention_zscore_24h": [
                "text_reddit_attention_zscore_24h",
                "text_social_reddit_attention_zscore_24h",
            ],
            "text_reddit_body_len_mean_1h": [
                "text_reddit_body_len_mean_1h",
                "text_social_reddit_body_len_mean_1h",
            ],
            "text_reddit_comment_count_1h": [
                "text_reddit_comment_count_1h",
                "text_social_reddit_comment_count_1h",
            ],
            "text_reddit_controversiality_ratio_1h": [
                "text_reddit_controversiality_ratio_1h",
                "text_social_reddit_controversiality_ratio_1h",
            ],
            "text_reddit_core_subreddit_ratio_1h": [
                "text_reddit_core_subreddit_ratio_1h",
                "text_social_reddit_core_subreddit_ratio_1h",
            ],
            "text_reddit_negative_ratio_1h": [
                "text_reddit_negative_ratio_1h",
                "text_social_reddit_negative_ratio_1h",
                "sentiment_social_negative_ratio",
            ],
            "text_reddit_positive_ratio_1h": [
                "text_reddit_positive_ratio_1h",
                "text_social_reddit_positive_ratio_1h",
                "sentiment_social_positive_ratio",
            ],
            "text_reddit_score_mean_1h": [
                "text_reddit_score_mean_1h",
                "text_social_reddit_score_mean_1h",
            ],
            "text_reddit_score_sum_1h": [
                "text_reddit_score_sum_1h",
                "text_social_reddit_score_sum_1h",
            ],
            "text_reddit_sentiment_mean_1h": [
                "text_reddit_sentiment_mean_1h",
                "text_social_reddit_sentiment_mean_1h",
            ],
            "text_reddit_sentiment_std_1h": [
                "text_reddit_sentiment_std_1h",
                "text_social_reddit_sentiment_std_1h",
            ],
            "text_reddit_unique_author_count_1h": [
                "text_reddit_unique_author_count_1h",
                "text_social_reddit_unique_author_count_1h",
            ],
            "sentiment_score": [
                "sentiment_score",
                "sentiment_social_score",
                "sentiment_reddit_score",
            ],
        }
        return alias_map.get(feature_name, [feature_name])

    def _official_multimodal_schema_matches(self, payload: dict[str, Any] | None) -> bool:
        if not isinstance(payload, dict):
            return False
        feature_view_ref = payload.get("feature_view_ref")
        if not isinstance(feature_view_ref, dict):
            return False
        feature_names = [
            str(item.get("name"))
            for item in (feature_view_ref.get("feature_schema") or [])
            if isinstance(item, dict) and isinstance(item.get("name"), str)
        ]
        if feature_names != self.official_multimodal_feature_names_v1():
            return False
        manifest = self._dataset_manifest(payload)
        acquisition_profile = manifest.get("acquisition_profile") or {}
        if acquisition_profile.get("official_benchmark_version") != OFFICIAL_MULTIMODAL_STANDARD_SCHEMA_VERSION:
            return False
        if self._str(acquisition_profile.get("base_dataset_id")) != OFFICIAL_MARKET_BENCHMARK_DATASET_ID:
            return False
        if self._str(acquisition_profile.get("market_anchor_dataset_id")) != OFFICIAL_MARKET_BENCHMARK_DATASET_ID:
            return False
        market_payload = self._dataset_ref(OFFICIAL_MARKET_BENCHMARK_DATASET_ID)
        if not isinstance(market_payload, dict):
            return False
        market_snapshot_version = self._str(acquisition_profile.get("market_snapshot_version"))
        sentiment_snapshot_version = self._str(acquisition_profile.get("sentiment_snapshot_version"))
        current_market_snapshot_version = self._str(self._dataset_manifest(market_payload).get("snapshot_version"))
        if market_snapshot_version and market_snapshot_version != current_market_snapshot_version:
            return False
        data_domains = {self._str(item) for item in (acquisition_profile.get("data_domains") or []) if self._str(item)}
        if not {"market", "macro", "on_chain", "derivatives", "sentiment_events"}.issubset(data_domains):
            return False
        source_specs = acquisition_profile.get("source_specs") or []
        if not isinstance(source_specs, list):
            return False
        market_specs = [
            item for item in source_specs if isinstance(item, dict) and self._str(item.get("data_domain")) == "market"
        ]
        domain_counts = {
            domain: len(
                [
                    item
                    for item in source_specs
                    if isinstance(item, dict) and self._str(item.get("data_domain")) == domain
                ]
            )
            for domain in ["market", "macro", "on_chain", "derivatives", "sentiment_events"]
        }
        if domain_counts["market"] != 1 or min(
            domain_counts["macro"],
            domain_counts["on_chain"],
            domain_counts["derivatives"],
            domain_counts["sentiment_events"],
        ) < 1:
            return False
        market_symbols = list(((market_specs[0].get("symbol_selector") or {}).get("symbols") or []))
        if self._str(market_specs[0].get("source_vendor")) != "binance" or market_symbols != ["BTCUSDT"]:
            return False
        fusion_sources = acquisition_profile.get("fusion_sources") or []
        if not fusion_sources:
            return False
        fusion_domains = {
            self._str(item.get("data_domain"))
            for item in fusion_sources
            if isinstance(item, dict) and self._str(item.get("data_domain"))
        }
        if not {"macro", "on_chain", "derivatives", "sentiment_events"}.issubset(fusion_domains):
            return False
        if sentiment_snapshot_version and not any(self._str(item.get("storage_uri")) for item in fusion_sources):
            return False
        readiness = self.get_dataset_readiness(OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID)
        if readiness is None:
            return False
        if readiness.readiness_status != "ready" or readiness.official_nlp_gate_status == "failed":
            return False
        return (readiness.usable_row_count or 0) >= 24

    @staticmethod
    def _official_nlp_feature_name_from_metric_name(metric_name: str) -> str | None:
        metric_map = {
            "event_count": "news_event_count",
            "reddit_attention_zscore_24h": "text_reddit_attention_zscore_24h",
            "reddit_body_len_mean_1h": "text_reddit_body_len_mean_1h",
            "reddit_comment_count_1h": "text_reddit_comment_count_1h",
            "reddit_controversiality_ratio_1h": "text_reddit_controversiality_ratio_1h",
            "reddit_core_subreddit_ratio_1h": "text_reddit_core_subreddit_ratio_1h",
            "reddit_negative_ratio_1h": "text_reddit_negative_ratio_1h",
            "reddit_positive_ratio_1h": "text_reddit_positive_ratio_1h",
            "reddit_score_mean_1h": "text_reddit_score_mean_1h",
            "reddit_score_sum_1h": "text_reddit_score_sum_1h",
            "reddit_sentiment_mean_1h": "text_reddit_sentiment_mean_1h",
            "reddit_sentiment_std_1h": "text_reddit_sentiment_std_1h",
            "reddit_unique_author_count_1h": "text_reddit_unique_author_count_1h",
            "sentiment_score": "sentiment_score",
        }
        return metric_map.get(metric_name)

    @staticmethod
    def _official_auxiliary_source_specs() -> list[dict[str, str]]:
        return [
            {
                "data_domain": "macro",
                "vendor": "fred",
                "identifier": "DFF",
                "frequency": "1d",
                "feature_name": "macro_dff_value",
                "metric_name": "value",
            },
            {
                "data_domain": "on_chain",
                "vendor": "defillama",
                "identifier": "ethereum",
                "frequency": "1h",
                "feature_name": "on_chain_ethereum_tvl",
                "metric_name": "tvl",
            },
            {
                "data_domain": "derivatives",
                "vendor": "binance_futures",
                "identifier": "BTCUSDT",
                "frequency": "1h",
                "feature_name": "derivatives_funding_rate",
                "metric_name": "funding_rate",
            },
            {
                "data_domain": "derivatives",
                "vendor": "binance_futures",
                "identifier": "BTCUSDT",
                "frequency": "1h",
                "feature_name": "derivatives_open_interest",
                "metric_name": "open_interest",
            },
            {
                "data_domain": "derivatives",
                "vendor": "binance_futures",
                "identifier": "BTCUSDT",
                "frequency": "1h",
                "feature_name": "derivatives_global_long_short_ratio",
                "metric_name": "global_long_short_ratio",
            },
            {
                "data_domain": "derivatives",
                "vendor": "binance_futures",
                "identifier": "BTCUSDT",
                "frequency": "1h",
                "feature_name": "derivatives_taker_buy_sell_ratio",
                "metric_name": "taker_buy_sell_ratio",
            },
        ]

    def _build_synthetic_official_auxiliary_points(
        self,
        *,
        market_samples: list[DatasetSample],
        feature_name: str,
        metric_name: str,
        data_domain: str,
        vendor: str,
        identifier: str,
        frequency: str,
    ) -> list[NormalizedSeriesPoint]:
        points: list[NormalizedSeriesPoint] = []
        for index, sample in enumerate(sorted(market_samples, key=lambda item: item.timestamp)):
            lag_return = float(sample.features.get("lag_return_1", 0.0))
            volume_ratio = float(sample.features.get("volume_ratio_3", 1.0))
            if metric_name == "value":
                value = 0.5 + index * 0.01
            elif metric_name == "tvl":
                value = 1_000_000.0 + 50_000.0 * volume_ratio + index * 100.0
            elif metric_name == "funding_rate":
                value = lag_return * 0.01
            elif metric_name == "open_interest":
                value = 10_000.0 + 100.0 * index + abs(lag_return) * 5_000.0
            elif metric_name == "global_long_short_ratio":
                value = 1.0 + max(min(lag_return * 5.0, 0.25), -0.25)
            elif metric_name == "taker_buy_sell_ratio":
                value = 1.0 + max(min(lag_return * 8.0, 0.3), -0.3)
            else:
                value = float(index)
            points.append(
                NormalizedSeriesPoint(
                    event_time=sample.timestamp,
                    available_time=sample.available_time,
                    series_key=f"{identifier}:{feature_name}",
                    entity_key=identifier,
                    domain=data_domain,
                    vendor=vendor,
                    metric_name=metric_name,
                    frequency=frequency,
                    value=float(value),
                    dimensions={"identifier": identifier, "feature_name": feature_name},
                )
            )
        return points

    def _build_synthetic_official_nlp_points(
        self,
        market_samples: list[DatasetSample],
    ) -> list[NormalizedSeriesPoint]:
        metric_names = [
            "event_count",
            "reddit_attention_zscore_24h",
            "reddit_body_len_mean_1h",
            "reddit_comment_count_1h",
            "reddit_controversiality_ratio_1h",
            "reddit_core_subreddit_ratio_1h",
            "reddit_negative_ratio_1h",
            "reddit_positive_ratio_1h",
            "reddit_score_mean_1h",
            "reddit_score_sum_1h",
            "reddit_sentiment_mean_1h",
            "reddit_sentiment_std_1h",
            "reddit_unique_author_count_1h",
            "sentiment_score",
        ]
        rows: list[NormalizedSeriesPoint] = []
        for index, sample in enumerate(sorted(market_samples, key=lambda item: item.timestamp)):
            lag_return = float(sample.features.get("lag_return_1", 0.0))
            preview_payload = [
                {
                    "event_id": f"official-nlp-{index}",
                    "title": f"BTC market checkpoint {index}",
                    "snippet": f"Deterministic archival benchmark event {index}",
                    "source": "reddit_archive",
                    "source_type": "social",
                    "symbol": sample.entity_key,
                    "event_time": sample.timestamp.isoformat(),
                    "available_time": sample.available_time.isoformat(),
                    "sentiment_score": round(max(min(lag_return * 5.0, 1.0), -1.0), 4),
                }
            ]
            keyword_payload = [
                {"term": "btc", "count": 4, "weight": 0.4},
                {"term": "benchmark", "count": 3, "weight": 0.3},
            ]
            metric_values = {
                "event_count": 1.0,
                "reddit_attention_zscore_24h": 0.5 + index * 0.01,
                "reddit_body_len_mean_1h": 120.0 + index,
                "reddit_comment_count_1h": 10.0 + index,
                "reddit_controversiality_ratio_1h": 0.05,
                "reddit_core_subreddit_ratio_1h": 1.0,
                "reddit_negative_ratio_1h": max(0.0, 0.2 - lag_return),
                "reddit_positive_ratio_1h": min(1.0, 0.2 + lag_return + 0.5),
                "reddit_score_mean_1h": 100.0 + index,
                "reddit_score_sum_1h": 200.0 + index * 2.0,
                "reddit_sentiment_mean_1h": max(min(lag_return * 5.0, 1.0), -1.0),
                "reddit_sentiment_std_1h": 0.05,
                "reddit_unique_author_count_1h": 1.0 + (index % 3),
                "sentiment_score": max(min(lag_return * 5.0, 1.0), -1.0),
            }
            for metric_name in metric_names:
                rows.append(
                    NormalizedSeriesPoint(
                        event_time=sample.timestamp,
                        available_time=sample.available_time,
                        series_key=f"BTC:{metric_name}",
                        entity_key="BTC",
                        domain="sentiment_events",
                        vendor="reddit_archive",
                        metric_name=metric_name,
                        frequency="1h",
                        value=float(metric_values[metric_name]),
                        dimensions={
                            "symbol": sample.entity_key,
                            "event_id": f"official-nlp-{index}",
                            "preview_events_json": json.dumps(preview_payload, ensure_ascii=False),
                            "keywords_json": json.dumps(keyword_payload, ensure_ascii=False),
                        },
                    )
                )
        return rows

    def ensure_official_multimodal_benchmark(self) -> str:
        if self.facade is None:
            return OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID
        self._ensure_official_market_benchmark()
        existing_payload = self._dataset_ref(OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID)
        if self._official_multimodal_schema_matches(existing_payload):
            existing_samples = self._load_dataset_samples(existing_payload)
            if existing_samples:
                self.facade.dataset_store[OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID] = existing_samples
            return OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID
        return self._materialize_official_multimodal_benchmark()

    def _ensure_official_market_benchmark(self) -> None:
        market_payload = self._dataset_ref(OFFICIAL_MARKET_BENCHMARK_DATASET_ID)
        if isinstance(market_payload, dict):
            return
        if self.facade is None:
            raise ValueError(
                f"Official market benchmark dataset '{OFFICIAL_MARKET_BENCHMARK_DATASET_ID}' is not available."
            )
        if self._prefer_offline_official_sources():
            synthetic_request = self.facade.prepare_workflow.build_synthetic_reference_request().model_copy(
                update={"dataset_id": OFFICIAL_MARKET_BENCHMARK_DATASET_ID}
            )
            self.facade.prepare_workflow.prepare(synthetic_request)
        else:
            try:
                self.facade.build_real_benchmark_dataset()
            except Exception:
                synthetic_request = self.facade.prepare_workflow.build_synthetic_reference_request().model_copy(
                    update={"dataset_id": OFFICIAL_MARKET_BENCHMARK_DATASET_ID}
                )
                self.facade.prepare_workflow.prepare(synthetic_request)
        self.dataset_registry.bootstrap_from_artifacts()

    def _load_existing_official_multimodal_sentiment_points(
        self,
        payload: dict[str, Any] | None,
    ) -> list[NormalizedSeriesPoint]:
        if not isinstance(payload, dict):
            return []
        feature_view_ref = payload.get("feature_view_ref") or {}
        input_data_refs = feature_view_ref.get("input_data_refs") or []
        candidate_uris: list[str] = []
        for input_ref in input_data_refs:
            if not isinstance(input_ref, dict):
                continue
            tags = {self._str(tag) for tag in (input_ref.get("tags") or []) if self._str(tag)}
            if (
                self._str(input_ref.get("asset_id")) != "sentiment_social_btc_1h"
                and self._str(input_ref.get("source")) != "reddit_archive"
                and self._str(input_ref.get("venue")) != "reddit_archive"
                and "sentiment_events" not in tags
            ):
                continue
            storage_uri = self._str(input_ref.get("storage_uri"))
            if storage_uri:
                candidate_uris.append(storage_uri)
        for storage_uri in candidate_uris:
            path = self._resolve_artifact_path(storage_uri)
            if not path.exists():
                continue
            loaded = self._load(path).get("rows", [])
            if not isinstance(loaded, list):
                continue
            points = [
                NormalizedSeriesPoint.model_validate(item)
                for item in loaded
                if isinstance(item, dict)
            ]
            if points:
                return points
        return []

    def _materialize_official_multimodal_benchmark(self) -> str:
        if self.facade is None:
            raise ValueError("Facade is required to materialize the official multimodal benchmark.")
        market_payload = self._dataset_ref(OFFICIAL_MARKET_BENCHMARK_DATASET_ID)
        if not isinstance(market_payload, dict):
            raise ValueError(
                f"Official market benchmark dataset '{OFFICIAL_MARKET_BENCHMARK_DATASET_ID}' is not available."
            )
        market_dataset_ref = DatasetRef.model_validate(market_payload)
        market_samples = self._load_dataset_samples(market_payload)
        if not market_samples:
            raise ValueError(
                f"Official market benchmark dataset '{OFFICIAL_MARKET_BENCHMARK_DATASET_ID}' has no samples."
            )
        market_feature_schema = {
            field.name: field for field in market_dataset_ref.feature_view_ref.feature_schema
        }
        missing_market_features = [
            name for name in OFFICIAL_MARKET_STANDARD_FEATURES_V1 if name not in market_feature_schema
        ]
        if missing_market_features:
            raise ValueError(
                "Official market benchmark is missing standard market features: "
                + ", ".join(missing_market_features)
            )
        market_input_ref = market_dataset_ref.feature_view_ref.input_data_refs[0]
        existing_payload = self._dataset_ref(OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID)
        sentiment_points: list[NormalizedSeriesPoint] = []
        if not self._prefer_offline_official_sources():
            sentiment_points = self._load_existing_official_multimodal_sentiment_points(existing_payload)
        if not sentiment_points and self._prefer_offline_official_sources():
            sentiment_points = self._build_synthetic_official_nlp_points(market_samples)
        if not sentiment_points:
            sentiment_connector = RedditArchiveSentimentConnector(self.repository.artifact_root)
            sentiment_points = sentiment_connector.store.query_points(
                asset_id="BTC",
                start_time=market_input_ref.time_range.start,
                end_time=market_input_ref.time_range.end,
                vendor="reddit_archive",
            )
            if not sentiment_points and not self._prefer_offline_official_sources():
                try:
                    sentiment_result = sentiment_connector.ingest(
                        IngestionRequest(
                            data_domain="sentiment_events",
                            vendor="reddit_archive",
                            request_id=(
                                "official-multimodal-benchmark-"
                                f"{market_input_ref.time_range.start.isoformat()}-"
                                f"{market_input_ref.time_range.end.isoformat()}"
                            ),
                            time_range={
                                "start": market_input_ref.time_range.start,
                                "end": market_input_ref.time_range.end,
                            },
                            identifiers=["BTC"],
                            frequency="1h",
                            options={"symbol": "BTCUSDT"},
                        )
                    )
                    sentiment_points = [
                        NormalizedSeriesPoint.model_validate(item)
                        for item in (sentiment_result.metadata.get("rows") or [])
                        if isinstance(item, dict)
                    ]
                except Exception:
                    sentiment_points = self._build_synthetic_official_nlp_points(market_samples)
        if not sentiment_points:
            sentiment_points = self._build_synthetic_official_nlp_points(market_samples)
        points_payload = {"rows": [point.model_dump(mode="json") for point in sentiment_points]}
        points_artifact = self.store.write_json(
            f"datasets/{OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID}_sentiment_points.json",
            points_payload,
        )
        auxiliary_contexts: list[dict[str, Any]] = []
        for spec in self._official_auxiliary_source_specs():
            if self._prefer_offline_official_sources():
                points = self._build_synthetic_official_auxiliary_points(
                    market_samples=market_samples,
                    feature_name=spec["feature_name"],
                    metric_name=spec["metric_name"],
                    data_domain=spec["data_domain"],
                    vendor=spec["vendor"],
                    identifier=spec["identifier"],
                    frequency=spec["frequency"],
                )
                fetch_status = "synthetic_fallback"
            else:
                try:
                    points, fetch_status = self.facade.runtime.ingestion_service.fetch_series_points(
                        data_domain=spec["data_domain"],
                        identifier=spec["identifier"],
                        vendor=spec["vendor"],
                        frequency=spec["frequency"],
                        start_time=market_input_ref.time_range.start,
                        end_time=market_input_ref.time_range.end,
                        options={"metric_name": spec["metric_name"]},
                    )
                except Exception:
                    points = self._build_synthetic_official_auxiliary_points(
                        market_samples=market_samples,
                        feature_name=spec["feature_name"],
                        metric_name=spec["metric_name"],
                        data_domain=spec["data_domain"],
                        vendor=spec["vendor"],
                        identifier=spec["identifier"],
                        frequency=spec["frequency"],
                    )
                    fetch_status = "synthetic_fallback"
            filtered_points = [
                point for point in points if point.metric_name == spec["metric_name"]
            ]
            if not filtered_points:
                filtered_points = self._build_synthetic_official_auxiliary_points(
                    market_samples=market_samples,
                    feature_name=spec["feature_name"],
                    metric_name=spec["metric_name"],
                    data_domain=spec["data_domain"],
                    vendor=spec["vendor"],
                    identifier=spec["identifier"],
                    frequency=spec["frequency"],
                )
                fetch_status = "synthetic_fallback"
            storage_uri = self._write_fusion_series_rows(
                OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID,
                spec["feature_name"],
                filtered_points,
            )
            auxiliary_contexts.append(
                {
                    **spec,
                    "points": filtered_points,
                    "fetch_status": fetch_status,
                    "storage_uri": storage_uri,
                    "resolve_point": self._build_series_point_resolver(
                        filtered_points,
                        alignment_policy_name="available_time_safe_asof",
                    ),
                }
            )

        nlp_feature_names_seen: set[str] = set()
        nlp_rows_by_key: dict[tuple[str, datetime], dict[str, Any]] = {}
        for point in sentiment_points:
            official_feature_name = self._official_nlp_feature_name_from_metric_name(point.metric_name)
            if official_feature_name is None:
                continue
            nlp_feature_names_seen.add(official_feature_name)
            row_key = (point.entity_key, point.event_time)
            row_entry = nlp_rows_by_key.setdefault(
                row_key,
                {
                    "available_time": point.available_time,
                    "values": {},
                },
            )
            if point.available_time > row_entry["available_time"]:
                row_entry["available_time"] = point.available_time
            row_entry["values"][official_feature_name] = float(point.value)
        missing_nlp_features = [
            name for name in OFFICIAL_MULTIMODAL_STANDARD_NLP_FEATURES_V1 if name not in nlp_feature_names_seen
        ]
        if missing_nlp_features:
            raise ValueError(
                "Official Reddit archive feed is missing standard NLP features: "
                + ", ".join(missing_nlp_features)
            )
        nlp_feature_rows = [
            FeatureRow(
                entity_key=entity_key,
                timestamp=timestamp,
                available_time=entry["available_time"],
                values=entry["values"],
            )
            for (entity_key, timestamp), entry in sorted(
                nlp_rows_by_key.items(),
                key=lambda item: (item[0][1], item[0][0]),
            )
        ]
        if not nlp_feature_rows:
            raise ValueError("Official multimodal benchmark could not build Reddit archive feature rows.")
        nlp_by_timestamp = {row.timestamp: row for row in nlp_feature_rows}
        enriched_rows: list[FeatureRow] = []
        labels: dict[tuple[str, datetime], float] = {}
        missing_counts = {
            name: 0
            for name in [
                *OFFICIAL_MULTIMODAL_STANDARD_AUX_FEATURES_V1,
                *OFFICIAL_MULTIMODAL_STANDARD_NLP_FEATURES_V1,
            ]
        }
        dropped_missing_rows = 0
        for market_sample in sorted(market_samples, key=lambda item: (item.timestamp, item.entity_key)):
            nlp_sample = nlp_by_timestamp.get(market_sample.timestamp)
            auxiliary_values: dict[str, float] = {}
            auxiliary_available_times: list[datetime] = []
            missing_aux_features: list[str] = []
            for context in auxiliary_contexts:
                aligned_point = context["resolve_point"](
                    market_sample.timestamp,
                    market_sample.available_time,
                )
                if aligned_point is None:
                    missing_aux_features.append(context["feature_name"])
                    continue
                auxiliary_values[context["feature_name"]] = float(aligned_point.value)
                auxiliary_available_times.append(aligned_point.available_time)
            if nlp_sample is None or missing_aux_features:
                dropped_missing_rows += 1
                for feature_name in missing_aux_features:
                    missing_counts[feature_name] += 1
                for feature_name in OFFICIAL_MULTIMODAL_STANDARD_NLP_FEATURES_V1:
                    missing_counts[feature_name] += 1
                continue
            missing_features = [
                feature_name
                for feature_name in OFFICIAL_MULTIMODAL_STANDARD_NLP_FEATURES_V1
                if feature_name not in nlp_sample.values
            ]
            if missing_features:
                dropped_missing_rows += 1
                for feature_name in missing_features:
                    missing_counts[feature_name] += 1
                continue
            values = {
                **{
                    feature_name: float(market_sample.features[feature_name])
                    for feature_name in OFFICIAL_MARKET_STANDARD_FEATURES_V1
                },
                **auxiliary_values,
                **{
                    feature_name: float(nlp_sample.values[feature_name])
                    for feature_name in OFFICIAL_MULTIMODAL_STANDARD_NLP_FEATURES_V1
                },
            }
            available_time = max(
                [market_sample.available_time, nlp_sample.available_time, *auxiliary_available_times]
            )
            enriched_rows.append(
                FeatureRow(
                    entity_key=market_sample.entity_key,
                    timestamp=market_sample.timestamp,
                    available_time=available_time,
                    values=values,
                )
            )
            labels[(market_sample.entity_key, market_sample.timestamp)] = market_sample.target
        if len(enriched_rows) < 3:
            raise ValueError("Official multimodal benchmark could not align enough market/NLP rows.")

        feature_schema = [
            market_feature_schema[name] for name in OFFICIAL_MARKET_STANDARD_FEATURES_V1
        ] + [
            FeatureField(
                name=context["feature_name"],
                dtype="float",
                lineage_source=context["data_domain"],
                max_available_time=max(point.available_time for point in context["points"]),
            )
            for context in auxiliary_contexts
        ] + [
            FeatureField(
                name=name,
                dtype="float",
                lineage_source="sentiment_events",
                max_available_time=max(point.available_time for point in sentiment_points),
            )
            for name in OFFICIAL_MULTIMODAL_STANDARD_NLP_FEATURES_V1
        ]
        sentiment_input_ref = DataAssetRef(
            asset_id="sentiment_social_btc_1h",
            schema_version=1,
            source="reddit_archive",
            symbol="BTC",
            venue="reddit_archive",
            frequency="1h",
            time_range=TimeRange(
                start=min(row.timestamp for row in nlp_feature_rows),
                end=max(row.timestamp for row in nlp_feature_rows) + timedelta(seconds=1),
            ),
            storage_uri=points_artifact.uri,
            content_hash=stable_digest(points_payload),
            entity_key="BTC",
            tags=["sentiment_events"],
            request_origin="official_template",
        )
        self.store.write_model(f"datasets/{sentiment_input_ref.asset_id}_ref.json", sentiment_input_ref)
        auxiliary_input_refs = [
            DataAssetRef(
                asset_id=f"{context['data_domain']}_{context['identifier']}_{context['frequency']}",
                schema_version=1,
                source=context["vendor"],
                symbol=context["identifier"],
                venue=context["vendor"],
                frequency=context["frequency"],
                time_range=TimeRange(
                    start=min(point.event_time for point in context["points"]),
                    end=max(point.event_time for point in context["points"]) + timedelta(seconds=1),
                ),
                storage_uri=context["storage_uri"],
                content_hash=stable_digest(
                    [point.model_dump(mode="json") for point in context["points"]]
                ),
                entity_key=context["identifier"],
                tags=["fusion_input", f"domain:{context['data_domain']}"],
                request_origin="official_template",
            )
            for context in auxiliary_contexts
        ]
        official_as_of_time = max(
            market_dataset_ref.feature_view_ref.as_of_time,
            *[max(point.available_time for point in context["points"]) for context in auxiliary_contexts],
            max(point.available_time for point in sentiment_points),
        )
        feature_view_ref = FeatureViewRef(
            feature_set_id="multi_domain_fusion_v1",
            input_data_refs=[
                *list(market_dataset_ref.feature_view_ref.input_data_refs),
                *auxiliary_input_refs,
                sentiment_input_ref,
            ],
            as_of_time=official_as_of_time,
            feature_schema=feature_schema,
            build_config_hash=stable_digest(
                {
                    "dataset_id": OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID,
                    "schema_version": OFFICIAL_MULTIMODAL_STANDARD_SCHEMA_VERSION,
                    "market_snapshot_version": self._str(self._dataset_manifest(market_payload).get("snapshot_version")),
                    "sentiment_snapshot_version": stable_digest(points_payload)[:12],
                    "feature_names": self.official_multimodal_feature_names_v1(),
                }
            ),
            storage_uri=f"artifact://datasets/{OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID}_feature_rows.json",
        )
        self.store.write_json(
            f"datasets/{OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID}_feature_rows.json",
            {"rows": [row.model_dump(mode="json") for row in enriched_rows]},
        )
        feature_result = FeatureViewBuildResult(feature_view_ref=feature_view_ref, rows=enriched_rows)
        sample_policy = market_dataset_ref.sample_policy.model_copy(
            update={"recommended_training_use": "fusion_training_panel"}
        )
        dataset_ref, samples, dataset_manifest = DatasetBuilder.build_dataset(
            dataset_id=OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID,
            feature_result=feature_result,
            labels=labels,
            label_spec=market_dataset_ref.label_spec,
            split_manifest=market_dataset_ref.split_manifest,
            sample_policy=sample_policy,
        )
        coverage_by_feature = {
            feature_name: (
                0.0
                if len(market_samples) == 0
                else (len(market_samples) - missing_counts[feature_name]) / len(market_samples)
            )
            for feature_name in [
                *OFFICIAL_MULTIMODAL_STANDARD_AUX_FEATURES_V1,
                *OFFICIAL_MULTIMODAL_STANDARD_NLP_FEATURES_V1,
            ]
        }
        market_manifest = self._dataset_manifest(market_payload)
        market_snapshot_version = self._str(market_manifest.get("snapshot_version"))
        sentiment_snapshot_version = stable_digest(points_payload)[:12]
        auxiliary_snapshot_versions = {
            context["feature_name"]: stable_digest(
                [point.model_dump(mode="json") for point in context["points"]]
            )[:12]
            for context in auxiliary_contexts
        }
        snapshot_version = stable_digest(
            {
                "dataset_id": OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID,
                "schema_version": OFFICIAL_MULTIMODAL_STANDARD_SCHEMA_VERSION,
                "market_snapshot_version": market_snapshot_version,
                "auxiliary_snapshot_versions": auxiliary_snapshot_versions,
                "sentiment_snapshot_version": sentiment_snapshot_version,
                "sample_count": len(samples),
            }
        )[:12]
        dataset_manifest = dataset_manifest.model_copy(
            update={
                "asset_id": market_manifest.get("asset_id") or OFFICIAL_MARKET_BENCHMARK_DATASET_ID,
                "feature_set_id": "multi_domain_fusion_v1",
                "dropped_rows": dataset_manifest.dropped_rows + dropped_missing_rows,
                "raw_row_count": len(enriched_rows),
                "usable_sample_count": len(samples),
                "snapshot_version": snapshot_version,
                "readiness_status": "ready" if samples else "not_ready",
                "alignment_status": "aligned_asof",
                "missing_feature_status": "clean",
                "label_alignment_status": "aligned",
                "split_integrity_status": "valid",
                "temporal_safety_status": "passed",
                "freshness_status": "fresh",
                "quality_status": "healthy",
                "build_config": {
                    "sample_policy_name": "fusion_training_panel_strict",
                    "alignment_policy_name": "available_time_safe_asof",
                    "missing_feature_policy_name": "drop_if_missing",
                    "sample_policy": {},
                    "alignment_policy": {"mode": "available_time_safe_asof"},
                    "missing_feature_policy": {
                        "strategy": "drop_if_missing",
                        "coverage_by_feature": coverage_by_feature,
                    },
                    "official_schema_version": OFFICIAL_MULTIMODAL_STANDARD_SCHEMA_VERSION,
                },
                "acquisition_profile": {
                    **dict(market_manifest.get("acquisition_profile") or {}),
                    "request_name": "official_reddit_pullpush_multimodal_v2",
                    "request_origin": "official_template",
                    "data_domain": "market",
                    "data_domains": ["market", "macro", "on_chain", "derivatives", "sentiment_events"],
                    "dataset_type": "fusion_training_panel",
                    "base_dataset_id": OFFICIAL_MARKET_BENCHMARK_DATASET_ID,
                    "market_anchor_dataset_id": OFFICIAL_MARKET_BENCHMARK_DATASET_ID,
                    "source_dataset_ids": [OFFICIAL_MARKET_BENCHMARK_DATASET_ID],
                    "fusion_domains": ["market", "macro", "on_chain", "derivatives", "sentiment_events"],
                    "source_vendor": "binance",
                    "source_specs": (
                        [
                            {
                                "data_domain": "market",
                                "source_vendor": "binance",
                                "exchange": "binance",
                                "frequency": "1h",
                                "symbol_selector": {"symbols": ["BTCUSDT"]},
                            }
                        ]
                        + [
                            {
                                "data_domain": context["data_domain"],
                                "source_vendor": context["vendor"],
                                "exchange": context["vendor"],
                                "frequency": context["frequency"],
                                "identifier": context["identifier"],
                                "feature_name": context["feature_name"],
                                "metric_name": context["metric_name"],
                            }
                            for context in auxiliary_contexts
                        ]
                        + [
                            {
                                "data_domain": "sentiment_events",
                                "source_vendor": "reddit_archive",
                                "exchange": "reddit_archive",
                                "frequency": "1h",
                                "identifier": "BTC",
                            }
                        ]
                    ),
                    "fusion_sources": (
                        [
                            {
                                "data_domain": context["data_domain"],
                                "vendor": context["vendor"],
                                "identifier": context["identifier"],
                                "feature_name": context["feature_name"],
                                "frequency": context["frequency"],
                                "metric_name": context["metric_name"],
                                "fetch_status": context["fetch_status"],
                                "storage_uri": context["storage_uri"],
                            }
                            for context in auxiliary_contexts
                        ]
                        + [
                            {
                                "data_domain": "sentiment_events",
                                "vendor": "reddit_archive",
                                "identifier": "BTC",
                                "feature_name": feature_name,
                                "frequency": "1h",
                                "metric_name": feature_name,
                                "fetch_status": "connector_direct",
                                "storage_uri": points_artifact.uri,
                            }
                            for feature_name in OFFICIAL_MULTIMODAL_STANDARD_NLP_FEATURES_V1
                        ]
                    ),
                    "coverage_by_feature": coverage_by_feature,
                    "connector_status_by_source": {
                        f"market:{OFFICIAL_MARKET_BENCHMARK_DATASET_ID}": self._str(
                            (market_manifest.get("acquisition_profile") or {}).get("request_origin")
                        )
                        or "unknown",
                        **{
                            f"{context['data_domain']}:{context['vendor']}:{context['identifier']}": context["fetch_status"]
                            for context in auxiliary_contexts
                        },
                        "sentiment_events:reddit_archive:BTC": "connector_direct",
                    },
                    "merge_policy_name": "available_time_safe_asof",
                    "official_benchmark_version": OFFICIAL_MULTIMODAL_STANDARD_SCHEMA_VERSION,
                    "market_snapshot_version": market_snapshot_version,
                    "auxiliary_snapshot_versions": auxiliary_snapshot_versions,
                    "sentiment_snapshot_version": sentiment_snapshot_version,
                    "archival_nlp_source_only": True,
                    "internal_visibility": "public",
                },
            }
        )
        dataset_samples_artifact = self.store.write_json(
            f"datasets/{OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID}_dataset_samples.json",
            {"samples": [sample.model_dump(mode="json") for sample in samples]},
        )
        feature_view_artifact = self.store.write_model(
            f"datasets/{OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID}_feature_view_ref.json",
            feature_view_ref,
        )
        dataset_manifest_artifact = self.store.write_model(
            f"datasets/{OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID}_dataset_manifest.json",
            dataset_manifest,
        )
        dataset_ref = dataset_ref.model_copy(
            update={
                "dataset_manifest_uri": dataset_manifest_artifact.uri,
                "dataset_samples_uri": dataset_samples_artifact.uri,
                "entity_scope": market_dataset_ref.entity_scope,
                "entity_count": market_dataset_ref.entity_count,
                "readiness_status": dataset_manifest.readiness_status,
            }
        )
        self.store.write_model(
            f"datasets/{OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID}_dataset_ref.json",
            dataset_ref,
        )
        self.facade.dataset_store[OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID] = samples
        self.dataset_registry.bootstrap_from_artifacts()
        return OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID

    @staticmethod
    def _system_recommended_dataset_ids() -> set[str]:
        return {
            "smoke_dataset",
            "baseline_benchmark_dataset",
            "baseline_real_benchmark_dataset",
            "baseline_reference_benchmark_dataset",
            OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID,
        }

    def _dataset_protection_meta(self, dataset_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        request_origin = self._str(payload.get("request_origin"))
        acquisition_profile = dict((payload.get("acquisition_profile") or {}))
        protected = (
            dataset_id in self._system_recommended_dataset_ids()
            or request_origin in {"system_recommendation", "official_template", "benchmark_preset"}
            or bool(acquisition_profile.get("system_recommended"))
        )
        return {
            "is_system_recommended": protected,
            "is_protected": protected,
            "deletion_policy": "system_protected" if protected else "user_managed",
            "download_available": True,
        }

    def _dataset_link(
        self,
        dataset_id: str,
        *,
        label: str | None = None,
        role: str | None = None,
        modality: str | None = None,
    ) -> DatasetLinkView:
        entry = self._dataset_entry(dataset_id)
        payload = entry.payload if entry is not None else None
        resolved_label = (
            label
            or (
                self._dataset_display_meta(payload).get("display_name")
                if isinstance(payload, dict)
                else None
            )
            or dataset_id
        )
        resolved_modality = (
            modality
            or (entry.data_domain if entry is not None else None)
            or None
        )
        return DatasetLinkView(
            dataset_id=dataset_id,
            label=resolved_label,
            href=f"/datasets/{dataset_id}",
            api_path=f"/api/datasets/{dataset_id}",
            role=role,
            modality=resolved_modality,
        )

    def _dataset_links_from_ids(
        self,
        dataset_ids: list[str],
        *,
        role_map: dict[str, str] | None = None,
        modality_map: dict[str, str] | None = None,
    ) -> list[DatasetLinkView]:
        links: list[DatasetLinkView] = []
        seen: set[str] = set()
        for dataset_id in dataset_ids:
            if not dataset_id or dataset_id in seen:
                continue
            seen.add(dataset_id)
            links.append(
                self._dataset_link(
                    dataset_id,
                    role=(role_map or {}).get(dataset_id),
                    modality=(modality_map or {}).get(dataset_id),
                )
            )
        return links

    @staticmethod
    def _dataset_modality_from_domain(data_domain: str | None) -> str | None:
        normalized = (data_domain or "").strip().lower()
        if not normalized:
            return None
        if normalized == "market":
            return "market"
        if normalized in {"sentiment_events", "sentiment", "text", "news"}:
            return "nlp"
        return normalized

    def _run_dataset_ids(
        self,
        *,
        dataset_id: str | None,
        manifest: dict[str, Any],
        metadata: dict[str, Any],
    ) -> list[str]:
        candidates: list[str] = []
        for value in [
            dataset_id,
            self._str(manifest.get("dataset_id")),
            self._str(metadata.get("primary_dataset_id")),
        ]:
            if value:
                candidates.append(value)
        for source in manifest.get("source_runs", []):
            if isinstance(source, dict):
                source_dataset_ids = source.get("dataset_ids")
                if isinstance(source_dataset_ids, list):
                    candidates.extend(
                        str(item) for item in source_dataset_ids if isinstance(item, str) and item
                    )
        for collection in [
            manifest.get("source_dataset_ids"),
            metadata.get("source_dataset_ids"),
            metadata.get("dataset_ids"),
        ]:
            if isinstance(collection, list):
                candidates.extend(str(item) for item in collection if isinstance(item, str) and item)
        deduped: list[str] = []
        for item in candidates:
            if item not in deduped:
                deduped.append(item)
        return deduped

    def _run_composition(self, manifest: dict[str, Any]) -> RunCompositionView | None:
        composition = manifest.get("composition")
        if not isinstance(composition, dict):
            return None
        source_runs_payload = composition.get("source_runs")
        if not isinstance(source_runs_payload, list) or not source_runs_payload:
            return None
        source_runs: list[RunCompositionSourceView] = []
        for source in source_runs_payload:
            if not isinstance(source, dict):
                continue
            source_run_id = self._str(source.get("run_id"))
            if not source_run_id:
                continue
            source_dataset_ids = [
                str(item)
                for item in source.get("dataset_ids", [])
                if isinstance(item, str) and item
            ]
            source_runs.append(
                RunCompositionSourceView(
                    run_id=source_run_id,
                    model_name=self._str(source.get("model_name")) or source_run_id,
                    modality=self._str(source.get("modality")),
                    weight=self._float(source.get("weight")),
                    dataset_ids=source_dataset_ids,
                    datasets=self._dataset_links_from_ids(source_dataset_ids),
                )
            )
        if not source_runs:
            return None
        rules = [
            str(item)
            for item in composition.get("rules", [])
            if isinstance(item, str) and item.strip()
        ]
        return RunCompositionView(
            fusion_strategy=self._str(composition.get("fusion_strategy")) or "late_score_blend",
            source_runs=source_runs,
            rules=rules,
        )

    def _official_composition_status(
        self,
        manifest: dict[str, Any],
    ) -> tuple[bool | None, list[str]]:
        composition = manifest.get("composition")
        if not isinstance(composition, dict):
            return None, []
        source_runs_payload = composition.get("source_runs")
        if not isinstance(source_runs_payload, list) or not source_runs_payload:
            return (
                False,
                ["This composed run is missing source-run metadata required for official backtests."],
            )

        blocking_reasons: list[str] = []
        for source in source_runs_payload:
            if not isinstance(source, dict):
                blocking_reasons.append(
                    "This composed run contains malformed source-run metadata required for official backtests."
                )
                continue
            source_run_id = self._str(source.get("run_id"))
            modality = self._str(source.get("modality"))
            if not source_run_id or not modality:
                blocking_reasons.append(
                    "This composed run has a source entry without run_id/modality metadata required for official backtests."
                )

        blocking_reasons = list(dict.fromkeys(blocking_reasons))
        return (len(blocking_reasons) == 0, blocking_reasons)

    def _backtest_dataset_ids(self, row: dict[str, Any]) -> list[str]:
        candidates: list[str] = []
        for value in [
            self._str(row.get("dataset_id")),
            self._str((row.get("protocol_metadata") or {}).get("primary_dataset_id"))
            if isinstance(row.get("protocol_metadata"), dict)
            else None,
        ]:
            if value:
                candidates.append(value)
        protocol_metadata = row.get("protocol_metadata")
        if isinstance(protocol_metadata, dict):
            for key in ["dataset_ids"]:
                collection = protocol_metadata.get(key)
                if isinstance(collection, list):
                    candidates.extend(
                        str(item) for item in collection if isinstance(item, str) and item
                    )
        deduped: list[str] = []
        for item in candidates:
            if item not in deduped:
                deduped.append(item)
        return deduped

    def _backtest_alignment(self, row: dict[str, Any]) -> BacktestAlignmentView | None:
        protocol_metadata = row.get("protocol_metadata")
        if not isinstance(protocol_metadata, dict):
            return None
        dataset_ids = self._backtest_dataset_ids(row)
        if not dataset_ids and not protocol_metadata.get("alignment_status"):
            return None
        dataset_roles = (
            protocol_metadata.get("dataset_roles")
            if isinstance(protocol_metadata.get("dataset_roles"), dict)
            else {}
        )
        dataset_modalities = (
            protocol_metadata.get("dataset_modalities")
            if isinstance(protocol_metadata.get("dataset_modalities"), dict)
            else {}
        )
        notes = [
            str(item)
            for item in protocol_metadata.get("alignment_notes", [])
            if isinstance(item, str) and item.strip()
        ]
        return BacktestAlignmentView(
            fusion_strategy=self._str(protocol_metadata.get("fusion_strategy")),
            dataset_ids=dataset_ids,
            datasets=self._dataset_links_from_ids(
                dataset_ids,
                role_map={str(key): str(value) for key, value in dataset_roles.items()},
                modality_map={str(key): str(value) for key, value in dataset_modalities.items()},
            ),
            alignment_status=self._str(protocol_metadata.get("alignment_status")),
            notes=notes,
        )

    @staticmethod
    def _is_multimodal_reference_model(model_name: str | None) -> bool:
        return (model_name or "").strip().lower() == "multimodal_reference"

    def workbench_overview(self, jobs: list[JobStatusView]) -> WorkbenchOverviewView:
        runs = self.list_runs(
            page=1,
            per_page=5,
            search=None,
            sort_by="created_at",
            sort_order="desc",
            model_name=None,
            dataset_id=None,
            status=None,
        ).items
        backtests = self.list_backtests(page=1, per_page=5, search=None, status=None).items
        benchmarks = self.list_benchmarks()[:5]
        datasets = self.list_datasets(page=1, per_page=5).items
        latest_dataset = max(
            datasets,
            key=lambda d: d.as_of_time or datetime.fromtimestamp(0, tz=UTC),
            default=None,
        )
        return WorkbenchOverviewView(
            generated_at=datetime.now(UTC),
            data_updated_at=(latest_dataset.as_of_time if latest_dataset else None),
            recent_runs=runs,
            recent_backtests=backtests,
            recent_benchmarks=benchmarks,
            recent_jobs=[
                RecentJobView(
                    job_id=job.job_id,
                    job_type=job.job_type,
                    status=job.status,
                    updated_at=job.updated_at,
                    dataset_id=job.result.dataset_id,
                    summary=job.result.summary,
                    result_links=job.result.result_links,
                    primary_stage=(job.stages[-1].name if job.stages else None),
                    deeplinks={link.kind: link.href for link in job.result.result_links},
                )
                for job in jobs[:5]
            ],
            data_freshness=DataFreshnessView(
                dataset_id=(latest_dataset.dataset_id if latest_dataset else None),
                as_of_time=(latest_dataset.as_of_time if latest_dataset else None),
                freshness=(latest_dataset.freshness.status if latest_dataset else "unknown"),
                source=(latest_dataset.data_source if latest_dataset else None),
            ),
            datasets=datasets,
            recommended_actions=[
                RecommendedActionView(
                    key="launch-train",
                    action_id="launch-train",
                    title="Launch training",
                    description="Start a training job from a model template.",
                    target_path="/runs",
                    href="/runs",
                ),
                RecommendedActionView(
                    key="launch-backtest",
                    action_id="launch-backtest",
                    title="Launch backtest",
                    description="Run backtest from a trained run prediction.",
                    target_path="/backtests",
                    href="/backtests",
                ),
            ],
        )

    def list_experiments(
        self,
        *,
        page: int,
        per_page: int,
        search: str | None,
        sort_by: str,
        sort_order: str,
        model_name: str | None,
        dataset_id: str | None,
        status: str | None,
    ) -> ExperimentsResponse:
        related_backtest_counts = self._related_backtest_count_map()
        items = [
            self._experiment_item_light(run_id, related_backtest_counts=related_backtest_counts)
            for run_id in self._run_ids()
        ]
        filtered = [
            item
            for item in items
            if (search is None or search.lower() in f"{item.run_id} {item.model_name}".lower())
            and (model_name is None or item.model_name == model_name)
            and (dataset_id is None or item.dataset_id == dataset_id)
            and (status is None or item.status == status)
        ]
        reverse = sort_order == "desc"
        if sort_by == "model_name":
            filtered.sort(key=lambda x: x.model_name, reverse=reverse)
        else:
            filtered.sort(
                key=lambda x: x.created_at or datetime.fromtimestamp(0, tz=UTC),
                reverse=reverse,
            )
        start = (page - 1) * per_page
        end = start + per_page
        return ExperimentsResponse(
            items=filtered[start:end],
            total=len(filtered),
            page=page,
            per_page=per_page,
            available_models=sorted({item.model_name for item in items}),
            available_datasets=sorted({item.dataset_id for item in items if item.dataset_id}),
            available_statuses=sorted({item.status for item in items}),
        )

    def list_runs(
        self,
        *,
        page: int,
        per_page: int,
        search: str | None,
        sort_by: str,
        sort_order: str,
        model_name: str | None,
        dataset_id: str | None,
        status: str | None,
    ) -> ExperimentsResponse:
        return self.list_experiments(
            page=page,
            per_page=per_page,
            search=search,
            sort_by=sort_by,
            sort_order=sort_order,
            model_name=model_name,
            dataset_id=dataset_id,
            status=status,
        )

    def get_run_detail(self, run_id: str) -> RunDetailView | None:
        tracking = self.repository.read_json_if_exists(f"tracking/{run_id}.json")
        if tracking is None and not (self.repository.artifact_root / "models" / run_id).exists():
            return None
        tracking = tracking or {}
        manifest = self.repository.read_json_if_exists(f"models/{run_id}/train_manifest.json") or self.repository.read_json_if_exists(f"models/{run_id}/manifest.json") or {}
        metadata = self.repository.read_json_if_exists(f"models/{run_id}/metadata.json") or {}
        evaluation_summary = self.repository.read_json_if_exists(
            f"models/{run_id}/evaluation_summary.json"
        ) or {}
        artifact_format_status, missing_artifacts, prediction_scopes = self._run_artifact_status(run_id)
        feature_importance_payload = (
            self.repository.read_json_if_exists(f"models/{run_id}/feature_importance.json") or {}
        )
        feature_importance = self._metrics(feature_importance_payload.get("feature_importance", {}))
        model_name = str((tracking.get("params") or {}).get("model_name") or metadata.get("model_name") or run_id)
        dataset_id = (tracking.get("params") or {}).get("dataset_id") or manifest.get("dataset_id")
        primary_dataset_id = str(dataset_id) if isinstance(dataset_id, str) else None
        dataset_ids = self._run_dataset_ids(
            dataset_id=primary_dataset_id,
            manifest=manifest,
            metadata=metadata,
        )
        composition = self._run_composition(manifest)
        official_template_eligible, official_blocking_reasons = self._official_composition_status(
            manifest
        )
        predictions = self._prediction_artifacts(
            run_id,
            evaluation_summary=evaluation_summary,
            prediction_scopes=prediction_scopes,
        )
        prediction_sample_total = sum(item.sample_count for item in predictions)
        dataset_summary = self._run_dataset_summary(
            dataset_id=(str(dataset_id) if isinstance(dataset_id, str) else None),
            manifest=manifest,
        )
        time_range = self._run_time_range(evaluation_summary, dataset_summary)
        prediction_summary = {
            "available_scopes": [item.scope for item in predictions] or prediction_scopes,
            "primary_scope": self._str(evaluation_summary.get("selected_scope"))
            or (predictions[0].scope if predictions else None),
            "sample_count": prediction_sample_total,
        }
        notes: list[str] = []
        if not evaluation_summary:
            notes.append("该 run 生成于评估快照增强前，暂无完整曲线与误差分布。")
        if missing_artifacts:
            notes.append(f"当前 run 缺少标准训练产物：{', '.join(missing_artifacts)}。")
        if dataset_summary.get("readiness_status") == "warning":
            notes.append("训练数据集带有 warning 状态，请结合数据详情页理解风险。")
        return RunDetailView(
            run_id=run_id,
            model_name=model_name,
            dataset_id=primary_dataset_id,
            dataset_ids=dataset_ids,
            datasets=self._dataset_links_from_ids(dataset_ids),
            primary_dataset_id=primary_dataset_id,
            composition=composition,
            task_type=self._str(evaluation_summary.get("task_type"))
            or self._str((metadata.get("model_spec") or {}).get("task_type"))
            or "regression",
            artifact_format_status=artifact_format_status,
            missing_artifacts=missing_artifacts,
            family=self.model_families.get(model_name) or self._str(metadata.get("model_family")),
            backend=self._str(metadata.get("backend")) or self._backend(model_name),
            status="success" if artifact_format_status == "complete" else ("partial" if tracking or manifest else "legacy"),
            created_at=self._dt(tracking.get("created_at")) or self._dt(manifest.get("created_at")),
            metrics=(
                self._metrics(evaluation_summary.get("regression_metrics") or {})
                or self._metrics(tracking.get("metrics") or {})
            ),
            tracking_params={str(k): str(v) for k, v in (tracking.get("params") or {}).items()},
            manifest_metrics=self._metrics(manifest.get("metrics") or {}),
            repro_context=dict(manifest.get("repro_context") or {}),
            dataset_summary=dataset_summary,
            evaluation_summary=evaluation_summary if isinstance(evaluation_summary, dict) else {},
            evaluation_artifacts=self._artifacts([
                ("evaluation_summary", self.repository.artifact_root / "models" / run_id / "evaluation_summary.json"),
                ("feature_importance", self.repository.artifact_root / "models" / run_id / "feature_importance.json"),
            ]),
            prediction_summary=prediction_summary,
            time_range=time_range,
            feature_importance=feature_importance,
            predictions=predictions,
            related_backtests=self._related_backtests(run_id),
            artifacts=self._artifacts([
                ("tracking_summary", self.repository.artifact_root / "tracking" / f"{run_id}.json"),
                ("train_manifest", self.repository.artifact_root / "models" / run_id / "train_manifest.json"),
                ("model_metadata", self.repository.artifact_root / "models" / run_id / "metadata.json"),
                ("evaluation_summary", self.repository.artifact_root / "models" / run_id / "evaluation_summary.json"),
            ]),
            notes=notes,
            official_template_eligible=official_template_eligible,
            official_blocking_reasons=official_blocking_reasons,
            summary=StableSummaryView(status="success", headline=f"Run {run_id}"),
            pipeline_summary=None,
            review_summary=self._review_unavailable(),
            warning_summary=WarningSummaryView(level="none", count=0, items=[]),
            glossary_hints=self._glossary(["mae", "prediction_scope"]),
        )

    def list_benchmarks(self) -> list[BenchmarkListItemView]:
        items: list[BenchmarkListItemView] = []
        for path in self.repository.list_paths("benchmarks/*.json"):
            payload = self._load_benchmark_payload(path)
            if payload is None:
                continue
            leaderboard = payload.get("leaderboard", [])
            top = leaderboard[0] if isinstance(leaderboard, list) and leaderboard else {}
            items.append(
                BenchmarkListItemView(
                    benchmark_name=path.stem,
                    dataset_id=str(payload.get("dataset_id", "unknown_dataset")),
                    data_source=self._str(payload.get("data_source")),
                    benchmark_type=str(payload.get("benchmark_type", "workflow")),
                    updated_at=datetime.fromtimestamp(path.stat().st_mtime, tz=UTC),
                    top_model_name=self._str(top.get("model_name")),
                    top_model_score=self._float(top.get("mean_test_mae")),
                )
            )
        return sorted(items, key=lambda x: x.updated_at, reverse=True)

    def get_benchmark_detail(self, benchmark_name: str) -> BenchmarkDetailView | None:
        payload, path = self._resolve_benchmark_payload(benchmark_name)
        if payload is None or path is None:
            return None

        def to_row(r: dict[str, Any]) -> BenchmarkRowView:
            return BenchmarkRowView(
                rank=int(r.get("rank", 0) or 0),
                model_name=str(r.get("model_name", "unknown")),
                family=str(r.get("family", "unknown")),
                advanced_kind=str(r.get("advanced_kind", "baseline")),
                backend=str(r.get("backend", "unknown")),
                window_count=int(r.get("window_count", 0) or 0),
                mean_valid_mae=float(r.get("mean_valid_mae", 0.0) or 0.0),
                mean_test_mae=float(r.get("mean_test_mae", 0.0) or 0.0),
                artifact_uri=self._str(r.get("artifact_uri")),
            )

        leaderboard = [to_row(r) for r in payload.get("leaderboard", []) if isinstance(r, dict)]
        return BenchmarkDetailView(
            benchmark_name=benchmark_name,
            dataset_id=str(payload.get("dataset_id", "unknown_dataset")),
            data_source=self._str(payload.get("data_source")),
            benchmark_type=str(payload.get("benchmark_type", "workflow")),
            updated_at=datetime.fromtimestamp(path.stat().st_mtime, tz=UTC),
            window_count=int(payload.get("window_count", 0) or 0),
            leaderboard=leaderboard,
            results=[to_row(r) for r in payload.get("results", []) if isinstance(r, dict)],
            deep_backend_comparison=list(payload.get("deep_backend_comparison", [])),
            validation_summary=dict(payload.get("validation_summary", {})),
            artifacts=self._artifacts([
                ("benchmark_json", path),
                ("benchmark_markdown", path.with_suffix(".md")),
                ("benchmark_csv", path.with_suffix(".csv")),
            ]),
            summary=StableSummaryView(
                status="success",
                headline=(f"Top model: {leaderboard[0].model_name}" if leaderboard else "Benchmark"),
            ),
            review_summary=self._review_unavailable(),
            warning_summary=WarningSummaryView(level="none", count=0, items=[]),
            glossary_hints=self._glossary(["benchmark", "mae"]),
        )

    def _resolve_benchmark_payload(
        self,
        benchmark_name: str,
    ) -> tuple[dict[str, Any] | None, Path | None]:
        path = self.repository.artifact_root / "benchmarks" / f"{benchmark_name}.json"
        payload = self._load_benchmark_payload(path)
        if payload is not None:
            return payload, path
        if (
            benchmark_name == "baseline_family_walk_forward"
            and self.facade is not None
            and not path.exists()
        ):
            self.facade.run_baseline_benchmark()
            payload = self._load_benchmark_payload(path)
            if payload is not None:
                return payload, path
        return None, None

    def _load_benchmark_payload(self, path: Path) -> dict[str, Any] | None:
        payload = self._load(path)
        if self._is_benchmark_payload(payload):
            return payload
        return None

    def _is_benchmark_payload(self, payload: dict[str, Any]) -> bool:
        leaderboard = payload.get("leaderboard")
        results = payload.get("results")
        return isinstance(leaderboard, list) and isinstance(results, list)

    def list_backtests(
        self,
        *,
        page: int,
        per_page: int,
        search: str | None,
        status: str | None,
    ) -> BacktestsResponse:
        items: list[BacktestListItemView] = []
        for row in self._backtest_history_rows():
            metrics = (
                row.get("simulation_metrics")
                if isinstance(row.get("simulation_metrics"), dict)
                else {}
            )
            dataset_ids = self._backtest_dataset_ids(row)
            primary_dataset_id = dataset_ids[0] if dataset_ids else self._str(row.get("dataset_id"))
            item = BacktestListItemView(
                backtest_id=str(row.get("backtest_id", "unknown_backtest")),
                run_id=self._str(row.get("run_id")),
                model_name=self._str(row.get("model_name")),
                dataset_id=primary_dataset_id,
                dataset_ids=dataset_ids,
                datasets=self._dataset_links_from_ids(dataset_ids),
                primary_dataset_id=primary_dataset_id,
                # Report materialization success is the primary lifecycle status.
                # Consistency checks remain visible via passed_consistency_checks and warnings.
                status="success",
                template_id=self._protocol_template_id(row),
                official=self._protocol_official(row),
                protocol_version=self._protocol_version(row),
                gate_status=self._protocol_gate_status(row),
                research_backend=self._research_backend(row),
                portfolio_method=self._portfolio_method(row),
                passed_consistency_checks=(
                    bool(row.get("passed_consistency_checks"))
                    if isinstance(row.get("passed_consistency_checks"), bool)
                    else None
                ),
                annual_return=self._float(metrics.get("annual_return")),
                max_drawdown=self._float(metrics.get("max_drawdown")),
                warning_count=len(
                    [
                        item
                        for item in row.get("comparison_warnings", [])
                        if isinstance(item, str)
                    ]
                ),
                updated_at=(
                    row.get("updated_at")
                    if isinstance(row.get("updated_at"), datetime)
                    else None
                ),
            )
            text = f"{item.backtest_id} {item.run_id or ''} {item.model_name or ''}".lower()
            if search and search.lower() not in text:
                continue
            if status and item.status != status:
                continue
            items.append(item)
        start = (page - 1) * per_page
        end = start + per_page
        return BacktestsResponse(
            items=items[start:end],
            total=len(items),
            page=page,
            per_page=per_page,
            available_statuses=sorted({i.status for i in items}),
        )

    def get_backtest_detail(self, backtest_id: str) -> BacktestReportView | None:
        for row in self._backtest_history_rows():
            if row.get("backtest_id") != backtest_id:
                continue
            artifacts = self._backtest_artifacts(row)
            protocol = self._protocol_result_from_row(row)
            dataset_ids = self._backtest_dataset_ids(row)
            primary_dataset_id = dataset_ids[0] if dataset_ids else self._str(row.get("dataset_id"))
            return BacktestReportView(
                backtest_id=backtest_id,
                model_name=self._str(row.get("model_name")),
                run_id=self._str(row.get("run_id")),
                dataset_id=primary_dataset_id,
                dataset_ids=dataset_ids,
                datasets=self._dataset_links_from_ids(dataset_ids),
                primary_dataset_id=primary_dataset_id,
                alignment=self._backtest_alignment(row),
                template_id=self._protocol_template_id(row),
                official=self._protocol_official(row),
                protocol_version=self._protocol_version(row),
                research_backend=self._research_backend(row),
                portfolio_method=self._portfolio_method(row),
                protocol=protocol,
                passed_consistency_checks=(
                    bool(row.get("passed_consistency_checks"))
                    if isinstance(row.get("passed_consistency_checks"), bool)
                    else None
                ),
                comparison_warnings=[
                    str(item)
                    for item in row.get("comparison_warnings", [])
                    if isinstance(item, str)
                ],
                divergence_metrics=self._metrics(row.get("divergence_metrics", {})),
                scenario_metrics=self._metrics(row.get("scenario_metrics", {})),
                research=self._engine(row.get("research_result_uri")),
                simulation=self._engine(row.get("simulation_result_uri")),
                artifacts=artifacts,
                summary=StableSummaryView(status="success", headline=f"Backtest {backtest_id}"),
                pipeline_summary=None,
                review_summary=self._review_unavailable(),
                warning_summary=WarningSummaryView(level="none", count=0, items=[]),
                glossary_hints=self._glossary(["consistency_check", "max_drawdown"]),
            )
        return None

    def delete_backtest(self, backtest_id: str) -> BacktestDeleteResponse | None:
        detail = self.get_backtest_detail(backtest_id)
        if detail is None:
            return None

        self._remove_backtest_from_summary(backtest_id)
        self._remove_backtest_job_references(backtest_id)

        artifact_uris: list[str] = []
        for artifact in [
            *detail.artifacts,
            *(detail.research.artifacts if detail.research is not None else []),
            *(detail.simulation.artifacts if detail.simulation is not None else []),
        ]:
            if artifact.uri:
                artifact_uris.append(artifact.uri)

        deleted_files = self._delete_artifact_uris(artifact_uris)

        return BacktestDeleteResponse(
            backtest_id=backtest_id,
            status="deleted",
            message=(
                "Backtest was permanently deleted from local artifacts and list indexes. "
                "Training runs remain available, but any links pointing at this backtest are removed."
            ),
            deleted_files=sorted(set(deleted_files)),
        )

    def compare_models(self, request: ModelComparisonRequest) -> ModelComparisonView:
        rows: list[ComparisonRowView] = []
        for run_id in request.run_ids:
            detail = self.get_run_detail(run_id)
            if detail is None:
                continue
            rows.append(
                ComparisonRowView(
                    row_id=f"run:{run_id}",
                    source_type="run",
                    label=run_id,
                    model_name=detail.model_name,
                    dataset_id=detail.dataset_id,
                    backend=detail.backend,
                    status=detail.status,
                    train_mae=self._float(detail.metrics.get("mae")),
                )
            )
            backtest_row = self._preferred_backtest_row_for_run(
                run_id,
                template_id=request.template_id,
                official_only=request.official_only,
            )
            if backtest_row is not None:
                simulation_metrics = (
                    backtest_row.get("simulation_metrics")
                    if isinstance(backtest_row.get("simulation_metrics"), dict)
                    else {}
                )
                rows.append(
                    ComparisonRowView(
                        row_id=f"backtest:{backtest_row.get('backtest_id')}",
                        source_type="backtest",
                        label=self._str(backtest_row.get("backtest_id")) or run_id,
                        model_name=detail.model_name,
                        dataset_id=detail.dataset_id,
                        backend=detail.backend,
                        status="success",
                        template_id=self._protocol_template_id(backtest_row),
                        official=self._protocol_official(backtest_row),
                        protocol_version=self._protocol_version(backtest_row),
                        gate_status=self._protocol_gate_status(backtest_row),
                        train_mae=self._float(detail.metrics.get("mae")),
                        annual_return=self._float(simulation_metrics.get("annual_return")),
                        max_drawdown=self._float(simulation_metrics.get("max_drawdown")),
                        turnover_total=self._float(simulation_metrics.get("turnover_total")),
                        implementation_shortfall=self._float(
                            simulation_metrics.get("implementation_shortfall")
                        ),
                    )
                )
        return ModelComparisonView(rows=rows)

    def preview_artifact(self, uri: str) -> ArtifactPreviewResponse:
        path = self.repository.resolve_uri(uri.replace("\\", "/"))
        text = path.read_text(encoding="utf-8")
        if path.suffix.lower() == ".json":
            return ArtifactPreviewResponse(uri=uri, kind="json", is_json=True, content=json.loads(text))
        return ArtifactPreviewResponse(uri=uri, kind="text", is_json=False, content=text)

    def resolve_run_model_artifact_uri(self, run_id: str) -> str | None:
        metadata = self.repository.artifact_root / "models" / run_id / "metadata.json"
        if metadata.exists():
            return str(metadata.resolve())
        legacy = self.repository.artifact_root / "models" / run_id / "manifest.json"
        if legacy.exists():
            payload = self._load(legacy)
            uri = (payload.get("model_artifact") or {}).get("uri")
            return uri if isinstance(uri, str) else None
        return None

    def get_run_manifest(self, run_id: str) -> dict[str, Any]:
        return self.repository.read_json_if_exists(
            f"models/{run_id}/train_manifest.json"
        ) or self.repository.read_json_if_exists(f"models/{run_id}/manifest.json") or {}

    def get_run_model_metadata(self, run_id: str) -> dict[str, Any]:
        return self.repository.read_json_if_exists(f"models/{run_id}/metadata.json") or {}

    def get_dataset_payload(self, dataset_id: str) -> dict[str, Any] | None:
        return self._dataset_ref(dataset_id)

    def load_market_bars_for_dataset(self, dataset_id: str) -> list[NormalizedMarketBar]:
        payload = self._dataset_ref(dataset_id)
        if payload is None:
            return []
        bars = self._normalized_market_bars_from_payload(payload)
        if bars:
            return bars
        acquisition_profile = self._dataset_manifest(payload).get("acquisition_profile") or {}
        anchor_dataset_id = self._str(acquisition_profile.get("market_anchor_dataset_id"))
        if anchor_dataset_id and anchor_dataset_id != dataset_id:
            anchor_payload = self._dataset_ref(anchor_dataset_id)
            if anchor_payload is not None:
                return self._normalized_market_bars_from_payload(anchor_payload)
        return []

    def list_model_templates(self, *, include_deleted: bool = False) -> ModelTemplateListResponse:
        items = self._builtin_templates() + self._custom_templates()
        if not include_deleted:
            items = [item for item in items if item.deleted_at is None]
        return ModelTemplateListResponse(items=items, total=len(items), model_options_source="registry")

    def get_model_template(self, template_id: str) -> ModelTemplateView | None:
        for item in self._builtin_templates():
            if item.template_id == template_id:
                return item
        path = self.templates_root / f"{template_id}.json"
        if not path.exists():
            return None
        return ModelTemplateView.model_validate_json(path.read_text(encoding="utf-8"))

    def create_model_template(self, request: ModelTemplateCreateRequest) -> ModelTemplateView:
        if request.model_name not in self._registry_models():
            raise ValueError(f"model '{request.model_name}' is not registered")
        if self._is_multimodal_reference_model(request.model_name):
            raise ValueError("multimodal_reference 仅作为系统内部融合模板，不允许直接创建。")
        now = datetime.now(UTC)
        item = ModelTemplateView(
            template_id=f"custom-{uuid.uuid4().hex}",
            name=request.name,
            model_name=request.model_name,
            description=request.description,
            source="custom",
            hyperparams=request.hyperparams,
            trainer_preset=request.trainer_preset,
            dataset_preset=request.dataset_preset,
            read_only=False,
            model_registered=True,
            created_at=now,
            updated_at=now,
        )
        (self.templates_root / f"{item.template_id}.json").write_text(
            item.model_dump_json(indent=2),
            encoding="utf-8",
        )
        return item

    def update_model_template(
        self,
        template_id: str,
        request: ModelTemplateUpdateRequest,
    ) -> ModelTemplateView | None:
        path = self.templates_root / f"{template_id}.json"
        if not path.exists():
            return None
        cur = ModelTemplateView.model_validate_json(path.read_text(encoding="utf-8"))
        if self._is_multimodal_reference_model(cur.model_name):
            raise ValueError("multimodal_reference 模板由系统保留，不能作为普通模板编辑。")
        nxt = cur.model_copy(
            update={
                "name": request.name if request.name is not None else cur.name,
                "description": request.description if request.description is not None else cur.description,
                "hyperparams": request.hyperparams if request.hyperparams is not None else cur.hyperparams,
                "trainer_preset": request.trainer_preset if request.trainer_preset is not None else cur.trainer_preset,
                "dataset_preset": request.dataset_preset if request.dataset_preset is not None else cur.dataset_preset,
                "updated_at": datetime.now(UTC),
            }
        )
        path.write_text(nxt.model_dump_json(indent=2), encoding="utf-8")
        return nxt

    def delete_model_template(self, template_id: str) -> bool:
        path = self.templates_root / f"{template_id}.json"
        if not path.exists():
            return False
        cur = ModelTemplateView.model_validate_json(path.read_text(encoding="utf-8"))
        nxt = cur.model_copy(update={"deleted_at": datetime.now(UTC), "updated_at": datetime.now(UTC)})
        path.write_text(nxt.model_dump_json(indent=2), encoding="utf-8")
        return True

    def list_trained_models(self, *, include_deleted: bool = False) -> TrainedModelListResponse:
        items: list[TrainedModelSummaryView] = []
        for run_id in self._run_ids():
            detail = self.get_trained_model(run_id)
            if detail is None:
                continue
            if detail.is_deleted and not include_deleted:
                continue
            items.append(
                TrainedModelSummaryView(
                    run_id=detail.run_id,
                    model_name=detail.model_name,
                    family=detail.family,
                    dataset_id=detail.dataset_id,
                    created_at=detail.created_at,
                    status=detail.status,
                    metrics=detail.metrics,
                    note=detail.note,
                    is_deleted=detail.is_deleted,
                    official_template_eligible=detail.official_template_eligible,
                    official_blocking_reasons=list(detail.official_blocking_reasons),
                    links=detail.links,
                )
            )
        return TrainedModelListResponse(items=items, total=len(items))

    def get_trained_model(self, run_id: str) -> TrainedModelDetailView | None:
        detail = self.get_run_detail(run_id)
        if detail is None:
            return None
        meta_path = self.trained_root / f"{run_id}.json"
        meta = self._load(meta_path) if meta_path.exists() else {}
        return TrainedModelDetailView(
            run_id=detail.run_id,
            model_name=detail.model_name,
            family=detail.family,
            dataset_id=detail.dataset_id,
            created_at=detail.created_at,
            status=detail.status,
            metrics=detail.metrics,
            note=self._str(meta.get("note")),
            is_deleted=bool(meta.get("is_deleted", False)),
            official_template_eligible=detail.official_template_eligible,
            official_blocking_reasons=list(detail.official_blocking_reasons),
            artifacts=detail.artifacts,
            tracking_params=detail.tracking_params,
            model_spec={},
            links=[
                DeepLinkView(
                    kind="run_detail",
                    label=f"Run {run_id}",
                    href=f"/runs/{run_id}",
                    api_path=f"/api/runs/{run_id}",
                )
            ],
        )

    def soft_delete_trained_model(self, run_id: str) -> TrainedModelDetailView | None:
        if self.get_run_detail(run_id) is None:
            return None
        payload = {
            "run_id": run_id,
            "is_deleted": True,
            "updated_at": datetime.now(UTC).isoformat(),
        }
        (self.trained_root / f"{run_id}.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return self.get_trained_model(run_id)

    def update_trained_model_note(self, run_id: str, note: str | None) -> TrainedModelDetailView | None:
        if self.get_run_detail(run_id) is None:
            return None
        current = self._load(self.trained_root / f"{run_id}.json")
        payload = {
            "run_id": run_id,
            "is_deleted": bool(current.get("is_deleted", False)),
            "note": note,
            "updated_at": datetime.now(UTC).isoformat(),
        }
        (self.trained_root / f"{run_id}.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return self.get_trained_model(run_id)

    def list_datasets(self, *, page: int, per_page: int) -> DatasetListResponse:
        items_by_id: dict[str, tuple[DatasetSummaryView, datetime, datetime]] = {}
        epoch = datetime.fromtimestamp(0, tz=UTC)
        for entry in self.dataset_registry.list_entries():
            payload = entry.payload
            if not self._is_public_dataset_payload(payload):
                continue
            summary = self._dataset_summary(payload)
            updated_at = self._dt(entry.updated_at) or epoch
            as_of_time = summary.as_of_time or epoch
            existing = items_by_id.get(summary.dataset_id)
            if existing is None or (updated_at, as_of_time) >= (existing[1], existing[2]):
                items_by_id[summary.dataset_id] = (summary, updated_at, as_of_time)
        items = [
            item[0]
            for item in sorted(
                items_by_id.values(),
                key=lambda item: (item[1], item[2]),
                reverse=True,
            )
        ]
        start = (page - 1) * per_page
        end = start + per_page
        return DatasetListResponse(items=items[start:end], total=len(items), page=page, per_page=per_page)

    def get_dataset_detail(self, dataset_id: str) -> DatasetDetailView | None:
        payload = self._dataset_ref(dataset_id)
        if payload is None:
            return None
        dataset_summary = self._dataset_summary(payload)
        detail_meta = self._dataset_detail_meta(payload)
        quality_summary = self._dataset_quality_summary(payload)
        readiness = self.get_dataset_readiness(dataset_id)
        return DatasetDetailView(
            dataset=dataset_summary,
            display_name=dataset_summary.display_name,
            subtitle=dataset_summary.subtitle,
            summary=detail_meta["summary"],
            intended_use=detail_meta["intended_use"],
            risk_note=detail_meta["risk_note"],
            row_count=dataset_summary.row_count,
            feature_count=dataset_summary.feature_count,
            label_count=dataset_summary.label_count,
            feature_columns_preview=detail_meta["feature_columns_preview"],
            label_columns=detail_meta["label_columns"],
            feature_groups=detail_meta["feature_groups"],
            quality_summary=quality_summary,
            glossary_hints=self._glossary(
                [
                    "as_of_time",
                    "freshness",
                    "label_horizon",
                    "split_strategy",
                    "sample_policy",
                    "temporal_safety",
                    "missing_ratio",
                    "duplicate_rows",
                    "feature_dimensions",
                    "label_columns",
                    "data_coverage",
                    "data_domain",
                    "alignment_policy",
                    "missing_feature_policy",
                    "entity_scope",
                    "snapshot_version",
                    "series_kind",
                ]
            ),
            label_spec=dict(payload.get("label_spec", {})),
            split_manifest=dict(payload.get("split_manifest", {})),
            sample_policy=dict(payload.get("sample_policy", {})),
            quality={
                "missing_ratio": quality_summary.missing_ratio,
                "duplicate_ratio": quality_summary.duplicate_ratio,
                "duplicate_rows": quality_summary.duplicate_rows,
                "status": quality_summary.status,
                "summary": quality_summary.summary,
                "checks": quality_summary.checks,
            },
            acquisition_profile=detail_meta["acquisition_profile"],
            build_profile=detail_meta["build_profile"],
            schema_profile=detail_meta["schema_profile"],
            readiness_profile=(
                readiness.model_dump(mode="json") if readiness is not None else detail_meta["readiness_profile"]
            ),
            training_profile=detail_meta["training_profile"],
            download_href=f"/api/datasets/{dataset_id}/download",
            links=[
                DeepLinkView(
                    kind="dataset_series",
                    label="Series",
                    href=f"/datasets/{dataset_id}/series",
                    api_path=f"/api/datasets/{dataset_id}/series",
                ),
                *(
                    [
                        DeepLinkView(
                            kind="dataset_ohlcv",
                            label="OHLCV",
                            href=f"/datasets/{dataset_id}/ohlcv",
                            api_path=f"/api/datasets/{dataset_id}/ohlcv",
                        )
                    ]
                    if self._dataset_has_market_ohlcv(payload)
                    else []
                ),
            ],
        )

    def _dataset_source_vendors(
        self,
        manifest: dict[str, Any],
        *,
        points: list[NormalizedSeriesPoint] | None = None,
        payload: dict[str, Any] | None = None,
        data_domain: str | None = None,
    ) -> list[str]:
        acquisition_profile = dict(manifest.get("acquisition_profile") or {})
        feature_view_ref = dict((payload or {}).get("feature_view_ref") or {})
        actual_vendors = list(
            dict.fromkeys(
                [
                    self._canonical_sentiment_vendor(
                        self._str(source.get("vendor")) or self._str(source.get("source_vendor"))
                    )
                    for source in (acquisition_profile.get("fusion_sources") or [])
                    if isinstance(source, dict)
                    and (
                        data_domain is None
                        or self._str(source.get("data_domain")) == data_domain
                    )
                    and (
                        self._str(source.get("vendor")) or self._str(source.get("source_vendor"))
                    )
                ]
                + [
                    self._canonical_sentiment_vendor(
                        self._str(source.get("source")) or self._str(source.get("venue"))
                    )
                    for source in (feature_view_ref.get("input_data_refs") or [])
                    if isinstance(source, dict)
                    and (
                        data_domain is None
                        or (
                            data_domain == "sentiment_events"
                            and "domain:sentiment_events"
                            in [str(tag) for tag in source.get("tags", []) if tag is not None]
                        )
                    )
                    and (
                        self._str(source.get("source")) or self._str(source.get("venue"))
                    )
                ]
            )
        )
        if actual_vendors:
            return actual_vendors
        if points:
            point_vendors = list(
                dict.fromkeys(
                    [
                        self._canonical_sentiment_vendor(point.vendor)
                        for point in points
                        if isinstance(point.vendor, str)
                        and point.vendor
                        and (data_domain is None or point.domain == data_domain)
                    ]
                )
            )
            if point_vendors:
                return point_vendors
        vendors = list(
            dict.fromkeys(
                [
                    self._canonical_sentiment_vendor(self._str(source.get("source_vendor")))
                    for source in (acquisition_profile.get("source_specs") or [])
                    if isinstance(source, dict)
                    and self._str(source.get("source_vendor"))
                    and (
                        data_domain is None
                        or self._str(source.get("data_domain")) == data_domain
                    )
                ]
                or (
                    [self._canonical_sentiment_vendor(self._str(acquisition_profile.get("source_vendor")))]
                    if self._str(acquisition_profile.get("source_vendor"))
                    and (
                        data_domain is None
                        or self._str(acquisition_profile.get("data_domain")) == data_domain
                    )
                    else []
                )
            )
        )
        if vendors:
            return vendors
        return []

    def _split_range(
        self,
        payload: dict[str, Any],
        key: str,
    ) -> tuple[datetime | None, datetime | None]:
        range_payload = (payload.get("split_manifest") or {}).get(key)
        if not isinstance(range_payload, dict):
            return None, None
        return self._dt(range_payload.get("start")), self._dt(range_payload.get("end"))

    def _frequency_step(self, frequency: str | None) -> timedelta | None:
        if not frequency:
            return None
        normalized = frequency.lower().strip()
        try:
            if normalized.endswith("m"):
                return timedelta(minutes=int(normalized[:-1]))
            if normalized.endswith("h"):
                return timedelta(hours=int(normalized[:-1]))
            if normalized.endswith("d"):
                return timedelta(days=int(normalized[:-1]))
        except ValueError:
            return None
        return None

    def _floor_time_to_frequency(
        self,
        value: datetime,
        frequency: str | None,
    ) -> datetime:
        step = self._frequency_step(frequency)
        if step is None:
            return value.astimezone(UTC)
        utc_value = value.astimezone(UTC)
        epoch = datetime(1970, 1, 1, tzinfo=UTC)
        bucket_seconds = max(int(step.total_seconds()), 1)
        elapsed_seconds = int((utc_value - epoch).total_seconds())
        return epoch + timedelta(seconds=(elapsed_seconds // bucket_seconds) * bucket_seconds)

    def _max_consecutive_empty_bars(
        self,
        expected_times: list[datetime],
        covered_times: set[datetime],
    ) -> int | None:
        if not expected_times:
            return None
        longest = 0
        streak = 0
        for timestamp in expected_times:
            if timestamp in covered_times:
                streak = 0
                continue
            streak += 1
            longest = max(longest, streak)
        return longest

    def _dataset_official_nlp_gate(
        self,
        dataset_id: str,
        payload: dict[str, Any],
        *,
        points: list[NormalizedSeriesPoint] | None = None,
        source_vendors: list[str] | None = None,
    ) -> dict[str, Any]:
        summary = self._dataset_summary(payload)
        manifest = self._dataset_manifest(payload)
        contains_nlp = self._dataset_contains_nlp(payload)
        resolved_points = (
            (
                points
                if points is not None
                else (self._official_reddit_archive_points(payload) or self._dataset_nlp_points(payload))
            )
            if contains_nlp
            else []
        )
        event_points = [point for point in resolved_points if point.metric_name == "event_count"]
        active_event_points = [point for point in event_points if float(point.value or 0.0) > 0.0]
        vendors = (
            source_vendors
            if source_vendors is not None
            else (
                self._dataset_source_vendors(
                    manifest,
                    points=resolved_points,
                    payload=payload,
                    data_domain="sentiment_events",
                )
                if contains_nlp
                else []
            )
        )
        market_bars = self.load_market_bars_for_dataset(dataset_id)
        market_times = sorted({bar.event_time.astimezone(UTC) for bar in market_bars})
        market_window_start = market_times[0] if market_times else summary.freshness.data_start_time
        market_window_end = market_times[-1] if market_times else summary.freshness.data_end_time
        official_backtest_start, official_backtest_end = self._split_range(payload, "test_range")
        archival_source_only = bool(vendors) and all(
            vendor in OFFICIAL_NLP_ARCHIVAL_VENDORS for vendor in vendors
        )
        actual_nlp_start = (
            min((point.available_time for point in event_points), default=None)
            if event_points
            else None
        )
        actual_nlp_end = (
            max((point.available_time for point in event_points), default=None)
            if event_points
            else None
        )
        frequency = summary.frequency or "1h"
        covered_times = {
            self._floor_time_to_frequency(point.available_time, frequency)
            for point in active_event_points
        }
        full_expected_times = [
            timestamp
            for timestamp in market_times
            if (market_window_start is None or timestamp >= market_window_start)
            and (market_window_end is None or timestamp <= market_window_end)
        ]
        test_expected_times = [
            timestamp
            for timestamp in market_times
            if (official_backtest_start is None or timestamp >= official_backtest_start)
            and (official_backtest_end is None or timestamp <= official_backtest_end)
        ]
        coverage_ratio = (
            round(
                sum(1 for timestamp in full_expected_times if timestamp in covered_times)
                / len(full_expected_times),
                4,
            )
            if full_expected_times
            else None
        )
        test_coverage_ratio = (
            round(
                sum(1 for timestamp in test_expected_times if timestamp in covered_times)
                / len(test_expected_times),
                4,
            )
            if test_expected_times
            else None
        )
        max_consecutive_empty_bars = self._max_consecutive_empty_bars(
            test_expected_times,
            covered_times,
        )

        seen_event_ids: set[str] = set()
        preview_event_total = 0
        duplicate_preview_events = 0
        linked_event_total = 0
        for point in active_event_points:
            preview_events = self._load_json_list(point.dimensions.get("preview_events_json"))
            candidates = (
                [item for item in preview_events if isinstance(item, dict)]
                if preview_events
                else [
                    {
                        "event_id": point.dimensions.get("event_id"),
                        "symbol": point.dimensions.get("symbol"),
                    }
                ]
            )
            for index, preview in enumerate(candidates):
                preview_event_total += 1
                event_id = (
                    self._str(preview.get("event_id"))
                    or self._str(preview.get("url"))
                    or stable_digest(
                        {
                            "point": point.series_key,
                            "available_time": point.available_time.isoformat(),
                            "index": index,
                        }
                    )
                )
                if event_id in seen_event_ids:
                    duplicate_preview_events += 1
                else:
                    seen_event_ids.add(event_id)
                if (
                    self._str(preview.get("symbol"))
                    or self._str(preview.get("entity_key"))
                    or point.entity_key
                ):
                    linked_event_total += 1
        duplicate_ratio = (
            round(duplicate_preview_events / preview_event_total, 4)
            if preview_event_total
            else 0.0
        )
        entity_link_coverage_ratio = (
            round(linked_event_total / preview_event_total, 4)
            if preview_event_total
            else (1.0 if event_points else None)
        )

        official_template_eligible = bool(
            market_window_start and market_window_end and official_backtest_start and official_backtest_end
        )
        reasons: list[str] = []
        gate_status: str | None = None
        if contains_nlp:
            if not official_template_eligible:
                gate_status = "failed"
                reasons.append("Market window or official test window is missing.")
            if not vendors:
                gate_status = "failed"
                reasons.append("No NLP source vendor metadata was recorded.")
            elif not archival_source_only:
                gate_status = "failed"
                reasons.append(
                    "Official same-template comparison only accepts archival NLP vendors "
                    f"({', '.join(OFFICIAL_NLP_ARCHIVAL_VENDORS)})."
                )
            if actual_nlp_start is None or actual_nlp_end is None:
                gate_status = "failed"
                reasons.append("No usable NLP event_count buckets were materialized.")
            if (
                test_coverage_ratio is not None
                and test_coverage_ratio < OFFICIAL_NLP_MIN_TEST_COVERAGE_RATIO
            ):
                gate_status = "failed"
                reasons.append(
                    f"Official test-window NLP coverage {test_coverage_ratio:.1%} is below "
                    f"{OFFICIAL_NLP_MIN_TEST_COVERAGE_RATIO:.0%}."
                )
            if (
                max_consecutive_empty_bars is not None
                and max_consecutive_empty_bars > OFFICIAL_NLP_MAX_TEST_EMPTY_BARS
            ):
                gate_status = "failed"
                reasons.append(
                    f"Max consecutive empty NLP gap {max_consecutive_empty_bars} exceeds "
                    f"{OFFICIAL_NLP_MAX_TEST_EMPTY_BARS} bars."
                )
            if duplicate_ratio > OFFICIAL_NLP_MAX_DUPLICATE_RATIO:
                gate_status = "failed"
                reasons.append(
                    f"Duplicate NLP event ratio {duplicate_ratio:.1%} exceeds "
                    f"{OFFICIAL_NLP_MAX_DUPLICATE_RATIO:.0%}."
                )
            if (
                entity_link_coverage_ratio is not None
                and entity_link_coverage_ratio < OFFICIAL_NLP_MIN_ENTITY_LINK_COVERAGE_RATIO
            ):
                gate_status = "failed"
                reasons.append(
                    f"Entity-link coverage {entity_link_coverage_ratio:.1%} is below "
                    f"{OFFICIAL_NLP_MIN_ENTITY_LINK_COVERAGE_RATIO:.0%}."
                )
            if gate_status is None:
                gate_status = "passed"
                reasons.append(
                    "Official NLP gate passed: archival source, aligned time window, and quality thresholds satisfied."
                )

        return {
            "requested_start_time": market_window_start,
            "requested_end_time": market_window_end,
            "actual_start_time": actual_nlp_start,
            "actual_end_time": actual_nlp_end,
            "market_window_start_time": market_window_start,
            "market_window_end_time": market_window_end,
            "official_backtest_start_time": official_backtest_start,
            "official_backtest_end_time": official_backtest_end,
            "official_template_eligible": official_template_eligible if contains_nlp else True,
            "archival_source_only": archival_source_only if contains_nlp else None,
            "coverage_ratio": coverage_ratio if contains_nlp else None,
            "test_coverage_ratio": test_coverage_ratio if contains_nlp else None,
            "max_consecutive_empty_bars": max_consecutive_empty_bars if contains_nlp else None,
            "duplicate_ratio": duplicate_ratio if contains_nlp else None,
            "entity_link_coverage_ratio": entity_link_coverage_ratio if contains_nlp else None,
            "official_template_gate_status": gate_status,
            "official_template_gate_reasons": reasons,
        }

    def _official_reddit_archive_points(self, payload: dict[str, Any]) -> list[NormalizedSeriesPoint]:
        manifest = self._dataset_manifest(payload)
        acquisition_profile = dict(manifest.get("acquisition_profile") or {})
        if acquisition_profile.get("official_benchmark_version") != OFFICIAL_MULTIMODAL_STANDARD_SCHEMA_VERSION:
            return []
        reddit_input_ref = next(
            (
                input_ref
                for input_ref in self._dataset_input_refs(payload)
                if self._canonical_sentiment_vendor(self._str(input_ref.get("source"))) == "reddit_archive"
            ),
            None,
        )
        if not isinstance(reddit_input_ref, dict):
            return []
        time_range = reddit_input_ref.get("time_range")
        if not isinstance(time_range, dict):
            return []
        start_time = self._dt(time_range.get("start"))
        end_time = self._dt(time_range.get("end"))
        if start_time is None or end_time is None:
            return []
        symbol = (
            self._str(reddit_input_ref.get("symbol"))
            or self._str(reddit_input_ref.get("entity_key"))
            or "BTC"
        ).upper()
        asset_id = "BTC" if "BTC" in symbol else symbol
        connector = RedditArchiveSentimentConnector(self.repository.artifact_root)
        return connector.store.query_points(
            asset_id=asset_id,
            start_time=start_time,
            end_time=end_time,
            vendor="reddit_archive",
        )

    def get_dataset_nlp_inspection(self, dataset_id: str) -> DatasetNlpInspectionView | None:
        payload = self._dataset_ref(dataset_id)
        if payload is None:
            return None
        manifest = self._dataset_manifest(payload)
        source_vendors = self._dataset_source_vendors(
            manifest,
            payload=payload,
            data_domain="sentiment_events",
        )
        gate_summary = self._dataset_official_nlp_gate(
            dataset_id,
            payload,
            source_vendors=source_vendors,
        )
        if not self._dataset_contains_nlp(payload):
            return DatasetNlpInspectionView(
                dataset_id=dataset_id,
                contains_nlp=False,
                coverage_summary="当前数据集不包含 NLP / 舆情特征。",
                requested_start_time=gate_summary["requested_start_time"],
                requested_end_time=gate_summary["requested_end_time"],
                source_vendors=source_vendors,
                official_template_eligible=gate_summary["official_template_eligible"],
                market_window_start_time=gate_summary["market_window_start_time"],
                market_window_end_time=gate_summary["market_window_end_time"],
                official_backtest_start_time=gate_summary["official_backtest_start_time"],
                official_backtest_end_time=gate_summary["official_backtest_end_time"],
            )

        points = self._dataset_nlp_points(payload)
        sample_features = self._dataset_nlp_sample_feature_preview(payload)
        gate_summary = self._dataset_official_nlp_gate(
            dataset_id,
            payload,
            points=points,
            source_vendors=source_vendors,
        )
        if not points:
            return DatasetNlpInspectionView(
                dataset_id=dataset_id,
                contains_nlp=True,
                coverage_summary="检测到 NLP 特征入口，但当前没有可供研究检查的事件快照。",
                requested_start_time=gate_summary["requested_start_time"],
                requested_end_time=gate_summary["requested_end_time"],
                actual_start_time=gate_summary["actual_start_time"],
                actual_end_time=gate_summary["actual_end_time"],
                source_vendors=source_vendors,
                sample_feature_preview=sample_features,
                official_template_gate_status=gate_summary["official_template_gate_status"],
                official_template_gate_reasons=gate_summary["official_template_gate_reasons"],
                official_template_eligible=gate_summary["official_template_eligible"],
                archival_source_only=gate_summary["archival_source_only"],
                coverage_ratio=gate_summary["coverage_ratio"],
                test_coverage_ratio=gate_summary["test_coverage_ratio"],
                max_consecutive_empty_bars=gate_summary["max_consecutive_empty_bars"],
                duplicate_ratio=gate_summary["duplicate_ratio"],
                entity_link_coverage_ratio=gate_summary["entity_link_coverage_ratio"],
                market_window_start_time=gate_summary["market_window_start_time"],
                market_window_end_time=gate_summary["market_window_end_time"],
                official_backtest_start_time=gate_summary["official_backtest_start_time"],
                official_backtest_end_time=gate_summary["official_backtest_end_time"],
            )

        preview_map: dict[str, DatasetNlpEventPreviewView] = {}
        keyword_counts: dict[str, int] = {}
        source_counts: dict[str, int] = {}
        timeline_stats: dict[str, dict[str, float]] = {}
        sentiment_distribution = {"positive": 0.0, "neutral": 0.0, "negative": 0.0}
        coverage_start: datetime | None = None
        coverage_end: datetime | None = None

        for point in points:
            coverage_start = (
                point.available_time
                if coverage_start is None
                else min(coverage_start, point.available_time)
            )
            coverage_end = (
                point.available_time
                if coverage_end is None
                else max(coverage_end, point.available_time)
            )
            if point.metric_name == "event_count":
                timeline = timeline_stats.setdefault(
                    point.event_time.isoformat(),
                    {"count": 0.0, "sentiment_total": 0.0, "sentiment_points": 0.0},
                )
                timeline["count"] += point.value
            if point.metric_name == "sentiment_score":
                timeline = timeline_stats.setdefault(
                    point.event_time.isoformat(),
                    {"count": 0.0, "sentiment_total": 0.0, "sentiment_points": 0.0},
                )
                timeline["sentiment_total"] += point.value
                timeline["sentiment_points"] += 1.0
                if point.value > 0.05:
                    sentiment_distribution["positive"] += 1.0
                elif point.value < -0.05:
                    sentiment_distribution["negative"] += 1.0
                else:
                    sentiment_distribution["neutral"] += 1.0

            if point.metric_name != "event_count":
                continue
            for term in self._load_json_list(point.dimensions.get("keywords_json")):
                if not isinstance(term, dict):
                    continue
                key = self._str(term.get("term"))
                count = self._int_or_none(term.get("count"))
                if key and count:
                    keyword_counts[key] = keyword_counts.get(key, 0) + count
            for preview in self._load_json_list(point.dimensions.get("preview_events_json")):
                if not isinstance(preview, dict):
                    continue
                preview_id = (
                    self._str(preview.get("event_id"))
                    or self._str(preview.get("url"))
                    or stable_digest(preview)
                )
                if not preview_id:
                    continue
                event_time = self._dt(preview.get("event_time"))
                if event_time is None:
                    continue
                source_name = self._str(preview.get("source")) or point.vendor
                current = preview_map.get(preview_id)
                candidate = DatasetNlpEventPreviewView(
                    event_id=preview_id,
                    title=self._str(preview.get("title")) or "(untitled)",
                    snippet=self._str(preview.get("snippet")) or "",
                    source=source_name,
                    source_type=self._str(preview.get("source_type")),
                    symbol=self._str(preview.get("symbol")) or point.dimensions.get("symbol"),
                    event_time=event_time,
                    available_time=self._dt(preview.get("available_time")),
                    sentiment_score=self._float(preview.get("sentiment_score")),
                    url=self._str(preview.get("url")),
                )
                if current is None or (
                    candidate.available_time or candidate.event_time
                ) > (current.available_time or current.event_time):
                    preview_map[preview_id] = candidate

        for preview in preview_map.values():
            source_counts[preview.source] = source_counts.get(preview.source, 0) + 1
        total_sources = sum(source_counts.values()) or 1
        sentiment_total = sum(sentiment_distribution.values()) or 1.0
        keyword_summary = sorted(keyword_counts.items(), key=lambda item: (-item[1], item[0]))[:12]
        word_cloud_terms = sorted(keyword_counts.items(), key=lambda item: (-item[1], item[0]))[:30]
        previews = sorted(
            preview_map.values(),
            key=lambda item: item.available_time or item.event_time,
            reverse=True,
        )[:12]
        event_timeline = [
            DatasetNlpTimelinePointView(
                label=label,
                event_count=int(stats["count"]),
                avg_sentiment=(
                    stats["sentiment_total"] / stats["sentiment_points"]
                    if stats["sentiment_points"] > 0
                    else None
                ),
            )
            for label, stats in sorted(timeline_stats.items())
        ]
        coverage_summary = (
            f"NLP 实际覆盖 {len(event_timeline)} 个 1h 时间桶，"
            f"去重后 {len(preview_map)} 条文本事件，"
            f"实际可用时间 {coverage_start.isoformat() if coverage_start else '--'}"
            f" 到 {coverage_end.isoformat() if coverage_end else '--'}。"
        )
        return DatasetNlpInspectionView(
            dataset_id=dataset_id,
            contains_nlp=True,
            coverage_summary=coverage_summary,
            requested_start_time=gate_summary["requested_start_time"],
            requested_end_time=gate_summary["requested_end_time"],
            actual_start_time=gate_summary["actual_start_time"] or coverage_start,
            actual_end_time=gate_summary["actual_end_time"] or coverage_end,
            source_vendors=source_vendors,
            keyword_summary=[
                DatasetNlpKeywordView(term=term, score=float(count), count=count)
                for term, count in keyword_summary
            ],
            word_cloud_terms=[
                DatasetNlpKeywordView(term=term, count=count, weight=float(count))
                for term, count in word_cloud_terms
            ],
            source_breakdown=[
                DatasetNlpSourceBreakdownView(
                    source=source,
                    count=count,
                    share=round(count / total_sources, 4),
                )
                for source, count in sorted(source_counts.items(), key=lambda item: (-item[1], item[0]))
            ],
            event_timeline=event_timeline,
            sentiment_distribution=[
                TimeValuePoint(label=label, value=round(value / sentiment_total, 4))
                for label, value in sentiment_distribution.items()
            ],
            recent_event_previews=previews,
            sample_feature_preview=sample_features,
            official_template_gate_status=gate_summary["official_template_gate_status"],
            official_template_gate_reasons=gate_summary["official_template_gate_reasons"],
            official_template_eligible=gate_summary["official_template_eligible"],
            archival_source_only=gate_summary["archival_source_only"],
            coverage_ratio=gate_summary["coverage_ratio"],
            test_coverage_ratio=gate_summary["test_coverage_ratio"],
            max_consecutive_empty_bars=gate_summary["max_consecutive_empty_bars"],
            duplicate_ratio=gate_summary["duplicate_ratio"],
            entity_link_coverage_ratio=gate_summary["entity_link_coverage_ratio"],
            market_window_start_time=gate_summary["market_window_start_time"],
            market_window_end_time=gate_summary["market_window_end_time"],
            official_backtest_start_time=gate_summary["official_backtest_start_time"],
            official_backtest_end_time=gate_summary["official_backtest_end_time"],
        )

    def get_dataset_dependencies(self, dataset_id: str) -> DatasetDependenciesResponse | None:
        payload = self._dataset_ref(dataset_id)
        if payload is None:
            return None
        dependencies = self.dataset_registry.list_dependencies(dataset_id)
        blocking_items = self._blocking_dataset_dependencies(dataset_id)
        protection_meta = self._dataset_protection_meta(dataset_id, payload)
        can_delete = not protection_meta["is_protected"]
        deletion_reason = (
            "系统推荐数据集会被固定展示并禁止删除。"
            if protection_meta["is_protected"]
            else None
        )
        return DatasetDependenciesResponse(
            dataset_id=dataset_id,
            items=[*[self._dependency_view(item) for item in dependencies], *blocking_items],
            can_delete=can_delete,
            deletion_reason=deletion_reason,
            blocking_items=blocking_items,
        )

    def delete_dataset(self, dataset_id: str) -> DatasetDeleteResponse | None:
        entry = self._dataset_entry(dataset_id)
        if entry is None:
            return None
        dependencies = self.get_dataset_dependencies(dataset_id)
        if dependencies is not None and not dependencies.can_delete:
            raise ValueError(
                dependencies.deletion_reason
                or f"Dataset '{dataset_id}' cannot be deleted because it is protected or still referenced."
            )
        blocking_items = dependencies.blocking_items if dependencies is not None else []
        delete_targets = [entry, *self._collect_internal_helper_entries(entry)]
        deleted_files: list[str] = []
        for target in delete_targets:
            deleted_files.extend(self._delete_dataset_artifacts(target))
            ref_path = self.repository.artifact_root / "datasets" / f"{target.dataset_id}_dataset_ref.json"
            if ref_path.exists():
                raise ValueError(f"Hard delete failed because dataset ref '{ref_path.name}' still exists.")
            self.dataset_registry.remove_dataset(target.dataset_id)
        return DatasetDeleteResponse(
            dataset_id=dataset_id,
            status="deleted",
            message=(
                "Dataset was permanently deleted from the registry and local artifacts. "
                "Existing runs, backtests, or downstream datasets will keep their ids and "
                "surface missing dataset references instead of blocking deletion."
            ),
            blocking_items=blocking_items,
            deleted_files=sorted(set(deleted_files)),
        )

    def download_dataset_archive(self, dataset_id: str) -> tuple[str, BytesIO] | None:
        entry = self._dataset_entry(dataset_id)
        if entry is None:
            return None
        buffer = BytesIO()
        archive_prefix = f"{dataset_id}_"
        datasets_root = self.repository.artifact_root / "datasets"
        with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for path in sorted(datasets_root.glob(f"{archive_prefix}*")):
                if not path.is_file():
                    continue
                archive.write(path, arcname=f"datasets/{path.name}")
        buffer.seek(0)
        return (f"{dataset_id}.zip", buffer)

    def build_fusion_dataset(self, request: DatasetFusionRequest) -> DatasetFusionBuildResponse:
        if self.facade is None:
            raise ValueError("Fusion dataset building is unavailable because the runtime facade is not configured.")
        if not request.sources:
            raise ValueError("Fusion dataset request requires at least one auxiliary source.")

        base_entry = self._dataset_entry(request.base_dataset_id)
        if base_entry is None:
            raise ValueError(f"Base dataset '{request.base_dataset_id}' was not found.")

        base_payload = base_entry.payload
        base_manifest_payload = self._dataset_manifest(base_payload)
        base_dataset_type = self._resolved_dataset_type(base_payload, base_manifest_payload)
        base_data_domain = (
            self._str((base_manifest_payload.get("acquisition_profile") or {}).get("data_domain"))
            or base_entry.data_domain
            or "market"
        )
        if base_data_domain != "market":
            raise ValueError("Fusion dataset building currently requires a market-domain base dataset.")
        if base_dataset_type not in {"training_panel", "fusion_training_panel"}:
            raise ValueError("Fusion dataset building requires a trainable market dataset as the base dataset.")

        dataset_id = self._slugify_dataset_id(request.request_name, suffix="fusion")
        if self._dataset_entry(dataset_id) is not None:
            raise ValueError(f"Fusion dataset '{dataset_id}' already exists.")

        base_dataset_ref = self.store.read_model(base_entry.ref_uri, DatasetRef)
        base_samples = self._load_dataset_samples(base_payload)
        if not base_samples:
            raise ValueError(f"Base dataset '{request.base_dataset_id}' has no materialized samples.")

        sample_timestamps = [sample.timestamp for sample in base_samples]
        start_time = min(sample_timestamps)
        end_time = max(sample_timestamps)
        missing_strategy = (
            self._str(request.missing_feature_policy.get("strategy"))
            or request.missing_feature_policy_name
            or "drop_if_missing"
        )
        keep_with_flags = missing_strategy == "keep_with_flags"

        source_contexts: list[dict[str, Any]] = []
        coverage_by_feature: dict[str, float] = {}
        missing_counts: dict[str, int] = {}
        fusion_domains: set[str] = {base_data_domain}
        for source in request.sources:
            if source.data_domain not in {"macro", "on_chain", "derivatives", "sentiment_events"}:
                raise ValueError(
                    "Fusion dataset building currently supports auxiliary sources from macro, on_chain, derivatives, and sentiment_events domains."
                )
            points, fetch_status = self.facade.runtime.ingestion_service.fetch_series_points(
                data_domain=source.data_domain,
                identifier=source.identifier,
                vendor=source.vendor,
                frequency=source.frequency,
                start_time=start_time,
                end_time=end_time + self._frequency_delta(source.frequency),
                options={
                    **source.options,
                    **({"exchange": source.exchange} if source.exchange else {}),
                    **({"metric_name": source.metric_name} if source.metric_name else {}),
                },
            )
            if source.metric_name:
                points = [point for point in points if point.metric_name == source.metric_name]
            if not points:
                raise ValueError(
                    f"Fusion source '{source.data_domain}/{source.vendor}/{source.identifier}' returned no rows."
                )
            feature_name = source.feature_name or self._fusion_feature_name(source)
            snapshot_uri = self._write_fusion_series_rows(dataset_id, feature_name, points)
            source_contexts.append(
                {
                    "source": source,
                    "feature_name": feature_name,
                    "points": points,
                    "resolve_point": self._build_series_point_resolver(
                        points,
                        alignment_policy_name=request.alignment_policy_name,
                    ),
                    "fetch_status": fetch_status,
                    "storage_uri": snapshot_uri,
                    "data_ref": DataAssetRef(
                        asset_id=f"{source.data_domain}_{source.identifier}_{source.frequency}",
                        schema_version=1,
                        source=source.vendor,
                        symbol=source.identifier,
                        venue=source.exchange or source.vendor,
                        frequency=source.frequency,
                        time_range=TimeRange(
                            start=min(point.event_time for point in points),
                            end=max(point.event_time for point in points),
                        ),
                        storage_uri=snapshot_uri,
                        content_hash=stable_digest([point.model_dump(mode="json") for point in points]),
                        entity_key=source.identifier,
                        tags=[
                            "fusion_input",
                            f"domain:{source.data_domain}",
                            f"metric:{source.metric_name or 'value'}",
                        ],
                        request_origin=fetch_status,
                        fallback_used=False,
                    ),
                }
            )
            missing_counts[feature_name] = 0
            fusion_domains.add(source.data_domain)

        feature_schema = list(base_dataset_ref.feature_view_ref.feature_schema)
        for context in source_contexts:
            feature_schema.append(
                FeatureField(
                    name=context["feature_name"],
                    dtype="float",
                    nullable=keep_with_flags,
                    description=(
                        f"Fusion input from {context['source'].data_domain}/"
                        f"{context['source'].vendor}/{context['source'].identifier}"
                    ),
                    lineage_source=(
                        f"{context['source'].data_domain}:"
                        f"{context['source'].vendor}:"
                        f"{context['source'].identifier}"
                    ),
                    max_available_time=min(
                        max(point.available_time for point in context["points"]),
                        base_dataset_ref.feature_view_ref.as_of_time,
                    ),
                )
            )
            if keep_with_flags:
                feature_schema.append(
                    FeatureField(
                        name=f"{context['feature_name']}__missing",
                        dtype="float",
                        nullable=False,
                        description=f"Missingness flag for {context['feature_name']}.",
                        lineage_source=f"fusion_missing_flag:{context['feature_name']}",
                        max_available_time=base_dataset_ref.feature_view_ref.as_of_time,
                    )
                )

        enriched_rows: list[FeatureRow] = []
        label_map = {(sample.entity_key, sample.timestamp): sample.target for sample in base_samples}
        dropped_missing_rows = 0
        for sample in sorted(base_samples, key=lambda item: (item.timestamp, item.entity_key)):
            values = dict(sample.features)
            row_missing = False
            for context in source_contexts:
                match = context["resolve_point"](
                    sample.timestamp,
                    sample.available_time,
                )
                if match is None:
                    missing_counts[context["feature_name"]] += 1
                    row_missing = True
                    if keep_with_flags:
                        values[context["feature_name"]] = 0.0
                        values[f"{context['feature_name']}__missing"] = 1.0
                    continue
                values[context["feature_name"]] = match.value
                if keep_with_flags:
                    values[f"{context['feature_name']}__missing"] = 0.0
            if row_missing and not keep_with_flags:
                dropped_missing_rows += 1
                continue
            enriched_rows.append(
                FeatureRow(
                    entity_key=sample.entity_key,
                    timestamp=sample.timestamp,
                    available_time=sample.available_time,
                    values=values,
                )
            )

        total_candidate_rows = len(base_samples)
        for feature_name, missing_count in missing_counts.items():
            coverage_by_feature[feature_name] = (
                0.0 if total_candidate_rows == 0 else (total_candidate_rows - missing_count) / total_candidate_rows
            )

        feature_view_ref = FeatureViewRef(
            feature_set_id="multi_domain_fusion_v1",
            input_data_refs=[
                *list(base_dataset_ref.feature_view_ref.input_data_refs),
                *[context["data_ref"] for context in source_contexts],
            ],
            as_of_time=base_dataset_ref.feature_view_ref.as_of_time,
            feature_schema=feature_schema,
            build_config_hash=stable_digest(
                {
                    "base_dataset_id": request.base_dataset_id,
                    "request": request.model_dump(mode="json"),
                    "feature_names": [field.name for field in feature_schema],
                }
            ),
            storage_uri=f"artifact://datasets/{dataset_id}_feature_rows.json",
        )
        self.store.write_json(
            f"datasets/{dataset_id}_feature_rows.json",
            {"rows": [row.model_dump(mode="json") for row in enriched_rows]},
        )

        sample_policy = base_dataset_ref.sample_policy.model_copy(
            update={
                "recommended_training_use": "fusion_training_panel",
            }
        )
        feature_result = FeatureViewBuildResult(feature_view_ref=feature_view_ref, rows=enriched_rows)
        dataset_ref, samples, dataset_manifest = DatasetBuilder.build_dataset(
            dataset_id=dataset_id,
            feature_result=feature_result,
            labels=label_map,
            label_spec=base_dataset_ref.label_spec,
            split_manifest=base_dataset_ref.split_manifest,
            sample_policy=sample_policy,
        )

        coverage_values = list(coverage_by_feature.values())
        min_feature_coverage = min(coverage_values, default=1.0)
        coverage_warning = min_feature_coverage < 0.9
        coverage_failed = min_feature_coverage < float(
            request.missing_feature_policy.get("min_feature_coverage_ratio", 0.5) or 0.5
        )
        temporal_safety_passed = True
        freshness_candidates = [self._str(base_manifest_payload.get("freshness_status")) or "unknown"]
        for context in source_contexts:
            latest_point = max(context["points"], key=lambda item: item.available_time)
            freshness_candidates.append(
                self._freshness_status(
                    base_dataset_ref.feature_view_ref.as_of_time,
                    latest_point.available_time,
                )
            )
        freshness_rank = {"fresh": 0, "warning": 1, "stale": 2, "outdated": 3, "unknown": 4}
        worst_freshness = sorted(
            freshness_candidates,
            key=lambda item: freshness_rank.get(item or "unknown", 99),
        )[-1]

        dataset_manifest = dataset_manifest.model_copy(
            update={
                "asset_id": base_manifest_payload.get("asset_id") or request.base_dataset_id,
                "feature_set_id": "multi_domain_fusion_v1",
                "dropped_rows": dataset_manifest.dropped_rows + dropped_missing_rows,
                "raw_row_count": len(enriched_rows),
                "usable_sample_count": len(samples),
                "snapshot_version": stable_digest(
                    {
                        "base_dataset_id": request.base_dataset_id,
                        "feature_schema_hash": dataset_ref.feature_schema_hash,
                        "sample_count": len(samples),
                    }
                )[:12],
                "readiness_status": (
                    "not_ready"
                    if not samples or coverage_failed or not temporal_safety_passed
                    else ("warning" if coverage_warning else "ready")
                ),
                "alignment_status": (
                    "aligned" if request.alignment_policy_name in {"event_time_inner", "exact_inner"} else "aligned_asof"
                ),
                "missing_feature_status": (
                    "failed"
                    if coverage_failed
                    else ("warning" if coverage_warning or keep_with_flags else "clean")
                ),
                "label_alignment_status": "aligned",
                "split_integrity_status": "valid",
                "temporal_safety_status": "passed" if temporal_safety_passed else "failed",
                "freshness_status": worst_freshness,
                "quality_status": "warning" if coverage_warning else "healthy",
                "build_config": {
                    "sample_policy_name": request.sample_policy_name,
                    "alignment_policy_name": request.alignment_policy_name,
                    "missing_feature_policy_name": request.missing_feature_policy_name,
                    "sample_policy": request.sample_policy,
                    "alignment_policy": {
                        "mode": request.alignment_policy_name,
                        **request.alignment_policy,
                    },
                    "missing_feature_policy": {
                        "strategy": missing_strategy,
                        "coverage_by_feature": coverage_by_feature,
                        **request.missing_feature_policy,
                    },
                },
                "acquisition_profile": {
                    **dict(base_manifest_payload.get("acquisition_profile") or {}),
                    "request_name": request.request_name,
                    "data_domain": base_data_domain,
                    "data_domains": sorted(fusion_domains),
                    "dataset_type": "fusion_training_panel",
                    "request_origin": "fusion_dataset_request",
                    "base_dataset_id": request.base_dataset_id,
                    "market_anchor_dataset_id": request.base_dataset_id,
                    "source_dataset_ids": [request.base_dataset_id],
                    "fusion_domains": sorted(fusion_domains),
                    "source_specs": [
                        {
                            "data_domain": base_data_domain,
                            "source_vendor": self._str((base_manifest_payload.get("acquisition_profile") or {}).get("source_vendor")),
                            "exchange": self._str((base_manifest_payload.get("acquisition_profile") or {}).get("exchange")),
                            "frequency": self._str((base_manifest_payload.get("acquisition_profile") or {}).get("frequency")),
                            "symbol_selector": {
                                "symbols": list((base_manifest_payload.get("acquisition_profile") or {}).get("symbols") or []),
                            },
                        },
                        *[
                            {
                                "data_domain": context["source"].data_domain,
                                "source_vendor": context["source"].vendor,
                                "exchange": context["source"].exchange,
                                "frequency": context["source"].frequency,
                                "identifier": context["source"].identifier,
                                "metric_name": context["source"].metric_name or "value",
                                "feature_name": context["feature_name"],
                            }
                            for context in source_contexts
                        ],
                    ],
                    "fusion_sources": [
                        {
                            "data_domain": context["source"].data_domain,
                            "vendor": context["source"].vendor,
                            "identifier": context["source"].identifier,
                            "feature_name": context["feature_name"],
                            "frequency": context["source"].frequency,
                            "metric_name": context["source"].metric_name or "value",
                            "fetch_status": context["fetch_status"],
                            "storage_uri": context["storage_uri"],
                        }
                        for context in source_contexts
                    ],
                    "coverage_by_feature": coverage_by_feature,
                    "connector_status_by_source": {
                        f"market:{request.base_dataset_id}": self._str(
                            (base_manifest_payload.get("acquisition_profile") or {}).get("request_origin")
                        )
                        or "unknown",
                        **{
                            (
                                f"{context['source'].data_domain}:"
                                f"{context['source'].vendor}:"
                                f"{context['source'].identifier}"
                            ): context["fetch_status"]
                            for context in source_contexts
                        },
                    },
                    "merge_policy_name": (
                        self._str(request.alignment_policy.get("merge_policy_name"))
                        or request.alignment_policy_name
                    ),
                },
            }
        )

        dataset_samples_artifact = self.store.write_json(
            f"datasets/{dataset_id}_dataset_samples.json",
            {"samples": [sample.model_dump(mode="json") for sample in samples]},
        )
        feature_view_artifact = self.store.write_model(
            f"datasets/{dataset_id}_feature_view_ref.json",
            feature_view_ref,
        )
        dataset_manifest_artifact = self.store.write_model(
            f"datasets/{dataset_id}_dataset_manifest.json",
            dataset_manifest,
        )
        dataset_ref = dataset_ref.model_copy(
            update={
                "dataset_manifest_uri": dataset_manifest_artifact.uri,
                "dataset_samples_uri": dataset_samples_artifact.uri,
                "entity_scope": base_dataset_ref.entity_scope,
                "entity_count": base_dataset_ref.entity_count,
                "readiness_status": dataset_manifest.readiness_status,
            }
        )
        self.store.write_model(f"datasets/{dataset_id}_dataset_ref.json", dataset_ref)
        self.dataset_registry.bootstrap_from_artifacts()

        readiness = self.get_dataset_readiness(dataset_id)
        payload = self._dataset_ref(dataset_id) or {}
        summary = self._dataset_summary(payload) if payload else None
        training_summary = (
            TrainingDatasetSummaryView(
                dataset_id=summary.dataset_id,
                display_name=summary.display_name or summary.dataset_id,
                dataset_type=summary.dataset_type or "fusion_training_panel",
                data_domain=summary.data_domain,
                data_domains=list(summary.data_domains),
                snapshot_version=summary.snapshot_version,
                entity_scope=summary.entity_scope,
                universe_summary={
                    "entity_scope": summary.entity_scope,
                    "entity_count": summary.entity_count,
                    "symbols_preview": summary.symbols_preview,
                },
                sample_count=(readiness.usable_row_count if readiness else summary.sample_count),
                feature_count=summary.feature_count,
                label_count=summary.label_count,
                label_horizon=summary.label_horizon,
                split_strategy=summary.split_strategy,
                source_vendor=summary.source_vendor,
                frequency=summary.frequency,
                freshness_status=(readiness.freshness_status if readiness else summary.freshness.status),
                quality_status=summary.quality_status,
                readiness_status=(readiness.readiness_status if readiness else summary.readiness_status),
                readiness_reason=(
                    readiness.blocking_issues[0]
                    if readiness and readiness.blocking_issues
                    else (readiness.warnings[0] if readiness and readiness.warnings else None)
                ),
            )
            if summary is not None
            else None
        )
        return DatasetFusionBuildResponse(
            dataset_id=dataset_id,
            status="created",
            message="Fusion dataset was materialized and indexed for training.",
            detail_href=f"/datasets/{dataset_id}",
            training_href="/datasets/training",
            feature_view_uri=feature_view_artifact.uri,
            dataset_manifest_uri=dataset_manifest_artifact.uri,
            training_summary=training_summary,
            readiness=readiness,
        )

    def build_merged_dataset_from_sources(
        self,
        *,
        request_name: str,
        market_anchor_dataset_id: str,
        sources: list[DatasetAcquisitionSourceRequest],
        merge_policy_name: str = "available_time_safe_asof",
        request_origin: str = "dataset_request_multi_domain",
    ) -> DatasetFusionBuildResponse:
        if self.facade is None:
            raise ValueError("Merged dataset building is unavailable because the runtime facade is not configured.")

        base_entry = self._dataset_entry(market_anchor_dataset_id)
        if base_entry is None:
            raise ValueError(f"Market anchor dataset '{market_anchor_dataset_id}' was not found.")
        base_manifest = self._dataset_manifest(base_entry.payload)
        base_acquisition_profile = dict(base_manifest.get("acquisition_profile") or {})
        base_frequency = (
            self._str(base_acquisition_profile.get("frequency"))
            or base_entry.frequency
            or "unknown"
        )
        base_samples = self._load_dataset_samples(base_entry.payload)
        if not base_samples:
            raise ValueError(f"Market anchor dataset '{market_anchor_dataset_id}' has no materialized samples.")
        base_timestamps = sorted({sample.timestamp for sample in base_samples})
        auxiliary_sources = [source for source in sources if source.data_domain != "market"]
        if not auxiliary_sources:
            raise ValueError("Multi-domain merged dataset requires at least one non-market source.")

        connector_status_by_source: dict[str, str] = {
            f"market:{market_anchor_dataset_id}": self._str(base_acquisition_profile.get("request_origin")) or "unknown"
        }
        fusion_sources: list[DatasetFusionSourceRequest] = []
        min_feature_coverage_ratio = 1.0
        for source in auxiliary_sources:
            if source.data_domain not in {"macro", "on_chain", "derivatives", "sentiment_events"}:
                raise ValueError(
                    "Multi-domain merged dataset currently supports "
                    f"macro/on_chain/derivatives/sentiment_events auxiliaries, got '{source.data_domain}'."
                )
            if source.data_domain == "sentiment_events":
                min_feature_coverage_ratio = min(min_feature_coverage_ratio, 0.8)
            if source.data_domain == "derivatives":
                min_feature_coverage_ratio = min(min_feature_coverage_ratio, 0.9)
            if source.frequency != base_frequency:
                raise ValueError(
                    f"Multi-domain source '{source.data_domain}' must use frequency '{base_frequency}', got '{source.frequency}'."
                )
            if not source.identifier:
                raise ValueError(
                    f"Multi-domain source '{source.data_domain}' requires an identifier for merged requests."
                )
            try:
                points, fetch_status = self.facade.runtime.ingestion_service.fetch_series_points(
                    data_domain=source.data_domain,
                    identifier=source.identifier,
                    vendor=source.vendor,
                    frequency=source.frequency,
                    start_time=base_timestamps[0],
                    end_time=base_timestamps[-1] + self._frequency_delta(source.frequency),
                    options={
                        **({"exchange": source.exchange} if source.exchange else {}),
                        **dict(source.filters),
                    },
                )
            except Exception as exc:  # noqa: BLE001
                if getattr(exc, "code", None) == "empty_result":
                    raise ValueError(
                        f"Empty result from {source.data_domain}/{source.vendor}/{source.identifier}: {exc}"
                    ) from exc
                raise
            if not points:
                raise ValueError(
                    f"Multi-domain source '{source.data_domain}/{source.vendor}/{source.identifier}' returned no rows."
                )
            if (
                merge_policy_name == "strict_timestamp_inner"
                and sorted({point.event_time for point in points}) != base_timestamps
            ):
                raise ValueError(
                    f"Multi-domain source '{source.data_domain}/{source.vendor}/{source.identifier}' timestamps do not match the market anchor under strict_timestamp_inner."
                )
            connector_status_by_source[
                f"{source.data_domain}:{source.vendor}:{source.identifier}"
            ] = fetch_status
            if source.data_domain == "derivatives":
                for metric_name in (
                    "funding_rate",
                    "open_interest",
                    "global_long_short_ratio",
                    "taker_buy_sell_ratio",
                ):
                    fusion_sources.append(
                        DatasetFusionSourceRequest(
                            data_domain=source.data_domain,
                            vendor=source.vendor,
                            identifier=source.identifier,
                            frequency=source.frequency,
                            feature_name=f"derivatives_{self._slugify_dataset_id(metric_name, suffix='')}",
                            exchange=source.exchange,
                            metric_name=metric_name,
                            options=dict(source.filters),
                        )
                    )
            else:
                fusion_sources.append(
                    DatasetFusionSourceRequest(
                        data_domain=source.data_domain,
                        vendor=source.vendor,
                        identifier=source.identifier,
                        frequency=source.frequency,
                        feature_name=self._fusion_feature_name(source),
                        exchange=source.exchange,
                        metric_name=(
                            self._str(source.filters.get("metric_name"))
                            or self._str(source.filters.get("feature_name"))
                            or (
                                "macro_dff_value"
                                if source.data_domain == "macro"
                                else ("on_chain_value" if source.data_domain == "on_chain" else "value")
                            )
                        ),
                        options=dict(source.filters),
                    )
                )

        response = self.build_fusion_dataset(
            DatasetFusionRequest(
                request_name=request_name,
                base_dataset_id=market_anchor_dataset_id,
                dataset_type="fusion_training_panel",
                sample_policy_name="fusion_training_panel_strict",
                alignment_policy_name=merge_policy_name,
                missing_feature_policy_name="drop_if_missing",
                alignment_policy={"merge_policy_name": merge_policy_name},
                missing_feature_policy={"min_feature_coverage_ratio": min_feature_coverage_ratio},
                sources=fusion_sources,
            )
        )
        source_specs = [
            {
                "data_domain": source.data_domain,
                "source_vendor": source.vendor,
                "exchange": source.exchange,
                "frequency": source.frequency,
                "identifier": source.identifier,
                "filters": dict(source.filters),
                "symbol_selector": (
                    source.symbol_selector.model_dump(mode="json")
                    if source.symbol_selector is not None
                    else None
                ),
            }
            for source in sources
        ]
        self._update_dataset_acquisition_profile(
            response.dataset_id,
            {
                "request_name": request_name,
                "request_origin": request_origin,
                "dataset_type": "training_panel",
                "data_domain": "market",
                "data_domains": list(dict.fromkeys([source.data_domain for source in sources])),
                "merge_policy_name": merge_policy_name,
                "market_anchor_dataset_id": market_anchor_dataset_id,
                "source_dataset_ids": [market_anchor_dataset_id],
                "source_specs": source_specs,
                "connector_status_by_source": connector_status_by_source,
                "internal_visibility": "public",
            },
        )
        return response.model_copy(update={"readiness": self.get_dataset_readiness(response.dataset_id)})

    def _update_dataset_acquisition_profile(
        self,
        dataset_id: str,
        acquisition_profile_updates: dict[str, Any],
    ) -> None:
        entry = self._dataset_entry(dataset_id)
        if entry is None:
            raise ValueError(f"Dataset '{dataset_id}' was not found.")
        manifest_path = self._resolve_artifact_path(entry.manifest_uri) if entry.manifest_uri else (
            self.repository.artifact_root / "datasets" / f"{dataset_id}_dataset_manifest.json"
        )
        dataset_manifest = self.store.read_model(str(manifest_path), DatasetBuildManifest)
        acquisition_profile = dict(dataset_manifest.acquisition_profile or {})
        acquisition_profile.update(acquisition_profile_updates)
        dataset_manifest = dataset_manifest.model_copy(
            update={"acquisition_profile": acquisition_profile}
        )
        self.store.write_model(f"datasets/{dataset_id}_dataset_manifest.json", dataset_manifest)

    def build_sentiment_dataset_from_request(
        self,
        request: DatasetAcquisitionRequest,
        source: DatasetAcquisitionSourceRequest,
    ) -> str:
        return self.build_sentiment_dataset_from_sources(request, [source])

    def build_sentiment_dataset_from_sources(
        self,
        request: DatasetAcquisitionRequest,
        sources: list[DatasetAcquisitionSourceRequest],
    ) -> str:
        if self.facade is None:
            raise ValueError("Facade is required to build sentiment datasets.")
        if not sources:
            raise ValueError("Sentiment dataset requests require at least one source.")

        sorted_sources = sorted(
            sources,
            key=lambda item: (item.vendor, self._resolve_sentiment_identifier(item, request)),
        )
        dataset_id = self._slugify_dataset_id(
            request.request_name or self._resolve_sentiment_identifier(sorted_sources[0], request)
        )
        sorted_points: list[NormalizedSeriesPoint] = []
        source_specs: list[dict[str, Any]] = []
        connector_status_by_source: dict[str, str] = {}
        identifiers: list[str] = []
        symbols: dict[str, str] = {}

        for source in sorted_sources:
            identifier = self._resolve_sentiment_identifier(source, request)
            identifiers.append(identifier)
            source_options = self._sentiment_source_options(
                source,
                request,
                identifier=identifier,
            )
            points, fetch_status = self.facade.runtime.ingestion_service.fetch_series_points(
                data_domain="sentiment_events",
                identifier=identifier,
                vendor=source.vendor,
                frequency=source.frequency,
                start_time=request.time_window.start_time,
                end_time=request.time_window.end_time,
                options=source_options,
            )
            if not points:
                raise ValueError(
                    f"Sentiment source 'sentiment_events/{source.vendor}/{identifier}' returned no rows."
                )
            sorted_points.extend(points)
            connector_status_by_source[
                f"sentiment_events:{source.vendor}:{identifier}"
            ] = fetch_status
            source_specs.append(
                {
                    "data_domain": source.data_domain,
                    "source_vendor": source.vendor,
                    "frequency": source.frequency,
                    "identifier": identifier,
                    "filters": source_options,
                }
            )

        if not sorted_points:
            raise ValueError("Sentiment request did not produce any aligned feature points.")
        sorted_points.sort(key=lambda item: (item.event_time, item.vendor, item.metric_name))
        points_payload = {"rows": [point.model_dump(mode="json") for point in sorted_points]}
        points_artifact = self.store.write_json(
            f"datasets/{dataset_id}_sentiment_points.json",
            points_payload,
        )

        grouped_rows: dict[tuple[str, datetime], dict[str, Any]] = {}
        feature_order: list[str] = []
        feature_seen: set[str] = set()
        include_vendor = len({point.vendor for point in sorted_points}) > 1
        for point in sorted_points:
            feature_name = self._sentiment_feature_name(
                point.metric_name,
                vendor=(point.vendor if include_vendor else None),
            )
            if feature_name not in feature_seen:
                feature_order.append(feature_name)
                feature_seen.add(feature_name)
            row_key = (point.entity_key, point.event_time)
            symbols[point.entity_key] = point.dimensions.get("symbol") or point.entity_key
            entry = grouped_rows.setdefault(
                row_key,
                {
                    "available_time": point.available_time,
                    "values": {},
                },
            )
            if point.available_time > entry["available_time"]:
                entry["available_time"] = point.available_time
            entry["values"][feature_name] = float(point.value)

        feature_rows: list[FeatureRow] = []
        for (entity_key, timestamp), entry in sorted(
            grouped_rows.items(),
            key=lambda item: (item[0][1], item[0][0]),
        ):
            feature_rows.append(
                FeatureRow(
                    entity_key=entity_key,
                    timestamp=timestamp,
                    available_time=entry["available_time"],
                    values=entry["values"],
                )
            )
        if len(feature_rows) < 3:
            raise ValueError("Sentiment dataset requires at least three aligned feature rows.")

        as_of_time = max(row.available_time for row in feature_rows)
        time_range = TimeRange(
            start=min(row.timestamp for row in feature_rows),
            end=max(row.timestamp for row in feature_rows) + timedelta(seconds=1),
        )
        primary_entity = feature_rows[0].entity_key
        primary_symbol = symbols.get(primary_entity, primary_entity)
        vendor_slug = "_".join(sorted({self._sentiment_vendor_tag(source.vendor) for source in sorted_sources}))
        primary_identifier = identifiers[0]
        primary_vendor = (
            sorted_sources[0].vendor
            if len({source.vendor for source in sorted_sources}) == 1
            else "multi_source"
        )
        data_ref = DataAssetRef(
            asset_id=(
                f"sentiment_{self._slugify_dataset_id(vendor_slug, suffix='')}_"
                f"{self._slugify_dataset_id(primary_identifier)}_{sorted_sources[0].frequency}"
            ),
            schema_version=1,
            source=primary_vendor,
            symbol=primary_symbol,
            venue=primary_vendor,
            frequency=sorted_sources[0].frequency,
            time_range=time_range,
            storage_uri=points_artifact.uri,
            content_hash=stable_digest(points_payload),
            entity_key=primary_entity,
            tags=["sentiment_events"],
            request_origin="sentiment_dataset_request",
        )
        self.store.write_model(f"datasets/{data_ref.asset_id}_ref.json", data_ref)

        feature_view_ref = FeatureViewRef(
            feature_set_id="sentiment_hourly_snapshot_v1",
            input_data_refs=[data_ref],
            as_of_time=as_of_time,
            feature_schema=[
                FeatureField(
                    name=feature_name,
                    dtype="float",
                    lineage_source="sentiment_events",
                    max_available_time=as_of_time,
                )
                for feature_name in feature_order
            ],
            build_config_hash=stable_digest(
                {
                    "dataset_id": dataset_id,
                    "identifiers": identifiers,
                    "feature_names": feature_order,
                    "as_of_time": as_of_time,
                }
            ),
            storage_uri=f"artifact://datasets/{dataset_id}_feature_rows.json",
        )
        feature_result = FeatureViewBuildResult(feature_view_ref=feature_view_ref, rows=feature_rows)
        self.store.write_json(
            f"datasets/{dataset_id}_feature_rows.json",
            {"rows": [row.model_dump(mode="json") for row in feature_rows]},
        )

        label_horizon = max(1, int(request.build_config.label_horizon))
        uses_real_market_labels = primary_vendor == "reddit_archive"
        label_feature = (
            "forward_return_1"
            if uses_real_market_labels
            else self._default_sentiment_label_feature(feature_order)
        )
        market_anchor_dataset_id: str | None = None
        labels: dict[tuple[str, datetime], float] = {}
        rows_by_entity: dict[str, list[FeatureRow]] = {}
        for row in feature_rows:
            rows_by_entity.setdefault(row.entity_key, []).append(row)
        if uses_real_market_labels:
            market_context = self._build_sentiment_market_context(
                dataset_id=dataset_id,
                rows=feature_rows,
                request=request,
            )
            feature_rows = market_context["feature_rows"]
            feature_result = FeatureViewBuildResult(
                feature_view_ref=feature_view_ref,
                rows=feature_rows,
            )
            self.store.write_json(
                f"datasets/{dataset_id}_feature_rows.json",
                {"rows": [row.model_dump(mode="json") for row in feature_rows]},
            )
            rows_by_entity = {}
            for row in feature_rows:
                rows_by_entity.setdefault(row.entity_key, []).append(row)
            labels = market_context["labels"]
            market_anchor_dataset_id = market_context["market_anchor_dataset_id"]
        else:
            for entity_rows in rows_by_entity.values():
                entity_rows.sort(key=lambda row: row.timestamp)
                for index, row in enumerate(entity_rows):
                    future_index = index + label_horizon
                    if future_index >= len(entity_rows):
                        continue
                    current_value = row.values.get(label_feature)
                    future_value = entity_rows[future_index].values.get(label_feature)
                    if current_value is None or future_value is None:
                        continue
                    labels[(row.entity_key, row.timestamp)] = float(future_value - current_value)

        label_spec = LabelSpec(
            target_column=(
                f"future_return_{label_horizon}"
                if uses_real_market_labels
                else f"future_{label_feature}_{label_horizon}"
            ),
            horizon=label_horizon,
            kind=LabelKind(request.build_config.label_kind),
        )
        timestamps = sorted({row.timestamp for row in feature_rows})
        train_end_index = max(1, len(timestamps) // 2)
        valid_end_index = max(train_end_index + 1, int(len(timestamps) * 0.75))
        split_manifest = TimeSeriesSplitPlanner.single_split(
            timestamps=timestamps,
            train_end_index=train_end_index,
            valid_end_index=valid_end_index,
        )
        sample_policy = SamplePolicy(
            min_history_bars=1,
            drop_missing_targets=True,
            universe=("multi_asset" if len(rows_by_entity) > 1 else "single_asset"),
            recommended_training_use=request.dataset_type,
        )
        dataset_ref, samples, dataset_manifest = DatasetBuilder.build_dataset(
            dataset_id=dataset_id,
            feature_result=feature_result,
            labels=labels,
            label_spec=label_spec,
            split_manifest=split_manifest,
            sample_policy=sample_policy,
        )
        freshness_status = self._freshness_status(
            as_of_time,
            max(point.available_time for point in sorted_points),
        )
        dataset_manifest = dataset_manifest.model_copy(
            update={
                "asset_id": data_ref.asset_id,
                "feature_set_id": "sentiment_hourly_snapshot_v1",
                "raw_row_count": len(feature_rows),
                "usable_sample_count": len(samples),
                "readiness_status": "ready" if samples else "not_ready",
                "alignment_status": "aligned",
                "missing_feature_status": "clean",
                "label_alignment_status": "aligned",
                "split_integrity_status": "valid",
                "temporal_safety_status": "passed",
                "freshness_status": freshness_status,
                "quality_status": "healthy",
                "build_config": {
                    "sample_policy_name": request.build_config.sample_policy_name,
                    "alignment_policy_name": request.build_config.alignment_policy_name,
                    "missing_feature_policy_name": request.build_config.missing_feature_policy_name,
                    "sample_policy": request.build_config.sample_policy,
                    "alignment_policy": request.build_config.alignment_policy,
                    "missing_feature_policy": request.build_config.missing_feature_policy,
                    "label_feature": label_feature,
                    "label_source": (
                        "bitstamp_archive_forward_return" if uses_real_market_labels else "sentiment_self_delta"
                    ),
                },
                "acquisition_profile": {
                    "request_name": request.request_name,
                    "data_domain": "sentiment_events",
                    "data_domains": ["sentiment_events"],
                    "dataset_type": request.dataset_type,
                    "request_origin": "sentiment_dataset_request",
                    "source_vendor": primary_vendor,
                    "frequency": sorted_sources[0].frequency,
                    "identifier": (primary_identifier if len(identifiers) == 1 else None),
                    "identifiers": identifiers,
                    "symbols": sorted(set(symbols.values())),
                    "source_specs": source_specs,
                    "source_dataset_ids": (
                        [market_anchor_dataset_id] if market_anchor_dataset_id else []
                    ),
                    "fusion_domains": [],
                    "market_anchor_dataset_id": market_anchor_dataset_id,
                    "label_source_vendor": (
                        "bitstamp_archive" if uses_real_market_labels else None
                    ),
                    "connector_status_by_source": connector_status_by_source,
                    "internal_visibility": "public",
                },
            }
        )
        dataset_samples_artifact = self.store.write_json(
            f"datasets/{dataset_id}_dataset_samples.json",
            {"samples": [sample.model_dump(mode="json") for sample in samples]},
        )
        dataset_manifest_artifact = self.store.write_model(
            f"datasets/{dataset_id}_dataset_manifest.json",
            dataset_manifest,
        )
        self.store.write_model(f"datasets/{dataset_id}_feature_view_ref.json", feature_view_ref)
        dataset_ref = dataset_ref.model_copy(
            update={
                "dataset_manifest_uri": dataset_manifest_artifact.uri,
                "dataset_samples_uri": dataset_samples_artifact.uri,
                "entity_scope": sample_policy.universe,
                "entity_count": len(rows_by_entity),
                "readiness_status": dataset_manifest.readiness_status,
            }
        )
        self.store.write_model(f"datasets/{dataset_id}_dataset_ref.json", dataset_ref)
        self.facade.dataset_store[dataset_id] = samples
        self.dataset_registry.bootstrap_from_artifacts()
        return dataset_id

    def _build_sentiment_market_context(
        self,
        *,
        dataset_id: str,
        rows: list[FeatureRow],
        request: DatasetAcquisitionRequest,
    ) -> dict[str, Any]:
        if self.facade is None:
            raise ValueError("Facade is required to build market-backed sentiment datasets.")
        if not rows:
            raise ValueError("Sentiment dataset requires feature rows before building market labels.")
        start_time = min(row.timestamp for row in rows)
        end_time = max(row.timestamp for row in rows) + self._frequency_delta("1h") * (
            request.build_config.label_horizon + 1
        )
        market_bars, _ = self.facade.runtime.ingestion_service.fetch_market_bars(
            symbol="BTCUSD",
            vendor="bitstamp_archive",
            exchange="bitstamp",
            frequency="1h",
            start_time=start_time,
            end_time=end_time,
        )
        if not market_bars:
            raise ValueError("bitstamp_archive returned no real market bars for sentiment dataset labeling.")
        available_times = {bar.event_time for bar in market_bars}
        aligned_rows = [row for row in rows if row.timestamp in available_times]
        if len(aligned_rows) < 3:
            raise ValueError("Too few rows overlap between Reddit history and Bitstamp archive for training.")
        labels = ForwardReturnLabeler().build(
            aligned_rows,
            {bar.event_time: bar.close for bar in market_bars},
            horizon=max(1, int(request.build_config.label_horizon)),
        )
        market_anchor_dataset_id = self._materialize_hidden_market_anchor_dataset(
            dataset_id=f"{dataset_id}_market_anchor",
            market_bars=market_bars,
            request=request,
        )
        return {
            "feature_rows": aligned_rows,
            "labels": labels,
            "market_anchor_dataset_id": market_anchor_dataset_id,
        }

    def _materialize_hidden_market_anchor_dataset(
        self,
        *,
        dataset_id: str,
        market_bars: list[NormalizedMarketBar],
        request: DatasetAcquisitionRequest,
    ) -> str:
        if self.facade is None:
            raise ValueError("Facade is required to build a market anchor dataset.")
        if self._dataset_entry(dataset_id) is not None:
            return dataset_id
        sorted_bars = sorted(market_bars, key=lambda item: item.event_time)
        data_ref, _ = self.facade.data_catalog.register_market_asset(
            asset_id=f"{dataset_id}_asset",
            source="bitstamp_archive",
            frequency="1h",
            rows=sorted_bars,
            tags=["market", "bitstamp_archive", "internal_helper"],
            request_origin="sentiment_market_anchor",
            fallback_used=False,
        )
        as_of_time = max(bar.available_time for bar in sorted_bars)
        feature_result = MarketFeatureBuilder().build(
            feature_set_id="baseline_market_features",
            data_ref=data_ref,
            bars=sorted_bars,
            as_of_time=as_of_time,
        )
        labels = ForwardReturnLabeler().build(
            feature_result.rows,
            {bar.event_time: bar.close for bar in sorted_bars},
            horizon=max(1, int(request.build_config.label_horizon)),
        )
        timestamps = sorted({row.timestamp for row in feature_result.rows})
        train_end_index = max(1, len(timestamps) // 2)
        valid_end_index = max(train_end_index + 1, int(len(timestamps) * 0.75))
        split_manifest = TimeSeriesSplitPlanner.single_split(
            timestamps=timestamps,
            train_end_index=train_end_index,
            valid_end_index=valid_end_index,
        )
        dataset_ref, samples, dataset_manifest = DatasetBuilder.build_dataset(
            dataset_id=dataset_id,
            feature_result=feature_result,
            labels=labels,
            label_spec=LabelSpec(
                target_column=f"future_return_{max(1, int(request.build_config.label_horizon))}",
                horizon=max(1, int(request.build_config.label_horizon)),
                kind=LabelKind(request.build_config.label_kind),
            ),
            split_manifest=split_manifest,
            sample_policy=SamplePolicy(
                min_history_bars=10,
                drop_missing_targets=True,
                universe="single_asset",
                recommended_training_use="training_panel",
            ),
        )
        dataset_manifest = dataset_manifest.model_copy(
            update={
                "asset_id": data_ref.asset_id,
                "feature_set_id": "baseline_market_features",
                "raw_row_count": len(feature_result.rows),
                "usable_sample_count": len(samples),
                "readiness_status": "ready" if samples else "not_ready",
                "alignment_status": "aligned",
                "missing_feature_status": "clean",
                "label_alignment_status": "aligned",
                "split_integrity_status": "valid",
                "temporal_safety_status": "passed",
                "freshness_status": self._freshness_status(
                    as_of_time,
                    max(bar.available_time for bar in sorted_bars),
                ),
                "quality_status": "healthy",
                "acquisition_profile": {
                    "request_name": f"{request.request_name}_market_anchor",
                    "data_domain": "market",
                    "data_domains": ["market"],
                    "dataset_type": "training_panel",
                    "request_origin": "sentiment_market_anchor",
                    "source_vendor": "bitstamp_archive",
                    "exchange": "bitstamp",
                    "frequency": "1h",
                    "symbols": ["BTCUSD"],
                    "source_specs": [
                        {
                            "data_domain": "market",
                            "source_vendor": "bitstamp_archive",
                            "exchange": "bitstamp",
                            "frequency": "1h",
                            "symbol_selector": {"symbols": ["BTCUSD"]},
                        }
                    ],
                    "source_dataset_ids": [],
                    "fusion_domains": [],
                    "connector_status_by_source": {"market:bitstamp_archive:BTCUSD": "archive_fetch"},
                    "internal_visibility": "hidden",
                },
            }
        )
        dataset_samples_artifact = self.store.write_json(
            f"datasets/{dataset_id}_dataset_samples.json",
            {"samples": [sample.model_dump(mode="json") for sample in samples]},
        )
        dataset_manifest_artifact = self.store.write_model(
            f"datasets/{dataset_id}_dataset_manifest.json",
            dataset_manifest,
        )
        self.store.write_model(f"datasets/{dataset_id}_feature_view_ref.json", feature_result.feature_view_ref)
        dataset_ref = dataset_ref.model_copy(
            update={
                "dataset_manifest_uri": dataset_manifest_artifact.uri,
                "dataset_samples_uri": dataset_samples_artifact.uri,
                "entity_scope": "single_asset",
                "entity_count": 1,
                "readiness_status": dataset_manifest.readiness_status,
            }
        )
        self.store.write_model(f"datasets/{dataset_id}_dataset_ref.json", dataset_ref)
        self.facade.dataset_store[dataset_id] = samples
        self.dataset_registry.bootstrap_from_artifacts()
        return dataset_id

    def get_dataset_slices(self, dataset_id: str) -> DatasetSlicesResponse | None:
        payload = self._dataset_ref(dataset_id)
        if payload is None:
            return None
        readiness = self.get_dataset_readiness(dataset_id)
        manifest = self._dataset_manifest(payload)
        summary = self._dataset_summary(payload)
        split_manifest = dict(payload.get("split_manifest") or {})
        items = [
            DatasetSliceView(
                slice_id="full_dataset",
                label="Full Dataset",
                slice_kind="full",
                start_time=summary.freshness.data_start_time,
                end_time=summary.freshness.data_end_time,
                row_count=self._int_or_none(manifest.get("raw_row_count")) or self._dataset_raw_row_count(payload),
                sample_count=self._int_or_none(manifest.get("usable_sample_count")) or self._dataset_sample_count(payload),
                readiness_status=(readiness.readiness_status if readiness else None),
                metadata={
                    "dataset_type": self._resolved_dataset_type(payload, manifest)
                },
            )
        ]
        for key in ["train_range", "valid_range", "test_range"]:
            range_payload = split_manifest.get(key)
            if not isinstance(range_payload, dict):
                continue
            items.append(
                DatasetSliceView(
                    slice_id=key,
                    label=key.replace("_range", "").title(),
                    slice_kind="split",
                    start_time=self._dt(range_payload.get("start")),
                    end_time=self._dt(range_payload.get("end")),
                    sample_count=self._int_or_none((manifest.get("split_counts") or {}).get(key.removesuffix("_range"))),
                    readiness_status=(readiness.readiness_status if readiness else None),
                    metadata={"strategy": split_manifest.get("strategy")},
                )
            )
        return DatasetSlicesResponse(dataset_id=dataset_id, items=items)

    def get_dataset_series(self, dataset_id: str) -> DatasetSeriesResponse | None:
        payload = self._dataset_ref(dataset_id)
        if payload is None:
            return None
        manifest = self._dataset_manifest(payload)
        summary = self._dataset_summary(payload)
        default_domain = str((manifest.get("acquisition_profile") or {}).get("data_domain") or "market")
        items: list[DatasetSeriesView] = []
        for input_ref in self._dataset_input_refs(payload):
            coverage = input_ref.get("time_range") if isinstance(input_ref.get("time_range"), dict) else {}
            entity_key = self._str(input_ref.get("symbol")) or self._str(input_ref.get("asset_id"))
            series_key = self._str(input_ref.get("asset_id")) or self._str(input_ref.get("storage_uri")) or str(uuid.uuid4())
            input_domain = self._input_ref_domain(input_ref) or default_domain
            items.append(
                DatasetSeriesView(
                    series_key=series_key,
                    label=self._str(input_ref.get("symbol")) or series_key,
                    series_kind=(
                        "fusion_input_series"
                        if self._resolved_dataset_type(payload, manifest) == "fusion_training_panel"
                        else "input_series"
                    ),
                    data_domain=input_domain,
                    entity_key=entity_key,
                    frequency=self._str(input_ref.get("frequency")) or self._str((manifest.get("acquisition_profile") or {}).get("frequency")),
                    coverage={
                        "start_time": self._dt(coverage.get("start")),
                        "end_time": self._dt(coverage.get("end")),
                    },
                    metadata={
                        "source": self._str(input_ref.get("source")),
                        "venue": self._str(input_ref.get("venue")),
                        "storage_uri": self._str(input_ref.get("storage_uri")),
                        "tags": input_ref.get("tags") if isinstance(input_ref.get("tags"), list) else [],
                    },
                )
            )
        label_columns = self._label_columns(payload)
        for label_column in label_columns:
            items.append(
                DatasetSeriesView(
                    series_key=f"label::{label_column}",
                    label=label_column,
                    series_kind="label",
                    data_domain=default_domain,
                    entity_key=None,
                    frequency=summary.frequency,
                    coverage={
                        "start_time": summary.freshness.data_start_time,
                        "end_time": summary.freshness.data_end_time,
                    },
                    metadata={"horizon": (payload.get("label_spec") or {}).get("horizon")},
                )
            )
        return DatasetSeriesResponse(dataset_id=dataset_id, items=items)

    def query_dataset_ohlcv(
        self,
        dataset_id: str,
        *,
        page: int,
        per_page: int,
        start_time: datetime | None,
        end_time: datetime | None,
    ) -> OhlcvBarsResponse | None:
        payload = self._dataset_ref(dataset_id)
        if payload is None:
            return None
        input_refs = self._dataset_input_refs(payload)
        rows = self._dataset_bars_rows(payload)
        items: list[OhlcvBarView] = []
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            if not {"open", "high", "low", "close"}.issubset(row):
                continue
            event_time = self._dt(row.get("event_time"))
            if event_time is None:
                continue
            if start_time and event_time < start_time:
                continue
            if end_time and event_time > end_time:
                continue
            items.append(
                OhlcvBarView(
                    event_time=event_time,
                    available_time=self._dt(row.get("available_time")),
                    symbol=str(row.get("symbol", "UNKNOWN")),
                    venue=self._str(row.get("venue")),
                    open=float(row.get("open", 0.0) or 0.0),
                    high=float(row.get("high", 0.0) or 0.0),
                    low=float(row.get("low", 0.0) or 0.0),
                    close=float(row.get("close", 0.0) or 0.0),
                    volume=float(row.get("volume", 0.0) or 0.0),
                )
            )
        items.sort(key=lambda x: (x.event_time, x.symbol))
        start = (page - 1) * per_page
        end = start + per_page
        asset_ids = [self._str(ref.get("asset_id")) for ref in input_refs if self._str(ref.get("asset_id"))]
        symbols = [self._str(ref.get("symbol")) for ref in input_refs if self._str(ref.get("symbol"))]
        frequencies = [self._str(ref.get("frequency")) for ref in input_refs if self._str(ref.get("frequency"))]
        return OhlcvBarsResponse(
            dataset_id=dataset_id,
            asset_id=(asset_ids[0] if len(set(asset_ids)) == 1 and asset_ids else None),
            symbol=(symbols[0] if len(set(symbols)) == 1 and symbols else None),
            frequency=(frequencies[0] if len(set(frequencies)) == 1 and frequencies else None),
            total=len(items),
            page=page,
            per_page=per_page,
            start_time=start_time,
            end_time=end_time,
            items=items[start:end],
        )

    def get_dataset_request_options(self) -> DatasetRequestOptionsView:
        return DatasetRequestOptionsView(
            domains=[
                self._dataset_option("market", "市场数据", "首批真实接入域，默认用于价格序列与训练面板。", True),
                self._dataset_option("derivatives", "衍生品", "本期冻结统一契约、校验与缓存目录。"),
                self._dataset_option("on_chain", "链上数据", "首批真实接入域，面向公开链上指标。"),
                self._dataset_option("macro", "宏观数据", "首批真实接入域，面向宏观时间序列。"),
                self._dataset_option("sentiment_events", "情绪事件", "本期冻结统一契约、校验与缓存目录。"),
            ],
            asset_modes=[
                self._dataset_option("single_asset", "单资产", "单币种切片或单资产训练面板。", True),
                self._dataset_option("multi_asset", "多资产", "共享时间轴的多资产训练面板。"),
            ],
            selection_modes=[
                self._dataset_option("explicit", "显式选择", "请求中直接给出 symbols、series ids 或实体列表。", True),
                self._dataset_option("top_n", "Top N", "按预定义筛选规则自动选取前 N 个实体。"),
                self._dataset_option("facet_filter", "Facet 筛选", "由后端 facets 和约束组合出实体范围。"),
            ],
            symbol_types=[
                self._dataset_option("spot", "现货交易对", "默认使用交易所现货风格 symbol。", True),
                self._dataset_option("macro_series", "宏观序列", "FRED 一类宏观序列代码。"),
                self._dataset_option("protocol_metric", "链上指标", "协议、链或 TVL/费用类公开指标。"),
            ],
            source_vendors=[
                self._dataset_option("binance", "Binance Spot", "market 域首批真实连接器。", True),
                self._dataset_option("ccxt", "CCXT Exchange Adapter", "market 域统一交易所接入层，适合 Binance、OKX 等多交易所 OHLCV。"),
                self._dataset_option("bitstamp_archive", "Bitstamp Archive", "BTC/USD 历史 1h 价格线，优先用于真实历史训练与回测。"),
                self._dataset_option("fred", "FRED", "macro 域首批真实连接器。"),
                self._dataset_option("defillama", "DeFiLlama", "on_chain 域首批真实连接器。"),
                self._dataset_option("binance_futures", "Binance Futures", "derivatives 域首批真实连接器。"),
                self._dataset_option("news_archive", "News Archive", "sentiment_events 本地新闻归档源，优先用于稳定历史回放。"),
                self._dataset_option("gnews", "GNews / Google News", "sentiment_events 实时新闻源。"),
                self._dataset_option("gdelt", "GDELT DOC 2", "sentiment_events 候选历史新闻源，当前环境可能受限流影响。"),
                self._dataset_option("reddit_archive", "Reddit Archive", "统一的 Reddit 历史归档源，优先查本地数据库，不足部分再补采。", True),
                self._dataset_option("contract_only", "Contract Only", "仅冻结接口，不承诺本期真实拉取。"),
                self._dataset_option("internal_smoke", "内部样例", "保留给 smoke 与现有自动化测试。"),
            ],
            exchanges=[
                self._dataset_option("binance", "Binance", "market 域默认交易场所。", True),
                self._dataset_option("okx", "OKX", "CCXT 市场接入的可选交易场所。"),
                self._dataset_option("fred", "FRED", "macro 域逻辑 source。"),
                self._dataset_option("defillama", "DeFiLlama", "on_chain 域逻辑 source。"),
                self._dataset_option("binance_futures", "Binance Futures", "derivatives 域逻辑 source。"),
            ],
            frequencies=[
                self._dataset_option("1h", "1小时", "适合价格与链上指标的训练面板主频率。", True),
                self._dataset_option("4h", "4小时", "适合低频链上指标。"),
                self._dataset_option("1d", "1天", "适合宏观与跨域对齐。"),
            ],
            feature_sets=[
                self._dataset_option(
                    "baseline_market_features",
                    "Baseline Market Features",
                    "内置市场基线特征集。",
                    True,
                ),
                self._dataset_option(
                    "macro_snapshot_features",
                    "Macro Snapshot Features",
                    "宏观序列标准化快照骨架。",
                ),
                self._dataset_option(
                    "on_chain_snapshot_features",
                    "On-chain Snapshot Features",
                    "链上指标标准化快照骨架。",
                ),
                self._dataset_option(
                    "multi_domain_fusion_v1",
                    "Multi-domain Fusion v1",
                    "首批 market + macro + on_chain 融合训练面板特征骨架。",
                ),
            ],
            label_horizons=[
                self._dataset_option("1", "1 Bar", "预测下一个 bar 的前向收益。", True),
                self._dataset_option("6", "6 Bar", "更适合低频跨域对齐的 horizon。"),
                self._dataset_option("24", "24 Bar", "适合日频或更长观察窗口。"),
            ],
            split_strategies=[
                self._dataset_option("time_series", "时间序列切分", "训练/验证/测试按时间顺序切分。", True)
            ],
            sample_policies=[
                self._dataset_option("training_panel_strict", "严格训练面板", "默认策略，丢弃缺失标签并要求最小历史长度。", True),
                self._dataset_option("fusion_training_panel_strict", "融合训练面板", "面向跨域对齐后的融合训练面板。"),
                self._dataset_option("display_slice_lenient", "宽松展示切片", "优先保留可浏览样本，适合详情与切片浏览。"),
            ],
            alignment_policies=[
                self._dataset_option("event_time_inner", "事件时间内连接", "按 entity_key + timestamp 对齐，只保留共同可用截面。", True),
                self._dataset_option("strict_timestamp_inner", "严格时间戳内连接", "显式严格模式，要求所有源同频且时间戳完全一致。"),
                self._dataset_option("available_time_safe", "可用时间安全对齐", "要求 available_time 不晚于目标训练截面。"),
                self._dataset_option("available_time_safe_asof", "安全 asof 对齐", "多域 request 主链默认策略，按最新可用时间向后对齐后直接产出 merged dataset。"),
            ],
            missing_feature_policies=[
                self._dataset_option("drop_if_missing", "缺失即丢弃", "默认训练策略，超过阈值直接剔除样本。", True),
                self._dataset_option("keep_with_flags", "保留并打标", "为浏览和调试保留缺失样本，同时打出缺失标记。"),
            ],
            domain_capabilities={
                "market": {
                    "supports_real_ingestion": True,
                    "supported_vendors": ["binance", "bitstamp_archive", "internal_smoke"],
                    "supported_dataset_types": ["display_slice", "training_panel"],
                    "supported_frequencies": ["1h", "4h", "1d"],
                },
                "macro": {
                    "supports_real_ingestion": True,
                    "supported_vendors": ["fred"],
                    "supported_dataset_types": ["display_slice", "training_panel"],
                    "supported_frequencies": ["1d"],
                },
                "on_chain": {
                    "supports_real_ingestion": True,
                    "supported_vendors": ["defillama"],
                    "supported_dataset_types": ["display_slice", "training_panel"],
                    "supported_frequencies": ["1h", "4h", "1d"],
                },
                "derivatives": {
                    "supports_real_ingestion": True,
                    "supported_vendors": ["binance_futures"],
                    "supported_dataset_types": ["display_slice", "training_panel"],
                    "supported_frequencies": ["1h"],
                },
                "sentiment_events": {
                    "supports_real_ingestion": True,
                    "supported_vendors": [
                        "news_archive",
                        "gnews",
                        "gdelt",
                        "reddit_archive",
                    ],
                    "supported_dataset_types": ["display_slice", "training_panel"],
                    "supported_frequencies": ["1h"],
                    "supports_multi_source_same_domain": True,
                },
            },
            constraints={
                "current_supported_domains": ["market", "macro", "on_chain", "sentiment_events"],
                "current_supported_asset_modes": ["single_asset", "multi_asset"],
                "current_supported_symbols": [
                    "BTCUSDT",
                    "BTCUSD",
                    "ETHUSDT",
                    "SOLUSDT",
                    "BNBUSDT",
                    "DFF",
                    "TOTAL",
                    "btc_news",
                    "BTC",
                    "ETH",
                ],
                "multi_asset_status": "multi_domain_registry_ready",
                "request_flow": "api_datasets_requests_via_jobs",
                "train_entry_mode": "dataset_id_gt_dataset_preset",
                "registry_backend": "sqlite_manifest_index",
                "artifact_source_of_truth": "registry_and_manifest",
                "fallback_mode": "disabled_on_mainline",
                "supported_merge_policies": ["available_time_safe_asof", "strict_timestamp_inner"],
                "multi_domain_request_requires_market": True,
                "multi_domain_request_direct_merge": True,
            },
        )

    def get_dataset_facets(self) -> DatasetFacetsView:
        entries = [
            entry for entry in self.dataset_registry.list_entries() if self._is_public_dataset_payload(entry.payload)
        ]
        return DatasetFacetsView(
            domains=self._facet_buckets(entries, "data_domain"),
            dataset_types=self._facet_buckets(entries, "dataset_type"),
            source_vendors=self._facet_buckets(entries, "source_vendor"),
            frequencies=self._facet_buckets(entries, "frequency"),
            readiness_statuses=self._facet_buckets(entries, "readiness_status"),
        )

    def list_training_datasets(self) -> TrainingDatasetsResponse:
        items: list[TrainingDatasetSummaryView] = []
        for payload in self._dataset_refs(visible_only=True):
            dataset_id = str(payload.get("dataset_id", "unknown"))
            readiness = self.get_dataset_readiness(dataset_id)
            if readiness is None or readiness.readiness_status == "not_ready":
                continue
            summary = self._dataset_summary(payload)
            items.append(
                TrainingDatasetSummaryView(
                    dataset_id=summary.dataset_id,
                    display_name=summary.display_name or summary.dataset_id,
                    dataset_type=self._resolved_dataset_type(payload, self._dataset_manifest(payload)),
                    data_domain=str(
                        ((self._dataset_manifest(payload).get("acquisition_profile") or {}).get("data_domain"))
                        or "market"
                    ),
                    data_domains=list(summary.data_domains),
                    snapshot_version=summary.snapshot_version,
                    entity_scope=summary.entity_scope,
                    universe_summary={
                        "entity_scope": summary.entity_scope,
                        "entity_count": summary.entity_count,
                        "symbols_preview": summary.symbols_preview,
                    },
                    sample_count=readiness.usable_row_count or summary.sample_count,
                    feature_count=summary.feature_count,
                    label_count=summary.label_count,
                    label_horizon=summary.label_horizon,
                    split_strategy=summary.split_strategy,
                    source_vendor=summary.source_vendor,
                    frequency=summary.frequency,
                    freshness_status=readiness.freshness_status or summary.freshness.status,
                    quality_status=summary.quality_status,
                    readiness_status=readiness.readiness_status,
                    readiness_reason=(
                        readiness.blocking_issues[0]
                        if readiness.blocking_issues
                        else (readiness.warnings[0] if readiness.warnings else None)
                    ),
                )
            )
        items.sort(
            key=lambda item: (
                0 if item.readiness_status == "ready" else 1,
                item.snapshot_version or "",
                item.dataset_id,
            )
        )
        return TrainingDatasetsResponse(items=items, total=len(items))

    def get_dataset_readiness(self, dataset_id: str) -> DatasetReadinessSummaryView | None:
        payload = self._dataset_ref(dataset_id)
        if payload is None:
            return None
        manifest = self._dataset_manifest(payload)
        summary = self._dataset_summary(payload)
        quality_summary = self._dataset_quality_summary(payload)
        feature_schema = self._feature_schema(payload)
        label_columns = self._label_columns(payload)
        acquisition_profile = dict(manifest.get("acquisition_profile") or {})
        build_status = self._str(manifest.get("build_status")) or "success"
        raw_row_count = self._int_or_none(manifest.get("raw_row_count")) or self._dataset_raw_row_count(payload)
        usable_row_count = self._int_or_none(manifest.get("usable_sample_count")) or self._dataset_sample_count(payload)
        dropped_row_count = self._int_or_none(manifest.get("dropped_rows"))
        if dropped_row_count is None and raw_row_count is not None and usable_row_count is not None:
            dropped_row_count = max(raw_row_count - usable_row_count, 0)
        entity_scope = (
            self._str(payload.get("entity_scope"))
            or self._str(manifest.get("entity_scope"))
            or summary.entity_scope
            or "single_asset"
        )
        entity_count = (
            self._int_or_none(payload.get("entity_count"))
            or self._int_or_none(manifest.get("entity_count"))
            or summary.entity_count
            or 1
        )
        alignment_status = self._str(manifest.get("alignment_status")) or "aligned"
        missing_feature_status = self._str(manifest.get("missing_feature_status")) or (
            "warning" if (quality_summary.missing_ratio or 0.0) > 0.05 else "clean"
        )
        label_alignment_status = self._str(manifest.get("label_alignment_status")) or "aligned"
        split_integrity_status = self._str(manifest.get("split_integrity_status")) or (
            "valid" if payload.get("split_manifest") else "missing"
        )
        temporal_safety_status = self._str(manifest.get("temporal_safety_status")) or "passed"
        freshness_status = self._str(manifest.get("freshness_status")) or summary.freshness.status
        nlp_gate = self._dataset_official_nlp_gate(dataset_id, payload)

        blocking_issues: list[str] = []
        warnings: list[str] = []
        if usable_row_count in {None, 0}:
            blocking_issues.append("usable_sample_count_is_zero")
        if not feature_schema:
            blocking_issues.append("feature_schema_missing")
        if not label_columns:
            blocking_issues.append("label_spec_missing")
        if not payload.get("split_manifest"):
            blocking_issues.append("split_manifest_missing")
        if self._resolved_dataset_type(payload, manifest) == "fusion_training_panel":
            market_anchor_dataset_id = self._str(acquisition_profile.get("market_anchor_dataset_id"))
            if not market_anchor_dataset_id and not self._dataset_has_market_ohlcv(payload):
                blocking_issues.append("market_anchor_missing")
        if entity_scope == "multi_asset" and entity_count <= 1:
            blocking_issues.append("multi_asset_requires_multiple_entities")
        if label_alignment_status not in {"aligned", "passed", "unknown"}:
            blocking_issues.append("label_alignment_failed")
        if split_integrity_status not in {"valid", "passed", "unknown"}:
            blocking_issues.append("split_integrity_failed")
        if temporal_safety_status not in {"passed", "unknown"}:
            blocking_issues.append("temporal_safety_failed")
        if quality_summary.status == "risk":
            blocking_issues.append("quality_checks_failed")
        if missing_feature_status in {"risk", "failed"}:
            blocking_issues.append("missing_feature_threshold_exceeded")

        if freshness_status in {"stale", "outdated", "warning"}:
            warnings.append(f"freshness_{freshness_status}")
        if quality_summary.status == "warning":
            warnings.append("quality_warning")
        if acquisition_profile.get("fallback_used"):
            warnings.append("fallback_source_used")
        if entity_scope == "multi_asset" and entity_count <= 3:
            warnings.append("multi_asset_universe_is_small")
        if nlp_gate["official_template_gate_status"] == "failed":
            warnings.append("official_nlp_gate_failed")

        readiness_status = self._str(payload.get("readiness_status")) or self._str(manifest.get("readiness_status"))
        if blocking_issues:
            readiness_status = "not_ready"
        elif readiness_status in {None, "unknown"}:
            readiness_status = "warning" if warnings else "ready"
        elif readiness_status == "ready" and warnings:
            readiness_status = "warning"

        recommended_next_actions: list[str] = []
        if "feature_schema_missing" in blocking_issues:
            recommended_next_actions.append("重新构建特征视图并校验 feature schema。")
        if "label_alignment_failed" in blocking_issues:
            recommended_next_actions.append("检查标签是否按 (entity_key, timestamp) 对齐。")
        if "split_manifest_missing" in blocking_issues:
            recommended_next_actions.append("补齐时间序列 split manifest 后再训练。")
        if "fallback_source_used" in warnings:
            recommended_next_actions.append("优先切换到真实来源，避免长期依赖 fallback 样本。")
        if freshness_status in {"stale", "outdated", "warning"}:
            recommended_next_actions.append("刷新或重采集底层数据，再生成训练数据集。")
        if nlp_gate["official_template_gate_status"] == "failed":
            recommended_next_actions.append("补齐与 market 模板一致时间窗的 archival NLP 数据，并通过官方质量门禁后再参与 official backtest。")

        return DatasetReadinessSummaryView(
            dataset_id=dataset_id,
            data_domains=self._resolved_data_domains(acquisition_profile),
            build_status=build_status,
            readiness_status=readiness_status,
            blocking_issues=blocking_issues,
            warnings=warnings,
            raw_row_count=raw_row_count,
            usable_row_count=usable_row_count,
            dropped_row_count=dropped_row_count,
            feature_count=len(feature_schema),
            feature_schema_hash=self._str(payload.get("feature_schema_hash")) or self._str(manifest.get("feature_schema_hash")),
            feature_dimension_consistent=bool(feature_schema),
            entity_scope=entity_scope,
            entity_count=entity_count,
            alignment_status=alignment_status,
            missing_feature_status=missing_feature_status,
            label_alignment_status=label_alignment_status,
            split_integrity_status=split_integrity_status,
            temporal_safety_status=temporal_safety_status,
            freshness_status=freshness_status,
            recommended_next_actions=list(dict.fromkeys(recommended_next_actions)),
            official_template_eligible=nlp_gate["official_template_eligible"],
            official_nlp_gate_status=nlp_gate["official_template_gate_status"],
            official_nlp_gate_reasons=nlp_gate["official_template_gate_reasons"],
            archival_nlp_source_only=nlp_gate["archival_source_only"],
            nlp_requested_start_time=nlp_gate["requested_start_time"],
            nlp_requested_end_time=nlp_gate["requested_end_time"],
            nlp_actual_start_time=nlp_gate["actual_start_time"],
            nlp_actual_end_time=nlp_gate["actual_end_time"],
            market_window_start_time=nlp_gate["market_window_start_time"],
            market_window_end_time=nlp_gate["market_window_end_time"],
            official_backtest_start_time=nlp_gate["official_backtest_start_time"],
            official_backtest_end_time=nlp_gate["official_backtest_end_time"],
            nlp_coverage_ratio=nlp_gate["coverage_ratio"],
            nlp_test_coverage_ratio=nlp_gate["test_coverage_ratio"],
            nlp_max_consecutive_empty_bars=nlp_gate["max_consecutive_empty_bars"],
            nlp_duplicate_ratio=nlp_gate["duplicate_ratio"],
            nlp_entity_link_coverage_ratio=nlp_gate["entity_link_coverage_ratio"],
        )

    def _resolved_dataset_type(
        self,
        payload: dict[str, Any],
        manifest: dict[str, Any] | None = None,
    ) -> str:
        manifest = manifest or self._dataset_manifest(payload)
        acquisition_profile = dict(manifest.get("acquisition_profile") or {})
        dataset_type = (
            self._str(acquisition_profile.get("dataset_type"))
            or self._str((payload.get("sample_policy") or {}).get("recommended_training_use"))
            or (
                "training_panel"
                if isinstance((payload.get("split_manifest") or {}).get("train_range"), dict)
                and bool(payload.get("label_spec"))
                else "display_slice"
            )
        )
        if dataset_type in {
            "display_slice",
            "training_panel",
            "feature_snapshot",
            "fusion_training_panel",
        }:
            return dataset_type
        return "display_slice"

    def _resolved_data_domains(self, acquisition_profile: dict[str, Any]) -> list[str]:
        value = acquisition_profile.get("data_domains")
        if isinstance(value, list):
            resolved = [str(item) for item in value if isinstance(item, str) and item]
            if resolved:
                return list(dict.fromkeys(resolved))
        primary = self._str(acquisition_profile.get("data_domain")) or "market"
        fusion_domains = acquisition_profile.get("fusion_domains")
        if isinstance(fusion_domains, list):
            resolved = [primary]
            resolved.extend(
                str(item) for item in fusion_domains if isinstance(item, str) and item and item not in resolved
            )
            return resolved
        return [primary]

    def _dataset_has_market_ohlcv(self, payload: dict[str, Any]) -> bool:
        return any(
            {"open", "high", "low", "close"}.issubset(row)
            for row in self._dataset_bars_rows(payload)
            if isinstance(row, dict)
        )

    def _input_ref_domain(self, input_ref: dict[str, Any]) -> str | None:
        tags = input_ref.get("tags")
        if isinstance(tags, list):
            for item in tags:
                if isinstance(item, str) and item.startswith("domain:"):
                    return item.split(":", 1)[1]
        source = self._str(input_ref.get("source")) or ""
        lowered = source.lower()
        if "fred" in lowered:
            return "macro"
        if "llama" in lowered or "chain" in lowered:
            return "on_chain"
        if "gnews" in lowered or "reddit" in lowered or "sentiment" in lowered or "news" in lowered:
            return "sentiment_events"
        if "binance" in lowered or "market" in lowered:
            return "market"
        return None

    def _load_dataset_samples(self, payload: dict[str, Any]) -> list[DatasetSample]:
        samples_uri = self._str(payload.get("dataset_samples_uri"))
        dataset_id = str(payload.get("dataset_id", "unknown"))
        path = (
            self._resolve_artifact_path(samples_uri)
            if samples_uri
            else self.repository.artifact_root / "datasets" / f"{dataset_id}_dataset_samples.json"
        )
        loaded = self._load(path).get("samples", [])
        if not isinstance(loaded, list):
            return []
        return [DatasetSample.model_validate(item) for item in loaded if isinstance(item, dict)]

    def _build_series_point_resolver(
        self,
        points: list[NormalizedSeriesPoint],
        *,
        alignment_policy_name: str,
    ):
        points_by_event_time: dict[datetime, list[NormalizedSeriesPoint]] = {}
        for point in sorted(points, key=lambda item: (item.event_time, item.available_time)):
            points_by_event_time.setdefault(point.event_time, []).append(point)
        ordered_event_times = sorted(points_by_event_time)
        available_times_by_event_time = {
            event_time: [point.available_time for point in event_points]
            for event_time, event_points in points_by_event_time.items()
        }
        exact_alignment = alignment_policy_name in {
            "event_time_inner",
            "exact_inner",
            "timestamp_inner",
            "strict_timestamp_inner",
        }

        def resolve(timestamp: datetime, available_time: datetime) -> NormalizedSeriesPoint | None:
            event_index = bisect_right(ordered_event_times, timestamp) - 1
            if event_index < 0:
                return None
            if exact_alignment and ordered_event_times[event_index] != timestamp:
                return None
            while event_index >= 0:
                event_time = ordered_event_times[event_index]
                available_times = available_times_by_event_time[event_time]
                point_index = bisect_right(available_times, available_time) - 1
                if point_index >= 0:
                    return points_by_event_time[event_time][point_index]
                if exact_alignment:
                    return None
                event_index -= 1
            return None

        return resolve

    def _align_series_point(
        self,
        points: list[NormalizedSeriesPoint],
        *,
        timestamp: datetime,
        available_time: datetime,
        alignment_policy_name: str,
    ) -> NormalizedSeriesPoint | None:
        if alignment_policy_name in {
            "event_time_inner",
            "exact_inner",
            "timestamp_inner",
            "strict_timestamp_inner",
        }:
            for point in reversed(points):
                if point.event_time == timestamp and point.available_time <= available_time:
                    return point
            return None
        candidate: NormalizedSeriesPoint | None = None
        for point in points:
            if point.event_time > timestamp or point.available_time > available_time:
                continue
            if candidate is None or point.event_time >= candidate.event_time:
                candidate = point
        return candidate

    def _write_fusion_series_rows(
        self,
        dataset_id: str,
        feature_name: str,
        points: list[NormalizedSeriesPoint],
    ) -> str:
        relative_path = f"datasets/{dataset_id}_{feature_name}_series_rows.json"
        self.store.write_json(
            relative_path,
            {"rows": [point.model_dump(mode="json") for point in points]},
        )
        return f"artifact://{relative_path}"

    def _fusion_feature_name(self, source: Any) -> str:
        identifier = getattr(source, "identifier", "series")
        domain = getattr(source, "data_domain", "aux")
        metric_name = getattr(source, "metric_name", None) or "value"
        raw = f"{domain}_{identifier}_{metric_name}"
        return self._slugify_dataset_id(raw, suffix="")

    def _sentiment_feature_name(self, metric_name: str, vendor: str | None = None) -> str:
        if vendor is None:
            mapping = {
                "sentiment_score": "sentiment_score",
                "positive_ratio": "sentiment_positive_ratio",
                "negative_ratio": "sentiment_negative_ratio",
                "mention_count": "news_mention_count",
                "source_count": "news_source_count",
                "event_count": "news_event_count",
                "event_intensity": "text_event_intensity",
            }
            if metric_name in mapping:
                return mapping[metric_name]
            return f"text_{self._slugify_dataset_id(metric_name, suffix='')}"

        source_tag = self._sentiment_vendor_tag(vendor)
        mapping = {
            "sentiment_score": f"sentiment_{source_tag}_score",
            "positive_ratio": f"sentiment_{source_tag}_positive_ratio",
            "negative_ratio": f"sentiment_{source_tag}_negative_ratio",
            "mention_count": f"text_{source_tag}_mention_count",
            "source_count": f"text_{source_tag}_source_count",
            "event_count": f"text_{source_tag}_event_count",
            "event_intensity": f"text_{source_tag}_event_intensity",
        }
        if metric_name in mapping:
            return mapping[metric_name]
        return f"text_{source_tag}_{self._slugify_dataset_id(metric_name, suffix='')}"

    def _sentiment_vendor_tag(self, vendor: str) -> str:
        lowered = vendor.lower()
        if "reddit" in lowered:
            return "social"
        if "gnews" in lowered or "news" in lowered:
            return "news"
        return self._slugify_dataset_id(vendor, suffix="")

    def _default_sentiment_label_feature(self, feature_order: list[str]) -> str:
        for feature_name in feature_order:
            if "sentiment" in feature_name and "score" in feature_name:
                return feature_name
        return feature_order[0]

    def _resolve_sentiment_identifier(
        self,
        source: DatasetAcquisitionSourceRequest,
        request: DatasetAcquisitionRequest,
    ) -> str:
        identifier = self._str(source.identifier)
        if identifier:
            return identifier
        symbol_selector = source.symbol_selector or request.symbol_selector
        if symbol_selector is not None and symbol_selector.symbols:
            raw_symbol = str(symbol_selector.symbols[0]).strip()
            if raw_symbol.endswith("USDT"):
                raw_symbol = raw_symbol[:-4]
            return raw_symbol
        query = self._str(source.filters.get("query"))
        if query:
            return query
        raise ValueError("Sentiment dataset requests require identifier or symbol_selector.")

    def _sentiment_source_options(
        self,
        source: DatasetAcquisitionSourceRequest,
        request: DatasetAcquisitionRequest,
        *,
        identifier: str,
    ) -> dict[str, Any]:
        symbol_selector = source.symbol_selector or request.symbol_selector
        symbol = None
        if symbol_selector is not None and symbol_selector.symbols:
            symbol = str(symbol_selector.symbols[0])
        return {
            **dict(source.filters),
            **({"symbol": symbol} if symbol else {}),
            "identifier": identifier,
        }

    def _slugify_dataset_id(self, value: str, suffix: str = "") -> str:
        cleaned = [character.lower() if character.isalnum() else "_" for character in value.strip()]
        slug = "".join(cleaned).strip("_")
        while "__" in slug:
            slug = slug.replace("__", "_")
        if suffix:
            if not slug.endswith(f"_{suffix}"):
                slug = f"{slug}_{suffix}" if slug else suffix
        return slug or f"dataset_{datetime.now(UTC):%Y%m%d%H%M%S}"

    @staticmethod
    def _frequency_delta(frequency: str) -> timedelta:
        mapping = {
            "1m": timedelta(minutes=1),
            "5m": timedelta(minutes=5),
            "15m": timedelta(minutes=15),
            "1h": timedelta(hours=1),
            "4h": timedelta(hours=4),
            "1d": timedelta(days=1),
        }
        return mapping.get(frequency, timedelta(hours=1))

    def _run_ids(self) -> list[str]:
        ids = {path.stem for path in self.repository.list_paths("tracking/*.json")}
        ids.update(path.name for path in self.repository.list_paths("models/*") if path.is_dir())
        return sorted(run_id for run_id in ids if self._run_artifact_status(run_id)[0] == "complete")

    def _related_backtests(self, run_id: str) -> list[RelatedBacktestView]:
        results: list[RelatedBacktestView] = []
        for row in self._backtest_history_rows():
            if row.get("run_id") != run_id:
                continue
            metrics = (
                row.get("simulation_metrics")
                if isinstance(row.get("simulation_metrics"), dict)
                else {}
            )
            results.append(
                RelatedBacktestView(
                    backtest_id=str(row.get("backtest_id", "unknown_backtest")),
                    model_name=str(row.get("model_name", "unknown")),
                    run_id=run_id,
                    annual_return=self._float(metrics.get("annual_return")),
                    max_drawdown=self._float(metrics.get("max_drawdown")),
                    passed_consistency_checks=(
                        bool(row.get("passed_consistency_checks"))
                        if isinstance(row.get("passed_consistency_checks"), bool)
                        else None
                    ),
                    research_backend=self._research_backend(row),
                    portfolio_method=self._portfolio_method(row),
                )
            )
        return results

    def _related_backtest_count_map(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for row in self._backtest_history_rows():
            run_id = self._str(row.get("run_id"))
            if not run_id:
                continue
            counts[run_id] = counts.get(run_id, 0) + 1
        return counts

    def _backtest_id(self, uri: Any) -> str:
        if not isinstance(uri, str) or not uri:
            return "unknown_backtest"
        path = self.repository.resolve_uri(uri.replace("\\", "/"))
        payload = self._load(path)
        backtest_id = payload.get("backtest_id")
        return str(backtest_id) if isinstance(backtest_id, str) and backtest_id else path.stem

    def _run_artifact_status(self, run_id: str) -> tuple[str, list[str], list[str]]:
        model_dir = self.repository.artifact_root / "models" / run_id
        prediction_dir = self.repository.artifact_root / "predictions" / run_id
        prediction_scopes = (
            sorted(path.stem for path in prediction_dir.glob("*.json"))
            if prediction_dir.exists()
            else []
        )
        missing: list[str] = []
        if not (model_dir / "metadata.json").exists():
            missing.append("metadata.json")
        if not ((model_dir / "train_manifest.json").exists() or (model_dir / "manifest.json").exists()):
            missing.append("train_manifest.json")
        if not (model_dir / "evaluation_summary.json").exists():
            missing.append("evaluation_summary.json")
        if "full" not in prediction_scopes:
            missing.append("predictions/full.json")
        return ("complete" if not missing else "legacy", missing, prediction_scopes)

    def _engine(self, uri: Any) -> BacktestEngineView | None:
        if not isinstance(uri, str) or not uri:
            return None
        payload = self._load(self.repository.resolve_uri(uri.replace("\\", "/")))
        if not payload:
            return None
        report_path = self.repository.resolve_uri(str(payload.get("report_uri", "")).replace("\\", "/"))
        pnl_path = self.repository.resolve_uri(str(payload.get("pnl_uri", "")).replace("\\", "/"))
        positions_path = self.repository.resolve_uri(str(payload.get("positions_uri", "")).replace("\\", "/"))
        diagnostics_path = self.repository.resolve_uri(
            str(payload.get("diagnostics_uri", "")).replace("\\", "/")
        )
        scenarios_path = self.repository.resolve_uri(
            str(payload.get("scenario_summary_uri", "")).replace("\\", "/")
        )
        leakage_path = self.repository.resolve_uri(
            str(payload.get("leakage_audit_uri", "")).replace("\\", "/")
        )
        report = self._load(report_path)
        pnl = self._load(pnl_path)
        positions = self._load(positions_path)
        diagnostics = self._load(diagnostics_path)
        scenarios = self._load(scenarios_path)
        leakage = self._load(leakage_path)
        merged_metrics = self._metrics(
            {
                **(diagnostics.get("performance_metrics") or {}),
                **(diagnostics.get("execution_metrics") or {}),
                **(diagnostics.get("risk_metrics") or {}),
            }
        )
        warnings = [str(item) for item in diagnostics.get("warnings", []) if isinstance(item, str)]
        warnings.extend(self._leakage_warnings(leakage))
        return BacktestEngineView(
            backtest_id=str(payload.get("backtest_id", "unknown")),
            engine_type=str(payload.get("engine_type", "unknown")),
            report_summary=self._str(report.get("summary")),
            metrics=merged_metrics or self._metrics(payload.get("risk_metrics", {})),
            diagnostics=diagnostics,
            pnl_snapshot=self._metrics(pnl),
            positions=self._portfolio_curve_points(positions),
            scenarios=self._scenario_deltas(scenarios),
            warnings=list(dict.fromkeys(warnings)),
            artifacts=self._artifacts(
                [
                    ("backtest_report", report_path),
                    ("backtest_pnl", pnl_path),
                    ("backtest_positions", positions_path),
                    ("backtest_diagnostics", diagnostics_path),
                    ("backtest_scenarios", scenarios_path),
                    ("backtest_leakage", leakage_path),
                ]
            ),
        )

    def _portfolio_curve_points(self, payload: dict[str, Any]) -> list[TimeValuePoint]:
        snapshots = payload.get("snapshots", [])
        if not isinstance(snapshots, list):
            return []
        return [
            TimeValuePoint(
                label=str(snapshot.get("timestamp", f"p{index}")),
                value=float(snapshot.get("equity", snapshot.get("nav", 0.0)) or 0.0),
            )
            for index, snapshot in enumerate(snapshots)
            if isinstance(snapshot, dict)
        ]

    def _scenario_deltas(self, payload: dict[str, Any]) -> list[ScenarioDeltaView]:
        scenarios = payload.get("scenarios", [])
        if not isinstance(scenarios, list):
            return []
        return [
            ScenarioDeltaView(
                scenario_name=str(item.get("scenario_name", f"scenario_{index}")),
                cumulative_return_delta=float(item.get("pnl_delta", 0.0) or 0.0),
            )
            for index, item in enumerate(scenarios)
            if isinstance(item, dict)
        ]

    def _leakage_warnings(self, payload: dict[str, Any]) -> list[str]:
        warnings: list[str] = []
        for key, value in payload.items():
            if isinstance(value, bool) and not value:
                warnings.append(f"{key} failed")
        return warnings

    def _backtest_artifacts(self, row: dict[str, Any]) -> list[ArtifactView]:
        pairs: list[tuple[str, Path]] = []
        for kind, uri in [
            ("research_result", row.get("research_result_uri")),
            ("simulation_result", row.get("simulation_result_uri")),
        ]:
            if isinstance(uri, str) and uri:
                pairs.append((kind, self.repository.resolve_uri(uri.replace("\\", "/"))))
        return self._artifacts(pairs)

    def _dataset_refs(self, *, visible_only: bool = False) -> list[dict[str, Any]]:
        payloads = [entry.payload for entry in self.dataset_registry.list_entries()]
        if not visible_only:
            return payloads
        return [payload for payload in payloads if self._is_public_dataset_payload(payload)]

    def _dataset_ref(self, dataset_id: str) -> dict[str, Any] | None:
        entry = self.dataset_registry.get_entry(dataset_id)
        return entry.payload if entry else None

    def _dataset_entry(self, dataset_id: str) -> DatasetRegistryEntry | None:
        return self.dataset_registry.get_entry(dataset_id)

    def _is_public_dataset_payload(self, payload: dict[str, Any]) -> bool:
        acquisition_profile = self._dataset_manifest(payload).get("acquisition_profile") or {}
        visibility = self._str(acquisition_profile.get("internal_visibility")) or "public"
        return visibility != "hidden"

    def _facet_buckets(
        self,
        entries: list[DatasetRegistryEntry],
        attribute: str,
    ) -> list[DatasetFacetBucketView]:
        counts: dict[str, int] = {}
        for entry in entries:
            value = getattr(entry, attribute) or "unknown"
            counts[value] = counts.get(value, 0) + 1
        return [
            DatasetFacetBucketView(value=value, label=value.replace("_", " "), count=count)
            for value, count in sorted(counts.items())
        ]

    def _dependency_view(self, item: DatasetDependencyEntry) -> DatasetDependencyView:
        return DatasetDependencyView(
            dependency_kind=item.dependency_kind,
            dependency_id=item.dependency_id,
            dependency_label=item.dependency_label,
            target_dataset_id=item.target_dataset_id,
            direction="depends_on",
            blocking=False,
            metadata=item.payload,
        )

    def _blocking_dataset_dependencies(self, dataset_id: str) -> list[DatasetDependencyView]:
        candidates = [
            *self._run_dataset_dependencies(dataset_id),
            *self._backtest_dataset_dependencies(dataset_id),
            *self._dataset_reference_dependencies(dataset_id),
        ]
        deduped: dict[tuple[str, str, str | None], DatasetDependencyView] = {}
        for item in candidates:
            deduped[(item.dependency_kind, item.dependency_id, item.target_dataset_id)] = item
        return sorted(
            deduped.values(),
            key=lambda item: (item.dependency_kind, item.dependency_label or item.dependency_id),
        )

    def _run_dataset_dependencies(self, dataset_id: str) -> list[DatasetDependencyView]:
        items: list[DatasetDependencyView] = []
        models_root = self.repository.artifact_root / "models"
        if not models_root.exists():
            return items

        seen_run_ids: set[str] = set()
        for model_dir in sorted(models_root.iterdir()):
            if not model_dir.is_dir():
                continue
            manifest = self._load(model_dir / "train_manifest.json") or self._load(model_dir / "manifest.json")
            tracking = self.repository.read_json_if_exists(f"tracking/{model_dir.name}.json") or {}
            metadata = self._load(model_dir / "metadata.json")
            run_dataset_ids = self._run_dataset_ids(
                dataset_id=self._str((tracking.get("params") or {}).get("dataset_id"))
                or self._str(manifest.get("dataset_id")),
                manifest=manifest,
                metadata=metadata,
            )
            if dataset_id not in run_dataset_ids and self._str(manifest.get("dataset_ref_uri")) != f"dataset://{dataset_id}":
                continue
            run_id = self._str(manifest.get("run_id")) or self._str(tracking.get("run_id")) or model_dir.name
            if run_id in seen_run_ids:
                continue
            seen_run_ids.add(run_id)
            model_name = (
                self._str((tracking.get("params") or {}).get("model_name"))
                or self._str((manifest.get("model_artifact") or {}).get("metadata", {}).get("model_name"))
                or self._str(metadata.get("model_name"))
                or run_id
            )
            items.append(
                DatasetDependencyView(
                    dependency_kind="run",
                    dependency_id=run_id,
                    dependency_label=model_name,
                    target_dataset_id=dataset_id,
                    direction="referenced_by",
                    blocking=True,
                    href=f"/runs/{run_id}",
                    metadata={
                        "run_id": run_id,
                        "model_name": model_name,
                        "dataset_ids": run_dataset_ids,
                        "artifact_dir": self.repository.display_uri(model_dir),
                    },
                )
            )
        return items

    def _backtest_dataset_dependencies(self, dataset_id: str) -> list[DatasetDependencyView]:
        run_ids = {item.dependency_id for item in self._run_dataset_dependencies(dataset_id)}
        items: list[DatasetDependencyView] = []
        for row in self._backtest_history_rows():
            run_id = self._str(row.get("run_id"))
            row_dataset_ids = self._backtest_dataset_ids(row)
            if dataset_id not in row_dataset_ids and run_id not in run_ids:
                continue
            backtest_id = str(row.get("backtest_id", "unknown_backtest"))
            model_name = self._str(row.get("model_name")) or run_id or backtest_id
            items.append(
                DatasetDependencyView(
                    dependency_kind="backtest",
                    dependency_id=backtest_id,
                    dependency_label=model_name,
                    target_dataset_id=dataset_id,
                    direction="referenced_by",
                    blocking=True,
                    href=f"/backtests/{backtest_id}",
                    metadata={
                        "run_id": run_id,
                        "model_name": model_name,
                        "dataset_ids": row_dataset_ids,
                        "prediction_scope": self._str(row.get("prediction_scope")),
                    },
                )
            )
        return items

    def _backtest_history_rows(self) -> list[dict[str, Any]]:
        summary_rows_by_id, summary_updated_at = self._latest_backtest_summary_rows()
        rows: list[dict[str, Any]] = []
        seen_backtest_ids: set[str] = set()
        simulation_candidates = self.repository.list_paths("workflows/backtest/*_simulation.json")
        used_simulation_paths: set[Path] = set()

        for job in self._successful_backtest_jobs():
            job_result = job.get("result") if isinstance(job.get("result"), dict) else {}
            backtest_ids = [
                str(item)
                for item in job_result.get("backtest_ids", [])
                if isinstance(item, str) and item
            ]
            run_ids = [
                str(item)
                for item in job_result.get("run_ids", [])
                if isinstance(item, str) and item
            ]
            for index, backtest_id in enumerate(backtest_ids):
                if backtest_id in seen_backtest_ids:
                    continue
                run_id = run_ids[index] if index < len(run_ids) else (run_ids[0] if run_ids else None)
                row = self._build_backtest_history_row(
                    backtest_id=backtest_id,
                    run_id=run_id,
                    summary_row=summary_rows_by_id.get(backtest_id),
                    job=job,
                    simulation_candidates=simulation_candidates,
                    used_simulation_paths=used_simulation_paths,
                )
                if row is None:
                    continue
                rows.append(row)
                seen_backtest_ids.add(backtest_id)

        for backtest_id, row in summary_rows_by_id.items():
            if backtest_id in seen_backtest_ids:
                continue
            fallback_row = dict(row)
            fallback_row["updated_at"] = summary_updated_at
            rows.append(fallback_row)

        rows.sort(
            key=lambda row: (
                row.get("updated_at")
                if isinstance(row.get("updated_at"), datetime)
                else datetime.fromtimestamp(0, tz=UTC)
            ),
            reverse=True,
        )
        return rows

    def _latest_backtest_summary_rows(self) -> tuple[dict[str, dict[str, Any]], datetime | None]:
        summary_path = self.repository.artifact_root / "workflows" / "backtest" / "backtest_summary.json"
        summary = self.repository.read_json_if_exists("workflows/backtest/backtest_summary.json") or {}
        comparison_warnings = [
            str(item) for item in summary.get("comparison_warnings", []) if isinstance(item, str)
        ]
        rows_by_id: dict[str, dict[str, Any]] = {}
        for row in summary.get("rows", []):
            if not isinstance(row, dict):
                continue
            backtest_id = self._backtest_id(row.get("research_result_uri"))
            rows_by_id[backtest_id] = {
                **row,
                "backtest_id": backtest_id,
                "dataset_id": self._str(summary.get("dataset_id")),
                "prediction_scope": self._str(summary.get("prediction_scope")),
                "comparison_warnings": comparison_warnings,
                "protocol_metadata": (
                    dict(row.get("protocol_metadata"))
                    if isinstance(row.get("protocol_metadata"), dict)
                    else {}
                ),
            }
        return rows_by_id, self._path_mtime(summary_path)

    def _successful_backtest_jobs(self) -> list[dict[str, Any]]:
        jobs: list[dict[str, Any]] = []
        for path in self.repository.list_paths("webapi/jobs/*.json"):
            payload = self._load(path)
            if payload.get("job_type") != "backtest" or payload.get("status") != "success":
                continue
            result = payload.get("result")
            if not isinstance(result, dict):
                continue
            backtest_ids = result.get("backtest_ids")
            if not isinstance(backtest_ids, list) or not backtest_ids:
                continue
            jobs.append(payload)
        jobs.sort(
            key=lambda payload: self._backtest_job_finished_at(payload)
            or self._dt(payload.get("updated_at"))
            or datetime.fromtimestamp(0, tz=UTC)
        )
        return jobs

    def _build_backtest_history_row(
        self,
        *,
        backtest_id: str,
        run_id: str | None,
        summary_row: dict[str, Any] | None,
        job: dict[str, Any],
        simulation_candidates: list[Path],
        used_simulation_paths: set[Path],
    ) -> dict[str, Any] | None:
        job_result = job.get("result") if isinstance(job.get("result"), dict) else {}
        updated_at = self._backtest_job_finished_at(job) or self._dt(job.get("updated_at"))
        if summary_row is not None:
            row = dict(summary_row)
            row["run_id"] = self._str(row.get("run_id")) or run_id
            row["model_name"] = self._str(row.get("model_name")) or run_id or backtest_id
            row["dataset_id"] = self._str(row.get("dataset_id")) or self._str(job_result.get("dataset_id"))
            row["prediction_scope"] = self._str(row.get("prediction_scope")) or self._str(
                job_result.get("prediction_scope")
            )
            row = self._hydrate_backtest_row_with_job_result(row=row, job_result=job_result)
            row["updated_at"] = updated_at or row.get("updated_at")
            simulation_uri = row.get("simulation_result_uri")
            if isinstance(simulation_uri, str) and simulation_uri:
                used_simulation_paths.add(
                    self.repository.resolve_uri(simulation_uri.replace("\\", "/")).resolve()
                )
            return row

        research_path = self.repository.artifact_root / "workflows" / "backtest" / f"{backtest_id}_research.json"
        if not research_path.exists():
            return None
        simulation_uri = self._match_simulation_result_uri(
            job=job,
            research_path=research_path,
            simulation_candidates=simulation_candidates,
            used_simulation_paths=used_simulation_paths,
        )
        row = self._reconstruct_backtest_summary_row(
            backtest_id=backtest_id,
            run_id=run_id,
            research_path=research_path,
            simulation_uri=simulation_uri,
            dataset_id=self._str(job_result.get("dataset_id")),
            prediction_scope=self._str(job_result.get("prediction_scope")),
            updated_at=updated_at,
        )
        if row is not None:
            return self._hydrate_backtest_row_with_job_result(row=row, job_result=job_result)
        return self._hydrate_backtest_row_with_job_result(
            row={
                "backtest_id": backtest_id,
                "run_id": run_id,
                "model_name": run_id or backtest_id,
                "research_result_uri": self.repository.display_uri(research_path),
                "simulation_result_uri": simulation_uri,
                "dataset_id": self._str(job_result.get("dataset_id")),
                "prediction_scope": self._str(job_result.get("prediction_scope")),
                "comparison_warnings": [],
                "updated_at": updated_at or self._path_mtime(research_path),
            },
            job_result=job_result,
        )

    def _reconstruct_backtest_summary_row(
        self,
        *,
        backtest_id: str,
        run_id: str | None,
        research_path: Path,
        simulation_uri: str | None,
        dataset_id: str | None,
        prediction_scope: str | None,
        updated_at: datetime | None,
    ) -> dict[str, Any] | None:
        if not simulation_uri:
            return None
        research_payload = self._load(research_path)
        simulation_path = self.repository.resolve_uri(simulation_uri.replace("\\", "/"))
        simulation_payload = self._load(simulation_path)
        if not research_payload or not simulation_payload:
            return None
        try:
            research_result = BacktestResult.model_validate(research_payload)
            simulation_result = BacktestResult.model_validate(simulation_payload)
        except Exception:  # noqa: BLE001
            return None
        prediction_frame_uri = self._prediction_frame_uri(run_id, prediction_scope)
        summary_row, row_warnings = build_backtest_summary_row(
            store=self.store,
            model_name=run_id or backtest_id,
            run_id=run_id or backtest_id,
            prediction_frame_uri=prediction_frame_uri,
            research_result_uri=self.repository.display_uri(research_path),
            research_result=research_result,
            simulation_result_uri=self.repository.display_uri(simulation_path),
            simulation_result=simulation_result,
        )
        row = summary_row.model_dump(mode="json")
        row.update(
            {
                "backtest_id": backtest_id,
                "dataset_id": dataset_id,
                "prediction_scope": prediction_scope,
                "comparison_warnings": row_warnings,
                "protocol_metadata": row.get("protocol_metadata", {}),
                "updated_at": updated_at or self._path_mtime(research_path),
            }
        )
        return row

    def _preferred_backtest_row_for_run(
        self,
        run_id: str,
        *,
        template_id: str | None,
        official_only: bool,
    ) -> dict[str, Any] | None:
        candidates = [
            row
            for row in self._backtest_history_rows()
            if self._str(row.get("run_id")) == run_id
        ]
        if template_id:
            candidates = [
                row for row in candidates if self._protocol_template_id(row) == template_id
            ]
        if official_only:
            candidates = [row for row in candidates if self._protocol_official(row)]
        if not candidates:
            return None
        candidates.sort(
            key=lambda row: (
                row.get("updated_at")
                if isinstance(row.get("updated_at"), datetime)
                else datetime.fromtimestamp(0, tz=UTC)
            ),
            reverse=True,
        )
        return candidates[0]

    def _protocol_result_from_row(self, row: dict[str, Any]):
        simulation_metrics = (
            row.get("simulation_metrics")
            if isinstance(row.get("simulation_metrics"), dict)
            else {}
        )
        divergence_metrics = (
            row.get("divergence_metrics")
            if isinstance(row.get("divergence_metrics"), dict)
            else {}
        )
        scenario_metrics = (
            row.get("scenario_metrics")
            if isinstance(row.get("scenario_metrics"), dict)
            else {}
        )
        comparison_warnings = [
            str(item)
            for item in row.get("comparison_warnings", [])
            if isinstance(item, str)
        ]
        return compute_protocol_result(
            protocol_metadata=(
                row.get("protocol_metadata")
                if isinstance(row.get("protocol_metadata"), dict)
                else None
            ),
            simulation_metrics={
                str(key): float(value)
                for key, value in simulation_metrics.items()
                if isinstance(value, (int, float))
            },
            divergence_metrics={
                str(key): float(value)
                for key, value in divergence_metrics.items()
                if isinstance(value, (int, float))
            },
            scenario_metrics={
                str(key): float(value)
                for key, value in scenario_metrics.items()
                if isinstance(value, (int, float))
            },
            comparison_warnings=comparison_warnings,
            passed_consistency_checks=(
                bool(row.get("passed_consistency_checks"))
                if isinstance(row.get("passed_consistency_checks"), bool)
                else None
            ),
        )

    def _protocol_metadata_from_job_result(self, job_result: dict[str, Any]) -> dict[str, Any]:
        template = custom_backtest_template()
        template_id = self._str(job_result.get("template_id")) or template.template_id
        official = bool(job_result.get("official"))
        protocol_version = (
            self._str(job_result.get("protocol_version"))
            or (template.protocol_version if not official else None)
        )
        research_backend = self._str(job_result.get("research_backend")) or "native"
        portfolio_method = self._str(job_result.get("portfolio_method")) or "proportional"
        template_name = (
            self._str(job_result.get("template_name"))
            or (template.name if template_id == template.template_id else "Official Backtest Protocol v1")
        )
        return {
            "template_id": template_id,
            "template_name": template_name,
            "official": official,
            "protocol_version": protocol_version,
            "primary_dataset_id": self._str(job_result.get("dataset_id")),
            "dataset_ids": [
                str(item)
                for item in job_result.get("dataset_ids", [])
                if isinstance(item, str) and item
            ],
            "research_backend": research_backend,
            "portfolio_method": portfolio_method,
        }

    def _hydrate_backtest_row_with_job_result(
        self,
        *,
        row: dict[str, Any],
        job_result: dict[str, Any],
    ) -> dict[str, Any]:
        metadata = dict(row.get("protocol_metadata")) if isinstance(row.get("protocol_metadata"), dict) else {}
        fallback_metadata = self._protocol_metadata_from_job_result(job_result)
        for key, value in fallback_metadata.items():
            current = metadata.get(key)
            if current is None or current == "" or (isinstance(current, list) and not current):
                metadata[key] = value
        row["protocol_metadata"] = metadata
        row["research_backend"] = self._str(row.get("research_backend")) or self._str(
            metadata.get("research_backend")
        )
        row["portfolio_method"] = self._str(row.get("portfolio_method")) or self._str(
            metadata.get("portfolio_method")
        )
        return row

    def _protocol_template_id(self, row: dict[str, Any]) -> str | None:
        protocol_metadata = row.get("protocol_metadata")
        if not isinstance(protocol_metadata, dict):
            return None
        return self._str(protocol_metadata.get("template_id"))

    def _protocol_official(self, row: dict[str, Any]) -> bool:
        protocol_metadata = row.get("protocol_metadata")
        if not isinstance(protocol_metadata, dict):
            return False
        return bool(protocol_metadata.get("official"))

    def _protocol_version(self, row: dict[str, Any]) -> str | None:
        protocol_metadata = row.get("protocol_metadata")
        if not isinstance(protocol_metadata, dict):
            return None
        return self._str(protocol_metadata.get("protocol_version"))

    def _research_backend(self, row: dict[str, Any]) -> str | None:
        value = self._str(row.get("research_backend"))
        if value:
            return value
        protocol_metadata = row.get("protocol_metadata")
        if not isinstance(protocol_metadata, dict):
            return None
        return self._str(protocol_metadata.get("research_backend"))

    def _portfolio_method(self, row: dict[str, Any]) -> str | None:
        value = self._str(row.get("portfolio_method"))
        if value:
            return value
        protocol_metadata = row.get("protocol_metadata")
        if not isinstance(protocol_metadata, dict):
            return None
        return self._str(protocol_metadata.get("portfolio_method"))

    def _protocol_gate_status(self, row: dict[str, Any]) -> str | None:
        protocol = self._protocol_result_from_row(row)
        return protocol.gate_status if protocol is not None else None

    def _match_simulation_result_uri(
        self,
        *,
        job: dict[str, Any],
        research_path: Path,
        simulation_candidates: list[Path],
        used_simulation_paths: set[Path],
    ) -> str | None:
        available_candidates = [
            path.resolve() for path in simulation_candidates if path.resolve() not in used_simulation_paths
        ]
        if not available_candidates:
            return None
        backtest_stage = self._job_stage(job, "backtest")
        stage_start = (
            self._dt(backtest_stage.get("started_at"))
            if isinstance(backtest_stage, dict)
            else None
        )
        stage_finish = (
            self._dt(backtest_stage.get("finished_at"))
            if isinstance(backtest_stage, dict)
            else None
        )
        if stage_start and stage_finish:
            in_window = [
                path
                for path in available_candidates
                if (mtime := self._path_mtime(path)) is not None
                and stage_start - timedelta(seconds=1) <= mtime <= stage_finish + timedelta(seconds=1)
            ]
            if in_window:
                selected = min(
                    in_window,
                    key=lambda path: abs(
                        (
                            (self._path_mtime(path) or stage_finish)
                            - (self._path_mtime(research_path) or stage_finish)
                        ).total_seconds()
                    ),
                )
                used_simulation_paths.add(selected)
                return self.repository.display_uri(selected)
        research_mtime = self._path_mtime(research_path)
        if research_mtime is None:
            return None
        nearby = [
            path
            for path in available_candidates
            if (mtime := self._path_mtime(path)) is not None
            and abs((mtime - research_mtime).total_seconds()) <= 30.0
        ]
        if not nearby:
            return None
        selected = min(
            nearby,
            key=lambda path: abs(
                ((self._path_mtime(path) or research_mtime) - research_mtime).total_seconds()
            ),
        )
        used_simulation_paths.add(selected)
        return self.repository.display_uri(selected)

    def _backtest_job_finished_at(self, payload: dict[str, Any]) -> datetime | None:
        stage = self._job_stage(payload, "backtest")
        if isinstance(stage, dict):
            return self._dt(stage.get("finished_at")) or self._dt(stage.get("started_at"))
        return None

    def _job_stage(self, payload: dict[str, Any], name: str) -> dict[str, Any] | None:
        stages = payload.get("stages")
        if not isinstance(stages, list):
            return None
        for stage in stages:
            if isinstance(stage, dict) and stage.get("name") == name:
                return stage
        return None

    def _prediction_frame_uri(self, run_id: str | None, prediction_scope: str | None) -> str:
        if not run_id:
            return ""
        scope = prediction_scope or "full"
        return self.repository.display_uri(
            self.repository.artifact_root / "predictions" / run_id / f"{scope}.json"
        )

    def _path_mtime(self, path: Path) -> datetime | None:
        if not path.exists():
            return None
        return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)

    def _dataset_reference_dependencies(self, dataset_id: str) -> list[DatasetDependencyView]:
        target_tokens = {dataset_id, f"dataset://{dataset_id}"}
        items: list[DatasetDependencyView] = []
        for entry in self.dataset_registry.list_entries():
            if entry.dataset_id == dataset_id:
                continue
            matched_paths = self._find_dataset_reference_paths(entry.payload, target_tokens)
            matched_paths.extend(self._find_dataset_reference_paths(entry.manifest, target_tokens))
            if not matched_paths:
                continue
            dependency_kind = "fusion_dataset" if self._is_fusion_entry(entry, matched_paths) else "training_panel"
            items.append(
                DatasetDependencyView(
                    dependency_kind=dependency_kind,
                    dependency_id=entry.dataset_id,
                    dependency_label=entry.snapshot_version or entry.dataset_id,
                    target_dataset_id=dataset_id,
                    direction="referenced_by",
                    blocking=True,
                    href=f"/datasets/{entry.dataset_id}",
                    metadata={
                        "dataset_type": entry.dataset_type,
                        "data_domain": entry.data_domain,
                        "matched_paths": matched_paths[:12],
                    },
                )
            )
        return items

    def _is_fusion_entry(self, entry: DatasetRegistryEntry, matched_paths: list[str]) -> bool:
        return "fusion" in " ".join([entry.dataset_type, entry.data_domain, *matched_paths]).lower()

    def _find_dataset_reference_paths(
        self,
        payload: Any,
        target_tokens: set[str],
        path: str = "$",
    ) -> list[str]:
        if isinstance(payload, dict):
            matched: list[str] = []
            for key, value in payload.items():
                matched.extend(self._find_dataset_reference_paths(value, target_tokens, f"{path}.{key}"))
            return matched
        if isinstance(payload, list):
            matched = []
            for index, value in enumerate(payload):
                matched.extend(self._find_dataset_reference_paths(value, target_tokens, f"{path}[{index}]"))
            return matched
        if isinstance(payload, str) and payload in target_tokens:
            return [path]
        return []

    def _dataset_option(
        self,
        value: str,
        label: str,
        description: str | None = None,
        recommended: bool = False,
    ) -> DatasetRequestOptionView:
        return DatasetRequestOptionView(
            value=value,
            label=label,
            description=description,
            recommended=recommended,
        )

    def _dataset_input_refs(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        input_refs = (payload.get("feature_view_ref") or {}).get("input_data_refs")
        if not isinstance(input_refs, list):
            return []
        return [item for item in input_refs if isinstance(item, dict)]

    def _dataset_manifest(self, payload: dict[str, Any]) -> dict[str, Any]:
        manifest_uri = self._str(payload.get("dataset_manifest_uri"))
        dataset_id = str(payload.get("dataset_id", "unknown"))
        if manifest_uri:
            return self._load(self._resolve_artifact_path(manifest_uri))
        fallback_path = self.repository.artifact_root / "datasets" / f"{dataset_id}_dataset_manifest.json"
        return self._load(fallback_path)

    def _resolve_artifact_path(self, uri: str) -> Path:
        if uri.startswith("artifact://"):
            return self.repository.artifact_root / uri.removeprefix("artifact://")
        return self.repository.resolve_uri(uri.replace("\\", "/"))

    def _delete_artifact_uris(self, uris: list[str]) -> list[str]:
        deleted_files: list[str] = []
        seen: set[Path] = set()
        for uri in uris:
            path = self._resolve_artifact_path(uri)
            resolved = path.resolve()
            if resolved in seen or not resolved.exists():
                continue
            seen.add(resolved)
            try:
                resolved.relative_to(self.repository.artifact_root)
            except ValueError:
                continue
            if resolved.is_dir():
                shutil.rmtree(resolved)
            else:
                resolved.unlink(missing_ok=True)
            deleted_files.append(self.repository.display_uri(resolved))
        return sorted(deleted_files)

    def _delete_dataset_artifacts(self, entry: DatasetRegistryEntry) -> list[str]:
        dataset_dir = self.repository.artifact_root / "datasets"
        candidate_paths: list[Path] = []
        for pattern in [f"{entry.dataset_id}_*.json", f"{entry.dataset_id}.json"]:
            candidate_paths.extend(dataset_dir.glob(pattern))
        for uri in [
            entry.ref_uri,
            entry.manifest_uri,
            entry.samples_uri,
            entry.feature_view_uri,
            self._str((entry.payload.get("feature_view_ref") or {}).get("storage_uri")),
        ]:
            if isinstance(uri, str) and uri:
                candidate_paths.append(self._resolve_artifact_path(uri))

        deleted_files: list[str] = []
        seen: set[Path] = set()
        for path in candidate_paths:
            resolved = path.resolve()
            if resolved in seen or not resolved.exists():
                continue
            seen.add(resolved)
            try:
                resolved.relative_to(self.repository.artifact_root)
            except ValueError:
                continue
            if resolved.is_dir():
                shutil.rmtree(resolved)
            else:
                resolved.unlink(missing_ok=True)
            deleted_files.append(self.repository.display_uri(resolved))
        return sorted(deleted_files)

    def _remove_backtest_from_summary(self, backtest_id: str) -> None:
        summary_path = self.repository.artifact_root / "workflows" / "backtest" / "backtest_summary.json"
        summary = self._load(summary_path)
        rows = summary.get("rows")
        if not isinstance(rows, list):
            return

        filtered_rows: list[Any] = []
        changed = False
        removable_ids = {
            backtest_id,
            f"{backtest_id}_research",
            f"{backtest_id}_simulation",
        }
        for row in rows:
            if not isinstance(row, dict):
                filtered_rows.append(row)
                continue
            row_backtest_id = self._backtest_id(row.get("research_result_uri"))
            simulation_backtest_id = self._backtest_id(row.get("simulation_result_uri"))
            if row_backtest_id in removable_ids or simulation_backtest_id in removable_ids:
                changed = True
                continue
            filtered_rows.append(row)

        if not changed:
            return

        summary["rows"] = filtered_rows
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _remove_backtest_job_references(self, backtest_id: str) -> None:
        jobs_root = self.repository.artifact_root / "webapi" / "jobs"
        if not jobs_root.exists():
            return

        for path in self.repository.list_paths("webapi/jobs/*.json"):
            payload = self._load(path)
            result = payload.get("result")
            if payload.get("job_type") != "backtest" or not isinstance(result, dict):
                continue

            backtest_ids = result.get("backtest_ids")
            if not isinstance(backtest_ids, list) or backtest_id not in backtest_ids:
                continue

            result["backtest_ids"] = [
                item for item in backtest_ids if isinstance(item, str) and item != backtest_id
            ]

            deeplinks = result.get("deeplinks")
            if isinstance(deeplinks, dict):
                detail_href = deeplinks.get("backtest_detail")
                if isinstance(detail_href, str) and detail_href.rstrip("/").endswith(f"/backtests/{backtest_id}"):
                    deeplinks["backtest_detail"] = None

            result_links = result.get("result_links")
            if isinstance(result_links, list):
                result["result_links"] = [
                    item
                    for item in result_links
                    if not (
                        isinstance(item, dict)
                        and (
                            item.get("kind") == "backtest_detail"
                            or (
                                isinstance(item.get("href"), str)
                                and str(item.get("href")).rstrip("/").endswith(f"/backtests/{backtest_id}")
                            )
                        )
                    )
                ]

            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _collect_internal_helper_entries(self, entry: DatasetRegistryEntry) -> list[DatasetRegistryEntry]:
        collected: list[DatasetRegistryEntry] = []
        seen = {entry.dataset_id}
        queue = list(self._internal_helper_dataset_ids(entry))
        while queue:
            helper_id = queue.pop(0)
            if helper_id in seen:
                continue
            seen.add(helper_id)
            helper_entry = self._dataset_entry(helper_id)
            if helper_entry is None or not self._is_internal_helper_dataset(helper_entry):
                continue
            collected.append(helper_entry)
            queue.extend(self._internal_helper_dataset_ids(helper_entry))
        return collected

    def _internal_helper_dataset_ids(self, entry: DatasetRegistryEntry) -> list[str]:
        manifest = entry.manifest or self._dataset_manifest(entry.payload)
        acquisition_profile = dict(manifest.get("acquisition_profile") or {})
        candidate_ids: list[str] = []
        market_anchor_dataset_id = self._str(acquisition_profile.get("market_anchor_dataset_id"))
        if market_anchor_dataset_id:
            candidate_ids.append(market_anchor_dataset_id)
        source_dataset_ids = acquisition_profile.get("source_dataset_ids")
        if isinstance(source_dataset_ids, list):
            candidate_ids.extend(
                source_dataset_id
                for source_dataset_id in source_dataset_ids
                if isinstance(source_dataset_id, str) and source_dataset_id
            )
        unique_ids: list[str] = []
        seen: set[str] = set()
        for candidate_id in candidate_ids:
            if candidate_id == entry.dataset_id or candidate_id in seen:
                continue
            seen.add(candidate_id)
            unique_ids.append(candidate_id)
        return unique_ids

    def _is_internal_helper_dataset(self, entry: DatasetRegistryEntry) -> bool:
        manifest = entry.manifest or self._dataset_manifest(entry.payload)
        acquisition_profile = dict(manifest.get("acquisition_profile") or {})
        internal_visibility = self._str(acquisition_profile.get("internal_visibility"))
        request_origin = self._str(acquisition_profile.get("request_origin")) or entry.request_origin
        return (
            internal_visibility == "hidden"
            or (request_origin or "").startswith("sentiment_market_anchor")
            or entry.dataset_id.endswith("_market_anchor")
        )

    def _dataset_sample_count(self, payload: dict[str, Any]) -> int | None:
        samples_uri = self._str(payload.get("dataset_samples_uri"))
        dataset_id = str(payload.get("dataset_id", "unknown"))
        path = (
            self._resolve_artifact_path(samples_uri)
            if samples_uri
            else self.repository.artifact_root / "datasets" / f"{dataset_id}_dataset_samples.json"
        )
        rows = self._load(path).get("samples", [])
        return len(rows) if isinstance(rows, list) else None

    def _dataset_feature_rows(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        feature_view_ref = payload.get("feature_view_ref") or {}
        storage_uri = self._str(feature_view_ref.get("storage_uri"))
        dataset_id = str(payload.get("dataset_id", "unknown"))
        path = (
            self._resolve_artifact_path(storage_uri)
            if storage_uri
            else self.repository.artifact_root / "datasets" / f"{dataset_id}_feature_rows.json"
        )
        rows = self._load(path).get("rows", [])
        return [item for item in rows if isinstance(item, dict)] if isinstance(rows, list) else []

    def _dataset_contains_nlp(self, payload: dict[str, Any]) -> bool:
        manifest = self._dataset_manifest(payload)
        acquisition_profile = dict(manifest.get("acquisition_profile") or {})
        data_domains = self._resolved_data_domains(acquisition_profile)
        if "sentiment_events" in data_domains:
            return True
        feature_names = {item.get("name") for item in self._feature_schema(payload) if isinstance(item, dict)}
        return any(
            isinstance(name, str) and name.startswith(("sentiment_", "text_", "news_", "social_"))
            for name in feature_names
        )

    def _dataset_nlp_points(self, payload: dict[str, Any]) -> list[NormalizedSeriesPoint]:
        dataset_id = str(payload.get("dataset_id", "unknown"))
        points: list[NormalizedSeriesPoint] = []
        seen: set[tuple[str, str, str, str]] = set()
        candidate_paths: list[Path] = [self.repository.artifact_root / "datasets" / f"{dataset_id}_sentiment_points.json"]
        for input_ref in self._dataset_input_refs(payload):
            if self._input_ref_domain(input_ref) != "sentiment_events":
                continue
            source = self._canonical_sentiment_vendor(self._str(input_ref.get("source")))
            if source == "reddit_archive":
                time_range = input_ref.get("time_range")
                start_time = self._dt(time_range.get("start")) if isinstance(time_range, dict) else None
                end_time = self._dt(time_range.get("end")) if isinstance(time_range, dict) else None
                if start_time is None or end_time is None:
                    continue
                symbol = (
                    self._str(input_ref.get("symbol"))
                    or self._str(input_ref.get("entity_key"))
                    or "BTC"
                ).upper()
                asset_id = "BTC" if "BTC" in symbol else symbol
                connector = RedditArchiveSentimentConnector(self.repository.artifact_root)
                for point in connector.store.query_points(
                    asset_id=asset_id,
                    start_time=start_time,
                    end_time=end_time,
                    vendor="reddit_archive",
                ):
                    key = (
                        point.vendor,
                        point.metric_name,
                        point.entity_key,
                        point.event_time.isoformat(),
                    )
                    if key in seen:
                        continue
                    seen.add(key)
                    points.append(point)
                continue
            storage_uri = self._str(input_ref.get("storage_uri"))
            if storage_uri:
                candidate_paths.append(self._resolve_artifact_path(storage_uri))
        for path in candidate_paths:
            if not path.exists():
                continue
            rows = self._load(path).get("rows", [])
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                try:
                    point = NormalizedSeriesPoint.model_validate(row)
                except Exception:  # noqa: BLE001
                    continue
                key = (
                    point.vendor,
                    point.metric_name,
                    point.entity_key,
                    point.event_time.isoformat(),
                )
                if key in seen:
                    continue
                seen.add(key)
                points.append(point)
        points.sort(key=lambda item: (item.event_time, item.vendor, item.metric_name))
        return points

    def _dataset_nlp_sample_feature_preview(self, payload: dict[str, Any]) -> dict[str, float | None]:
        samples = self._load_dataset_samples(payload)
        if not samples:
            return {}
        preview: dict[str, float | None] = {}
        for feature_name, value in samples[0].features.items():
            if feature_name.startswith(("sentiment_", "text_", "news_", "social_")):
                preview[feature_name] = float(value) if value is not None else None
            if len(preview) >= 12:
                break
        return preview

    def _load_json_list(self, value: object) -> list[Any]:
        if not isinstance(value, str) or not value:
            return []
        try:
            payload = json.loads(value)
        except json.JSONDecodeError:
            return []
        return payload if isinstance(payload, list) else []

    def _dataset_bars_rows(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        input_refs = self._dataset_input_refs(payload)
        explicit_market_refs = [
            input_ref
            for input_ref in input_refs
            if self._input_ref_domain(input_ref) == "market"
        ]
        candidate_refs = explicit_market_refs or [
            input_ref
            for input_ref in input_refs
            if self._input_ref_domain(input_ref) != "sentiment_events"
        ]
        for input_ref in candidate_refs:
            storage_uri = self._str(input_ref.get("storage_uri"))
            if not storage_uri:
                continue
            loaded_rows = self._load(self._resolve_artifact_path(storage_uri)).get("rows", [])
            if isinstance(loaded_rows, list):
                rows.extend(
                    item
                    for item in loaded_rows
                    if isinstance(item, dict)
                    and {"event_time", "available_time", "symbol", "open", "high", "low", "close", "volume"}.issubset(item)
                )
        return rows

    def _normalized_market_bars_from_payload(self, payload: dict[str, Any]) -> list[NormalizedMarketBar]:
        bars: list[NormalizedMarketBar] = []
        for row in self._dataset_bars_rows(payload):
            if not {"event_time", "available_time", "symbol", "open", "high", "low", "close", "volume"}.issubset(row):
                continue
            try:
                bars.append(NormalizedMarketBar.model_validate(row))
            except Exception:  # noqa: BLE001
                continue
        bars.sort(key=lambda item: (item.event_time, item.symbol))
        return bars

    def _dataset_raw_row_count(self, payload: dict[str, Any]) -> int | None:
        rows = self._dataset_bars_rows(payload)
        if rows:
            return len(rows)
        feature_rows = self._dataset_feature_rows(payload)
        return len(feature_rows) if feature_rows else None

    def _dataset_summary(self, payload: dict[str, Any]) -> DatasetSummaryView:
        input_refs = self._dataset_input_refs(payload)
        manifest = self._dataset_manifest(payload)
        as_of_time = self._dt((payload.get("feature_view_ref") or {}).get("as_of_time"))
        display_meta = self._dataset_display_meta(payload)
        feature_schema = self._feature_schema(payload)
        label_columns = self._label_columns(payload)
        sample_count = self._int_or_none(manifest.get("usable_sample_count")) or self._dataset_sample_count(payload)
        row_count = self._int_or_none(manifest.get("raw_row_count")) or self._dataset_raw_row_count(payload) or sample_count
        starts = [
            self._dt(ref.get("time_range", {}).get("start"))
            for ref in input_refs
            if isinstance(ref.get("time_range"), dict)
        ]
        ends = [
            self._dt(ref.get("time_range", {}).get("end"))
            for ref in input_refs
            if isinstance(ref.get("time_range"), dict)
        ]
        data_start = min((item for item in starts if item is not None), default=None)
        data_end = max((item for item in ends if item is not None), default=None)
        symbols_preview = list(
            dict.fromkeys(
                [
                    value
                    for value in (
                        self._str(ref.get("symbol")) or self._asset_label(self._str(ref.get("asset_id")))
                        for ref in input_refs
                    )
                    if value
                ]
            )
        )
        frequencies = list(dict.fromkeys([value for value in (self._str(ref.get("frequency")) for ref in input_refs) if value]))
        sources = list(dict.fromkeys([value for value in (self._str(ref.get("source")) for ref in input_refs) if value]))
        venues = list(dict.fromkeys([value for value in (self._str(ref.get("venue")) for ref in input_refs) if value]))
        entity_scope = (
            self._str(payload.get("entity_scope"))
            or self._str(manifest.get("entity_scope"))
            or self._str((payload.get("sample_policy") or {}).get("universe"))
            or ("multi_asset" if len(symbols_preview) > 1 else "single_asset")
        )
        entity_count = (
            self._int_or_none(payload.get("entity_count"))
            or self._int_or_none(manifest.get("entity_count"))
            or max(len(symbols_preview), 1)
        )
        acquisition_profile = dict(manifest.get("acquisition_profile") or {})
        data_domains = self._resolved_data_domains(acquisition_profile)
        source_vendor = self._canonical_sentiment_vendor(
            self._str(acquisition_profile.get("source_vendor")) or (sources[0] if len(sources) == 1 else None)
        )
        exchange = self._str(acquisition_profile.get("exchange")) or (venues[0] if len(venues) == 1 else None)
        frequency = frequencies[0] if len(frequencies) == 1 else (frequencies[0] if frequencies else None)
        asset_ids = [self._str(ref.get("asset_id")) for ref in input_refs if self._str(ref.get("asset_id"))]
        dataset_id = str(payload.get("dataset_id", "unknown"))
        quality_status = self._str(manifest.get("quality_status"))
        readiness_status = self._str(payload.get("readiness_status")) or self._str(manifest.get("readiness_status"))
        build_status = self._str(manifest.get("build_status"))
        request_origin = self._str(acquisition_profile.get("request_origin")) or next(
            (self._str(ref.get("request_origin")) for ref in input_refs if self._str(ref.get("request_origin"))),
            None,
        )
        protection = self._dataset_protection_meta(dataset_id, payload)
        return DatasetSummaryView(
            dataset_id=dataset_id,
            display_name=display_meta["display_name"],
            subtitle=display_meta["subtitle"],
            dataset_category=display_meta["dataset_category"],
            data_domain=self._str(acquisition_profile.get("data_domain")) or "market",
            data_domains=data_domains,
            dataset_type=self._resolved_dataset_type(payload, manifest),
            asset_id=(asset_ids[0] if len(asset_ids) == 1 else None),
            data_source=source_vendor or (sources[0] if len(sources) == 1 else None),
            frequency=frequency,
            as_of_time=as_of_time,
            sample_count=sample_count,
            row_count=row_count,
            feature_count=len(feature_schema),
            label_count=len(label_columns),
            label_horizon=int((payload.get("label_spec") or {}).get("horizon", 0) or 0),
            split_strategy=self._str((payload.get("split_manifest") or {}).get("strategy")),
            time_range_label=self._time_range_label(data_start, data_end, frequency),
            is_smoke="smoke" in dataset_id,
            freshness=DatasetFreshnessView(
                as_of_time=as_of_time,
                data_start_time=data_start,
                data_end_time=data_end,
                lag_seconds=self._lag_seconds(as_of_time, data_end),
                status=self._freshness_status(as_of_time, data_end),
                summary=self._freshness_summary(as_of_time, data_end),
            ),
            temporal_safety_summary=self._temporal_safety_summary(payload),
            source_vendor=source_vendor,
            exchange=exchange,
            entity_scope=entity_scope,
            entity_count=entity_count,
            symbols_preview=symbols_preview[:6],
            snapshot_version=self._str(manifest.get("snapshot_version")),
            quality_status=quality_status,
            readiness_status=readiness_status,
            build_status=build_status,
            request_origin=request_origin,
            is_system_recommended=protection["is_system_recommended"],
            is_protected=protection["is_protected"],
            deletion_policy=protection["deletion_policy"],
            download_available=protection["download_available"],
            links=[
                DeepLinkView(
                    kind="dataset_detail",
                    label=display_meta["display_name"],
                    href=f"/datasets/{dataset_id}",
                    api_path=f"/api/datasets/{dataset_id}",
                )
            ],
        )

    def _experiment_item(self, run_id: str) -> ExperimentListItem:
        detail = self.get_run_detail(run_id)
        if detail is None:
            return ExperimentListItem(run_id=run_id, model_name=run_id, status="unknown")
        mae = detail.metrics.get("mae")
        return ExperimentListItem(
            run_id=detail.run_id,
            model_name=detail.model_name,
            dataset_id=detail.dataset_id,
            dataset_ids=detail.dataset_ids,
            datasets=detail.datasets,
            primary_dataset_id=detail.primary_dataset_id,
            composition=detail.composition,
            family=detail.family,
            backend=detail.backend,
            status=detail.status,
            created_at=detail.created_at,
            primary_metric_name=("mae" if mae is not None else None),
            primary_metric_value=mae,
            metrics=detail.metrics,
            backtest_count=len(detail.related_backtests),
            prediction_scopes=[p.scope for p in detail.predictions],
            tags={},
        )

    def _experiment_item_light(
        self,
        run_id: str,
        *,
        related_backtest_counts: dict[str, int] | None = None,
    ) -> ExperimentListItem:
        tracking = self.repository.read_json_if_exists(f"tracking/{run_id}.json") or {}
        manifest = (
            self.repository.read_json_if_exists(f"models/{run_id}/train_manifest.json")
            or self.repository.read_json_if_exists(f"models/{run_id}/manifest.json")
            or {}
        )
        metadata = self.repository.read_json_if_exists(f"models/{run_id}/metadata.json") or {}
        evaluation_summary = (
            self.repository.read_json_if_exists(f"models/{run_id}/evaluation_summary.json") or {}
        )
        artifact_format_status, _, prediction_scopes = self._run_artifact_status(run_id)
        model_name = str(
            (tracking.get("params") or {}).get("model_name") or metadata.get("model_name") or run_id
        )
        metrics = (
            self._metrics(evaluation_summary.get("regression_metrics") or {})
            or self._metrics(tracking.get("metrics") or {})
        )
        mae = metrics.get("mae")
        dataset_id = (
            self._str((tracking.get("params") or {}).get("dataset_id"))
            or self._str(manifest.get("dataset_id"))
        )
        dataset_ids = self._run_dataset_ids(
            dataset_id=dataset_id,
            manifest=manifest,
            metadata=metadata,
        )
        official_template_eligible, official_blocking_reasons = self._official_composition_status(
            manifest
        )
        return ExperimentListItem(
            run_id=run_id,
            model_name=model_name,
            dataset_id=dataset_id,
            dataset_ids=dataset_ids,
            datasets=self._dataset_links_from_ids(dataset_ids),
            primary_dataset_id=dataset_id,
            composition=self._run_composition(manifest),
            family=self.model_families.get(model_name) or self._str(metadata.get("model_family")),
            backend=self._str(metadata.get("backend")) or self._backend(model_name),
            status=(
                "success"
                if artifact_format_status == "complete"
                else ("partial" if tracking or manifest else "legacy")
            ),
            created_at=self._dt(tracking.get("created_at")) or self._dt(manifest.get("created_at")),
            primary_metric_name=("mae" if mae is not None else None),
            primary_metric_value=mae,
            metrics=metrics,
            backtest_count=(related_backtest_counts or {}).get(run_id, 0),
            prediction_scopes=prediction_scopes,
            official_template_eligible=official_template_eligible,
            official_blocking_reasons=official_blocking_reasons,
            tags={},
        )

    def _prediction_artifacts(
        self,
        run_id: str,
        *,
        evaluation_summary: dict[str, Any],
        prediction_scopes: list[str],
    ) -> list[PredictionArtifactView]:
        coverage = evaluation_summary.get("coverage", {})
        partition_counts = coverage.get("partition_sample_count", {})
        predictions: list[PredictionArtifactView] = []
        for path in self.repository.list_paths(f"predictions/{run_id}/*.json"):
            sample_count = 0
            if isinstance(partition_counts, dict):
                sample_count = self._int_or_none(partition_counts.get(path.stem)) or 0
            if path.stem == "full" and sample_count <= 0:
                sample_count = (
                    self._int_or_none(evaluation_summary.get("sample_count"))
                    or self._int_or_none(coverage.get("sample_count"))
                    or 0
                )
            predictions.append(
                PredictionArtifactView(
                    scope=path.stem,
                    sample_count=sample_count,
                    uri=self.repository.display_uri(path),
                )
            )
        if predictions:
            return predictions
        return [
            PredictionArtifactView(
                scope=scope,
                sample_count=0,
                uri=self.repository.display_uri(
                    self.repository.artifact_root / "predictions" / run_id / f"{scope}.json"
                ),
            )
            for scope in prediction_scopes
        ]

    def _builtin_templates(self) -> list[ModelTemplateView]:
        now = datetime.now(UTC)
        return [
            ModelTemplateView(
                template_id=f"registry::{name}",
                name=f"{name} default",
                model_name=name,
                description="Template sourced from model registry.",
                source="registry",
                hyperparams=dict(self._entry(entry, "default_hyperparams", {})),
                trainer_preset="fast",
                dataset_preset="smoke",
                read_only=True,
                model_registered=True,
                created_at=now,
                updated_at=now,
            )
            for name, entry in sorted(self.model_registry_entries.items())
            if bool(self._entry(entry, "enabled", True))
            and not self._is_multimodal_reference_model(name)
        ]

    def _custom_templates(self) -> list[ModelTemplateView]:
        items: list[ModelTemplateView] = []
        for path in self.templates_root.glob("*.json"):
            try:
                items.append(ModelTemplateView.model_validate_json(path.read_text(encoding="utf-8")))
            except Exception:  # noqa: BLE001
                continue
        return items

    def _registry_models(self) -> set[str]:
        return {
            name
            for name, entry in self.model_registry_entries.items()
            if bool(self._entry(entry, "enabled", True))
            and not self._is_multimodal_reference_model(name)
        }

    def _entry(self, entry: Any, key: str, default: Any) -> Any:
        return entry.get(key, default) if isinstance(entry, dict) else getattr(entry, key, default)

    def _review_unavailable(self) -> ReviewSummaryView:
        return ReviewSummaryView(
            status="unavailable",
            title="Review unavailable",
            summary="No review artifact found.",
            suggested_actions=[],
            proposed_actions=[],
        )

    def _glossary(self, keys: list[str]) -> list[GlossaryHintView]:
        dictionary = {
            "mae": ("MAE", "Average absolute prediction error."),
            "prediction_scope": ("预测范围", "说明预测结果覆盖全量样本还是测试切片。"),
            "benchmark": ("基准测试", "在同一规则下横向评估多个模型。"),
            "consistency_check": ("一致性检查", "检查研究引擎和模拟引擎结果是否一致。"),
            "max_drawdown": ("最大回撤", "累计收益从高点回落到低点的最大跌幅。"),
            "as_of_time": ("as_of_time", "这份数据在什么可用时点被截面固定下来。"),
            "freshness": ("新鲜度", "数据距离最新可用市场状态有多近。"),
            "label_horizon": ("标签窗口", "每个样本要预测未来多少个 bar。"),
            "split_strategy": ("切分方式", "训练、验证、测试样本是按什么规则拆分的。"),
            "sample_policy": ("样本策略", "哪些样本能进入训练，以及缺失标签如何处理。"),
            "temporal_safety": ("时间安全", "是否严格只使用当时能观测到的信息。"),
            "missing_ratio": ("缺失率", "关键字段中缺失值占全部样本的比例。"),
            "duplicate_rows": ("重复率", "重复时间点或重复样本写入的情况。"),
            "feature_dimensions": ("特征维度", "模型可用输入特征的数量和类别。"),
            "label_columns": ("标签列", "训练目标字段，也就是模型要学会预测的值。"),
            "data_coverage": ("数据覆盖范围", "数据覆盖的资产、周期和时间范围。"),
        }
        return [
            GlossaryHintView(key=key, term=dictionary[key][0], short=dictionary[key][1])
            for key in keys
            if key in dictionary
        ]

    def _dataset_display_meta(self, payload: dict[str, Any]) -> dict[str, str]:
        dataset_id = str(payload.get("dataset_id", "unknown_dataset"))
        input_refs = self._dataset_input_refs(payload)
        manifest = self._dataset_manifest(payload)
        acquisition_profile = dict(manifest.get("acquisition_profile") or {})
        frequency_candidates = [
            self._str(ref.get("frequency")) for ref in input_refs if self._str(ref.get("frequency"))
        ]
        symbols = [
            self._str(ref.get("symbol")) or self._asset_label(self._str(ref.get("asset_id")))
            for ref in input_refs
        ]
        symbols = [value for value in symbols if value]
        source_candidates = [
            self._str(ref.get("source")) for ref in input_refs if self._str(ref.get("source"))
        ]
        frequency = frequency_candidates[0] if frequency_candidates else "unknown"
        source_vendor = self._canonical_sentiment_vendor(
            self._str(acquisition_profile.get("source_vendor")) or (
                source_candidates[0] if source_candidates else "unknown_source"
            )
        )
        entity_scope = self._str(manifest.get("entity_scope")) or (
            "multi_asset" if len(set(symbols)) > 1 else "single_asset"
        )
        snapshot_version = self._str(manifest.get("snapshot_version"))
        known = {
            "smoke_dataset": {
                "display_name": "烟雾测试数据集 / Smoke Dataset",
                "subtitle": "最小可运行样例，用于快速验证训练与回测链路。",
                "dataset_category": "演示与联调",
            },
            "baseline_real_benchmark_dataset": {
                "display_name": "BTC 1小时真实基准数据集 / Real Benchmark",
                "subtitle": "真实 Binance 行情驱动的 1 小时收益预测基准集。",
                "dataset_category": "真实研究基准",
            },
            "baseline_reference_benchmark_dataset": {
                "display_name": "BTC 1小时参考基准数据集 / Reference Benchmark",
                "subtitle": "参考样本版 benchmark，用于和真实链路做对照。",
                "dataset_category": "参考基准",
            },
            "baseline_benchmark_dataset": {
                "display_name": "BTC 1小时基准数据集 / Baseline Benchmark",
                "subtitle": "基础收益预测 benchmark，适合作为模型横向比较底板。",
                "dataset_category": "研究基准",
            },
        }.get(dataset_id)
        if known:
            return known
        dataset_type_key = self._resolved_dataset_type(payload, manifest)
        dataset_type = {
            "training_panel": "训练面板",
            "fusion_training_panel": "融合训练面板",
            "feature_snapshot": "特征快照",
            "display_slice": "展示切片",
        }.get(dataset_type_key, "展示切片")
        domain_label = {
            "market": "市场",
            "derivatives": "衍生品",
            "macro": "宏观",
            "on_chain": "链上",
            "sentiment_events": "情绪事件",
        }.get(str(acquisition_profile.get("data_domain") or "market"), "数据")
        scope_label = "多资产" if entity_scope == "multi_asset" else (symbols[0] if symbols else "单资产")
        version = snapshot_version or dataset_id
        base_name = f"{domain_label} / {source_vendor} / {dataset_type} / {version}"
        subtitle = f"{scope_label} · {self._frequency_label(frequency)} · 技术标识 {dataset_id}"
        category = dataset_type
        return {
            "display_name": base_name,
            "subtitle": subtitle,
            "dataset_category": category,
        }

    def _dataset_detail_meta(self, payload: dict[str, Any]) -> dict[str, Any]:
        summary = self._dataset_summary(payload)
        manifest = self._dataset_manifest(payload)
        label_spec = payload.get("label_spec") or {}
        feature_schema = self._feature_schema(payload)
        label_columns = self._label_columns(payload)
        feature_groups = self._feature_groups(feature_schema)
        symbols = summary.symbols_preview or ["目标市场"]
        symbol = "、".join(symbols[:3])
        freq_label = self._frequency_label(summary.frequency)
        time_label = summary.time_range_label or "时间范围待确认"
        task_kind = self._str(label_spec.get("kind")) or "预测"
        acquisition_profile = dict(manifest.get("acquisition_profile") or {})
        dataset_type = self._resolved_dataset_type(payload, manifest)
        if dataset_type == "fusion_training_panel":
            fusion_domains = acquisition_profile.get("fusion_domains")
            fusion_label = (
                " / ".join(str(item) for item in fusion_domains)
                if isinstance(fusion_domains, list) and fusion_domains
                else "market / macro / on_chain"
            )
            intended_use = (
                f"适合把 {symbol} 的 {freq_label} 市场样本与 {fusion_label} 额外信号对齐后，用作跨域训练与 readiness 验证。"
            )
        else:
            intended_use = (
                f"适合用来做 {symbol} 的 {freq_label} 频率 {task_kind} 训练、walk-forward 基准比较和回测前置数据准备。"
            )
        risk_note = (
            "如果时间覆盖过短、缺失率偏高，或者标签窗口和你的交易周期不匹配，模型结论会明显失真。"
        )
        summary_text = (
            f"这是一份围绕 {symbol} 构建的 {freq_label} 频率数据集，"
            f"当前覆盖 {time_label}，包含 {summary.feature_count or 0} 个特征维度和 {summary.label_count or 0} 个标签列。"
        )
        acquisition_profile = {
            "source_vendor": summary.source_vendor,
            "exchange": summary.exchange,
            "request_origin": summary.request_origin,
            **acquisition_profile,
        }
        build_profile = {
            "build_status": self._str(manifest.get("build_status")) or "success",
            "snapshot_version": summary.snapshot_version,
            "entity_scope": summary.entity_scope,
            "entity_count": summary.entity_count,
            "raw_row_count": self._int_or_none(manifest.get("raw_row_count")) or self._dataset_raw_row_count(payload),
            "usable_sample_count": self._int_or_none(manifest.get("usable_sample_count")) or self._dataset_sample_count(payload),
            "dropped_rows": self._int_or_none(manifest.get("dropped_rows")),
            "input_asset_ids": list(manifest.get("input_asset_ids") or []),
            "build_config": dict(manifest.get("build_config") or {}),
        }
        schema_profile = {
            "feature_count": len(feature_schema),
            "feature_schema_hash": self._str(payload.get("feature_schema_hash")) or self._str(manifest.get("feature_schema_hash")),
            "label_columns": label_columns,
            "label_schema_hash": self._str(payload.get("label_schema_hash")) or self._str(manifest.get("label_schema_hash")),
            "feature_dimension_consistent": bool(feature_schema),
            "missing_feature_policy": dict((manifest.get("build_config") or {}).get("missing_feature_policy") or {}),
        }
        readiness_profile = {
            "readiness_status": self._str(payload.get("readiness_status")) or self._str(manifest.get("readiness_status")) or "unknown",
            "quality_status": self._str(manifest.get("quality_status")) or "unknown",
            "freshness_status": self._str(manifest.get("freshness_status")) or summary.freshness.status,
            "temporal_safety_status": self._str(manifest.get("temporal_safety_status")) or "unknown",
            "alignment_status": self._str(manifest.get("alignment_status")) or "unknown",
            "label_alignment_status": self._str(manifest.get("label_alignment_status")) or "unknown",
        }
        training_profile = {
            "sample_policy": dict(payload.get("sample_policy") or {}),
            "split_manifest": dict(payload.get("split_manifest") or {}),
            "entity_scope": summary.entity_scope,
            "entity_count": summary.entity_count,
            "symbols_preview": summary.symbols_preview,
            "label_horizon": summary.label_horizon,
            "recommended_training_use": (payload.get("sample_policy") or {}).get("recommended_training_use"),
        }
        return {
            "summary": summary_text,
            "intended_use": intended_use,
            "risk_note": risk_note,
            "feature_columns_preview": [item.get("name", "") for item in feature_schema[:8] if item.get("name")],
            "label_columns": label_columns,
            "feature_groups": feature_groups,
            "acquisition_profile": acquisition_profile,
            "build_profile": build_profile,
            "schema_profile": schema_profile,
            "readiness_profile": readiness_profile,
            "training_profile": training_profile,
        }

    def _dataset_quality_summary(self, payload: dict[str, Any]) -> DatasetQualitySummaryView:
        manifest = self._dataset_manifest(payload)
        acquisition_profile = dict(manifest.get("acquisition_profile") or {})
        missing_ratio = 0.0
        duplicate_rows = 0
        duplicate_ratio = 0.0
        checks: list[str] = []
        dataset_type = self._resolved_dataset_type(payload, manifest)
        is_multi_domain = len(self._resolved_data_domains(acquisition_profile)) > 1
        is_sentiment_only = (
            self._str(acquisition_profile.get("data_domain")) == "sentiment_events"
            and not is_multi_domain
        )
        if dataset_type == "fusion_training_panel" or is_multi_domain:
            samples = self._load_dataset_samples(payload)
            if samples:
                total_cells = 0
                missing_cells = 0
                seen_keys: set[tuple[str, str]] = set()
                duplicates = 0
                for sample in samples:
                    total_cells += len(sample.features)
                    missing_cells += sum(
                        1
                        for value in sample.features.values()
                        if value is None or value == ""
                    )
                    key = (sample.entity_key, sample.timestamp.isoformat())
                    if key in seen_keys:
                        duplicates += 1
                    else:
                        seen_keys.add(key)
                missing_ratio = (missing_cells / total_cells) if total_cells else 0.0
                duplicate_rows = duplicates
                duplicate_ratio = (duplicates / len(samples)) if samples else 0.0
                checks.append("已基于融合训练样本统计缺失特征和重复键情况。")
            else:
                checks.append("暂未找到物化后的融合训练样本，质量指标使用保守空值。")
        elif is_sentiment_only:
            feature_rows = self._dataset_feature_rows(payload)
            if feature_rows:
                total_cells = 0
                missing_cells = 0
                seen_keys: set[tuple[str, str]] = set()
                duplicates = 0
                for row in feature_rows:
                    values = row.get("values")
                    if not isinstance(values, dict):
                        continue
                    total_cells += len(values)
                    missing_cells += sum(
                        1 for value in values.values() if value is None or value == ""
                    )
                    entity_key = str(row.get("entity_key") or "")
                    timestamp = str(row.get("timestamp") or row.get("event_time") or "")
                    key = (entity_key, timestamp)
                    if key in seen_keys:
                        duplicates += 1
                    else:
                        seen_keys.add(key)
                missing_ratio = (missing_cells / total_cells) if total_cells else 0.0
                duplicate_rows = duplicates
                duplicate_ratio = (duplicates / len(feature_rows)) if feature_rows else 0.0
                checks.append("已基于情绪特征行快照统计缺失特征和重复键情况。")
            else:
                checks.append("暂未找到情绪特征快照，质量指标使用保守空值。")
        else:
            rows = self._dataset_bars_rows(payload)
            if isinstance(rows, list) and rows:
                total_cells = 0
                missing_cells = 0
                seen_keys: set[tuple[str, str]] = set()
                duplicates = 0
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    total_cells += len(row)
                    missing_cells += sum(
                        1 for value in row.values() if value is None or value == ""
                    )
                    event_time = str(row.get("event_time", ""))
                    symbol = str(row.get("symbol") or "")
                    key = (event_time, symbol)
                    if key in seen_keys:
                        duplicates += 1
                    else:
                        seen_keys.add(key)
                missing_ratio = (missing_cells / total_cells) if total_cells else 0.0
                duplicate_rows = duplicates
                duplicate_ratio = (duplicates / len(rows)) if rows else 0.0
                checks.append("已基于 OHLCV 原始记录统计缺失和重复情况。")
            else:
                checks.append("暂未找到可供统计的底层行数据，质量指标使用保守空值。")

        status = self._str(manifest.get("quality_status")) or "healthy"
        if missing_ratio > 0.05 or duplicate_ratio > 0.01:
            status = "warning"
        if missing_ratio > 0.15 or duplicate_ratio > 0.05:
            status = "risk"
        summary = "数据质量整体稳定，可直接进入研究使用。"
        if status == "warning":
            summary = "存在轻度质量风险，建议先检查缺失段和重复时点。"
        if status == "risk":
            summary = "质量风险较高，建议清洗后再用于训练或回测。"
        return DatasetQualitySummaryView(
            status=status,
            summary=summary,
            missing_ratio=missing_ratio,
            duplicate_ratio=duplicate_ratio,
            duplicate_rows=duplicate_rows,
            checks=checks,
        )

    def _feature_schema(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        feature_schema = (payload.get("feature_view_ref") or {}).get("feature_schema")
        return [item for item in feature_schema if isinstance(item, dict)] if isinstance(feature_schema, list) else []

    def _label_columns(self, payload: dict[str, Any]) -> list[str]:
        label_spec = payload.get("label_spec") or {}
        labels: list[str] = []
        target = label_spec.get("target_column")
        if isinstance(target, str) and target:
            labels.append(target)
        extra = label_spec.get("label_columns")
        if isinstance(extra, list):
            labels.extend(str(item) for item in extra if isinstance(item, str) and item)
        return list(dict.fromkeys(labels))

    def _feature_groups(self, feature_schema: list[dict[str, Any]]) -> list[DatasetFieldGroupView]:
        buckets: dict[str, list[str]] = {
            "price_action": [],
            "volume_liquidity": [],
            "volatility_range": [],
            "other": [],
        }
        for item in feature_schema:
            name = str(item.get("name", ""))
            lowered = name.lower()
            if any(token in lowered for token in ["return", "momentum", "close", "open"]):
                buckets["price_action"].append(name)
            elif any(token in lowered for token in ["volume", "turnover", "liquidity"]):
                buckets["volume_liquidity"].append(name)
            elif any(token in lowered for token in ["vol", "range", "drawdown"]):
                buckets["volatility_range"].append(name)
            else:
                buckets["other"].append(name)
        labels = {
            "price_action": ("价格行为特征", "帮助模型判断趋势、收益和方向变化。"),
            "volume_liquidity": ("成交与流动性特征", "帮助识别量能放大、拥挤交易和流动性状态。"),
            "volatility_range": ("波动与区间特征", "帮助刻画波动率、振幅和风险抬升。"),
            "other": ("其他特征", "补充上下文或暂未归类的工程字段。"),
        }
        return [
            DatasetFieldGroupView(
                key=key,
                label=labels[key][0],
                description=labels[key][1],
                count=len(columns),
                columns=columns[:6],
            )
            for key, columns in buckets.items()
            if columns
        ]

    def _frequency_label(self, frequency: str | None) -> str:
        mapping = {"1m": "1分钟", "5m": "5分钟", "1h": "1小时", "1d": "日线"}
        return mapping.get(frequency or "", frequency or "未知周期")

    def _asset_label(self, asset_id: str | None) -> str | None:
        if not asset_id:
            return None
        parts = asset_id.replace("-", "_").split("_")
        for part in parts:
            if part.endswith("USDT") or part.endswith("USD"):
                return part
        return asset_id

    def _time_range_label(
        self,
        start_time: datetime | None,
        end_time: datetime | None,
        frequency: str | None,
    ) -> str | None:
        if start_time is None and end_time is None:
            return None
        start_label = start_time.strftime("%Y-%m-%d") if start_time else "未知开始"
        end_label = end_time.strftime("%Y-%m-%d") if end_time else "未知结束"
        freq_label = self._frequency_label(frequency)
        return f"{start_label} 至 {end_label} · {freq_label}"

    def _lag_seconds(self, as_of_time: datetime | None, data_end_time: datetime | None) -> float | None:
        if as_of_time is None or data_end_time is None:
            return None
        return max((as_of_time - data_end_time).total_seconds(), 0.0)

    def _freshness_status(self, as_of_time: datetime | None, data_end_time: datetime | None) -> str:
        lag_seconds = self._lag_seconds(as_of_time, data_end_time)
        if lag_seconds is None:
            return "unknown"
        if lag_seconds <= 3600:
            return "fresh"
        if lag_seconds <= 86400:
            return "stale"
        return "outdated"

    def _freshness_summary(self, as_of_time: datetime | None, data_end_time: datetime | None) -> str:
        status = self._freshness_status(as_of_time, data_end_time)
        mapping = {
            "fresh": "数据时间与最新可用截面基本对齐。",
            "stale": "数据存在轻微滞后，适合研究但应关注时效性。",
            "outdated": "数据已明显过时，结论可能无法反映当前市场。",
            "unknown": "缺少足够时间信息，暂时无法判断新鲜度。",
        }
        return mapping[status]

    def _temporal_safety_summary(self, payload: dict[str, Any]) -> str:
        as_of_time = self._dt((payload.get("feature_view_ref") or {}).get("as_of_time"))
        end_candidates = [
            self._dt(ref.get("time_range", {}).get("end"))
            for ref in self._dataset_input_refs(payload)
            if isinstance(ref.get("time_range"), dict)
        ]
        end_time = max((item for item in end_candidates if item is not None), default=None)
        if as_of_time and end_time and as_of_time >= end_time:
            return "以 as_of_time 固定观测边界，当前看起来没有明显前视泄漏。"
        return "需要结合 available_time 和切分边界继续确认时间安全性。"

    def _artifacts(self, pairs: list[tuple[str, Path]]) -> list[ArtifactView]:
        return [
            ArtifactView(
                kind=kind,
                label=kind.replace("_", " "),
                uri=self.repository.display_uri(path),
                exists=True,
                previewable=path.suffix.lower() in {".json", ".md", ".txt", ".csv"},
            )
            for kind, path in pairs
            if path.exists()
        ]

    def _run_dataset_summary(
        self,
        *,
        dataset_id: str | None,
        manifest: dict[str, Any],
    ) -> dict[str, Any]:
        if not dataset_id:
            return {}
        dataset_summary = {
            "dataset_type": self._str(manifest.get("dataset_type")),
            "data_domain": self._str(manifest.get("data_domain")),
            "data_domains": manifest.get("data_domains")
            if isinstance(manifest.get("data_domains"), list)
            else [],
            "entity_scope": self._str(manifest.get("entity_scope")),
            "entity_count": self._int_or_none(manifest.get("entity_count")),
            "feature_schema_hash": self._str(manifest.get("feature_schema_hash")),
            "snapshot_version": self._str(manifest.get("snapshot_version")),
            "readiness_status": self._str(manifest.get("dataset_readiness_status")),
        }
        entry = self._dataset_entry(dataset_id)
        if entry is None:
            return dataset_summary
        payload = entry.payload
        registry_manifest = entry.manifest or self._dataset_manifest(payload)
        display_meta = self._dataset_display_meta(payload)
        acquisition_profile = dict(registry_manifest.get("acquisition_profile") or {})
        dataset_summary.update(
            {
                "dataset_type": entry.dataset_type or dataset_summary["dataset_type"],
                "data_domain": entry.data_domain or dataset_summary["data_domain"],
                "data_domains": self._resolved_data_domains(acquisition_profile)
                or dataset_summary["data_domains"],
                "entity_scope": entry.entity_scope or dataset_summary["entity_scope"],
                "entity_count": entry.entity_count or dataset_summary["entity_count"],
                "feature_schema_hash": self._str(payload.get("feature_schema_hash"))
                or self._str(registry_manifest.get("feature_schema_hash"))
                or dataset_summary["feature_schema_hash"],
                "snapshot_version": entry.snapshot_version or dataset_summary["snapshot_version"],
                "readiness_status": entry.readiness_status or dataset_summary["readiness_status"],
                "dataset_category": display_meta.get("dataset_category"),
                "sample_count": entry.usable_row_count,
                "feature_count": entry.feature_count,
                "label_count": entry.label_count,
                "data_start_time": entry.data_start_time,
                "data_end_time": entry.data_end_time,
            }
        )
        return dataset_summary

    def _run_time_range(
        self,
        evaluation_summary: dict[str, Any],
        dataset_summary: dict[str, Any],
    ) -> dict[str, Any]:
        coverage = evaluation_summary.get("coverage") if isinstance(evaluation_summary, dict) else {}
        if isinstance(coverage, dict):
            start_time = self._str(coverage.get("start_time"))
            end_time = self._str(coverage.get("end_time"))
            if start_time or end_time:
                return {
                    "start_time": start_time,
                    "end_time": end_time,
                    "selected_scope": self._str(evaluation_summary.get("selected_scope")),
                }
        return {
            "start_time": self._str(dataset_summary.get("data_start_time")),
            "end_time": self._str(dataset_summary.get("data_end_time")),
            "selected_scope": self._str(evaluation_summary.get("selected_scope"))
            if isinstance(evaluation_summary, dict)
            else None,
        }

    def _load(self, path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            return {}

    def _metrics(self, value: Any) -> dict[str, float]:
        if not isinstance(value, dict):
            return {}
        return {str(k): float(v) for k, v in value.items() if isinstance(v, (int, float))}

    def _dt(self, value: Any) -> datetime | None:
        if not isinstance(value, str) or not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None

    def _str(self, value: Any) -> str | None:
        return str(value) if isinstance(value, str) and value else None

    def _int_or_none(self, value: Any) -> int | None:
        return int(value) if isinstance(value, (int, float)) else None

    def _float(self, value: Any) -> float | None:
        return float(value) if isinstance(value, (int, float)) else None

    def _backend(self, model_name: str) -> str | None:
        if model_name in {"mlp", "gru"}:
            return "torch"
        if model_name in {"elastic_net", "lightgbm", "mean_baseline"}:
            return "native"
        return None
