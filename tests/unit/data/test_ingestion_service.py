from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from quant_platform.common.types.core import TimeRange
from quant_platform.data.connectors.macro import FredSeriesConnector
from quant_platform.data.connectors.market import BinanceSpotKlinesConnector
from quant_platform.data.connectors.on_chain import DefiLlamaConnector
from quant_platform.data.connectors.sentiment import (
    GdeltSentimentConnector,
    RedditArchiveSentimentConnector,
    SentimentEvent,
    _aggregate_events,
)
from quant_platform.data.contracts.ingestion import (
    ConnectorRegistration,
    DataConnector,
    DataConnectorError,
    IngestionCoverage,
    IngestionRequest,
    IngestionResult,
)
from quant_platform.data.contracts.market import NormalizedMarketBar
from quant_platform.data.ingestion.service import DomainIngestionCoordinator


class _FakeResponse:
    def __init__(self, payload: object) -> None:
        self.payload = payload

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
        return None

    def read(self) -> bytes:
        return json.dumps(self.payload).encode("utf-8")


class FakeMarketConnector(DataConnector):
    def __init__(self) -> None:
        self.registration = ConnectorRegistration(
            data_domain="market",
            vendor="fake_market",
            display_name="fake",
            status="active",
        )
        self.calls: list[tuple[datetime, datetime]] = []

    def ingest(self, request: IngestionRequest) -> IngestionResult:
        self.calls.append((request.time_range.start, request.time_range.end))
        rows: list[dict[str, Any]] = []
        cursor = request.time_range.start
        price = 100.0
        while cursor <= request.time_range.end:
            rows.append(
                NormalizedMarketBar(
                    event_time=cursor,
                    available_time=cursor,
                    symbol=request.identifiers[0],
                    venue="fake",
                    open=price,
                    high=price + 1.0,
                    low=price - 1.0,
                    close=price + 0.5,
                    volume=10.0,
                ).model_dump(mode="json")
            )
            cursor = cursor.replace(hour=cursor.hour + 1) if cursor.hour < 23 else cursor.replace(day=cursor.day + 1, hour=0)
            price += 1.0
        return IngestionResult(
            request_id=request.request_id,
            data_domain="market",
            vendor="fake_market",
            storage_uri="",
            normalized_uri="",
            coverage=IngestionCoverage(
                start_time=request.time_range.start,
                end_time=request.time_range.end,
                complete=True,
            ),
            metadata={"rows": rows},
        )


def test_binance_connector_parses_klines_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    connector = BinanceSpotKlinesConnector()
    payload = [
        [
            1704067200000,
            "100.0",
            "102.0",
            "99.0",
            "101.5",
            "12.0",
            1704070799000,
        ]
    ]
    monkeypatch.setattr(connector, "_request_json", lambda url: payload)

    result = connector.ingest(
        IngestionRequest(
            data_domain="market",
            vendor="binance",
            request_id="binance-test",
            time_range=TimeRange(
                start=datetime(2024, 1, 1, tzinfo=UTC),
                end=datetime(2024, 1, 1, 1, tzinfo=UTC),
            ),
            identifiers=["BTCUSDT"],
            frequency="1h",
        )
    )

    assert result.coverage.complete is True
    assert result.metadata["rows"][0]["symbol"] == "BTCUSDT"
    assert result.metadata["rows"][0]["close"] == 101.5


def test_fred_connector_requires_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    connector = FredSeriesConnector()
    monkeypatch.delenv("FRED_API_KEY", raising=False)

    with pytest.raises(DataConnectorError) as exc_info:
        connector.ingest(
            IngestionRequest(
                data_domain="macro",
                vendor="fred",
                request_id="fred-test",
                time_range=TimeRange(
                    start=datetime(2024, 1, 1, tzinfo=UTC),
                    end=datetime(2024, 1, 31, tzinfo=UTC),
                ),
                identifiers=["DFF"],
                frequency="1d",
            )
        )
    assert exc_info.value.code == "credentials_missing"
    assert exc_info.value.vendor == "fred"


def test_defillama_connector_parses_points(monkeypatch: pytest.MonkeyPatch) -> None:
    connector = DefiLlamaConnector()
    payload = [{"date": 1704067200, "tvl": 123.4}, {"date": 1704153600, "tvl": 130.2}]
    monkeypatch.setattr(
        "quant_platform.data.connectors.on_chain.urlopen",
        lambda *args, **kwargs: _FakeResponse(payload),
    )

    result = connector.ingest(
        IngestionRequest(
            data_domain="on_chain",
            vendor="defillama",
            request_id="llama-test",
            time_range=TimeRange(
                start=datetime(2024, 1, 1, tzinfo=UTC),
                end=datetime(2024, 1, 3, tzinfo=UTC),
            ),
            identifiers=["ethereum"],
            frequency="1d",
        )
    )

    assert result.coverage.complete is True
    assert result.metadata["rows"][0]["series_key"] == "ethereum"
    assert result.metadata["rows"][0]["value"] == 123.4


def test_ingestion_service_uses_cache_and_incremental_gap_fetch(tmp_path: Path) -> None:
    service = DomainIngestionCoordinator(tmp_path / "artifacts", register_defaults=False)
    connector = FakeMarketConnector()
    service.register(connector)

    start = datetime(2024, 1, 1, tzinfo=UTC)
    first_end = datetime(2024, 1, 1, 3, tzinfo=UTC)
    second_end = datetime(2024, 1, 1, 5, tzinfo=UTC)

    rows, status = service.fetch_market_bars(
        symbol="BTCUSDT",
        vendor="fake_market",
        exchange="fake",
        frequency="1h",
        start_time=start,
        end_time=first_end,
    )
    assert status == "live_fetch"
    assert len(rows) == 4
    assert len(connector.calls) == 1

    rows, status = service.fetch_market_bars(
        symbol="BTCUSDT",
        vendor="fake_market",
        exchange="fake",
        frequency="1h",
        start_time=start,
        end_time=second_end,
    )
    assert status == "incremental_refresh"
    assert len(rows) == 6
    assert len(connector.calls) == 2
    assert connector.calls[1][0] == first_end

    rows, status = service.fetch_market_bars(
        symbol="BTCUSDT",
        vendor="fake_market",
        exchange="fake",
        frequency="1h",
        start_time=start,
        end_time=second_end,
    )
    assert status == "cache_hit"
    assert len(rows) == 6
    assert len(connector.calls) == 2


def test_ingestion_service_fails_without_fallback_on_missing_gap(tmp_path: Path) -> None:
    class FailingConnector(FakeMarketConnector):
        def ingest(self, request: IngestionRequest) -> IngestionResult:
            if self.calls:
                raise ValueError("upstream unavailable")
            return super().ingest(request)

    service = DomainIngestionCoordinator(tmp_path / "artifacts", register_defaults=False)
    connector = FailingConnector()
    service.register(connector)
    start = datetime(2024, 1, 1, tzinfo=UTC)

    service.fetch_market_bars(
        symbol="BTCUSDT",
        vendor="fake_market",
        exchange="fake",
        frequency="1h",
        start_time=start,
        end_time=datetime(2024, 1, 1, 2, tzinfo=UTC),
    )

    with pytest.raises(DataConnectorError) as exc_info:
        service.fetch_market_bars(
            symbol="BTCUSDT",
            vendor="fake_market",
            exchange="fake",
            frequency="1h",
            start_time=start,
            end_time=datetime(2024, 1, 1, 4, tzinfo=UTC),
        )
    assert exc_info.value.code in {"empty_result", "connector_ingest_failed"}
    assert exc_info.value.data_domain == "market"
    assert exc_info.value.vendor == "fake_market"


def test_ingestion_service_raises_explicit_error_for_unknown_connector(tmp_path: Path) -> None:
    service = DomainIngestionCoordinator(tmp_path / "artifacts", register_defaults=False)

    with pytest.raises(DataConnectorError) as exc_info:
        service.fetch_series_points(
            data_domain="macro",
            identifier="DFF",
            vendor="fred",
            frequency="1d",
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 5, tzinfo=UTC),
        )

    assert exc_info.value.code == "connector_not_registered"
    assert exc_info.value.identifier == "DFF"


def test_sentiment_aggregate_filters_by_available_time_window() -> None:
    request = IngestionRequest(
        data_domain="sentiment_events",
        vendor="gnews",
        request_id="sentiment-window",
        time_range=TimeRange(
            start=datetime(2024, 1, 1, 0, tzinfo=UTC),
            end=datetime(2024, 1, 1, 1, tzinfo=UTC),
        ),
        identifiers=["btc"],
        frequency="1h",
    )

    with pytest.raises(DataConnectorError, match="Connector returned no rows"):
        _aggregate_events(
            request=request,
            vendor="gnews",
            identifier="btc",
            source_type="news",
            events=[
                SentimentEvent(
                    event_id="late-article",
                    source_type="news",
                    source_name="late-source",
                    event_time=datetime(2024, 1, 1, 0, 30, tzinfo=UTC),
                    available_time=datetime(2024, 1, 1, 2, tzinfo=UTC),
                    title="late",
                    text_preview="published later",
                    url=None,
                    symbol="BTC",
                    raw_metadata={},
                )
            ],
            storage_uri="memory://sentiment",
            transport="test",
        )


def test_sentiment_aggregate_tracks_source_breakdown_counts() -> None:
    request = IngestionRequest(
        data_domain="sentiment_events",
        vendor="gnews",
        request_id="sentiment-breakdown",
        time_range=TimeRange(
            start=datetime(2024, 1, 1, 0, tzinfo=UTC),
            end=datetime(2024, 1, 1, 2, tzinfo=UTC),
        ),
        identifiers=["btc"],
        frequency="1h",
    )

    result = _aggregate_events(
        request=request,
        vendor="gnews",
        identifier="btc",
        source_type="news",
        events=[
            SentimentEvent(
                event_id="article-1",
                source_type="news",
                source_name="alpha",
                event_time=datetime(2024, 1, 1, 0, 5, tzinfo=UTC),
                available_time=datetime(2024, 1, 1, 0, 15, tzinfo=UTC),
                title="gain",
                text_preview="strong gain",
                url=None,
                symbol="BTC",
                raw_metadata={},
            ),
            SentimentEvent(
                event_id="article-2",
                source_type="news",
                source_name="alpha",
                event_time=datetime(2024, 1, 1, 0, 20, tzinfo=UTC),
                available_time=datetime(2024, 1, 1, 0, 40, tzinfo=UTC),
                title="rally",
                text_preview="positive rally",
                url=None,
                symbol="BTC",
                raw_metadata={},
            ),
            SentimentEvent(
                event_id="article-3",
                source_type="news",
                source_name="beta",
                event_time=datetime(2024, 1, 1, 0, 30, tzinfo=UTC),
                available_time=datetime(2024, 1, 1, 0, 45, tzinfo=UTC),
                title="slump",
                text_preview="negative slump",
                url=None,
                symbol="BTC",
                raw_metadata={},
            ),
        ],
        storage_uri="memory://sentiment",
        transport="test",
    )

    source_breakdown = json.loads(result.metadata["rows"][0]["dimensions"]["source_breakdown_json"])
    assert source_breakdown == [
        {"source": "alpha", "count": 2},
        {"source": "beta", "count": 1},
    ]


def test_gdelt_connector_materializes_hourly_timeline_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = GdeltSentimentConnector()

    def _fake_payload(*, identifier: str, url: str, mode: str) -> dict[str, object]:
        _ = (identifier, url)
        if mode == "TimelineVolRaw":
            return {
                "timeline": [
                    {
                        "data": [
                            {"date": "20240401T000000Z", "value": 4},
                            {"date": "20240401T010000Z", "value": 0},
                            {"date": "20240401T020000Z", "value": 2},
                        ]
                    }
                ]
            }
        return {
            "timeline": [
                {
                    "data": [
                        {"date": "20240401T000000Z", "value": 1.0},
                        {"date": "20240401T001500Z", "value": -1.0},
                        {"date": "20240401T020000Z", "value": 0.5},
                        {"date": "20240401T021500Z", "value": 0.75},
                    ]
                }
            ]
        }

    monkeypatch.setattr(connector, "_request_gdelt_payload", _fake_payload)

    result = connector.ingest(
        IngestionRequest(
            data_domain="sentiment_events",
            vendor="gdelt",
            request_id="gdelt-timeline-test",
            time_range=TimeRange(
                start=datetime(2024, 4, 1, 0, 0, tzinfo=UTC),
                end=datetime(2024, 4, 1, 2, 0, tzinfo=UTC),
            ),
            identifiers=["btc"],
            frequency="1h",
            options={"subreddits": ["bitcoin"]},
        )
    )

    rows = result.metadata["rows"]
    event_count_rows = [row for row in rows if row["metric_name"] == "event_count"]
    sentiment_rows = [row for row in rows if row["metric_name"] == "sentiment_score"]

    assert [row["event_time"] for row in event_count_rows] == [
        "2024-04-01T00:00:00Z",
        "2024-04-01T01:00:00Z",
        "2024-04-01T02:00:00Z",
    ]
    assert [row["value"] for row in event_count_rows] == [4.0, 0.0, 2.0]
    assert sentiment_rows[0]["value"] == pytest.approx(0.0)
    assert sentiment_rows[1]["value"] == 0.0
    assert sentiment_rows[2]["value"] == pytest.approx(0.625)
    assert event_count_rows[1]["dimensions"]["preview_events_json"] == "[]"


def test_reddit_archive_connector_aggregates_submission_archive(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    connector = RedditArchiveSentimentConnector(
        tmp_path / "artifacts",
        csv_path=tmp_path / "missing.csv",
    )

    batches = [
        [
            {
                "id": "abc",
                "created_utc": 1711929605,
                "title": "Bitcoin rally continues",
                "selftext": "Strong adoption momentum.",
                "subreddit": "bitcoin",
                "score": 10,
                "num_comments": 5,
                "permalink": "/r/Bitcoin/comments/abc/test/",
            },
            {
                "id": "def",
                "created_utc": 1711933205,
                "title": "Bitcoin slips",
                "selftext": "Short term risk rises.",
                "subreddit": "bitcoin",
                "score": 2,
                "num_comments": 1,
                "permalink": "/r/Bitcoin/comments/def/test/",
            },
        ],
        [],
    ]

    def _fake_batch(*, subreddit: str, after_epoch: int, before_epoch: int) -> list[dict[str, object]]:
        _ = (subreddit, after_epoch, before_epoch)
        return batches.pop(0)

    monkeypatch.setattr(connector, "_fetch_submission_batch", _fake_batch)

    result = connector.ingest(
        IngestionRequest(
            data_domain="sentiment_events",
            vendor="reddit_archive",
            request_id="reddit-pullpush-test",
            time_range=TimeRange(
                start=datetime(2024, 4, 1, 0, 0, tzinfo=UTC),
                end=datetime(2024, 4, 1, 2, 0, tzinfo=UTC),
            ),
            identifiers=["btc"],
            frequency="1h",
            options={"subreddits": ["bitcoin"]},
        )
    )

    rows = result.metadata["rows"]
    event_count_rows = [row for row in rows if row["metric_name"] == "event_count"]
    assert [row["value"] for row in event_count_rows] == [1.0, 1.0]
    assert event_count_rows[0]["vendor"] == "reddit_archive"


def test_reddit_archive_connector_backfills_when_csv_window_has_zero_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "empty_history.csv"
    csv_path.write_text("datetime,body,subreddit,author,score,controversiality\n", encoding="utf-8")
    connector = RedditArchiveSentimentConnector(
        tmp_path / "artifacts",
        csv_path=csv_path,
    )

    batches = [
        [
            {
                "id": "ghi",
                "created_utc": 1704067279,
                "title": "Bitcoin opens 2024 strong",
                "selftext": "Fresh inflows arrive.",
                "subreddit": "bitcoin",
                "score": 12,
                "num_comments": 6,
                "permalink": "/r/Bitcoin/comments/ghi/test/",
            }
        ],
        [],
    ]

    def _fake_batch(*, subreddit: str, after_epoch: int, before_epoch: int) -> list[dict[str, object]]:
        _ = (subreddit, after_epoch, before_epoch)
        return batches.pop(0)

    monkeypatch.setattr(connector, "_fetch_submission_batch", _fake_batch)

    result = connector.ingest(
        IngestionRequest(
            data_domain="sentiment_events",
            vendor="reddit_archive",
            request_id="reddit-archive-zero-csv-test",
            time_range=TimeRange(
                start=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
                end=datetime(2024, 1, 1, 1, 0, tzinfo=UTC),
            ),
            identifiers=["btc_news"],
            frequency="1h",
            options={"subreddits": ["bitcoin"]},
        )
    )

    rows = result.metadata["rows"]
    event_count_rows = [row for row in rows if row["metric_name"] == "event_count"]
    assert [row["value"] for row in event_count_rows] == [1.0]
    assert event_count_rows[0]["series_key"] == "BTC:reddit_archive:event_count"
