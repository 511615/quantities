from __future__ import annotations

import csv
import json
import math
import os
import re
import sqlite3
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from html import unescape
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote_plus
from urllib.request import Request, urlopen
from xml.etree import ElementTree

from quant_platform.data.contracts.ingestion import (
    ConnectorRegistration,
    DataConnector,
    DataConnectorError,
    IngestionCoverage,
    IngestionRequest,
    IngestionResult,
)
from quant_platform.data.contracts.series import NormalizedSeriesPoint

_WORD_RE = re.compile(r"[A-Za-z][A-Za-z0-9_'-]{1,}")
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_STOP_WORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "has",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "to",
    "was",
    "were",
    "will",
    "with",
    "after",
    "before",
    "into",
    "over",
    "under",
    "amid",
    "latest",
    "crypto",
    "price",
    "market",
    "markets",
    "articles",
    "blank",
    "com",
    "google",
    "href",
    "http",
    "https",
    "nbsp",
    "news",
    "rss",
    "target",
    "www",
}
_POSITIVE_WORDS = {
    "adoption",
    "beat",
    "bullish",
    "gain",
    "growth",
    "improve",
    "launch",
    "positive",
    "profit",
    "rally",
    "record",
    "recovery",
    "rise",
    "strong",
    "surge",
    "upgrade",
}
_NEGATIVE_WORDS = {
    "bearish",
    "crash",
    "decline",
    "drop",
    "fall",
    "hack",
    "lawsuit",
    "loss",
    "negative",
    "outage",
    "risk",
    "scam",
    "selloff",
    "slump",
    "weak",
}
_IDENTIFIER_QUERY_MAP = {
    "btc": "bitcoin OR btc",
    "btcusdt": "bitcoin OR btc",
    "bitcoin": "bitcoin OR btc",
    "eth": "ethereum OR eth",
    "ethusdt": "ethereum OR eth",
    "ethereum": "ethereum OR eth",
    "sol": "solana OR sol",
    "solusdt": "solana OR sol",
    "solana": "solana OR sol",
}

_GNEWS_RSS_MAX_LOOKBACK = timedelta(days=7)
_REDDIT_PUBLIC_MAX_LOOKBACK = timedelta(days=31)
_GDELT_CHUNK_DAYS = 30
_GDELT_MAX_RECORDS = 250
_GDELT_MIN_INTERVAL_SECONDS = 5.1
_CORE_SUBREDDITS = {
    "bitcoin",
    "btc",
    "bitcoinmarkets",
    "cryptocurrency",
    "bitcoinbeginners",
    "bitcoinmining",
}


@dataclass(frozen=True)
class SentimentEvent:
    event_id: str
    source_type: str
    source_name: str
    event_time: datetime
    available_time: datetime
    title: str
    text_preview: str
    url: str | None
    symbol: str
    raw_metadata: dict[str, object]


def _parse_dt(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _identifier_from_request(request: IngestionRequest) -> str:
    if request.identifiers:
        return str(request.identifiers[0]).strip()
    fallback = request.options.get("symbol") or request.options.get("identifier")
    if isinstance(fallback, str) and fallback.strip():
        return fallback.strip()
    raise DataConnectorError(
        data_domain="sentiment_events",
        vendor=request.vendor,
        message="sentiment source requires an identifier or symbol.",
        retryable=False,
        code="identifier_required",
    )


def _normalized_identifier(identifier: str) -> str:
    cleaned = identifier.strip().lower()
    if cleaned.endswith("_news"):
        cleaned = cleaned[:-5]
    return cleaned


def _symbol_from_identifier(identifier: str) -> str:
    normalized = _normalized_identifier(identifier)
    alias = {
        "btc": "BTC",
        "btcusdt": "BTC",
        "bitcoin": "BTC",
        "eth": "ETH",
        "ethusdt": "ETH",
        "ethereum": "ETH",
        "sol": "SOL",
        "solusdt": "SOL",
        "solana": "SOL",
    }.get(normalized)
    return alias or normalized.upper()


def _query_terms(identifier: str, request: IngestionRequest) -> str:
    explicit = request.options.get("query")
    if isinstance(explicit, str) and explicit.strip():
        return explicit.strip()
    normalized = _normalized_identifier(identifier)
    return _IDENTIFIER_QUERY_MAP.get(normalized, normalized.replace("_", " "))


def _rss_query_terms(identifier: str, request: IngestionRequest) -> str:
    return _query_terms(identifier, request).replace(" OR ", " ")


def _bucket_time(event_time: datetime, frequency: str) -> datetime:
    if frequency != "1h":
        raise ValueError(f"Unsupported sentiment frequency '{frequency}'.")
    return event_time.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


def _tokenize(text: str) -> list[str]:
    words = [match.group(0).lower() for match in _WORD_RE.finditer(text)]
    return [word for word in words if word not in _STOP_WORDS and len(word) > 2]


def _clean_text(text: str) -> str:
    if not text:
        return ""
    cleaned = unescape(_HTML_TAG_RE.sub(" ", text))
    return re.sub(r"\s+", " ", cleaned).strip()


def _sentiment_score(text: str) -> float:
    tokens = _tokenize(text)
    if not tokens:
        return 0.0
    positive = sum(1 for token in tokens if token in _POSITIVE_WORDS)
    negative = sum(1 for token in tokens if token in _NEGATIVE_WORDS)
    if positive == 0 and negative == 0:
        return 0.0
    return (positive - negative) / max(1, positive + negative)


def _top_terms(counts: dict[str, int], *, limit: int) -> list[dict[str, object]]:
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:limit]
    total = sum(count for _, count in ranked) or 1
    return [
        {"term": term, "count": count, "weight": round(count / total, 4)}
        for term, count in ranked
    ]


def _request_url(url: str, *, headers: dict[str, str] | None = None) -> object:
    request = Request(
        url,
        headers={
            "Accept": "application/json, application/rss+xml, application/xml;q=0.9, */*;q=0.8",
            "User-Agent": "quant-platform/0.1 (+https://localhost)",
            **(headers or {}),
        },
    )
    return urlopen(request, timeout=20)


def _window_span(request: IngestionRequest) -> timedelta:
    return request.time_range.end - request.time_range.start


def _gdelt_dt(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y%m%d%H%M%S")


def _aggregate_events(
    *,
    request: IngestionRequest,
    vendor: str,
    identifier: str,
    source_type: str,
    events: list[SentimentEvent],
    storage_uri: str,
    transport: str,
) -> IngestionResult:
    frequency = request.frequency or "1h"
    if frequency != "1h":
        raise DataConnectorError(
            data_domain="sentiment_events",
            vendor=vendor,
            identifier=identifier,
            message=f"{vendor} currently supports only 1h sentiment snapshots.",
            retryable=False,
            code="unsupported_frequency",
        )
    filtered = [
        event
        for event in events
        if request.time_range.start <= event.event_time <= request.time_range.end
    ]
    if not filtered:
        raise DataConnectorError(
            data_domain="sentiment_events",
            vendor=vendor,
            identifier=identifier,
            message="Connector returned no rows and no synthetic fallback is allowed.",
            retryable=False,
            code="empty_result",
        )

    entity_key = _symbol_from_identifier(identifier)
    bucket_payloads: dict[datetime, dict[str, object]] = {}
    for event in filtered:
        bucket = _bucket_time(event.available_time, frequency)
        payload = bucket_payloads.setdefault(
            bucket,
            {
                "available_time": event.available_time,
                "events": [],
                "sources": set(),
                "word_counts": {},
                "positive": 0,
                "negative": 0,
                "sentiment_total": 0.0,
            },
        )
        payload["available_time"] = max(payload["available_time"], event.available_time)
        text = _clean_text(f"{event.title} {event.text_preview}".strip())
        score = _sentiment_score(text)
        if score > 0:
            payload["positive"] += 1
        elif score < 0:
            payload["negative"] += 1
        payload["sentiment_total"] += score
        payload["sources"].add(event.source_name)
        tokens = payload["word_counts"]
        for token in _tokenize(text):
            tokens[token] = int(tokens.get(token, 0)) + 1
        payload["events"].append(
            {
                "event_id": event.event_id,
                "title": event.title,
                "snippet": _clean_text(event.text_preview),
                "source": event.source_name,
                "source_type": event.source_type,
                "symbol": event.symbol,
                "url": event.url,
                "event_time": event.event_time.isoformat(),
                "available_time": event.available_time.isoformat(),
                "sentiment_score": round(score, 6),
            }
        )

    points: list[NormalizedSeriesPoint] = []
    for event_time in sorted(bucket_payloads):
        payload = bucket_payloads[event_time]
        bucket_events = list(payload["events"])
        event_count = len(bucket_events)
        source_count = len(payload["sources"])
        avg_sentiment = float(payload["sentiment_total"]) / max(event_count, 1)
        positive_ratio = float(payload["positive"]) / max(event_count, 1)
        negative_ratio = float(payload["negative"]) / max(event_count, 1)
        event_intensity = abs(avg_sentiment) * math.log1p(event_count)
        word_terms = _top_terms(payload["word_counts"], limit=30)
        keyword_terms = _top_terms(payload["word_counts"], limit=12)
        preview_events = sorted(
            bucket_events,
            key=lambda item: str(item.get("available_time") or item.get("event_time") or ""),
            reverse=True,
        )[:6]
        dimensions = {
            "identifier": identifier,
            "symbol": entity_key,
            "source_type": source_type,
            "source_name": vendor,
            "headline": str(preview_events[0]["title"]) if preview_events else "",
            "event_id": str(preview_events[0]["event_id"]) if preview_events else "",
            "event_type": source_type,
            "preview_events_json": json.dumps(preview_events, ensure_ascii=False),
            "keywords_json": json.dumps(keyword_terms, ensure_ascii=False),
            "word_counts_json": json.dumps(word_terms, ensure_ascii=False),
            "source_breakdown_json": json.dumps(
                [{"source": source, "count": 1} for source in sorted(payload["sources"])],
                ensure_ascii=False,
            ),
            "bucket_event_count": str(event_count),
            "transport": transport,
        }
        metric_rows = {
            "sentiment_score": avg_sentiment,
            "positive_ratio": positive_ratio,
            "negative_ratio": negative_ratio,
            "mention_count": float(event_count),
            "source_count": float(source_count),
            "event_count": float(event_count),
            "event_intensity": event_intensity,
        }
        for metric_name, value in metric_rows.items():
            points.append(
                NormalizedSeriesPoint(
                    event_time=event_time,
                    available_time=payload["available_time"],
                    series_key=f"{entity_key}:{vendor}:{metric_name}",
                    entity_key=entity_key,
                    domain="sentiment_events",
                    vendor=vendor,
                    metric_name=metric_name,
                    frequency=frequency,
                    value=float(value),
                    dimensions=dimensions,
                )
            )

    return IngestionResult(
        request_id=request.request_id,
        data_domain=request.data_domain,
        vendor=vendor,
        storage_uri=storage_uri,
        normalized_uri=storage_uri,
        coverage=IngestionCoverage(
            start_time=min(point.event_time for point in points),
            end_time=max(point.event_time for point in points),
            complete=bool(points),
        ),
        metadata={
            "identifier": identifier,
            "frequency": frequency,
            "transport": transport,
            "rows": [point.model_dump(mode="json") for point in points],
        },
    )


class NewsArchiveSentimentConnector(DataConnector):
    def __init__(
        self,
        *,
        archive_root: Path | None = None,
        archive_env: str = "QUANT_SENTIMENT_DATA_DIR",
    ) -> None:
        configured_root = os.getenv(archive_env)
        resolved_root = archive_root or (
            Path(configured_root) if configured_root else Path.cwd() / "data" / "raw" / "sentiment"
        )
        self.archive_root = resolved_root.resolve()
        self.registration = ConnectorRegistration(
            data_domain="sentiment_events",
            vendor="news_archive",
            display_name="Local news sentiment archive",
            capabilities=["event_archive", "series_features", "incremental_fetch"],
            status="active",
        )

    def ingest(self, request: IngestionRequest) -> IngestionResult:
        identifier = _identifier_from_request(request)
        frequency = request.frequency or "1h"
        if frequency != "1h":
            raise DataConnectorError(
                data_domain="sentiment_events",
                vendor="news_archive",
                identifier=identifier,
                message="news_archive currently supports only 1h sentiment snapshots.",
                retryable=False,
                code="unsupported_frequency",
            )
        path = self.archive_root / f"{identifier.lower()}.jsonl"
        if not path.exists():
            raise DataConnectorError(
                data_domain="sentiment_events",
                vendor="news_archive",
                identifier=identifier,
                message=f"Sentiment archive file was not found at '{path}'.",
                retryable=False,
                code="identifier_not_found",
            )
        points = self._load_points(path=path, identifier=identifier, frequency=frequency)
        filtered = [
            point
            for point in points
            if request.time_range.start <= point.event_time <= request.time_range.end
        ]
        return IngestionResult(
            request_id=request.request_id,
            data_domain=request.data_domain,
            vendor=request.vendor,
            storage_uri=str(path),
            normalized_uri=str(path),
            coverage=IngestionCoverage(
                start_time=(filtered[0].event_time if filtered else None),
                end_time=(filtered[-1].event_time if filtered else None),
                complete=bool(filtered),
            ),
            metadata={
                "identifier": identifier,
                "frequency": frequency,
                "archive_path": str(path),
                "rows": [point.model_dump(mode="json") for point in filtered],
            },
        )

    def _load_points(
        self,
        *,
        path: Path,
        identifier: str,
        frequency: str,
    ) -> list[NormalizedSeriesPoint]:
        points: list[NormalizedSeriesPoint] = []
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            payload = json.loads(line)
            event_time = _parse_dt(payload.get("event_time"))
            available_time = _parse_dt(payload.get("available_time"))
            entity_key = str(
                payload.get("entity_key") or payload.get("symbol") or identifier.upper()
            )
            metrics = payload.get("metrics")
            if event_time is None or available_time is None or not isinstance(metrics, dict):
                continue
            dimensions = {
                "identifier": identifier,
                "source": str(payload.get("source") or "news_archive"),
                "headline": str(payload.get("headline") or ""),
                "symbol": str(payload.get("symbol") or entity_key),
                "event_id": str(payload.get("event_id") or ""),
                "event_type": str(payload.get("event_type") or "news"),
            }
            for metric_name, value in metrics.items():
                if not isinstance(metric_name, str) or not isinstance(value, (int, float)):
                    continue
                points.append(
                    NormalizedSeriesPoint(
                        event_time=event_time,
                        available_time=available_time,
                        series_key=f"{identifier}:{metric_name}",
                        entity_key=entity_key,
                        domain="sentiment_events",
                        vendor="news_archive",
                        metric_name=metric_name,
                        frequency=frequency,
                        value=float(value),
                        dimensions=dimensions,
                    )
                )
        points.sort(key=lambda item: (item.event_time, item.series_key))
        return points


class GNewsSentimentConnector(DataConnector):
    def __init__(self, *, api_key_env: str = "GNEWS_API_KEY") -> None:
        self.api_key_env = api_key_env
        self.registration = ConnectorRegistration(
            data_domain="sentiment_events",
            vendor="gnews",
            display_name="GNews / Google News",
            capabilities=["news_search", "series_features", "incremental_fetch"],
            requires_credentials=False,
            status="active",
        )

    def ingest(self, request: IngestionRequest) -> IngestionResult:
        identifier = _identifier_from_request(request)
        api_key = os.getenv(self.api_key_env, "").strip()
        if api_key:
            events = self._fetch_gnews_api(identifier, request, api_key=api_key)
            transport = "gnews_api"
            storage_uri = "https://gnews.io/api/v4/search"
        else:
            if _window_span(request) > _GNEWS_RSS_MAX_LOOKBACK:
                raise DataConnectorError(
                    data_domain="sentiment_events",
                    vendor="gnews",
                    identifier=identifier,
                    message=(
                        "Google News RSS fallback is near-real-time only and cannot satisfy historical "
                        "ranges longer than 7 days. Configure GNEWS_API_KEY or switch to the gdelt vendor "
                        "for long historical windows."
                    ),
                    retryable=False,
                    code="historical_range_not_supported",
                )
            events = self._fetch_google_news_rss(identifier, request)
            transport = "google_news_rss"
            storage_uri = "https://news.google.com/rss/search"
        return _aggregate_events(
            request=request,
            vendor="gnews",
            identifier=identifier,
            source_type="news",
            events=events,
            storage_uri=storage_uri,
            transport=transport,
        )

    def _fetch_gnews_api(
        self,
        identifier: str,
        request: IngestionRequest,
        *,
        api_key: str,
    ) -> list[SentimentEvent]:
        query = quote_plus(_query_terms(identifier, request))
        start = request.time_range.start.astimezone(UTC).isoformat().replace("+00:00", "Z")
        end = request.time_range.end.astimezone(UTC).isoformat().replace("+00:00", "Z")
        url = (
            "https://gnews.io/api/v4/search"
            f"?q={query}&lang=en&max=50&from={quote_plus(start)}&to={quote_plus(end)}"
            f"&token={quote_plus(api_key)}"
        )
        try:
            with _request_url(url) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            raise DataConnectorError(
                data_domain="sentiment_events",
                vendor="gnews",
                identifier=identifier,
                message=f"GNews fetch failed: {exc}",
                retryable=True,
                code="connector_ingest_failed",
            ) from exc
        articles = payload.get("articles")
        if not isinstance(articles, list):
            return []
        events: list[SentimentEvent] = []
        symbol = _symbol_from_identifier(identifier)
        for article in articles:
            if not isinstance(article, dict):
                continue
            published_at = _parse_dt(article.get("publishedAt"))
            if published_at is None:
                continue
            title = str(article.get("title") or "").strip()
            description = _clean_text(str(article.get("description") or "").strip())
            source = article.get("source") if isinstance(article.get("source"), dict) else {}
            source_name = str(source.get("name") or "gnews")
            event_id = str(article.get("url") or article.get("title") or published_at.isoformat())
            events.append(
                SentimentEvent(
                    event_id=event_id,
                    source_type="news",
                    source_name=source_name,
                    event_time=published_at,
                    available_time=published_at,
                    title=title,
                    text_preview=description[:280],
                    url=(str(article.get("url")) if article.get("url") else None),
                    symbol=symbol,
                    raw_metadata={
                        "transport": "gnews_api",
                        "publisher": source_name,
                    },
                )
            )
        return events

    def _fetch_google_news_rss(
        self,
        identifier: str,
        request: IngestionRequest,
    ) -> list[SentimentEvent]:
        query = quote_plus(_rss_query_terms(identifier, request))
        url = (
            "https://news.google.com/rss/search"
            f"?q={query}&hl=en-US&gl=US&ceid=US:en"
        )
        try:
            with _request_url(url) as response:
                xml_text = response.read().decode("utf-8", errors="ignore")
        except Exception as exc:  # noqa: BLE001
            raise DataConnectorError(
                data_domain="sentiment_events",
                vendor="gnews",
                identifier=identifier,
                message=f"Google News RSS fetch failed: {exc}",
                retryable=True,
                code="connector_ingest_failed",
            ) from exc
        try:
            root = ElementTree.fromstring(xml_text)
        except ElementTree.ParseError as exc:
            raise DataConnectorError(
                data_domain="sentiment_events",
                vendor="gnews",
                identifier=identifier,
                message=f"Google News RSS parse failed: {exc}",
                retryable=True,
                code="connector_ingest_failed",
            ) from exc
        symbol = _symbol_from_identifier(identifier)
        events: list[SentimentEvent] = []
        for item in root.findall(".//item"):
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip() or None
            pub_date = item.findtext("pubDate")
            event_time = (
                parsedate_to_datetime(pub_date).astimezone(UTC)
                if pub_date
                else None
            )
            if event_time is None:
                continue
            description = _clean_text(item.findtext("description") or "")
            source_name = (item.findtext("{http://search.yahoo.com/mrss/}source") or "google_news").strip()
            event_id = link or title or event_time.isoformat()
            events.append(
                SentimentEvent(
                    event_id=event_id,
                    source_type="news",
                    source_name=source_name,
                    event_time=event_time,
                    available_time=event_time,
                    title=title,
                    text_preview=description[:280],
                    url=link,
                    symbol=symbol,
                    raw_metadata={"transport": "google_news_rss"},
                )
            )
        return events


class GdeltSentimentConnector(DataConnector):
    _rate_lock = threading.Lock()
    _last_request_at = 0.0

    def __init__(self) -> None:
        self.registration = ConnectorRegistration(
            data_domain="sentiment_events",
            vendor="gdelt",
            display_name="GDELT DOC 2",
            capabilities=["historical_news_search", "series_features", "incremental_fetch"],
            status="active",
        )

    def ingest(self, request: IngestionRequest) -> IngestionResult:
        identifier = _identifier_from_request(request)
        events = self._fetch_gdelt_articles(identifier, request)
        return _aggregate_events(
            request=request,
            vendor="gdelt",
            identifier=identifier,
            source_type="news",
            events=events,
            storage_uri="https://api.gdeltproject.org/api/v2/doc/doc",
            transport="gdelt_doc_v2",
        )

    def _fetch_gdelt_articles(
        self,
        identifier: str,
        request: IngestionRequest,
    ) -> list[SentimentEvent]:
        symbol = _symbol_from_identifier(identifier)
        query = quote_plus(_query_terms(identifier, request))
        cursor = request.time_range.start.astimezone(UTC)
        end_time = request.time_range.end.astimezone(UTC)
        events_by_id: dict[str, SentimentEvent] = {}
        while cursor < end_time:
            chunk_end = min(cursor + timedelta(days=_GDELT_CHUNK_DAYS), end_time)
            url = (
                "https://api.gdeltproject.org/api/v2/doc/doc"
                f"?query={query}"
                "&mode=ArtList"
                f"&maxrecords={_GDELT_MAX_RECORDS}"
                f"&startdatetime={_gdelt_dt(cursor)}"
                f"&enddatetime={_gdelt_dt(chunk_end)}"
                "&format=json"
            )
            try:
                with self._rate_limited_request(url) as response:
                    payload = json.loads(response.read().decode("utf-8"))
            except Exception as exc:  # noqa: BLE001
                raise DataConnectorError(
                    data_domain="sentiment_events",
                    vendor="gdelt",
                    identifier=identifier,
                    message=f"GDELT fetch failed: {exc}",
                    retryable=True,
                    code="connector_ingest_failed",
                ) from exc
            articles = payload.get("articles")
            if isinstance(articles, list):
                for article in articles:
                    if not isinstance(article, dict):
                        continue
                    published_at = _parse_dt(article.get("seendate")) or _parse_dt(article.get("socialimage"))
                    if published_at is None:
                        raw_date = article.get("seendate")
                        if isinstance(raw_date, str) and raw_date:
                            try:
                                published_at = datetime.strptime(raw_date[:14], "%Y%m%d%H%M%S").replace(tzinfo=UTC)
                            except ValueError:
                                published_at = None
                    if published_at is None:
                        continue
                    if published_at < request.time_range.start or published_at > request.time_range.end:
                        continue
                    title = str(article.get("title") or "").strip()
                    description = _clean_text(str(article.get("snippet") or article.get("description") or "").strip())
                    source_name = str(
                        article.get("domain")
                        or article.get("sourcecountry")
                        or "gdelt"
                    ).strip()
                    url_value = str(article.get("url") or "").strip() or None
                    event_id = url_value or title or published_at.isoformat()
                    events_by_id[event_id] = SentimentEvent(
                        event_id=event_id,
                        source_type="news",
                        source_name=source_name,
                        event_time=published_at,
                        available_time=published_at,
                        title=title or "(untitled)",
                        text_preview=description[:280],
                        url=url_value,
                        symbol=symbol,
                        raw_metadata={
                            "transport": "gdelt_doc_v2",
                            "domain": source_name,
                            "lang": article.get("language"),
                        },
                    )
            cursor = chunk_end
        return sorted(events_by_id.values(), key=lambda item: (item.event_time, item.event_id))

    def _rate_limited_request(self, url: str) -> object:
        with self._rate_lock:
            wait_seconds = max(0.0, _GDELT_MIN_INTERVAL_SECONDS - (time.monotonic() - self._last_request_at))
            if wait_seconds > 0:
                time.sleep(wait_seconds)
            self._last_request_at = time.monotonic()
        try:
            return _request_url(url)
        except HTTPError as exc:
            if exc.code != 429:
                raise
            time.sleep(_GDELT_MIN_INTERVAL_SECONDS)
            with self._rate_lock:
                self._last_request_at = time.monotonic()
            return _request_url(url)


class _RedditHistoryStore:
    def __init__(self, artifact_root: Path, csv_path: Path) -> None:
        self.artifact_root = artifact_root.resolve()
        self.csv_path = csv_path.resolve()
        self.db_path = self.artifact_root / "data_cache" / "nlp_history.sqlite3"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def ensure_window(self, *, start_time: datetime, end_time: datetime) -> None:
        effective_start = (start_time - timedelta(hours=24)).astimezone(UTC)
        effective_end = end_time.astimezone(UTC)
        if self._is_window_loaded(effective_start, effective_end):
            return
        if not self.csv_path.exists():
            raise FileNotFoundError(f"Reddit history csv not found at '{self.csv_path}'.")

        bucket_stats: dict[datetime, dict[str, object]] = {}
        inserted_rows = 0
        skipped_rows = 0
        batch: list[tuple[object, ...]] = []
        with self._connect() as conn:
            with self.csv_path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
                reader = csv.DictReader(handle)
                for row_index, row in enumerate(reader):
                    event_time = _parse_dt(row.get("datetime"))
                    if event_time is None:
                        skipped_rows += 1
                        continue
                    if event_time < effective_start or event_time > effective_end:
                        continue
                    body = str(row.get("body") or "").strip()
                    if not body:
                        skipped_rows += 1
                        continue
                    cleaned_body = self._normalize_reddit_text(body)
                    if not cleaned_body:
                        skipped_rows += 1
                        continue
                    author = str(row.get("author") or "").strip() or "unknown"
                    subreddit = str(row.get("subreddit") or "").strip() or "reddit"
                    score = self._float(row.get("score"))
                    controversiality = self._float(row.get("controversiality"))
                    sentiment_score = _sentiment_score(cleaned_body)
                    keywords = _top_terms(
                        self._count_tokens(cleaned_body),
                        limit=12,
                    )
                    event_id = f"reddit_csv:{row_index + 1}"
                    batch.append(
                        (
                            event_id,
                            "BTC",
                            event_time.isoformat(),
                            event_time.isoformat(),
                            "social",
                            subreddit,
                            author,
                            score,
                            controversiality,
                            body,
                            cleaned_body,
                            len(cleaned_body),
                            sentiment_score,
                            json.dumps(keywords, ensure_ascii=False),
                            json.dumps(
                                {
                                    "author": author,
                                    "score": score,
                                    "controversiality": controversiality,
                                    "row_index": row_index + 1,
                                    "created_utc": row.get("created_utc"),
                                },
                                ensure_ascii=False,
                            ),
                        )
                    )
                    inserted_rows += 1
                    bucket = _bucket_time(event_time, "1h")
                    payload = bucket_stats.setdefault(
                        bucket,
                        {
                            "available_time": event_time,
                            "comment_count": 0,
                            "authors": set(),
                            "score_sum": 0.0,
                            "controversial_sum": 0.0,
                            "sentiment_sum": 0.0,
                            "sentiment_sq_sum": 0.0,
                            "positive_count": 0,
                            "negative_count": 0,
                            "body_len_sum": 0,
                            "core_subreddit_count": 0,
                            "word_counts": {},
                            "source_counts": {},
                            "preview_events": [],
                        },
                    )
                    payload["available_time"] = max(payload["available_time"], event_time)
                    payload["comment_count"] = int(payload["comment_count"]) + 1
                    cast_authors = payload["authors"]
                    if isinstance(cast_authors, set):
                        cast_authors.add(author)
                    payload["score_sum"] = float(payload["score_sum"]) + score
                    payload["controversial_sum"] = float(payload["controversial_sum"]) + controversiality
                    payload["sentiment_sum"] = float(payload["sentiment_sum"]) + sentiment_score
                    payload["sentiment_sq_sum"] = float(payload["sentiment_sq_sum"]) + (
                        sentiment_score * sentiment_score
                    )
                    if sentiment_score > 0:
                        payload["positive_count"] = int(payload["positive_count"]) + 1
                    elif sentiment_score < 0:
                        payload["negative_count"] = int(payload["negative_count"]) + 1
                    payload["body_len_sum"] = int(payload["body_len_sum"]) + len(cleaned_body)
                    if subreddit.lower() in _CORE_SUBREDDITS:
                        payload["core_subreddit_count"] = int(payload["core_subreddit_count"]) + 1
                    source_counts = payload["source_counts"]
                    if isinstance(source_counts, dict):
                        source_counts[subreddit] = int(source_counts.get(subreddit, 0)) + 1
                    word_counts = payload["word_counts"]
                    if isinstance(word_counts, dict):
                        for token in _tokenize(cleaned_body):
                            word_counts[token] = int(word_counts.get(token, 0)) + 1
                    preview_events = payload["preview_events"]
                    if isinstance(preview_events, list):
                        preview_events.append(
                            {
                                "event_id": event_id,
                                "title": cleaned_body[:120] or "(empty)",
                                "snippet": cleaned_body[:280],
                                "source": subreddit,
                                "source_type": "social",
                                "symbol": "BTC",
                                "event_time": event_time.isoformat(),
                                "available_time": event_time.isoformat(),
                                "sentiment_score": round(sentiment_score, 6),
                            }
                        )
                        if len(preview_events) > 12:
                            preview_events.sort(
                                key=lambda item: str(
                                    item.get("available_time") or item.get("event_time") or ""
                                ),
                                reverse=True,
                            )
                            del preview_events[12:]
                    if len(batch) >= 2000:
                        self._insert_raw_events(conn, batch)
                        batch.clear()
            if batch:
                self._insert_raw_events(conn, batch)
                batch.clear()
            self._replace_snapshot_rows(conn, bucket_stats)
            conn.execute(
                """
                INSERT INTO nlp_ingestion_runs (
                    run_id, imported_start, imported_end, source_path,
                    rows_imported, rows_skipped, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"reddit_history::{effective_start.isoformat()}::{effective_end.isoformat()}",
                    effective_start.isoformat(),
                    effective_end.isoformat(),
                    str(self.csv_path),
                    inserted_rows,
                    skipped_rows,
                    datetime.now(UTC).isoformat(),
                ),
            )
            conn.commit()

    def query_points(
        self,
        *,
        start_time: datetime,
        end_time: datetime,
    ) -> list[NormalizedSeriesPoint]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT bucket_time, available_time, comment_count, unique_author_count,
                       score_sum, score_mean, controversiality_ratio, sentiment_mean,
                       sentiment_std, positive_ratio, negative_ratio, body_len_mean,
                       core_subreddit_ratio, attention_zscore_24h, preview_events_json,
                       keywords_json, word_counts_json, source_breakdown_json
                FROM nlp_feature_snapshots_1h
                WHERE asset_id = 'BTC' AND bucket_time >= ? AND bucket_time <= ?
                ORDER BY bucket_time ASC
                """,
                (start_time.astimezone(UTC).isoformat(), end_time.astimezone(UTC).isoformat()),
            ).fetchall()
        points: list[NormalizedSeriesPoint] = []
        for row in rows:
            event_time = _parse_dt(row["bucket_time"])
            available_time = _parse_dt(row["available_time"])
            if event_time is None or available_time is None:
                continue
            dimensions = {
                "identifier": "BTC",
                "symbol": "BTC",
                "source_type": "social",
                "source_name": "reddit_history_csv",
                "preview_events_json": str(row["preview_events_json"] or "[]"),
                "keywords_json": str(row["keywords_json"] or "[]"),
                "word_counts_json": str(row["word_counts_json"] or "[]"),
                "source_breakdown_json": str(row["source_breakdown_json"] or "[]"),
                "transport": "reddit_history_csv",
            }
            metric_rows = {
                "reddit_comment_count_1h": float(row["comment_count"] or 0.0),
                "reddit_unique_author_count_1h": float(row["unique_author_count"] or 0.0),
                "reddit_score_sum_1h": float(row["score_sum"] or 0.0),
                "reddit_score_mean_1h": float(row["score_mean"] or 0.0),
                "reddit_controversiality_ratio_1h": float(row["controversiality_ratio"] or 0.0),
                "reddit_sentiment_mean_1h": float(row["sentiment_mean"] or 0.0),
                "reddit_sentiment_std_1h": float(row["sentiment_std"] or 0.0),
                "reddit_positive_ratio_1h": float(row["positive_ratio"] or 0.0),
                "reddit_negative_ratio_1h": float(row["negative_ratio"] or 0.0),
                "reddit_body_len_mean_1h": float(row["body_len_mean"] or 0.0),
                "reddit_core_subreddit_ratio_1h": float(row["core_subreddit_ratio"] or 0.0),
                "reddit_attention_zscore_24h": float(row["attention_zscore_24h"] or 0.0),
                "event_count": float(row["comment_count"] or 0.0),
                "sentiment_score": float(row["sentiment_mean"] or 0.0),
            }
            for metric_name, value in metric_rows.items():
                points.append(
                    NormalizedSeriesPoint(
                        event_time=event_time,
                        available_time=available_time,
                        series_key=f"BTC:reddit_history_csv:{metric_name}",
                        entity_key="BTC",
                        domain="sentiment_events",
                        vendor="reddit_history_csv",
                        metric_name=metric_name,
                        frequency="1h",
                        value=value,
                        dimensions=dimensions,
                    )
                )
        return points

    def _replace_snapshot_rows(
        self,
        conn: sqlite3.Connection,
        bucket_stats: dict[datetime, dict[str, object]],
    ) -> None:
        if not bucket_stats:
            return
        buckets = sorted(bucket_stats)
        counts_window: deque[float] = deque(maxlen=24)
        snapshot_rows: list[tuple[object, ...]] = []
        for bucket in buckets:
            payload = bucket_stats[bucket]
            comment_count = int(payload["comment_count"])
            score_sum = float(payload["score_sum"])
            unique_author_count = len(payload["authors"]) if isinstance(payload["authors"], set) else 0
            positive_count = int(payload["positive_count"])
            negative_count = int(payload["negative_count"])
            mean_sentiment = (
                float(payload["sentiment_sum"]) / comment_count if comment_count else 0.0
            )
            variance = 0.0
            if comment_count:
                variance = max(
                    0.0,
                    (float(payload["sentiment_sq_sum"]) / comment_count) - (mean_sentiment ** 2),
                )
            counts_mean = (
                sum(counts_window) / len(counts_window) if counts_window else float(comment_count)
            )
            counts_var = (
                sum((value - counts_mean) ** 2 for value in counts_window) / len(counts_window)
                if counts_window
                else 0.0
            )
            counts_std = math.sqrt(counts_var) if counts_var > 0 else 0.0
            attention_zscore = (
                0.0 if counts_std == 0.0 else (comment_count - counts_mean) / counts_std
            )
            counts_window.append(float(comment_count))
            word_terms = _top_terms(payload["word_counts"], limit=30) if isinstance(payload["word_counts"], dict) else []
            keyword_terms = _top_terms(payload["word_counts"], limit=12) if isinstance(payload["word_counts"], dict) else []
            source_breakdown = []
            if isinstance(payload["source_counts"], dict):
                source_breakdown = [
                    {"source": source, "count": count}
                    for source, count in sorted(
                        payload["source_counts"].items(),
                        key=lambda item: (-int(item[1]), item[0]),
                    )
                ]
            preview_events = payload["preview_events"] if isinstance(payload["preview_events"], list) else []
            preview_events = sorted(
                preview_events,
                key=lambda item: str(item.get("available_time") or item.get("event_time") or ""),
                reverse=True,
            )[:6]
            snapshot_rows.append(
                (
                    "BTC",
                    bucket.isoformat(),
                    payload["available_time"].isoformat(),
                    comment_count,
                    unique_author_count,
                    score_sum,
                    score_sum / comment_count if comment_count else 0.0,
                    float(payload["controversial_sum"]) / comment_count if comment_count else 0.0,
                    mean_sentiment,
                    math.sqrt(variance),
                    positive_count / comment_count if comment_count else 0.0,
                    negative_count / comment_count if comment_count else 0.0,
                    int(payload["body_len_sum"]) / comment_count if comment_count else 0.0,
                    int(payload["core_subreddit_count"]) / comment_count if comment_count else 0.0,
                    attention_zscore,
                    json.dumps(preview_events, ensure_ascii=False),
                    json.dumps(keyword_terms, ensure_ascii=False),
                    json.dumps(word_terms, ensure_ascii=False),
                    json.dumps(source_breakdown, ensure_ascii=False),
                )
            )
        conn.executemany(
            """
            INSERT OR REPLACE INTO nlp_feature_snapshots_1h (
                asset_id, bucket_time, available_time, comment_count, unique_author_count,
                score_sum, score_mean, controversiality_ratio, sentiment_mean, sentiment_std,
                positive_ratio, negative_ratio, body_len_mean, core_subreddit_ratio,
                attention_zscore_24h, preview_events_json, keywords_json, word_counts_json,
                source_breakdown_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            snapshot_rows,
        )

    def _insert_raw_events(
        self,
        conn: sqlite3.Connection,
        rows: list[tuple[object, ...]],
    ) -> None:
        conn.executemany(
            """
            INSERT OR IGNORE INTO nlp_raw_events (
                event_id, asset_id, event_time, available_time, source_type, source_name,
                author, score, controversiality, body, body_clean, body_length,
                sentiment_score, keywords_json, raw_metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

    def _is_window_loaded(self, start_time: datetime, end_time: datetime) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM nlp_ingestion_runs
                WHERE imported_start <= ? AND imported_end >= ?
                LIMIT 1
                """,
                (start_time.isoformat(), end_time.isoformat()),
            ).fetchone()
        return row is not None

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS nlp_raw_events (
                    event_id TEXT PRIMARY KEY,
                    asset_id TEXT NOT NULL,
                    event_time TEXT NOT NULL,
                    available_time TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    author TEXT,
                    score REAL NOT NULL,
                    controversiality REAL NOT NULL,
                    body TEXT NOT NULL,
                    body_clean TEXT NOT NULL,
                    body_length INTEGER NOT NULL,
                    sentiment_score REAL NOT NULL,
                    keywords_json TEXT NOT NULL,
                    raw_metadata_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS nlp_feature_snapshots_1h (
                    asset_id TEXT NOT NULL,
                    bucket_time TEXT PRIMARY KEY,
                    available_time TEXT NOT NULL,
                    comment_count INTEGER NOT NULL,
                    unique_author_count INTEGER NOT NULL,
                    score_sum REAL NOT NULL,
                    score_mean REAL NOT NULL,
                    controversiality_ratio REAL NOT NULL,
                    sentiment_mean REAL NOT NULL,
                    sentiment_std REAL NOT NULL,
                    positive_ratio REAL NOT NULL,
                    negative_ratio REAL NOT NULL,
                    body_len_mean REAL NOT NULL,
                    core_subreddit_ratio REAL NOT NULL,
                    attention_zscore_24h REAL NOT NULL,
                    preview_events_json TEXT NOT NULL,
                    keywords_json TEXT NOT NULL,
                    word_counts_json TEXT NOT NULL,
                    source_breakdown_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS nlp_ingestion_runs (
                    run_id TEXT PRIMARY KEY,
                    imported_start TEXT NOT NULL,
                    imported_end TEXT NOT NULL,
                    source_path TEXT NOT NULL,
                    rows_imported INTEGER NOT NULL,
                    rows_skipped INTEGER NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_nlp_raw_events_event_time ON nlp_raw_events(event_time)"
            )
            conn.commit()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout = 30000")
        return conn

    @staticmethod
    def _count_tokens(text: str) -> dict[str, int]:
        counts: dict[str, int] = {}
        for token in _tokenize(text):
            counts[token] = counts.get(token, 0) + 1
        return counts

    @staticmethod
    def _normalize_reddit_text(text: str) -> str:
        normalized = _clean_text(text)
        normalized = re.sub(r"\[([^\]]+)\]\((https?://[^)]+)\)", r"\1", normalized)
        normalized = re.sub(r"(?i)(?:\$?btc|bitcoin)", "bitcoin", normalized)
        normalized = re.sub(r"\s+", " ", normalized)
        return normalized.strip()

    @staticmethod
    def _float(value: object) -> float:
        try:
            return float(value or 0.0)
        except (TypeError, ValueError):
            return 0.0


class RedditHistoryCsvSentimentConnector(DataConnector):
    ENV_CSV_PATH = "QUANT_PLATFORM_REDDIT_HISTORY_CSV"

    def __init__(self, artifact_root: Path, csv_path: Path | None = None) -> None:
        repo_root = Path(__file__).resolve().parents[4]
        env_csv_path = os.getenv(self.ENV_CSV_PATH, "").strip()
        resolved_csv_path = (
            csv_path
            or (Path(env_csv_path).expanduser() if env_csv_path else None)
            or repo_root / "data" / "raw" / "sentiment" / "bitcoin_reddit_all.csv"
        )
        self.store = _RedditHistoryStore(artifact_root, resolved_csv_path)
        self.registration = ConnectorRegistration(
            data_domain="sentiment_events",
            vendor="reddit_history_csv",
            display_name="Reddit history csv",
            capabilities=["historical_social_archive", "series_features", "db_first"],
            status="active",
        )

    def ingest(self, request: IngestionRequest) -> IngestionResult:
        identifier = _identifier_from_request(request)
        frequency = (request.frequency or "1h").lower()
        if frequency != "1h":
            raise DataConnectorError(
                data_domain="sentiment_events",
                vendor="reddit_history_csv",
                identifier=identifier,
                message="reddit_history_csv currently supports only 1h snapshots.",
                retryable=False,
                code="unsupported_frequency",
            )
        try:
            self.store.ensure_window(
                start_time=request.time_range.start,
                end_time=request.time_range.end,
            )
            rows = self.store.query_points(
                start_time=request.time_range.start,
                end_time=request.time_range.end,
            )
        except FileNotFoundError as exc:
            raise DataConnectorError(
                data_domain="sentiment_events",
                vendor="reddit_history_csv",
                identifier=identifier,
                message=str(exc),
                retryable=False,
                code="source_file_missing",
            ) from exc
        if not rows:
            raise DataConnectorError(
                data_domain="sentiment_events",
                vendor="reddit_history_csv",
                identifier=identifier,
                message="No Reddit history rows matched the requested time range.",
                retryable=False,
                code="empty_result",
            )
        return IngestionResult(
            request_id=request.request_id,
            data_domain=request.data_domain,
            vendor=request.vendor,
            storage_uri=str(self.store.db_path),
            normalized_uri=str(self.store.db_path),
            coverage=IngestionCoverage(
                start_time=min(row.event_time for row in rows),
                end_time=max(row.event_time for row in rows),
                complete=True,
            ),
            metadata={
                "identifier": identifier,
                "frequency": frequency,
                "rows": [row.model_dump(mode="json") for row in rows],
            },
        )


class RedditPublicSentimentConnector(DataConnector):
    def __init__(self) -> None:
        self.registration = ConnectorRegistration(
            data_domain="sentiment_events",
            vendor="reddit_public",
            display_name="Reddit Public",
            capabilities=["public_social_search", "series_features", "incremental_fetch"],
            status="active",
        )

    def ingest(self, request: IngestionRequest) -> IngestionResult:
        identifier = _identifier_from_request(request)
        if _window_span(request) > _REDDIT_PUBLIC_MAX_LOOKBACK:
            raise DataConnectorError(
                data_domain="sentiment_events",
                vendor="reddit_public",
                identifier=identifier,
                message=(
                    "reddit_public currently exposes only recent public search windows up to about 31 days. "
                    "Switch to gdelt for long historical requests."
                ),
                retryable=False,
                code="historical_range_not_supported",
            )
        events = self._fetch_reddit_posts(identifier, request)
        return _aggregate_events(
            request=request,
            vendor="reddit_public",
            identifier=identifier,
            source_type="social",
            events=events,
            storage_uri="https://www.reddit.com/search.json",
            transport="reddit_public_json",
        )

    def _fetch_reddit_posts(
        self,
        identifier: str,
        request: IngestionRequest,
    ) -> list[SentimentEvent]:
        query = quote_plus(_rss_query_terms(identifier, request))
        url = (
            "https://www.reddit.com/search.json"
            f"?q={query}&sort=new&t=month&limit=100"
        )
        try:
            with _request_url(
                url,
                headers={"User-Agent": "quant-platform/0.1 (sentiment research)"},
            ) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            raise DataConnectorError(
                data_domain="sentiment_events",
                vendor="reddit_public",
                identifier=identifier,
                message=f"Reddit public fetch failed: {exc}",
                retryable=True,
                code="connector_ingest_failed",
            ) from exc
        posts = (((payload.get("data") or {}).get("children")) if isinstance(payload, dict) else None)
        if not isinstance(posts, list):
            return []
        symbol = _symbol_from_identifier(identifier)
        events: list[SentimentEvent] = []
        for item in posts:
            data = item.get("data") if isinstance(item, dict) else None
            if not isinstance(data, dict):
                continue
            created = data.get("created_utc")
            if not isinstance(created, (int, float)):
                continue
            event_time = datetime.fromtimestamp(float(created), tz=UTC)
            title = str(data.get("title") or "").strip()
            selftext = str(data.get("selftext") or "").strip()
            permalink = str(data.get("permalink") or "").strip()
            url = f"https://www.reddit.com{permalink}" if permalink else None
            post_id = str(data.get("id") or permalink or title or event_time.isoformat())
            events.append(
                SentimentEvent(
                    event_id=post_id,
                    source_type="social",
                    source_name=f"r/{str(data.get('subreddit') or 'reddit')}",
                    event_time=event_time,
                    available_time=event_time,
                    title=title,
                    text_preview=selftext[:280],
                    url=url,
                    symbol=symbol,
                    raw_metadata={
                        "score": data.get("score"),
                        "num_comments": data.get("num_comments"),
                    },
                )
            )
        return events
