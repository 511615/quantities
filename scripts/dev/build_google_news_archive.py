from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.parse
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from xml.etree import ElementTree

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from quant_platform.common.types.core import TimeRange
from quant_platform.data.connectors.sentiment import (  # noqa: E402
    SentimentEvent,
    _aggregate_events,
    _clean_text,
    _request_url,
    _rss_query_terms,
)
from quant_platform.data.contracts.ingestion import IngestionRequest  # noqa: E402


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a local news_archive JSONL file from Google News historical RSS queries."
    )
    parser.add_argument("--identifier", default="btc_news", help="Archive identifier, e.g. btc_news.")
    parser.add_argument(
        "--start",
        default="2024-01-01T00:00:00Z",
        help="UTC start timestamp in ISO8601 format.",
    )
    parser.add_argument(
        "--end",
        default="2026-04-11T02:00:00Z",
        help="UTC end timestamp in ISO8601 format.",
    )
    parser.add_argument(
        "--output",
        default=str(PROJECT_ROOT / "data" / "raw" / "sentiment" / "btc_news.jsonl"),
        help="Output archive JSONL path.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.25,
        help="Delay between RSS requests to stay polite.",
    )
    return parser.parse_args()


def _parse_iso_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _fetch_daily_events(
    *,
    identifier: str,
    start_time: datetime,
    end_time: datetime,
) -> list[SentimentEvent]:
    query_before = (start_time + timedelta(days=1)).date()
    query = (
        f"{_rss_query_terms(identifier, IngestionRequest.model_validate({'data_domain': 'sentiment_events', 'vendor': 'gnews', 'request_id': 'archive-bootstrap', 'time_range': {'start': start_time, 'end': end_time}, 'identifiers': [identifier], 'frequency': '1h'}))} "
        f"after:{start_time.date()} before:{query_before}"
    )
    url = (
        "https://news.google.com/rss/search"
        f"?q={urllib.parse.quote(query)}&hl=en-US&gl=US&ceid=US:en"
    )
    with _request_url(url, headers={"User-Agent": "Mozilla/5.0"}) as response:
        xml_text = response.read().decode("utf-8", errors="ignore")
    root = ElementTree.fromstring(xml_text)
    symbol = "BTC"
    seen: set[str] = set()
    events: list[SentimentEvent] = []
    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip() or None
        pub_date = item.findtext("pubDate")
        event_time = parsedate_to_datetime(pub_date).astimezone(UTC) if pub_date else None
        if event_time is None or event_time < start_time or event_time > end_time:
            continue
        description = _clean_text(item.findtext("description") or "")
        source_name = (
            item.findtext("{http://search.yahoo.com/mrss/}source") or "google_news"
        ).strip()
        event_id = link or title or event_time.isoformat()
        if event_id in seen:
            continue
        seen.add(event_id)
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
                raw_metadata={"transport": "google_news_rss_archive"},
            )
        )
    return events


def _group_archive_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str], dict[str, object]] = {}
    for row in rows:
        event_time = str(row.get("event_time") or "")
        available_time = str(row.get("available_time") or event_time)
        dimensions = dict(row.get("dimensions") or {})
        key = (event_time, str(dimensions.get("symbol") or "BTC"))
        entry = grouped.setdefault(
            key,
            {
                "event_time": event_time,
                "available_time": available_time,
                "source": "google_news_rss_archive",
                "headline": str(dimensions.get("headline") or ""),
                "symbol": str(dimensions.get("symbol") or "BTC"),
                "event_id": str(dimensions.get("event_id") or ""),
                "event_type": str(dimensions.get("event_type") or "news"),
                "metrics": {},
            },
        )
        if available_time > str(entry["available_time"]):
            entry["available_time"] = available_time
        if not entry["headline"] and dimensions.get("headline"):
            entry["headline"] = str(dimensions["headline"])
        if not entry["event_id"] and dimensions.get("event_id"):
            entry["event_id"] = str(dimensions["event_id"])
        metrics = entry["metrics"]
        if isinstance(metrics, dict) and isinstance(row.get("metric_name"), str):
            metrics[str(row["metric_name"])] = float(row.get("value") or 0.0)
    return sorted(grouped.values(), key=lambda item: str(item["event_time"]))


def main() -> int:
    args = _parse_args()
    start_time = _parse_iso_utc(args.start)
    end_time = _parse_iso_utc(args.end)
    output_path = Path(args.output).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    all_events: dict[str, SentimentEvent] = {}
    cursor = start_time
    day_index = 0
    while cursor <= end_time:
        day_start = cursor.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = min(day_start + timedelta(days=1), end_time + timedelta(seconds=1))
        daily_events = _fetch_daily_events(
            identifier=args.identifier,
            start_time=day_start,
            end_time=day_end,
        )
        for event in daily_events:
            all_events.setdefault(event.event_id, event)
        day_index += 1
        if day_index % 30 == 0:
            print(
                f"[archive] processed {day_index} days, unique events={len(all_events)}, through {day_end.date()}",
                flush=True,
            )
        if args.sleep_seconds > 0:
            time.sleep(args.sleep_seconds)
        cursor = day_start + timedelta(days=1)

    request = IngestionRequest(
        data_domain="sentiment_events",
        vendor="gnews",
        request_id="google-news-archive-build",
        time_range=TimeRange(start=start_time, end=end_time),
        identifiers=[args.identifier],
        frequency="1h",
    )
    result = _aggregate_events(
        request=request,
        vendor="gnews",
        identifier=args.identifier,
        source_type="news",
        events=sorted(all_events.values(), key=lambda item: (item.event_time, item.event_id)),
        storage_uri="https://news.google.com/rss/search",
        transport="google_news_rss_archive",
    )
    archive_rows = _group_archive_rows(result.metadata["rows"])
    output_path.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in archive_rows) + "\n",
        encoding="utf-8",
    )
    nonzero_hours = sum(
        1 for row in archive_rows if float((row.get("metrics") or {}).get("event_count") or 0.0) > 0.0
    )
    print(
        json.dumps(
            {
                "output_path": str(output_path),
                "hours": len(archive_rows),
                "nonzero_hours": nonzero_hours,
                "unique_events": len(all_events),
                "start_time": archive_rows[0]["event_time"] if archive_rows else None,
                "end_time": archive_rows[-1]["event_time"] if archive_rows else None,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
