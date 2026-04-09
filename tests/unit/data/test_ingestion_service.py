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
