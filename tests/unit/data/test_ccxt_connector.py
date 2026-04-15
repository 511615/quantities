from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from quant_platform.common.types.core import TimeRange
from quant_platform.data.connectors.ccxt_market import CcxtMarketConnector
from quant_platform.data.contracts.ingestion import DataConnectorError, IngestionRequest
from quant_platform.data.ingestion.service import DomainIngestionCoordinator


class _FakeExchange:
    def __init__(self, pages: list[list[list[float]]]) -> None:
        self.pages = pages
        self.calls: list[tuple[str, str, int, int]] = []

    def fetch_ohlcv(
        self,
        symbol: str,
        *,
        timeframe: str,
        since: int,
        limit: int,
    ) -> list[list[float]]:
        self.calls.append((symbol, timeframe, since, limit))
        if self.pages:
            return self.pages.pop(0)
        return []


def test_ccxt_connector_parses_paginated_ohlcv() -> None:
    exchange = _FakeExchange(
        pages=[
            [
                [1704067200000, 100.0, 101.0, 99.0, 100.5, 10.0],
                [1704070800000, 100.5, 102.0, 100.0, 101.5, 11.0],
            ],
            [
                [1704074400000, 101.5, 103.0, 101.0, 102.0, 12.0],
            ],
        ]
    )
    connector = CcxtMarketConnector(exchange_factory=lambda _exchange_id, _opts: exchange)

    result = connector.ingest(
        IngestionRequest(
            data_domain="market",
            vendor="ccxt",
            request_id="ccxt-test",
            time_range=TimeRange(
                start=datetime(2024, 1, 1, 0, tzinfo=UTC),
                end=datetime(2024, 1, 1, 3, tzinfo=UTC),
            ),
            identifiers=["BTC/USDT"],
            frequency="1h",
            options={"exchange": "binance", "market_type": "spot"},
        )
    )

    assert result.coverage.complete is True
    assert result.metadata["exchange"] == "binance"
    assert result.metadata["rows"][0]["symbol"] == "BTC/USDT"
    assert result.metadata["rows"][-1]["close"] == 102.0
    assert len(exchange.calls) == 2
    assert exchange.calls[0][1] == "1h"


def test_ccxt_connector_requires_exchange_option() -> None:
    connector = CcxtMarketConnector(exchange_factory=lambda _exchange_id, _opts: _FakeExchange([]))

    with pytest.raises(DataConnectorError) as exc_info:
        connector.ingest(
            IngestionRequest(
                data_domain="market",
                vendor="ccxt",
                request_id="ccxt-missing-exchange",
                time_range=TimeRange(
                    start=datetime(2024, 1, 1, 0, tzinfo=UTC),
                    end=datetime(2024, 1, 1, 1, tzinfo=UTC),
                ),
                identifiers=["BTC/USDT"],
                frequency="1h",
                options={},
            )
        )
    assert exc_info.value.code == "exchange_required"


def test_ingestion_service_supports_ccxt_cache_and_incremental_refresh(tmp_path: Path) -> None:
    service = DomainIngestionCoordinator(tmp_path / "artifacts", register_defaults=False)
    first_exchange = _FakeExchange(
        pages=[
            [
                [1704067200000, 100.0, 101.0, 99.0, 100.5, 10.0],
                [1704070800000, 100.5, 102.0, 100.0, 101.5, 11.0],
            ]
        ]
    )
    second_exchange = _FakeExchange(
        pages=[
            [
                [1704074400000, 101.5, 103.0, 101.0, 102.0, 12.0],
                [1704078000000, 102.0, 103.5, 101.5, 102.5, 13.0],
            ]
        ]
    )
    exchanges = [first_exchange, second_exchange]
    service.register(
        CcxtMarketConnector(exchange_factory=lambda _exchange_id, _opts: exchanges.pop(0))
    )

    start = datetime(2024, 1, 1, 0, tzinfo=UTC)
    first_end = datetime(2024, 1, 1, 1, tzinfo=UTC)
    second_end = datetime(2024, 1, 1, 3, tzinfo=UTC)

    rows, status = service.fetch_market_bars(
        symbol="BTC/USDT",
        vendor="ccxt",
        exchange="binance",
        frequency="1h",
        start_time=start,
        end_time=first_end,
    )
    assert status == "live_fetch"
    assert len(rows) == 2

    rows, status = service.fetch_market_bars(
        symbol="BTC/USDT",
        vendor="ccxt",
        exchange="binance",
        frequency="1h",
        start_time=start,
        end_time=second_end,
    )
    assert status == "incremental_refresh"
    assert len(rows) == 4

    rows, status = service.fetch_market_bars(
        symbol="BTC/USDT",
        vendor="ccxt",
        exchange="binance",
        frequency="1h",
        start_time=start,
        end_time=second_end,
    )
    assert status == "cache_hit"
    assert len(rows) == 4


def test_ingestion_service_separates_ccxt_cache_by_exchange_and_market_type(tmp_path: Path) -> None:
    service = DomainIngestionCoordinator(tmp_path / "artifacts", register_defaults=False)
    factory_calls: list[tuple[str, dict[str, object]]] = []
    exchange_pages = {
        ("okx", "spot"): [
            [[1704067200000, 100.0, 101.0, 99.0, 100.5, 10.0]],
        ],
        ("binance", "spot"): [
            [[1704067200000, 200.0, 201.0, 199.0, 200.5, 20.0]],
        ],
        ("okx", "swap"): [
            [[1704067200000, 300.0, 301.0, 299.0, 300.5, 30.0]],
        ],
    }

    def exchange_factory(exchange_id: str, opts: dict[str, object]) -> _FakeExchange:
        factory_calls.append((exchange_id, opts))
        market_type = str(opts.get("market_type", "spot"))
        return _FakeExchange(exchange_pages[(exchange_id, market_type)])

    service.register(CcxtMarketConnector(exchange_factory=exchange_factory))
    start = datetime(2024, 1, 1, 0, tzinfo=UTC)
    end = start + timedelta(hours=1)

    okx_spot_rows, okx_spot_status = service.fetch_market_bars(
        symbol="BTC/USDT",
        vendor="ccxt",
        exchange="okx",
        frequency="1h",
        start_time=start,
        end_time=end,
    )
    binance_spot_rows, binance_spot_status = service.fetch_market_bars(
        symbol="BTC/USDT",
        vendor="ccxt",
        exchange="binance",
        frequency="1h",
        start_time=start,
        end_time=end,
    )
    okx_swap_rows, okx_swap_status = service.fetch_market_bars(
        symbol="BTC/USDT",
        vendor="ccxt",
        exchange="okx",
        frequency="1h",
        start_time=start,
        end_time=end,
        market_type="swap",
    )

    assert okx_spot_status == "live_fetch"
    assert binance_spot_status == "live_fetch"
    assert okx_swap_status == "live_fetch"
    assert okx_spot_rows[0].close == 100.5
    assert binance_spot_rows[0].close == 200.5
    assert okx_swap_rows[0].close == 300.5
    assert factory_calls == [
        ("okx", {"market_type": "spot"}),
        ("binance", {"market_type": "spot"}),
        ("okx", {"market_type": "swap"}),
    ]
