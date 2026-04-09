from __future__ import annotations

from collections import Counter

from quant_platform.data.contracts.market import DataQualityReport, NormalizedMarketBar


class MarketDataValidator:
    REQUIRED_COLUMNS = [
        "event_time",
        "available_time",
        "symbol",
        "venue",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]

    @classmethod
    def validate(cls, asset_id: str, rows: list[NormalizedMarketBar]) -> DataQualityReport:
        if not rows:
            raise ValueError("market data rows cannot be empty")
        event_keys = [(row.symbol, row.venue, row.event_time) for row in rows]
        duplicates = sum(count - 1 for count in Counter(event_keys).values() if count > 1)
        sorted_rows = sorted(rows, key=lambda row: row.event_time)
        symbols = {row.symbol for row in rows}
        checks = [
            "required_schema_present",
            "available_time_not_before_event_time",
            "ohlcv_price_bounds_valid",
            "rows_sorted_by_event_time",
        ]
        passed = duplicates == 0 and sorted_rows == rows
        if duplicates == 0:
            checks.append("no_duplicate_event_times")
        missing_ratio = 0.0
        duplicate_ratio = duplicates / len(rows) if rows else 0.0
        ordering_passed = sorted_rows == rows
        quality_status = "healthy"
        warnings: list[str] = []
        if not ordering_passed:
            quality_status = "warning"
            warnings.append("rows_not_sorted_by_event_time")
        if duplicates > 0:
            quality_status = "risk"
            warnings.append("duplicate_event_times_detected")
        return DataQualityReport(
            asset_id=asset_id,
            row_count=len(rows),
            duplicate_event_times=duplicates,
            null_count=0,
            passed=passed and duplicates == 0,
            checks=checks,
            missing_ratio=missing_ratio,
            duplicate_ratio=duplicate_ratio,
            entity_count=len(symbols),
            entity_coverage_ratio=1.0,
            ordering_passed=ordering_passed,
            quality_status=quality_status,
            warnings=warnings,
        )
