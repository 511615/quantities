from __future__ import annotations

from datetime import datetime, timezone

from quant_platform.features.transforms.market_features import MarketFeatureBuilder


def test_feature_builder_uses_only_bars_available_by_as_of_time(facade, market_bars) -> None:
    as_of_time = datetime(2024, 1, 1, 3, tzinfo=timezone.utc)
    data_ref, _ = facade.data_catalog.register_market_asset(
        asset_id="asset_temporal_rule",
        source="internal",
        frequency="1h",
        rows=market_bars,
    )
    result = MarketFeatureBuilder().build(
        feature_set_id="baseline_market_features",
        data_ref=data_ref,
        bars=market_bars,
        as_of_time=as_of_time,
    )
    assert len(result.rows) == 3
    assert all(row.available_time <= result.feature_view_ref.as_of_time for row in result.rows)
    assert result.rows[-1].timestamp == datetime(2024, 1, 1, 3, tzinfo=timezone.utc)
    assert set(result.rows[-1].values) == set(MarketFeatureBuilder.FEATURE_NAMES)
