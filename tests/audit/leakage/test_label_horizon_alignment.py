from __future__ import annotations

from quant_platform.datasets.labeling.forward_return import ForwardReturnLabeler


def test_labels_are_built_from_timestamp_plus_horizon(market_bars, facade) -> None:
    data_ref, _ = facade.data_catalog.register_market_asset(
        asset_id="asset_labels",
        source="internal",
        frequency="1h",
        rows=market_bars,
    )
    feature_result = facade.feature_builder.build(
        feature_set_id="baseline_market_features",
        data_ref=data_ref,
        bars=market_bars,
        as_of_time=market_bars[-1].available_time,
    )
    closes_by_timestamp = {bar.event_time: bar.close for bar in market_bars}
    labels = ForwardReturnLabeler().build(feature_result.rows, closes_by_timestamp, horizon=1)
    first_row = feature_result.rows[0]
    first_timestamp = first_row.timestamp
    expected = (
        closes_by_timestamp[feature_result.rows[1].timestamp] / closes_by_timestamp[first_timestamp]
    ) - 1.0
    assert labels[(first_row.entity_key, first_timestamp)] == expected
