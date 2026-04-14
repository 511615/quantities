from __future__ import annotations

from datetime import datetime
from statistics import mean, pstdev

from quant_platform.common.hashing.digest import stable_digest
from quant_platform.common.types.core import FeatureField
from quant_platform.data.contracts.data_asset import DataAssetRef
from quant_platform.data.contracts.market import NormalizedMarketBar
from quant_platform.features.contracts.feature_view import (
    FeatureRow,
    FeatureViewBuildResult,
    FeatureViewRef,
)


class MarketFeatureBuilder:
    """Deterministic feature builder using only historical bars."""

    FEATURE_NAMES = [
        "lag_return_1",
        "lag_return_2",
        "momentum_3",
        "realized_vol_3",
        "close_to_open",
        "range_frac",
        "volume_zscore",
        "volume_ratio_3",
    ]

    def build(
        self,
        feature_set_id: str,
        data_ref: DataAssetRef,
        bars: list[NormalizedMarketBar],
        as_of_time: datetime,
    ) -> FeatureViewBuildResult:
        eligible = [
            bar
            for bar in sorted(bars, key=lambda row: row.event_time)
            if bar.available_time <= as_of_time
        ]
        if len(eligible) < 2:
            raise ValueError("at least two eligible bars are required to build features")
        volumes = [bar.volume for bar in eligible]
        rows: list[FeatureRow] = []
        closes = [bar.close for bar in eligible]
        for index, (previous_bar, current_bar) in enumerate(
            zip(eligible[:-1], eligible[1:]), start=1
        ):
            lag_return_1 = (current_bar.close / previous_bar.close) - 1.0
            lag_return_2 = (
                (current_bar.close / eligible[index - 2].close) - 1.0
                if index >= 2
                else lag_return_1
            )
            momentum_3 = (
                (current_bar.close / eligible[index - 3].close) - 1.0
                if index >= 3
                else lag_return_2
            )
            trailing_returns = [
                (closes[offset] / closes[offset - 1]) - 1.0
                for offset in range(max(1, index - 2), index + 1)
            ]
            realized_vol_3 = pstdev(trailing_returns) if len(trailing_returns) > 1 else 0.0
            close_to_open = (
                0.0 if current_bar.open == 0.0 else (current_bar.close / current_bar.open) - 1.0
            )
            range_frac = (
                0.0
                if current_bar.close == 0.0
                else (current_bar.high - current_bar.low) / current_bar.close
            )
            historical_volumes = volumes[: index + 1]
            volume_mean = mean(historical_volumes)
            volume_std = pstdev(historical_volumes) if len(historical_volumes) > 1 else 0.0
            volume_zscore = (
                0.0 if volume_std == 0.0 else (current_bar.volume - volume_mean) / volume_std
            )
            trailing_volumes = volumes[max(0, index - 2) : index + 1]
            trailing_volume_mean = (
                mean(trailing_volumes) if trailing_volumes else current_bar.volume
            )
            volume_ratio_3 = (
                0.0 if trailing_volume_mean == 0.0 else current_bar.volume / trailing_volume_mean
            )
            rows.append(
                FeatureRow(
                    entity_key=current_bar.symbol,
                    timestamp=current_bar.event_time,
                    available_time=current_bar.available_time,
                    values={
                        "lag_return_1": lag_return_1,
                        "lag_return_2": lag_return_2,
                        "momentum_3": momentum_3,
                        "realized_vol_3": realized_vol_3,
                        "close_to_open": close_to_open,
                        "range_frac": range_frac,
                        "volume_zscore": volume_zscore,
                        "volume_ratio_3": volume_ratio_3,
                    },
                )
            )
        feature_view_ref = FeatureViewRef(
            feature_set_id=feature_set_id,
            input_data_refs=[data_ref],
            as_of_time=as_of_time,
            feature_schema=[
                FeatureField(
                    name="lag_return_1",
                    dtype="float",
                    lineage_source="close",
                    max_available_time=as_of_time,
                ),
                FeatureField(
                    name="lag_return_2",
                    dtype="float",
                    lineage_source="close",
                    max_available_time=as_of_time,
                ),
                FeatureField(
                    name="momentum_3",
                    dtype="float",
                    lineage_source="close",
                    max_available_time=as_of_time,
                ),
                FeatureField(
                    name="realized_vol_3",
                    dtype="float",
                    lineage_source="close",
                    max_available_time=as_of_time,
                ),
                FeatureField(
                    name="close_to_open",
                    dtype="float",
                    lineage_source="open_close",
                    max_available_time=as_of_time,
                ),
                FeatureField(
                    name="range_frac",
                    dtype="float",
                    lineage_source="high_low",
                    max_available_time=as_of_time,
                ),
                FeatureField(
                    name="volume_zscore",
                    dtype="float",
                    lineage_source="volume",
                    max_available_time=as_of_time,
                ),
                FeatureField(
                    name="volume_ratio_3",
                    dtype="float",
                    lineage_source="volume",
                    max_available_time=as_of_time,
                ),
            ],
            build_config_hash=stable_digest(
                {
                    "feature_set_id": feature_set_id,
                    "asset_id": data_ref.asset_id,
                    "as_of_time": as_of_time,
                }
            ),
            storage_uri=f"memory://feature_view/{feature_set_id}",
        )
        return FeatureViewBuildResult(feature_view_ref=feature_view_ref, rows=rows)
