from __future__ import annotations

from collections import defaultdict
from datetime import datetime

from quant_platform.features.contracts.feature_view import FeatureRow


class ForwardReturnLabeler:
    def build(
        self,
        rows: list[FeatureRow],
        closes_by_timestamp: dict[object, float] | dict[tuple[str, datetime], float],
        horizon: int = 1,
    ) -> dict[tuple[str, datetime], float]:
        if horizon <= 0:
            raise ValueError("horizon must be positive")
        labels: dict[tuple[str, datetime], float] = {}
        rows_by_entity: dict[str, list[FeatureRow]] = defaultdict(list)
        for row in rows:
            rows_by_entity[row.entity_key].append(row)
        for entity_key, entity_rows in rows_by_entity.items():
            ordered_rows = sorted(entity_rows, key=lambda item: item.timestamp)
            timestamps = [row.timestamp for row in ordered_rows]
            for index, row in enumerate(ordered_rows):
                future_index = index + horizon
                if future_index >= len(timestamps):
                    continue
                current_key = (entity_key, row.timestamp)
                future_key = (entity_key, timestamps[future_index])
                current_close = self._lookup_close(closes_by_timestamp, current_key, row.timestamp)
                future_close = self._lookup_close(closes_by_timestamp, future_key, timestamps[future_index])
                labels[current_key] = (future_close / current_close) - 1.0
        return labels

    @staticmethod
    def _lookup_close(
        closes_by_timestamp: dict[object, float] | dict[tuple[str, datetime], float],
        entity_timestamp_key: tuple[str, datetime],
        timestamp: datetime,
    ) -> float:
        if entity_timestamp_key in closes_by_timestamp:
            return float(closes_by_timestamp[entity_timestamp_key])
        if timestamp in closes_by_timestamp:
            return float(closes_by_timestamp[timestamp])
        raise KeyError(f"Missing close for {entity_timestamp_key}")
