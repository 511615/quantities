from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from urllib.parse import urlencode
from urllib.request import urlopen

from quant_platform.data.contracts.ingestion import (
    DataConnectorError,
    ConnectorRegistration,
    DataConnector,
    IngestionCoverage,
    IngestionRequest,
    IngestionResult,
)
from quant_platform.data.contracts.series import NormalizedSeriesPoint


class FredSeriesConnector(DataConnector):
    BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

    def __init__(self, api_key_env: str = "FRED_API_KEY") -> None:
        self.api_key_env = api_key_env
        self.registration = ConnectorRegistration(
            data_domain="macro",
            vendor="fred",
            display_name="FRED time series API",
            capabilities=["series_observations", "incremental_fetch"],
            requires_credentials=True,
            status="active",
        )

    def ingest(self, request: IngestionRequest) -> IngestionResult:
        series_id = self._require_identifier(request)
        api_key = os.getenv(self.api_key_env)
        if not api_key:
            raise DataConnectorError(
                data_domain="macro",
                vendor="fred",
                identifier=series_id,
                message=f"{self.api_key_env} is not configured for FRED ingestion.",
                retryable=False,
                code="credentials_missing",
            )
        points = self._fetch_observations(
            series_id=series_id,
            start_time=request.time_range.start,
            end_time=request.time_range.end,
            api_key=api_key,
            frequency=request.frequency or "1d",
        )
        return IngestionResult(
            request_id=request.request_id,
            data_domain=request.data_domain,
            vendor=request.vendor,
            storage_uri="",
            normalized_uri="",
            coverage=IngestionCoverage(
                start_time=(points[0].event_time if points else None),
                end_time=(points[-1].event_time if points else None),
                complete=bool(points),
            ),
            metadata={
                "series_id": series_id,
                "frequency": request.frequency or "1d",
                "rows": [point.model_dump(mode="json") for point in points],
            },
        )

    def _fetch_observations(
        self,
        *,
        series_id: str,
        start_time: datetime,
        end_time: datetime,
        api_key: str,
        frequency: str,
    ) -> list[NormalizedSeriesPoint]:
        query = urlencode(
            {
                "series_id": series_id,
                "api_key": api_key,
                "file_type": "json",
                "sort_order": "asc",
                "observation_start": start_time.date().isoformat(),
                "observation_end": end_time.date().isoformat(),
            }
        )
        with urlopen(f"{self.BASE_URL}?{query}", timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
        observations = payload.get("observations", [])
        if not isinstance(observations, list):
            raise ValueError("FRED observations response is not a list.")
        points: list[NormalizedSeriesPoint] = []
        for item in observations:
            if not isinstance(item, dict):
                continue
            value = item.get("value")
            date_value = item.get("date")
            if value in {None, "."} or not isinstance(date_value, str):
                continue
            event_time = datetime.fromisoformat(f"{date_value}T00:00:00+00:00")
            points.append(
                NormalizedSeriesPoint(
                    event_time=event_time,
                    available_time=event_time,
                    series_key=series_id,
                    entity_key=series_id,
                    domain="macro",
                    vendor="fred",
                    metric_name="value",
                    frequency=frequency,
                    value=float(value),
                    dimensions={"series_id": series_id},
                )
            )
        points.sort(key=lambda item: item.event_time)
        return points

    @staticmethod
    def _require_identifier(request: IngestionRequest) -> str:
        if not request.identifiers:
            raise DataConnectorError(
                data_domain="macro",
                vendor="fred",
                message="FRED connector requires at least one series identifier.",
                retryable=False,
                code="identifier_required",
            )
        return request.identifiers[0]
