from __future__ import annotations

import json
from datetime import UTC, datetime
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


class DefiLlamaConnector(DataConnector):
    BASE_URL = "https://api.llama.fi"

    def __init__(self, base_url: str | None = None) -> None:
        self.base_url = (base_url or self.BASE_URL).rstrip("/")
        self.registration = ConnectorRegistration(
            data_domain="on_chain",
            vendor="defillama",
            display_name="DeFiLlama historical chain tvl API",
            capabilities=["historical_chain_tvl", "incremental_fetch"],
            status="active",
        )

    def ingest(self, request: IngestionRequest) -> IngestionResult:
        chain = self._require_identifier(request)
        metric_name = str(request.options.get("metric_name") or "tvl")
        points = self._fetch_chain_tvl(
            chain=chain,
            start_time=request.time_range.start,
            end_time=request.time_range.end,
            frequency=request.frequency or "1d",
            metric_name=metric_name,
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
                "chain": chain,
                "metric_name": metric_name,
                "frequency": request.frequency or "1d",
                "rows": [point.model_dump(mode="json") for point in points],
            },
        )

    def _fetch_chain_tvl(
        self,
        *,
        chain: str,
        start_time: datetime,
        end_time: datetime,
        frequency: str,
        metric_name: str,
    ) -> list[NormalizedSeriesPoint]:
        url = f"{self.base_url}/v2/historicalChainTvl/{chain}"
        with urlopen(url, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
        if not isinstance(payload, list):
            raise DataConnectorError(
                data_domain="on_chain",
                vendor="defillama",
                identifier=chain,
                message="DeFiLlama historicalChainTvl response is not a list.",
                retryable=True,
                code="invalid_payload",
            )
        points: list[NormalizedSeriesPoint] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            timestamp = item.get("date")
            value = item.get("tvl")
            if not isinstance(timestamp, (int, float)) or not isinstance(value, (int, float)):
                continue
            event_time = datetime.fromtimestamp(int(timestamp), tz=UTC)
            if event_time < start_time or event_time > end_time:
                continue
            points.append(
                NormalizedSeriesPoint(
                    event_time=event_time,
                    available_time=event_time,
                    series_key=chain,
                    entity_key=chain,
                    domain="on_chain",
                    vendor="defillama",
                    metric_name=metric_name,
                    frequency=frequency,
                    value=float(value),
                    dimensions={"chain": chain},
                )
            )
        points.sort(key=lambda item: item.event_time)
        return points

    @staticmethod
    def _require_identifier(request: IngestionRequest) -> str:
        if not request.identifiers:
            raise DataConnectorError(
                data_domain="on_chain",
                vendor="defillama",
                message="DeFiLlama connector requires at least one chain identifier.",
                retryable=False,
                code="identifier_required",
            )
        return request.identifiers[0]
