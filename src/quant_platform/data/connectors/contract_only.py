from __future__ import annotations

from quant_platform.data.contracts.ingestion import (
    ConnectorRegistration,
    DataConnector,
    IngestionRequest,
    IngestionResult,
)


class ContractOnlyConnector(DataConnector):
    def __init__(self, *, data_domain: str, vendor: str = "contract_only") -> None:
        self.registration = ConnectorRegistration(
            data_domain=data_domain,
            vendor=vendor,
            display_name=f"{data_domain} contract-only",
            status="contract_only",
        )

    def ingest(self, request: IngestionRequest) -> IngestionResult:
        raise ValueError(
            f"{request.data_domain}/{request.vendor} is contract-only in this release and "
            "does not support live ingestion yet."
        )
