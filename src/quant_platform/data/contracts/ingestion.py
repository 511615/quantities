from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Literal

from pydantic import Field

from quant_platform.common.types.core import FrozenModel, TimeRange

DataDomain = Literal["market", "derivatives", "on_chain", "macro", "sentiment_events"]


class DataConnectorError(RuntimeError):
    def __init__(
        self,
        *,
        data_domain: DataDomain,
        vendor: str,
        identifier: str | None = None,
        message: str,
        retryable: bool = False,
        code: str = "connector_error",
    ) -> None:
        self.data_domain = data_domain
        self.vendor = vendor
        self.identifier = identifier
        self.retryable = retryable
        self.code = code
        details = [f"{data_domain}/{vendor}"]
        if identifier:
            details.append(identifier)
        details.append(message)
        super().__init__(" :: ".join(details))

    def to_dict(self) -> dict[str, object]:
        return {
            "code": self.code,
            "data_domain": self.data_domain,
            "vendor": self.vendor,
            "identifier": self.identifier,
            "retryable": self.retryable,
            "message": str(self),
        }


class ConnectorRegistration(FrozenModel):
    data_domain: DataDomain
    vendor: str
    display_name: str
    capabilities: list[str] = Field(default_factory=list)
    requires_credentials: bool = False
    status: str = "contract_only"


class IngestionRequest(FrozenModel):
    data_domain: DataDomain
    vendor: str
    request_id: str
    time_range: TimeRange
    identifiers: list[str] = Field(default_factory=list)
    frequency: str | None = None
    options: dict[str, Any] = Field(default_factory=dict)


class IngestionCoverage(FrozenModel):
    start_time: datetime | None = None
    end_time: datetime | None = None
    complete: bool = False


class IngestionResult(FrozenModel):
    request_id: str
    data_domain: DataDomain
    vendor: str
    storage_uri: str
    normalized_uri: str | None = None
    manifest_uri: str | None = None
    coverage: IngestionCoverage = Field(default_factory=IngestionCoverage)
    status: str = "success"
    error_message: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class DataConnector(ABC):
    registration: ConnectorRegistration

    @abstractmethod
    def ingest(self, request: IngestionRequest) -> IngestionResult:
        raise NotImplementedError


class DomainIngestionService(ABC):
    @abstractmethod
    def register(self, connector: DataConnector) -> None:
        raise NotImplementedError

    @abstractmethod
    def resolve(self, data_domain: DataDomain, vendor: str) -> DataConnector | None:
        raise NotImplementedError
