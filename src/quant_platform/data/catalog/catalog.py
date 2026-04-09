from __future__ import annotations

from pathlib import Path

from quant_platform.common.hashing.digest import stable_digest
from quant_platform.common.io.files import LocalArtifactStore
from quant_platform.common.types.core import TimeRange
from quant_platform.data.contracts.data_asset import DataAssetRef
from quant_platform.data.contracts.market import DataSnapshotManifest, NormalizedMarketBar
from quant_platform.data.quality.validators import MarketDataValidator


class DataCatalog:
    def __init__(self, artifact_root: Path) -> None:
        self.store = LocalArtifactStore(artifact_root)

    def register_market_asset(
        self,
        asset_id: str,
        source: str,
        frequency: str,
        rows: list[NormalizedMarketBar],
        schema_version: int = 1,
        *,
        tags: list[str] | None = None,
        request_origin: str | None = None,
        fallback_used: bool = False,
    ) -> tuple[DataAssetRef, DataSnapshotManifest]:
        quality_report = MarketDataValidator.validate(asset_id, rows)
        if not quality_report.passed:
            raise ValueError(f"data quality checks failed for asset '{asset_id}'")
        quality_artifact = self.store.write_model(
            f"datasets/{asset_id}_quality.json", quality_report
        )
        sorted_rows = sorted(rows, key=lambda row: row.event_time)
        payload = {
            "rows": [row.model_dump(mode="json") for row in sorted_rows],
        }
        data_artifact = self.store.write_json(f"datasets/{asset_id}_bars.json", payload)
        manifest = DataSnapshotManifest(
            asset_id=asset_id,
            schema_name="normalized_market_bar",
            schema_version=schema_version,
            symbol=sorted_rows[0].symbol,
            venue=sorted_rows[0].venue,
            frequency=frequency,
            row_count=len(sorted_rows),
            event_start=sorted_rows[0].event_time,
            event_end=sorted_rows[-1].event_time,
            available_start=sorted_rows[0].available_time,
            available_end=sorted_rows[-1].available_time,
            columns=MarketDataValidator.REQUIRED_COLUMNS,
            quality_report_uri=quality_artifact.uri,
            entity_scope=("multi_asset" if quality_report.entity_count > 1 else "single_asset"),
            entity_count=quality_report.entity_count,
            request_origin=request_origin,
            fallback_used=fallback_used,
        )
        manifest_artifact = self.store.write_model(f"datasets/{asset_id}_manifest.json", manifest)
        asset_ref = DataAssetRef(
            asset_id=asset_id,
            schema_version=schema_version,
            source=source,
            symbol=sorted_rows[0].symbol,
            venue=sorted_rows[0].venue,
            frequency=frequency,
            time_range=TimeRange(start=sorted_rows[0].event_time, end=sorted_rows[-1].event_time),
            storage_uri=f"artifact://datasets/{asset_id}_bars.json",
            content_hash=stable_digest(payload),
            entity_key=sorted_rows[0].symbol,
            tags=list(tags or []),
            request_origin=request_origin,
            fallback_used=fallback_used,
        )
        self.store.write_model(f"datasets/{asset_id}_ref.json", asset_ref)
        _ = manifest_artifact
        return asset_ref, manifest
