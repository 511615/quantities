from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from quant_platform.webapi.repositories.artifacts import ArtifactRepository


@dataclass(frozen=True)
class DatasetRegistryEntry:
    dataset_id: str
    ref_uri: str
    manifest_uri: str | None
    samples_uri: str | None
    feature_view_uri: str | None
    data_domain: str
    dataset_type: str
    source_vendor: str | None
    exchange: str | None
    frequency: str | None
    entity_scope: str | None
    entity_count: int | None
    snapshot_version: str | None
    build_status: str | None
    readiness_status: str | None
    quality_status: str | None
    as_of_time: str | None
    data_start_time: str | None
    data_end_time: str | None
    raw_row_count: int | None
    usable_row_count: int | None
    feature_count: int | None
    label_count: int | None
    request_origin: str | None
    payload_json: str
    manifest_json: str | None
    updated_at: str

    @property
    def payload(self) -> dict[str, Any]:
        return json.loads(self.payload_json)

    @property
    def manifest(self) -> dict[str, Any]:
        if not self.manifest_json:
            return {}
        return json.loads(self.manifest_json)


@dataclass(frozen=True)
class DatasetDependencyEntry:
    dataset_id: str
    dependency_kind: str
    dependency_id: str
    dependency_label: str | None
    target_dataset_id: str | None
    payload_json: str

    @property
    def payload(self) -> dict[str, Any]:
        return json.loads(self.payload_json)


class DatasetRegistryRepository:
    def __init__(self, artifact_root: Path, repository: ArtifactRepository) -> None:
        self.artifact_root = artifact_root.resolve()
        self.repository = repository
        self.registry_root = self.artifact_root / "webapi" / "registry"
        self.registry_root.mkdir(parents=True, exist_ok=True)
        self.db_path = self.registry_root / "datasets.sqlite3"
        self._lock = threading.RLock()
        self._bootstrap_signature: tuple[int, int] | None = None
        self._init_db()

    def list_entries(self) -> list[DatasetRegistryEntry]:
        with self._lock:
            self.bootstrap_from_artifacts()
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT dataset_id, ref_uri, manifest_uri, samples_uri, feature_view_uri, data_domain,
                           dataset_type, source_vendor, exchange, frequency, entity_scope, entity_count,
                           snapshot_version, build_status, readiness_status, quality_status, as_of_time,
                           data_start_time, data_end_time, raw_row_count, usable_row_count, feature_count,
                           label_count, request_origin, payload_json, manifest_json, updated_at
                    FROM datasets
                    ORDER BY COALESCE(as_of_time, updated_at) DESC, dataset_id ASC
                    """
                ).fetchall()
        return [DatasetRegistryEntry(*row) for row in rows]

    def get_entry(self, dataset_id: str) -> DatasetRegistryEntry | None:
        with self._lock:
            self.bootstrap_from_artifacts()
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT dataset_id, ref_uri, manifest_uri, samples_uri, feature_view_uri, data_domain,
                           dataset_type, source_vendor, exchange, frequency, entity_scope, entity_count,
                           snapshot_version, build_status, readiness_status, quality_status, as_of_time,
                           data_start_time, data_end_time, raw_row_count, usable_row_count, feature_count,
                           label_count, request_origin, payload_json, manifest_json, updated_at
                    FROM datasets
                    WHERE dataset_id = ?
                    """,
                    (dataset_id,),
                ).fetchone()
        return DatasetRegistryEntry(*row) if row else None

    def list_dependencies(self, dataset_id: str) -> list[DatasetDependencyEntry]:
        with self._lock:
            self.bootstrap_from_artifacts()
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT dataset_id, dependency_kind, dependency_id, dependency_label, target_dataset_id, payload_json
                    FROM dataset_dependencies
                    WHERE dataset_id = ?
                    ORDER BY dependency_kind ASC, dependency_id ASC
                    """,
                    (dataset_id,),
                ).fetchall()
        return [DatasetDependencyEntry(*row) for row in rows]

    def remove_dataset(self, dataset_id: str) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute("DELETE FROM dataset_dependencies WHERE dataset_id = ?", (dataset_id,))
                conn.execute("DELETE FROM datasets WHERE dataset_id = ?", (dataset_id,))
                conn.commit()

    def bootstrap_from_artifacts(self) -> None:
        with self._lock:
            ref_paths = sorted(self.artifact_root.glob("datasets/*_dataset_ref.json"))
            signature = self._bootstrap_signature_for(ref_paths)
            if signature == self._bootstrap_signature:
                return
            live_dataset_ids: set[str] = set()
            with self._connect() as conn:
                for ref_path in ref_paths:
                    payload = self._safe_load(ref_path)
                    dataset_id = str(payload.get("dataset_id", "")).strip()
                    if not dataset_id:
                        continue
                    live_dataset_ids.add(dataset_id)
                    manifest_path = self._manifest_path_for_payload(dataset_id, payload)
                    manifest = self._safe_load(manifest_path) if manifest_path else {}
                    record = self._build_dataset_record(ref_path, payload, manifest_path, manifest)
                    conn.execute(
                        """
                        INSERT INTO datasets (
                            dataset_id, ref_uri, manifest_uri, samples_uri, feature_view_uri, data_domain,
                            dataset_type, source_vendor, exchange, frequency, entity_scope, entity_count,
                            snapshot_version, build_status, readiness_status, quality_status, as_of_time,
                            data_start_time, data_end_time, raw_row_count, usable_row_count, feature_count,
                            label_count, request_origin, payload_json, manifest_json, updated_at
                        ) VALUES (
                            :dataset_id, :ref_uri, :manifest_uri, :samples_uri, :feature_view_uri, :data_domain,
                            :dataset_type, :source_vendor, :exchange, :frequency, :entity_scope, :entity_count,
                            :snapshot_version, :build_status, :readiness_status, :quality_status, :as_of_time,
                            :data_start_time, :data_end_time, :raw_row_count, :usable_row_count, :feature_count,
                            :label_count, :request_origin, :payload_json, :manifest_json, :updated_at
                        )
                        ON CONFLICT(dataset_id) DO UPDATE SET
                            ref_uri=excluded.ref_uri,
                            manifest_uri=excluded.manifest_uri,
                            samples_uri=excluded.samples_uri,
                            feature_view_uri=excluded.feature_view_uri,
                            data_domain=excluded.data_domain,
                            dataset_type=excluded.dataset_type,
                            source_vendor=excluded.source_vendor,
                            exchange=excluded.exchange,
                            frequency=excluded.frequency,
                            entity_scope=excluded.entity_scope,
                            entity_count=excluded.entity_count,
                            snapshot_version=excluded.snapshot_version,
                            build_status=excluded.build_status,
                            readiness_status=excluded.readiness_status,
                            quality_status=excluded.quality_status,
                            as_of_time=excluded.as_of_time,
                            data_start_time=excluded.data_start_time,
                            data_end_time=excluded.data_end_time,
                            raw_row_count=excluded.raw_row_count,
                            usable_row_count=excluded.usable_row_count,
                            feature_count=excluded.feature_count,
                            label_count=excluded.label_count,
                            request_origin=excluded.request_origin,
                            payload_json=excluded.payload_json,
                            manifest_json=excluded.manifest_json,
                            updated_at=excluded.updated_at
                        """,
                        record,
                    )
                    conn.execute("DELETE FROM dataset_dependencies WHERE dataset_id = ?", (dataset_id,))
                    for dependency in self._build_dependency_rows(payload, manifest):
                        conn.execute(
                            """
                            INSERT INTO dataset_dependencies (
                                dataset_id, dependency_kind, dependency_id, dependency_label, target_dataset_id, payload_json
                            ) VALUES (
                                :dataset_id, :dependency_kind, :dependency_id, :dependency_label, :target_dataset_id, :payload_json
                            )
                            """,
                            dependency,
                        )
                existing_ids = {
                    str(row[0])
                    for row in conn.execute("SELECT dataset_id FROM datasets").fetchall()
                }
                stale_ids = existing_ids - live_dataset_ids
                for stale_id in stale_ids:
                    conn.execute("DELETE FROM dataset_dependencies WHERE dataset_id = ?", (stale_id,))
                    conn.execute("DELETE FROM datasets WHERE dataset_id = ?", (stale_id,))
                conn.commit()
            self._bootstrap_signature = signature

    def _init_db(self) -> None:
        with self._connect() as conn:
            self._create_schema(conn)
            conn.commit()

    def _connect(self) -> sqlite3.Connection:
        self.registry_root.mkdir(parents=True, exist_ok=True)
        try:
            return self._open_connection()
        except sqlite3.DatabaseError as exc:
            if not self._is_recoverable_db_error(exc):
                raise
            self._reset_db_files()
            return self._open_connection()

    def _open_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL")
        except sqlite3.OperationalError:
            conn.execute("PRAGMA journal_mode=DELETE")
        conn.execute("PRAGMA busy_timeout = 30000")
        self._create_schema(conn)
        quick_check = conn.execute("PRAGMA quick_check").fetchone()
        if quick_check and str(quick_check[0]).lower() != "ok":
            raise sqlite3.DatabaseError(f"database integrity check failed: {quick_check[0]}")
        return conn

    def _create_schema(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS datasets (
                dataset_id TEXT PRIMARY KEY,
                ref_uri TEXT NOT NULL,
                manifest_uri TEXT,
                samples_uri TEXT,
                feature_view_uri TEXT,
                data_domain TEXT NOT NULL,
                dataset_type TEXT NOT NULL,
                source_vendor TEXT,
                exchange TEXT,
                frequency TEXT,
                entity_scope TEXT,
                entity_count INTEGER,
                snapshot_version TEXT,
                build_status TEXT,
                readiness_status TEXT,
                quality_status TEXT,
                as_of_time TEXT,
                data_start_time TEXT,
                data_end_time TEXT,
                raw_row_count INTEGER,
                usable_row_count INTEGER,
                feature_count INTEGER,
                label_count INTEGER,
                request_origin TEXT,
                payload_json TEXT NOT NULL,
                manifest_json TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dataset_dependencies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id TEXT NOT NULL,
                dependency_kind TEXT NOT NULL,
                dependency_id TEXT NOT NULL,
                dependency_label TEXT,
                target_dataset_id TEXT,
                payload_json TEXT NOT NULL
            )
            """
        )

    @staticmethod
    def _is_recoverable_db_error(exc: sqlite3.DatabaseError) -> bool:
        message = str(exc).lower()
        return any(
            marker in message
            for marker in (
                "malformed",
                "not a database",
                "disk image is malformed",
                "integrity check failed",
            )
        )

    def _reset_db_files(self) -> None:
        for path in (
            self.db_path,
            self.registry_root / f"{self.db_path.name}-wal",
            self.registry_root / f"{self.db_path.name}-shm",
        ):
            path.unlink(missing_ok=True)
        self._bootstrap_signature = None

    @staticmethod
    def _bootstrap_signature_for(ref_paths: list[Path]) -> tuple[int, int]:
        latest_mtime_ns = max((path.stat().st_mtime_ns for path in ref_paths), default=0)
        return (len(ref_paths), latest_mtime_ns)

    def _manifest_path_for_payload(self, dataset_id: str, payload: dict[str, Any]) -> Path | None:
        manifest_uri = payload.get("dataset_manifest_uri")
        if isinstance(manifest_uri, str) and manifest_uri:
            return self.repository.resolve_uri(manifest_uri.replace("\\", "/"))
        fallback = self.artifact_root / "datasets" / f"{dataset_id}_dataset_manifest.json"
        return fallback if fallback.exists() else None

    def _build_dataset_record(
        self,
        ref_path: Path,
        payload: dict[str, Any],
        manifest_path: Path | None,
        manifest: dict[str, Any],
    ) -> dict[str, Any]:
        dataset_id = str(payload.get("dataset_id"))
        acquisition_profile = manifest.get("acquisition_profile") or {}
        input_refs = (payload.get("feature_view_ref") or {}).get("input_data_refs") or []
        feature_schema = (payload.get("feature_view_ref") or {}).get("feature_schema") or []
        label_spec = payload.get("label_spec") or {}
        starts = [
            self._safe_iso((item.get("time_range") or {}).get("start"))
            for item in input_refs
            if isinstance(item, dict)
        ]
        ends = [
            self._safe_iso((item.get("time_range") or {}).get("end"))
            for item in input_refs
            if isinstance(item, dict)
        ]
        symbols = [
            str(item.get("symbol"))
            for item in input_refs
            if isinstance(item, dict) and isinstance(item.get("symbol"), str)
        ]
        recommended_use = self._as_str((payload.get("sample_policy") or {}).get("recommended_training_use"))
        dataset_type = (
            self._as_str(acquisition_profile.get("dataset_type"))
            or recommended_use
            or "display_slice"
        )
        if dataset_type not in {
            "display_slice",
            "training_panel",
            "feature_snapshot",
            "fusion_training_panel",
        }:
            dataset_type = "training_panel" if recommended_use == "training_panel" else "display_slice"
        frequency = self._first_str([acquisition_profile.get("frequency"), *(item.get("frequency") for item in input_refs if isinstance(item, dict))])
        timestamp_paths = [ref_path]
        if manifest_path is not None and manifest_path.exists():
            timestamp_paths.append(manifest_path)
        updated_at = datetime.fromtimestamp(
            max(path.stat().st_mtime for path in timestamp_paths),
            tz=UTC,
        ).isoformat()
        return {
            "dataset_id": dataset_id,
            "ref_uri": str(ref_path.resolve()),
            "manifest_uri": str(manifest_path.resolve()) if manifest_path else None,
            "samples_uri": self._as_str(payload.get("dataset_samples_uri")),
            "feature_view_uri": str(
                (self.artifact_root / "datasets" / f"{dataset_id}_feature_view_ref.json").resolve()
            ),
            "data_domain": self._as_str(acquisition_profile.get("data_domain")) or "market",
            "dataset_type": dataset_type,
            "source_vendor": self._as_str(acquisition_profile.get("source_vendor")),
            "exchange": self._as_str(acquisition_profile.get("exchange")),
            "frequency": frequency,
            "entity_scope": self._as_str(payload.get("entity_scope"))
            or self._as_str(manifest.get("entity_scope"))
            or ("multi_asset" if len(set(symbols)) > 1 else "single_asset"),
            "entity_count": self._as_int(payload.get("entity_count"))
            or self._as_int(manifest.get("entity_count"))
            or max(len(set(symbols)), 1),
            "snapshot_version": self._as_str(manifest.get("snapshot_version")),
            "build_status": self._as_str(manifest.get("build_status")),
            "readiness_status": self._as_str(payload.get("readiness_status"))
            or self._as_str(manifest.get("readiness_status")),
            "quality_status": self._as_str(manifest.get("quality_status")),
            "as_of_time": self._safe_iso((payload.get("feature_view_ref") or {}).get("as_of_time")),
            "data_start_time": min((item for item in starts if item is not None), default=None),
            "data_end_time": max((item for item in ends if item is not None), default=None),
            "raw_row_count": self._as_int(manifest.get("raw_row_count")),
            "usable_row_count": self._as_int(manifest.get("usable_sample_count")),
            "feature_count": len([item for item in feature_schema if isinstance(item, dict)]),
            "label_count": len(
                [
                    item
                    for item in [label_spec.get("target_column"), *((label_spec.get("label_columns") or []))]
                    if isinstance(item, str) and item
                ]
            ),
            "request_origin": self._as_str(acquisition_profile.get("request_origin")),
            "payload_json": json.dumps(payload, sort_keys=True, default=str),
            "manifest_json": json.dumps(manifest, sort_keys=True, default=str) if manifest else None,
            "updated_at": updated_at,
        }

    def _build_dependency_rows(
        self,
        payload: dict[str, Any],
        manifest: dict[str, Any],
    ) -> list[dict[str, Any]]:
        dataset_id = str(payload.get("dataset_id", "unknown"))
        input_refs = (payload.get("feature_view_ref") or {}).get("input_data_refs") or []
        rows: list[dict[str, Any]] = []
        for item in input_refs:
            if not isinstance(item, dict):
                continue
            dependency_id = self._as_str(item.get("asset_id")) or self._as_str(item.get("storage_uri"))
            if not dependency_id:
                continue
            rows.append(
                {
                    "dataset_id": dataset_id,
                    "dependency_kind": "data_asset",
                    "dependency_id": dependency_id,
                    "dependency_label": self._as_str(item.get("symbol")) or dependency_id,
                    "target_dataset_id": None,
                    "payload_json": json.dumps(item, sort_keys=True, default=str),
                }
            )
        acquisition_profile = manifest.get("acquisition_profile") if isinstance(manifest, dict) else {}
        source_dataset_ids = (
            acquisition_profile.get("source_dataset_ids")
            if isinstance(acquisition_profile, dict)
            else []
        )
        for source_dataset_id in source_dataset_ids if isinstance(source_dataset_ids, list) else []:
            if not isinstance(source_dataset_id, str) or not source_dataset_id:
                continue
            rows.append(
                {
                    "dataset_id": dataset_id,
                    "dependency_kind": "source_dataset",
                    "dependency_id": source_dataset_id,
                    "dependency_label": source_dataset_id,
                    "target_dataset_id": source_dataset_id,
                    "payload_json": json.dumps(
                        {"source_dataset_id": source_dataset_id},
                        sort_keys=True,
                        default=str,
                    ),
                }
            )
        return rows

    def _safe_load(self, path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            return {}

    @staticmethod
    def _as_str(value: Any) -> str | None:
        return value if isinstance(value, str) and value else None

    @staticmethod
    def _as_int(value: Any) -> int | None:
        return int(value) if isinstance(value, (int, float)) else None

    @staticmethod
    def _safe_iso(value: Any) -> str | None:
        return value if isinstance(value, str) and value else None

    @staticmethod
    def _first_str(values: list[Any]) -> str | None:
        for value in values:
            if isinstance(value, str) and value:
                return value
        return None
