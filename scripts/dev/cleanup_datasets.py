from __future__ import annotations

from argparse import ArgumentParser
from pathlib import Path
import sys

from quant_platform.common.config.loader import load_app_config
from quant_platform.webapi.repositories.artifacts import ArtifactRepository
from quant_platform.webapi.repositories.dataset_registry import DatasetRegistryRepository
from quant_platform.webapi.services.dataset_cleanup import DatasetCleanupService

PATTERN_PREFIXES = [
    "workbench_",
    "live_",
    "real_",
    "smoke_",
    "btc_sentiment_smoke_",
    "pipeline_",
    "multi_domain_",
    "fusion_",
    "baseline_",
    "debug_",
    "frontend_contract_",
    "research_",
    "blocked_",
    "multi_asset_request",
    "market_macro_onchain",
]

PATTERN_SUBSTRINGS = [
    "market_anchor",
    "btc_sentiment_smoke",
    "workbench_",
    "live_",
    "real_",
    "smoke_",
    "pipeline_",
    "multi_domain_",
    "fusion_",
    "baseline_",
    "internal_smoke",
    "_fusion",
    "multi_asset_request",
    "market_macro_onchain",
    "frontend_contract",
    "blocked_train",
    "dataset_id_train_request",
    "research_dataset",
    "macro_onchain_fusion",
]


def is_cleanup_target(entry, *, delete_all: bool = False) -> bool:
    if delete_all:
        return True
    searchable = " ".join(
        filter(
            None,
            [
                entry.dataset_id,
                entry.source_vendor,
                entry.request_origin,
                entry.data_domain,
                entry.dataset_type,
            ],
        )
    ).lower()
    return any(entry.dataset_id.startswith(prefix) for prefix in PATTERN_PREFIXES) or any(
        token in searchable for token in PATTERN_SUBSTRINGS
    )


def list_targets(
    registry: DatasetRegistryRepository,
    *,
    delete_all: bool = False,
) -> list[str]:
    return [
        entry.dataset_id
        for entry in registry.list_entries()
        if is_cleanup_target(entry, delete_all=delete_all)
    ]


def main(
    *,
    dry_run: bool = False,
    artifact_root: Path | None = None,
    delete_all: bool = False,
) -> int:
    config = load_app_config()
    root = artifact_root or Path(config.env.artifact_root)
    repository = ArtifactRepository(root)
    registry = DatasetRegistryRepository(root, repository)
    cleanup_service = DatasetCleanupService(repository, registry)
    targets = list_targets(registry, delete_all=delete_all)
    if not targets:
        print("No datasets matching cleanup prefixes were found.")
        return 0
    scope_label = "all datasets" if delete_all else "datasets that match cleanup prefixes"
    print(f"Found {len(targets)} {scope_label}.")
    if dry_run:
        for dataset_id in targets:
            print(f" - {dataset_id}")
        return 0
    deleted_total = 0
    for dataset_id in targets:
        deleted = cleanup_service.hard_delete_dataset(dataset_id)
        deleted_total += len(deleted)
        print(f"Deleted {dataset_id}: {len(deleted)} artifact paths.")
    print(f"Removed {len(targets)} datasets and {deleted_total} artifact files.")
    return 0


if __name__ == "__main__":
    parser = ArgumentParser(description="Prune old workbench/test datasets.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List matching datasets without deleting anything.",
    )
    parser.add_argument(
        "--artifact-root",
        type=Path,
        help="Override artifact root used for cleanup.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Hard delete every dataset currently registered under the target artifact root.",
    )
    args = parser.parse_args()
    sys.exit(main(dry_run=args.dry_run, artifact_root=args.artifact_root, delete_all=args.all))
