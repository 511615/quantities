# Dataset Information Architecture

## Purpose

This document freezes the control-layer semantics for the dataset experience refactor. It is the handoff contract between the `control`, `data`, `frontend`, and `test-audit` workstreams.

The scope of this document is:

- information architecture
- route map
- page responsibilities
- field dictionary
- compatibility strategy
- acceptance checks

It does not redefine low-level storage or model-training behavior.

## Route Map

### Frontend routes

- `/datasets`
  - dataset overview landing page
- `/datasets/browser`
  - filtered dataset browser
- `/datasets/:datasetId`
  - dataset detail page
- `/datasets/training`
  - training-panel comparison page

### API routes

- `GET /api/datasets/request-options`
  - request form options for dataset acquisition
- `POST /api/datasets/requests`
  - dataset acquisition/build launch endpoint
- `GET /api/datasets`
  - main dataset directory with filters
- `GET /api/datasets/facets`
  - filter values for browser UI
- `GET /api/datasets/{datasetId}`
  - dataset detail
- `GET /api/datasets/{datasetId}/slices`
  - visual slice directory
- `GET /api/datasets/{datasetId}/series`
  - graph-ready series payload
- `GET /api/datasets/training`
  - training panel summaries
- `GET /api/datasets/{datasetId}/ohlcv`
  - compatibility alias for market OHLCV access

## Page Responsibilities

### `/datasets`

Purpose:
- global map of dataset domains
- latest freshness and health snapshot
- entry points into browser and training pages

Must show:
- domain cards
- latest refresh summary
- health summary
- training panel shortcut
- concise explanation of `display_slice` vs `training_panel`

Must not do:
- primary time-series browsing
- tabbed pseudo-catalog mixing detail and education content

### `/datasets/browser`

Purpose:
- generalized browsing across all dataset domains and types

Filter order:
1. `data_domain`
2. `dataset_type`
3. `source_vendor`
4. `exchange`
5. `symbol`
6. `frequency`
7. `snapshot_version`
8. `time_from/time_to`

Result columns:
- `name`
- `data_domain`
- `dataset_type`
- `source/exchange`
- `coverage`
- `frequency`
- `version`
- `freshness`
- `health`

### `/datasets/:datasetId`

Purpose:
- one dataset, fully explained

Section order:
1. identity summary
2. source profile
3. coverage profile
4. schema and label summary
5. quality and freshness
6. visual slices
7. training compatibility
8. lineage

### `/datasets/training`

Purpose:
- compare `training_panel` datasets only

Primary comparison fields:
- dataset id
- snapshot version
- entity scope
- universe summary
- sample count
- feature count
- label count
- label horizon
- split strategy
- freshness
- health

## Field Dictionary

### Shared enums

#### `data_domain`

- `market`
- `derivatives`
- `on_chain`
- `macro`
- `sentiment_events`

#### `dataset_type`

- `display_slice`
- `training_panel`
- `feature_snapshot`

#### `entity_scope`

- `single_asset`
- `multi_asset`
- `cross_sectional`
- `global_macro`
- `event_stream`

These values may expand later, but frontend and backend must treat them as backend-owned enums.

### `DatasetSummaryView`

The summary contract is the directory contract.

Required fields:
- `dataset_id`
- `name`
- `data_domain`
- `dataset_type`
- `source_vendor`
- `exchange`
- `snapshot_version`
- `entity_scope`
- `entity_count`
- `symbols_preview`
- `supported_frequencies`
- `coverage_start`
- `coverage_end`
- `freshness_status`
- `health_status`
- `slice_count`

Notes:
- `symbol` is not the title.
- `exchange` may be `null`.
- `symbols_preview` may be empty for macro and event datasets.
- `supported_frequencies` may be empty where not applicable.

### `DatasetDetailView`

The detail contract is grouped, not flat.

Required top-level groups:
- `identity`
- `source_profile`
- `coverage_profile`
- `schema_profile`
- `quality_profile`
- `update_profile`
- `training_profile`
- `visual_slices`
- `lineage`

Rules:
- `identity` describes what the dataset is.
- `coverage_profile` describes what it covers.
- `training_profile` describes whether and how it is used for training.
- `visual_slices` lists sliceable subviews; it is not a training profile.

### `DatasetFacetView`

Purpose:
- backend-owned filter metadata

Required fields:
- `facet_name`
- `label`
- `values`

Each facet value should include:
- `value`
- `label`
- `count`

### `DatasetSliceView`

Purpose:
- detail-page slice selector

Required fields:
- `slice_id`
- `slice_type`
- `symbol`
- `exchange`
- `frequency`
- `metrics_available`
- `time_range`

Rules:
- a dataset may have zero slices
- a multi-asset training panel may expose many display slices
- non-market datasets may expose slices without `symbol`

### `DatasetSeriesView`

Purpose:
- unified chart payload across domains

Required fields:
- `slice_id`
- `series_kind`
- `columns`
- `points`
- `missing_segments`
- `last_updated_at`

Rules:
- market OHLCV is one `series_kind`, not the only one
- chain, macro, derivatives, and sentiment series use the same transport contract

### `TrainingDatasetSummaryView`

Purpose:
- compare training-ready datasets without drilling into detail pages

Required fields:
- `dataset_id`
- `snapshot_version`
- `entity_scope`
- `universe_summary`
- `sample_count`
- `feature_count`
- `label_count`
- `label_horizon`
- `split_strategy`
- `temporal_safety_summary`
- `source_dependencies`

## Naming Rules

### Dataset display name

Primary display name should follow:

`{data_domain} / {source_vendor} / {dataset_type} / {snapshot_version}`

Optional subtitle may include:
- exchange
- entity scope
- symbol preview
- frequency preview

Do not use `symbol + frequency` as the primary title for dataset summary cards.

### Symbol placement

- `symbol` belongs in filters, slice lists, and subtitles
- `symbol` does not define dataset identity for training panels

## Aggregation Rules

The service layer must aggregate all relevant input refs when producing dataset identity.

It must not:
- derive the dataset identity from only `input_data_refs[0]`
- collapse multi-asset datasets into the first symbol
- assume `exchange` always exists
- assume `symbol` always exists

It must derive:
- combined universe coverage
- multi-source profile where applicable
- supported frequency set
- sliceability summary

## Compatibility Strategy

### Preserved

- `/api/datasets`
- `/api/datasets/{datasetId}`
- `/api/datasets/{datasetId}/ohlcv`
- `DatasetSummaryView`
- `DatasetDetailView`

### Added incrementally

- `/api/datasets/facets`
- `/api/datasets/{datasetId}/slices`
- `/api/datasets/{datasetId}/series`
- `/api/datasets/training`

### Migration rule

The old OHLCV endpoint remains valid for one migration cycle.

Frontend target:
- dataset request submission should use `POST /api/datasets/requests`
- dataset request status should use the existing `/api/jobs/{job_id}` flow
- browser and detail pages should move to `facets`, `slices`, and `series`
- market detail pages may keep calling `ohlcv` until the new series path is adopted

## Acceptance Checks

The control-layer design is accepted when:

1. A user can find a `market`, `macro`, or `training_panel` dataset in two to three steps without symbol-first navigation.
2. Multi-asset training datasets visibly show multi-entity scope rather than a single symbol identity.
3. Dataset detail pages clearly expose domain, source, version, coverage, freshness, health, and training compatibility from backend-owned fields.
4. Browser filters are backend-driven and URL-restorable.
5. The old OHLCV API remains available during frontend migration.
