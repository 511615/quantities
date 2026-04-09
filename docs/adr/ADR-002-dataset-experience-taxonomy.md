# ADR-002: Dataset Experience Taxonomy And BFF Contract Freeze

## Status

Accepted

## Context

The current dataset experience is still organized around asset symbols and market-only browsing assumptions. This creates four problems:

1. `DatasetSummaryView` and `DatasetDetailView` are biased toward `symbol + frequency`, which breaks down for multi-asset training panels.
2. The current dataset browse experience mixes directory, detail, education, and visualization concerns in one place.
3. The existing dataset service logic still tends to derive identity from `input_data_refs[0]`, which is incorrect for multi-input and multi-asset datasets.
4. The current `/api/datasets/{datasetId}/ohlcv` endpoint is useful for market slices, but cannot serve as the long-term abstraction for macro, on-chain, derivatives, or event series.

The workbench MVP now needs a stable control-layer contract that the data thread and frontend thread can implement against without re-negotiating core semantics.

## Decision

The repo will adopt a dataset taxonomy centered on `data_domain` and `dataset_type`, not on coin symbols.

### Fixed first-level taxonomy

- `data_domain`
  - `market`
  - `derivatives`
  - `on_chain`
  - `macro`
  - `sentiment_events`

### Fixed second-level taxonomy

- `dataset_type`
  - `display_slice`
  - `training_panel`
  - `feature_snapshot`

### Shared filtering dimensions

- `source_vendor`
- `exchange`
- `symbol`
- `frequency`
- `time_range`
- `snapshot_version`
- `label_horizon`
- `entity_scope`

### Required identity model

Every dataset must express two identities:

1. `dataset identity`
   - domain
   - type
   - source/vendor
   - snapshot/version
2. `coverage identity`
   - entity scope
   - entity count
   - time coverage
   - supported frequencies

`symbol` remains a filter and slice dimension, not the primary dataset identity.

## Control-Layer Rules

### Summary contract rules

- `DatasetSummaryView` remains the top-level browse contract name.
- It must evolve from a single-asset summary into a dataset-directory summary.
- It must not be derived from only the first input ref when multiple refs exist.

### Detail contract rules

- `DatasetDetailView` remains the detail contract name.
- It must expose grouped sections instead of a flat market-centric payload.
- It must distinguish visual slices from training-panel metadata.

### New BFF support contracts

- `DatasetFacetView`
  - powers filter UIs without frontend hardcoded enums
- `DatasetSliceView`
  - lists available visual slices for a dataset
- `DatasetSeriesView`
  - normalizes graph-ready time series payloads across domains
- `TrainingDatasetSummaryView`
  - surfaces training-panel-centric metadata

### Routing model

The target route set is:

- `/datasets`
- `/datasets/browser`
- `/datasets/:datasetId`
- `/datasets/training`

The target API set is:

- `GET /api/datasets`
- `GET /api/datasets/facets`
- `GET /api/datasets/{datasetId}`
- `GET /api/datasets/{datasetId}/slices`
- `GET /api/datasets/{datasetId}/series`
- `GET /api/datasets/training`
- `GET /api/datasets/{datasetId}/ohlcv`
  - retained as a temporary compatibility alias for market-series access

## Compatibility

- The `/api/datasets` namespace is preserved.
- Existing `DatasetSummaryView`, `DatasetDetailView`, and `/ohlcv` stay in place as names and compatibility surfaces.
- The current market-only series shape may remain temporarily, but new frontend work should prefer `slices` and `series`.
- No unrelated training, benchmark, backtest, agent, or model-runtime contracts are redefined by this ADR.

## Consequences

### Positive

- The frontend can navigate by dataset semantics instead of symbol assumptions.
- Multi-asset training datasets stop collapsing into a misleading single-symbol identity.
- The repo gains a scalable dataset browse model for non-market domains.
- Dataset filtering becomes backend-driven instead of frontend-hardcoded.

### Tradeoffs

- Existing dataset metadata assembly must become aggregation-based instead of first-ref-based.
- The frontend dataset pages must be split into clearer browse/detail/training responsibilities.
- Additional schema and compatibility tests are required during the transition.

## Acceptance Alignment

This ADR is considered correctly applied when:

1. Users can discover datasets by domain and type without relying on symbol-first navigation.
2. Multi-asset training panels clearly show multi-entity scope in summary and detail views.
3. The frontend can render filters, slices, and training summaries without guessing artifact internals.
4. `/api/datasets/{datasetId}/ohlcv` still works during the migration window.
