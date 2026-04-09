# Interface Contracts

## Core Domain Contracts

- `DataAssetRef`
  Immutable reference to normalized market data.
- `FeatureViewRef`
  Feature snapshot constrained by `as_of_time` and lineage.
- `DatasetRef`
  The only supported entrypoint for training and evaluation inputs.
- `ModelSpec`
  Declares model family, schemas, version, and hyperparameters.
- `FitRequest` / `FitResult`
  Stable training contracts used by the workflow train stage.
- `PredictRequest` / `PredictionFrame`
  Stable prediction contracts used by the workflow predict stage.
- `BacktestRequest` / `BacktestResult`
  Stable backtest contracts used by the workflow backtest stage.
- `ResearchAgentRequest` / `ResearchAgentResponse`
  Stable agent contracts used by the workflow review stage.

## Workflow Contracts

### Stage Requests

- `PrepareWorkflowRequest`
  Declares data source, feature set, label spec, split policy, and explicit market bars or equivalent inputs.
- `TrainWorkflowRequest`
  Declares `DatasetRef`, model specs, trainer config, tracking context, and seed.
- `PredictWorkflowRequest`
  Declares `DatasetRef`, fit results or fit result refs, and prediction scope.
- `BenchmarkWorkflowRequest`
  Declares dataset, model set, trainer config, prediction scope, and rolling window spec.
- `BacktestWorkflowRequest`
  Declares prediction inputs and a `BacktestRequest` template.
- `ReviewWorkflowRequest`
  Declares artifact refs, review goal, comparison mode, allowed tools, and guardrails.
- `WorkflowRunRequest`
  Declares workflow id, stage order, and per-stage requests for pipeline execution.

### Stage Results

- `PrepareWorkflowResult`
  Returns `data_asset_ref`, `feature_view_ref`, `dataset_ref`, and persisted manifest refs.
- `TrainWorkflowResult`
  Returns fit results and a training leaderboard.
- `PredictWorkflowResult`
  Returns prediction frames and prediction artifact refs.
- `BenchmarkWorkflowResult`
  Returns ranked benchmark rows plus summary artifacts.
- `BacktestWorkflowResult`
  Returns backtest results keyed by prediction input.
- `ReviewWorkflowResult`
  Returns the structured research-agent response and audit refs.
- `WorkflowRunResult`
  Returns completed stages, per-stage status, and accumulated artifact refs.

## Boundary Rules

- Features cannot generate labels.
- Training must consume `DatasetRef`, not raw tables.
- Backtest must consume prediction artifacts, not training internals.
- Review must consume artifacts and public contracts, not internal objects.
- Workflow services coordinate stages but do not redefine lower-layer contract semantics.

## Dataset BFF Contracts

The dataset experience keeps the `/api/datasets` namespace and extends it with browse-oriented and training-oriented summaries.

### Taxonomy

- `data_domain`
  - `market`
  - `derivatives`
  - `on_chain`
  - `macro`
  - `sentiment_events`
- `dataset_type`
  - `display_slice`
  - `training_panel`
  - `feature_snapshot`

### Directory Contracts

- `DatasetAcquisitionRequest`
  Launch payload for `POST /api/datasets/requests`.
  This request is submitted through the existing job system rather than a separate async mechanism.

- `DatasetRequestOptionsView`
  Request-form options returned by `GET /api/datasets/request-options`.

- `DatasetSummaryView`
  Directory-level summary used by `/api/datasets`.
  Required fields include:
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

- `DatasetFacetView`
  Filter metadata used by `/api/datasets/facets`.

- `TrainingDatasetSummaryView`
  Training-panel summary used by `/api/datasets/training`.

### Detail Contracts

- `DatasetDetailView`
  Grouped detail response for `/api/datasets/{datasetId}`.
  Expected sections:
  - `identity`
  - `source_profile`
  - `coverage_profile`
  - `schema_profile`
  - `quality_profile`
  - `update_profile`
  - `training_profile`
  - `visual_slices`
  - `lineage`

- `DatasetSliceView`
  Slice directory contract for `/api/datasets/{datasetId}/slices`.

- `DatasetSeriesView`
  Unified time-series contract for `/api/datasets/{datasetId}/series`.

### Compatibility Notes

- Dataset request submission must reuse `LaunchJobResponse` and `/api/jobs/{job_id}`.
- `/api/datasets/{datasetId}/ohlcv` remains valid during migration.
- Frontend should treat `ohlcv` as a compatibility alias for market-series rendering, not as the permanent cross-domain series contract.
- Dataset identity must be aggregated across all relevant input refs; services must not treat the first input ref as the whole dataset.
- Frontend handoff into training must converge on `/models?launchTrain=1&datasetId=<dataset_id>`.
- `LaunchTrainDrawer` dataset-aware contract is frozen around `defaultOpen`, `datasetId`, and `datasetLabel`.
- When `datasetId` is present, frontend must send `dataset_id` and must not silently fall back to preset-only training mode.
