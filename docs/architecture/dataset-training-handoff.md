# Dataset To Training Handoff

## Purpose

This document freezes the control-thread rules for the frontend handoff from dataset request to model training.

The scope is intentionally narrow:

- dataset request success state
- training-entry routing
- `LaunchTrainDrawer` interface freeze
- query-string behavior
- readiness gating behavior
- acceptance checks for frontend and test-audit threads

It does not redefine dataset architecture, training internals, or page hierarchy.

## Fixed Page Responsibilities

### `/datasets` and dataset request drawers

- Dataset request submission stays inside the existing dataset pages.
- After a dataset request succeeds, the primary CTA must guide the user into training.
- The primary success CTA target is:
  - `/models?launchTrain=1&datasetId=<dataset_id>`
- Opening dataset detail remains a secondary CTA:
  - `/datasets/<dataset_id>`

### `/models`

- `/models` is the only page that owns training configuration and training submission.
- `/models` must accept dataset-driven launches from query params.
- `/models` must not silently fall back to preset-only mode when `datasetId` is present.

### `/datasets/training`

- `/datasets/training` is the dataset selection page for training-ready datasets.
- It may expose per-row training CTA buttons.
- It must not take over model configuration or training submission from `/models`.

## Route And Query Contract

### Training handoff query

The canonical handoff query is:

- `launchTrain=1`
- `datasetId=<dataset_id>`

Canonical target:

- `/models?launchTrain=1&datasetId=<dataset_id>`

### Behavior rules

- When `launchTrain=1` and `datasetId` are both present, `/models` must auto-open the training drawer.
- Refreshing the page with the same query must preserve the same open-and-prefilled behavior.
- The frontend must not drop `datasetId` during navigation from dataset pages into `/models`.
- After training submission, the app may keep or clear the query, but the behavior must remain deterministic.

## `LaunchTrainDrawer` Interface Freeze

The frontend implementation must treat the drawer contract as frozen for this round.

Required props:

- `defaultOpen?: boolean`
- `datasetId?: string`
- `datasetLabel?: string`

Existing optional presentation props may remain, but these three props define the dataset-to-training handoff contract.

## Drawer Behavior Rules

### Dataset-aware mode

When `datasetId` is provided:

- the drawer enters dataset-aware mode
- the request body must send `dataset_id`
- preset-only dataset selection UI must be hidden or disabled
- the UI must clearly show which dataset is being trained

### Preset-only mode

When `datasetId` is absent:

- the legacy preset-based flow may remain available
- this is a legacy compatibility path, not the preferred handoff path from dataset pages

### No silent fallback

If the page entered dataset-aware mode with a `datasetId`, the drawer must not quietly switch back to preset-only behavior because of partial UI state, refreshes, or query parsing issues.

## Readiness Gating

Readiness is backend-owned and must not be guessed by the frontend.

### Required rules

- if `readiness_status == "not_ready"`, training must be blocked
- the blocking reason must be visible inside the drawer
- the user should be guided to:
  - `/datasets/<dataset_id>`, or
  - `/datasets/training`

### Allowed rules

- `ready` may train normally
- `warning` may train, but warning text should stay visible

## Dataset Request Success State

`DatasetRequestDrawer` success UI is frozen as:

Primary CTA:

- label: `用此数据集训练`
- target: `/models?launchTrain=1&datasetId=<new_dataset_id>`

Secondary CTA:

- label equivalent to opening dataset detail
- target: `/datasets/<new_dataset_id>`

Required query invalidation after success:

- `datasets`
- `datasets/training`
- `jobs`

## `/datasets/training` CTA Rules

For each row:

- `readinessStatus === "not_ready"`
  - do not show a training button
  - show the blocking reason
- `readinessStatus === "ready"` or `readinessStatus === "warning"`
  - show a training CTA
  - CTA target: `/models?launchTrain=1&datasetId=<dataset_id>`

## Acceptance Checks

This handoff is accepted when:

1. A user can request a dataset and reach `/models` training setup in no more than two steps after success.
2. `/models` opens the training drawer automatically when loaded with `launchTrain=1&datasetId=...`.
3. The drawer sends `dataset_id` in dataset-aware mode.
4. `not_ready` datasets are blocked from training from both:
   - `/models`
   - `/datasets/training`
5. `/datasets/training` and dataset-request success CTA both converge on the same `/models` handoff path.
6. Existing page hierarchy and route hierarchy remain unchanged for this round.

## Escalation Rules

Do not open additional workstreams unless one of these blockers is found during frontend integration:

- the readiness response shape is incompatible with the drawer gating UX
- the train launch request cannot reliably consume `dataset_id`
- run-detail deeplinks returned by train jobs are incompatible with the existing success flow

If such blockers appear:

- contract mismatch goes to the data thread
- runtime launch mismatch goes to the training thread
