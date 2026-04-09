# Architecture Overview

## Layering

1. `data`
   Responsible for ingestion, normalization, storage, and immutable data asset snapshots.
2. `features`
   Responsible for deterministic feature transforms with explicit `as_of_time` and lineage.
3. `datasets`
   Responsible for labels, temporal splits, filtering, and the anti-leakage boundary.
4. `models`
   Responsible for model specs, runtimes, serialization, and inference adapters.
5. `training`
   Responsible for fit execution, metrics, manifests, and experiment artifacts.
6. `backtest`
   Responsible for converting predictions into portfolio outcomes and reports.
7. `agents`
   Responsible for structured research assistance through guarded tool access.
8. `workflows`
   Responsible for stable orchestration of `prepare -> train -> predict -> benchmark -> backtest -> review`.

## Workflow Layer

- `WorkflowRuntime` is the single dependency assembly point for workflow services.
- Stage services are split by responsibility:
  - `PrepareWorkflowService`
  - `TrainWorkflowService`
  - `PredictWorkflowService`
  - `BenchmarkWorkflowService`
  - `BacktestWorkflowService`
  - `ReviewWorkflowService`
  - `WorkflowPipelineService`
- `QuantPlatformFacade` remains as a legacy compatibility wrapper and now delegates orchestration to workflow services.
- CLI now exposes formal workflow entrypoints under `workflow ...` while keeping legacy groups intact.

## Runtime Shape

- CLI loads config through the existing configuration loader.
- `WorkflowRuntime.build()` wires:
  - `DataCatalog`
  - `MarketFeatureBuilder`
  - `ForwardReturnLabeler`
  - `ModelRegistry`
  - `LocalTrainingRunner`
  - `PredictionRunner`
  - `BacktestFacade`
  - `ResearchAgentService`
  - `LocalArtifactStore`
  - `ToolRegistry`
- Workflow services only depend on the runtime and stage contracts, not on each other’s internal state.

## Reproducibility

- All workflow stages emit structured request/result contracts.
- Artifacts are persisted at each stage boundary so downstream stages consume explicit outputs rather than hidden in-memory state.
- Training and backtest outputs remain deterministic under the same data, config, code, and seed constraints already enforced by the lower layers.

## Compatibility Policy

- Existing model, dataset, training, backtest, and agent contracts remain compatible.
- Existing facade methods and legacy CLI groups are preserved.
- New workflow contracts sit above the existing contracts and do not replace them.

## Dataset Experience Governance

- Dataset experience control semantics are frozen in [ADR-002](/C:/Users/1/Desktop/AI/quantities_economy/docs/adr/ADR-002-dataset-experience-taxonomy.md).
- Dataset information architecture, route map, and field dictionary are defined in [dataset-information-architecture.md](/C:/Users/1/Desktop/AI/quantities_economy/docs/architecture/dataset-information-architecture.md).
- Dataset request to model-training frontend handoff rules are defined in [dataset-training-handoff.md](/C:/Users/1/Desktop/AI/quantities_economy/docs/architecture/dataset-training-handoff.md).
- Backend runtime cleanup, restart, and live smoke procedure are defined in [backend-runtime-control.md](/C:/Users/1/Desktop/AI/quantities_economy/docs/architecture/backend-runtime-control.md).
- `symbol` is a filter and slice dimension, not the primary dataset identity for browse or training-panel views.
- Dataset browse and detail contracts must aggregate all relevant input refs rather than collapsing to `input_data_refs[0]`.
