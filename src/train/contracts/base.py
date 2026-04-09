"""Core contracts for the training framework."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, MutableMapping, Optional, Protocol, Sequence


MetricMap = Dict[str, float]
MetricCollection = Dict[str, MetricMap]


@dataclass(slots=True)
class ArtifactRef:
    name: str
    path: str
    kind: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SplitArtifacts:
    predictions: Optional[ArtifactRef] = None
    reports: list[ArtifactRef] = field(default_factory=list)


@dataclass(slots=True)
class EvalResult:
    split_name: str
    metrics: MetricMap
    step: Optional[int] = None
    epoch: Optional[int] = None
    artifacts: SplitArtifacts = field(default_factory=SplitArtifacts)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SearchResult:
    best_run_id: str
    best_metric_name: str
    best_metric_value: float
    candidate_run_ids: list[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class TrainerContext:
    run_id: str
    seed: int
    device: str
    objective_type: str
    output_dir: Path
    window_id: Optional[str] = None
    fold_id: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class TrainerInput:
    model: Any
    train_dataset: Any
    valid_dataset: Any = None
    test_dataset: Any = None
    objective: Any = None
    metrics: Sequence[Callable[..., Mapping[str, float]]] = field(default_factory=tuple)
    callbacks: Sequence[Any] = field(default_factory=tuple)
    optimizer_factory: Optional[Callable[[Any], Any]] = None
    scheduler_factory: Optional[Callable[[Any], Any]] = None
    train_dataloader: Any = None
    valid_dataloader: Any = None
    test_dataloader: Any = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class TrainerOutput:
    run_id: str
    primary_metric_name: str
    primary_metric_value: float
    best_step: Optional[int] = None
    best_epoch: Optional[int] = None
    metrics_by_split: MetricCollection = field(default_factory=dict)
    checkpoint_paths: Dict[str, str] = field(default_factory=dict)
    prediction_artifacts: Dict[str, str] = field(default_factory=dict)
    feature_schema_ref: Optional[str] = None
    dataset_manifest_ref: Optional[str] = None
    artifacts: list[ArtifactRef] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return dataclass_to_dict(self)


class TrackingClient(Protocol):
    def start_run(self, run_name: str, tags: Optional[Mapping[str, str]] = None) -> str: ...
    def log_params(self, params: Mapping[str, Any]) -> None: ...
    def log_metrics(self, metrics: Mapping[str, float], step: Optional[int] = None) -> None: ...
    def log_artifact(self, local_path: str, artifact_path: Optional[str] = None) -> None: ...
    def set_tags(self, tags: Mapping[str, str]) -> None: ...
    def end_run(self, status: str = "FINISHED") -> None: ...


class SearchReporter(Protocol):
    def report(
        self, metrics: Mapping[str, float], checkpoint_path: Optional[str] = None
    ) -> None: ...


def dataclass_to_dict(value: Any) -> Any:
    if is_dataclass(value):
        return {key: dataclass_to_dict(item) for key, item in asdict(value).items()}
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): dataclass_to_dict(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [dataclass_to_dict(item) for item in value]
    return value


def flatten_mapping(
    payload: Mapping[str, Any],
    prefix: str = "",
    separator: str = ".",
    target: Optional[MutableMapping[str, Any]] = None,
) -> Dict[str, Any]:
    result: MutableMapping[str, Any] = target if target is not None else {}
    for key, value in payload.items():
        flat_key = f"{prefix}{separator}{key}" if prefix else str(key)
        if isinstance(value, Mapping):
            flatten_mapping(value, prefix=flat_key, separator=separator, target=result)
        else:
            result[flat_key] = value
    return dict(result)
