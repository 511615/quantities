from enum import StrEnum


class SplitStrategy(StrEnum):
    TIME_SERIES = "time_series"
    WALK_FORWARD = "walk_forward"
    PURGED_KFOLD = "purged_kfold"


class LabelKind(StrEnum):
    REGRESSION = "regression"
    CLASSIFICATION = "classification"


class ModelFamily(StrEnum):
    BASELINE = "baseline"
    LINEAR = "linear"
    TREE = "tree"
    DEEP = "deep"
    SEQUENCE = "sequence"


class TrackingBackend(StrEnum):
    FILE = "file"
    MLFLOW = "mlflow"


class AgentTaskType(StrEnum):
    SUMMARIZE_EXPERIMENT = "summarize_experiment"
    INSPECT_BACKTEST = "inspect_backtest"


class AgentTaskStatus(StrEnum):
    SUCCESS = "success"
    FAILED = "failed"
    BLOCKED = "blocked"


class AgentKind(StrEnum):
    RESEARCH = "research"
    STRATEGY = "strategy"
    RISK = "risk"
    EXECUTION = "execution"


class AgentResponseMode(StrEnum):
    SUMMARY_ONLY = "summary_only"
    INLINE_TOOL_RESULTS = "inline_tool_results"
    ARTIFACT_ONLY = "artifact_only"


class ToolSideEffectLevel(StrEnum):
    READ_ONLY = "read_only"
    PROPOSAL_ONLY = "proposal_only"
    EXTERNAL_ACTION = "external_action"


class ToolCallStatus(StrEnum):
    SUCCESS = "success"
    FAILED = "failed"
    BLOCKED = "blocked"
