from __future__ import annotations

from copy import deepcopy


def build_app_config_payload() -> dict[str, object]:
    return {
        "project": {
            "name": "quant-platform",
            "version": "0.1.0",
            "artifact_root": "artifacts",
            "manifest_root": "data/manifests",
        },
        "env": {
            "name": "ci",
            "tracking_backend": "file",
            "tracking_uri": "./mlruns",
            "artifact_root": "./artifacts",
            "strict_mode": True,
        },
        "data": {
            "sources": {"market": {"provider": "internal"}},
            "schema": {
                "required_columns": ["event_time", "available_time", "close", "volume"],
                "timestamp_timezone": "UTC",
            },
            "feature_store": {
                "default_feature_set": "baseline_market_features",
                "enforce_available_time": True,
                "lineage_required": True,
            },
            "dataset_policies": {
                "allowed_splitters": ["time_series", "rolling_windows"],
                "forbid_random_split": True,
                "default_label_horizon": 1,
            },
        },
        "model": {
            "default_model": "elastic_net",
            "models": {
                "mean_baseline": {
                    "family": "baseline",
                    "advanced_kind": "baseline",
                    "entrypoint": "quant_platform.models.baselines.mean_baseline.MeanBaselineModel",
                    "input_adapter_key": "tabular_passthrough",
                    "prediction_adapter_key": "standard_prediction",
                    "artifact_adapter_key": "json_manifest",
                },
                "elastic_net": {
                    "family": "linear",
                    "advanced_kind": "baseline",
                    "entrypoint": "quant_platform.models.baselines.elastic_net.ElasticNetModel",
                    "input_adapter_key": "tabular_passthrough",
                    "prediction_adapter_key": "standard_prediction",
                    "artifact_adapter_key": "json_manifest",
                },
                "lightgbm": {
                    "family": "tree",
                    "advanced_kind": "baseline",
                    "entrypoint": "quant_platform.models.baselines.lightgbm.LightGBMModel",
                    "input_adapter_key": "tabular_passthrough",
                    "prediction_adapter_key": "standard_prediction",
                    "artifact_adapter_key": "json_manifest",
                },
                "mlp": {
                    "family": "deep",
                    "advanced_kind": "baseline",
                    "entrypoint": "quant_platform.models.baselines.mlp.MLPModel",
                    "input_adapter_key": "tabular_passthrough",
                    "prediction_adapter_key": "standard_prediction",
                    "artifact_adapter_key": "json_manifest",
                },
                "gru": {
                    "family": "sequence",
                    "advanced_kind": "baseline",
                    "entrypoint": "quant_platform.models.baselines.gru.GRUSequenceModel",
                    "input_adapter_key": "sequence_market",
                    "prediction_adapter_key": "standard_prediction",
                    "artifact_adapter_key": "json_manifest",
                    "capabilities": ["sequence_input"],
                    "default_hyperparams": {"lookback": 2},
                },
            },
        },
        "train": {
            "seed": 7,
            "epochs": 1,
            "batch_size": 32,
            "runner": "local",
            "deterministic": True,
            "metrics": {"train": ["mae"]},
            "callbacks": {"early_stopping": False},
            "hpo": {"enabled": False},
        },
        "backtest": {
            "engine": "vectorized",
            "position_mode": "target_weight",
            "allow_short": True,
            "initial_cash": 100000.0,
            "event_driven": {"enabled": False},
            "costs": {"fee_bps": 5.0, "slippage_bps": 2.0},
            "portfolio_rules": {"max_gross_leverage": 1.0},
        },
        "experiment": {
            "tracking": {"enabled": True},
            "lineage": {"required": True},
            "reproducibility": {"capture_seed": True},
        },
        "agent": {
            "enabled": True,
            "task_types": ["summarize_experiment"],
            "guardrails": {"strict": True},
            "tools": {"research": ["backtest.read_report"]},
        },
    }


def clone_payload() -> dict[str, object]:
    return deepcopy(build_app_config_payload())
