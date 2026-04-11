from __future__ import annotations

from quant_platform.common.config.models import ModelConfig
from quant_platform.models.advanced.multimodal_fusion import MultimodalFusionModel
from quant_platform.models.advanced.patch_mixer import PatchMixerModel
from quant_platform.models.advanced.temporal_fusion import TemporalFusionModel
from quant_platform.models.advanced.transformer_sequence import TransformerSequenceModel
from quant_platform.models.baselines.elastic_net import ElasticNetModel
from quant_platform.models.baselines.gru import GRUSequenceModel
from quant_platform.models.baselines.lstm import LSTMSequenceModel
from quant_platform.models.baselines.lightgbm import LightGBMModel
from quant_platform.models.baselines.mean_baseline import MeanBaselineModel
from quant_platform.models.baselines.mlp import MLPModel
from quant_platform.models.registry.model_registry import ModelRegistry


def register_default_models(
    registry: ModelRegistry,
    model_config: ModelConfig | None = None,
) -> None:
    registry.register_model_class(
        "quant_platform.models.baselines.mean_baseline.MeanBaselineModel",
        MeanBaselineModel,
        require_config=True,
    )
    registry.register_model_class(
        "quant_platform.models.baselines.elastic_net.ElasticNetModel",
        ElasticNetModel,
        require_config=True,
    )
    registry.register_model_class(
        "quant_platform.models.baselines.lightgbm.LightGBMModel",
        LightGBMModel,
        require_config=True,
    )
    registry.register_model_class(
        "quant_platform.models.baselines.mlp.MLPModel",
        MLPModel,
        require_config=True,
    )
    registry.register_model_class(
        "quant_platform.models.baselines.gru.GRUSequenceModel",
        GRUSequenceModel,
        require_config=True,
    )
    registry.register_model_class(
        "quant_platform.models.baselines.lstm.LSTMSequenceModel",
        LSTMSequenceModel,
        require_config=True,
    )
    registry.register_model_class(
        "quant_platform.models.advanced.transformer_sequence.TransformerSequenceModel",
        TransformerSequenceModel,
        require_config=True,
    )
    registry.register_model_class(
        "quant_platform.models.advanced.temporal_fusion.TemporalFusionModel",
        TemporalFusionModel,
        require_config=True,
    )
    registry.register_model_class(
        "quant_platform.models.advanced.patch_mixer.PatchMixerModel",
        PatchMixerModel,
        require_config=True,
    )
    registry.register_model_class(
        "quant_platform.models.advanced.multimodal_fusion.MultimodalFusionModel",
        MultimodalFusionModel,
        require_config=True,
    )
    registry.load_model_config(_merge_default_model_config(model_config))


def _default_model_config() -> ModelConfig:
    return ModelConfig.model_validate(
        {
            "default_model": "mean_baseline",
            "models": {
                "mean_baseline": {
                    "family": "baseline",
                    "advanced_kind": "baseline",
                    "entrypoint": "quant_platform.models.baselines.mean_baseline.MeanBaselineModel",
                    "input_adapter_key": "tabular_passthrough",
                    "prediction_adapter_key": "standard_prediction",
                    "artifact_adapter_key": "json_manifest",
                    "benchmark_eligible": True,
                    "default_eligible": True,
                },
                "elastic_net": {
                    "family": "linear",
                    "advanced_kind": "baseline",
                    "entrypoint": "quant_platform.models.baselines.elastic_net.ElasticNetModel",
                    "input_adapter_key": "tabular_passthrough",
                    "prediction_adapter_key": "standard_prediction",
                    "artifact_adapter_key": "json_manifest",
                    "default_hyperparams": {"alpha": 0.001, "l1_ratio": 0.5},
                    "benchmark_eligible": True,
                    "default_eligible": True,
                },
                "lightgbm": {
                    "family": "tree",
                    "advanced_kind": "baseline",
                    "entrypoint": "quant_platform.models.baselines.lightgbm.LightGBMModel",
                    "input_adapter_key": "tabular_passthrough",
                    "prediction_adapter_key": "standard_prediction",
                    "artifact_adapter_key": "json_manifest",
                    "benchmark_eligible": True,
                    "default_eligible": True,
                },
                "mlp": {
                    "family": "deep",
                    "advanced_kind": "baseline",
                    "entrypoint": "quant_platform.models.baselines.mlp.MLPModel",
                    "input_adapter_key": "tabular_passthrough",
                    "prediction_adapter_key": "standard_prediction",
                    "artifact_adapter_key": "json_manifest",
                    "benchmark_eligible": True,
                    "default_eligible": True,
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
                    "benchmark_eligible": True,
                    "default_eligible": True,
                },
                "lstm": {
                    "family": "sequence",
                    "advanced_kind": "baseline",
                    "entrypoint": "quant_platform.models.baselines.lstm.LSTMSequenceModel",
                    "input_adapter_key": "sequence_market",
                    "prediction_adapter_key": "standard_prediction",
                    "artifact_adapter_key": "json_manifest",
                    "capabilities": ["sequence_input"],
                    "default_hyperparams": {"lookback": 3},
                    "benchmark_eligible": True,
                    "default_eligible": True,
                },
                "transformer_reference": {
                    "family": "deep",
                    "advanced_kind": "transformer",
                    "entrypoint": (
                        "quant_platform.models.advanced.transformer_sequence."
                        "TransformerSequenceModel"
                    ),
                    "input_adapter_key": "sequence_market",
                    "prediction_adapter_key": "standard_prediction",
                    "artifact_adapter_key": "json_manifest",
                    "capabilities": ["sequence_input"],
                    "default_hyperparams": {"lookback": 3},
                    "aliases": ["transformer"],
                    "benchmark_eligible": True,
                    "default_eligible": False,
                },
                "temporal_fusion_reference": {
                    "family": "deep",
                    "advanced_kind": "temporal_fusion",
                    "entrypoint": (
                        "quant_platform.models.advanced.temporal_fusion.TemporalFusionModel"
                    ),
                    "input_adapter_key": "temporal_fusion",
                    "prediction_adapter_key": "standard_prediction",
                    "artifact_adapter_key": "json_manifest",
                    "capabilities": ["sequence_input"],
                    "default_hyperparams": {"lookback": 3},
                    "aliases": ["tft_reference"],
                    "benchmark_eligible": True,
                    "default_eligible": False,
                },
                "patch_mixer_reference": {
                    "family": "deep",
                    "advanced_kind": "patch_mixer",
                    "entrypoint": "quant_platform.models.advanced.patch_mixer.PatchMixerModel",
                    "input_adapter_key": "patch_sequence",
                    "prediction_adapter_key": "standard_prediction",
                    "artifact_adapter_key": "json_manifest",
                    "capabilities": ["sequence_input", "moe_router_ready"],
                    "default_hyperparams": {"lookback": 4, "patch_size": 2},
                    "aliases": ["mixer_reference"],
                    "benchmark_eligible": True,
                    "default_eligible": False,
                },
                "multimodal_reference": {
                    "family": "deep",
                    "advanced_kind": "multimodal",
                    "entrypoint": (
                        "quant_platform.models.advanced.multimodal_fusion.MultimodalFusionModel"
                    ),
                    "input_adapter_key": "market_text_aligned",
                    "prediction_adapter_key": "standard_prediction",
                    "artifact_adapter_key": "json_manifest",
                    "capabilities": [
                        "sequence_input",
                        "aligned_text",
                        "allow_missing_text",
                        "moe_router_ready",
                    ],
                    "default_hyperparams": {
                        "lookback": 3,
                        "text_feature_prefixes": ["text_", "sentiment_", "news_"],
                        "text_weight": 0.5,
                    },
                    "aliases": ["multimodal"],
                    "benchmark_eligible": True,
                    "default_eligible": False,
                },
            },
        }
    )


def _merge_default_model_config(model_config: ModelConfig | None) -> ModelConfig:
    default_config = _default_model_config()
    if model_config is None:
        return default_config
    payload = model_config.model_dump(mode="json")
    payload.setdefault("models", {})
    for model_name, entry in default_config.model_dump(mode="json")["models"].items():
        payload["models"].setdefault(model_name, entry)
    payload.setdefault("default_model", default_config.default_model)
    return ModelConfig.model_validate(payload)
