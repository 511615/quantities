from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from quant_platform.common.config.models import ModelConfig, ModelRegistryEntryConfig
from quant_platform.common.enums.core import ModelFamily
from quant_platform.common.io.files import LocalArtifactStore
from quant_platform.models.adapters import (
    ArtifactAdapter,
    CapabilityValidator,
    ModelInputAdapter,
    PredictionAdapter,
)
from quant_platform.models.adapters.defaults import build_default_adapters
from quant_platform.models.contracts import (
    AdvancedModelKind,
    BaseModelPlugin,
    ModelArtifactMeta,
    ModelRegistration,
    ModelSpec,
)
from quant_platform.models.serialization.artifact import read_artifact_meta


@dataclass(frozen=True)
class ResolvedModelRuntime:
    registration: ModelRegistration
    model_cls: type[BaseModelPlugin]
    input_adapter: ModelInputAdapter
    prediction_adapter: PredictionAdapter
    artifact_adapter: ArtifactAdapter


class ModelRegistry:
    def __init__(self) -> None:
        self._model_classes: dict[str, type[BaseModelPlugin]] = {}
        self._input_adapters: dict[str, ModelInputAdapter] = {}
        self._prediction_adapters: dict[str, PredictionAdapter] = {}
        self._artifact_adapters: dict[str, ArtifactAdapter] = {}
        self._registrations: dict[str, ModelRegistration] = {}
        self._aliases: dict[str, str] = {}
        self._required_config_entrypoints: set[str] = set()
        self._capability_validator: CapabilityValidator | None = None
        self._register_builtin_adapters()

    def set_capability_validator(self, validator: CapabilityValidator) -> None:
        self._capability_validator = validator

    @property
    def capability_validator(self) -> CapabilityValidator:
        if self._capability_validator is None:
            raise ValueError("capability validator is not configured")
        return self._capability_validator

    def register_model_class(
        self,
        entrypoint: str,
        model_cls: type[BaseModelPlugin],
        *,
        require_config: bool = False,
    ) -> None:
        self._model_classes[entrypoint] = model_cls
        if require_config:
            self._required_config_entrypoints.add(entrypoint)

    def register_input_adapter(self, key: str, adapter: ModelInputAdapter) -> None:
        self._input_adapters[key] = adapter

    def register_prediction_adapter(self, key: str, adapter: PredictionAdapter) -> None:
        self._prediction_adapters[key] = adapter

    def register_artifact_adapter(self, key: str, adapter: ArtifactAdapter) -> None:
        self._artifact_adapters[key] = adapter

    def register(self, model_name: str, model_cls: type[BaseModelPlugin]) -> None:
        self._ensure_default_infrastructure()
        entrypoint = f"{model_cls.__module__}.{model_cls.__name__}"
        self.register_model_class(entrypoint, model_cls)
        registration = ModelRegistration(
            model_name=model_name,
            family=getattr(model_cls, "default_family", ModelFamily.BASELINE),
            advanced_kind=getattr(model_cls, "advanced_kind", AdvancedModelKind.BASELINE),
            entrypoint=entrypoint,
            input_adapter_key=getattr(
                model_cls, "default_input_adapter_key", "tabular_passthrough"
            ),
            prediction_adapter_key="standard_prediction",
            artifact_adapter_key="json_manifest",
            default_hyperparams={},
        )
        self.register_model(registration)

    def register_model(self, registration: ModelRegistration) -> None:
        self._validate_components(registration)
        if registration.model_name in self._registrations:
            self._remove_aliases(self._registrations[registration.model_name])
        self._registrations[registration.model_name] = registration
        self._aliases[registration.model_name] = registration.model_name
        for alias in registration.aliases:
            owner = self._aliases.get(alias)
            if owner and owner != registration.model_name:
                raise ValueError(f"alias '{alias}' is already assigned to model '{owner}'")
            self._aliases[alias] = registration.model_name

    def load_model_config(self, model_config: ModelConfig) -> None:
        configured_entrypoints: set[str] = set()
        for model_name, entry in model_config.models.items():
            if not entry.enabled:
                continue
            registration = self._registration_from_config(model_name, entry)
            self.register_model(registration)
            configured_entrypoints.add(registration.entrypoint)
        missing = sorted(self._required_config_entrypoints - configured_entrypoints)
        if missing:
            raise ValueError(
                f"required model entrypoints are missing from config: {', '.join(missing)}"
            )
        if model_config.default_model not in self._aliases:
            raise ValueError(f"default model '{model_config.default_model}' is not registered")
        default_registration = self.resolve_registration(model_config.default_model)
        if not default_registration.default_eligible:
            raise ValueError(
                f"default model '{model_config.default_model}' is not default eligible"
            )

    def create(self, spec: ModelSpec) -> BaseModelPlugin:
        runtime = self.resolve_runtime(spec.model_name)
        return runtime.model_cls(spec)

    def resolve_registration(self, model_name: str) -> ModelRegistration:
        canonical_name = self._resolve_name(model_name)
        return self._registrations[canonical_name]

    def resolve_runtime(self, model_name: str) -> ResolvedModelRuntime:
        registration = self.resolve_registration(model_name)
        return ResolvedModelRuntime(
            registration=registration,
            model_cls=self._model_classes[registration.entrypoint],
            input_adapter=self._input_adapters[registration.input_adapter_key],
            prediction_adapter=self._prediction_adapters[registration.prediction_adapter_key],
            artifact_adapter=self._artifact_adapters[registration.artifact_adapter_key],
        )

    def load_from_artifact(
        self, artifact_uri: str | Path
    ) -> tuple[BaseModelPlugin, ModelArtifactMeta]:
        meta = read_artifact_meta(artifact_uri)
        runtime = self.resolve_runtime(meta.model_name)
        artifact_root = (
            Path(meta.artifact_dir).parents[1]
            if len(Path(meta.artifact_dir).parents) >= 2
            else Path(meta.artifact_dir).parent
        )
        plugin = runtime.artifact_adapter.load_model(
            runtime.model_cls,
            spec=meta.model_spec,
            artifact_meta=meta,
            artifact_store=LocalArtifactStore(artifact_root),
        )
        return plugin, meta

    def _registration_from_config(
        self, model_name: str, entry: ModelRegistryEntryConfig
    ) -> ModelRegistration:
        return ModelRegistration(
            model_name=model_name,
            family=entry.family,
            advanced_kind=entry.advanced_kind,
            entrypoint=entry.entrypoint,
            input_adapter_key=entry.input_adapter_key,
            prediction_adapter_key=entry.prediction_adapter_key,
            artifact_adapter_key=entry.artifact_adapter_key,
            capabilities=list(entry.capabilities),
            default_hyperparams=dict(entry.default_hyperparams),
            config_schema_version=entry.config_schema_version,
            aliases=list(entry.aliases),
            benchmark_eligible=entry.benchmark_eligible,
            default_eligible=entry.default_eligible,
            enabled=entry.enabled,
        )

    def _resolve_name(self, model_name: str) -> str:
        if model_name not in self._aliases:
            raise KeyError(f"model '{model_name}' is not registered")
        return self._aliases[model_name]

    def _remove_aliases(self, registration: ModelRegistration) -> None:
        for alias, owner in list(self._aliases.items()):
            if owner == registration.model_name:
                del self._aliases[alias]

    def _validate_components(self, registration: ModelRegistration) -> None:
        if registration.entrypoint not in self._model_classes:
            raise ValueError(
                f"model entrypoint '{registration.entrypoint}' is not registered in code"
            )
        if registration.input_adapter_key not in self._input_adapters:
            raise ValueError(
                f"input adapter '{registration.input_adapter_key}' is not registered in code"
            )
        if registration.prediction_adapter_key not in self._prediction_adapters:
            raise ValueError(
                "prediction adapter "
                f"'{registration.prediction_adapter_key}' is not registered in code"
            )
        if registration.artifact_adapter_key not in self._artifact_adapters:
            raise ValueError(
                f"artifact adapter '{registration.artifact_adapter_key}' is not registered in code"
            )

    def _ensure_default_infrastructure(self) -> None:
        if (
            self._input_adapters
            and self._prediction_adapters
            and self._artifact_adapters
            and self._capability_validator
        ):
            return
        adapters = build_default_adapters()
        for key, adapter in adapters["input_adapters"].items():
            self._input_adapters.setdefault(key, adapter)
        for key, adapter in adapters["prediction_adapters"].items():
            self._prediction_adapters.setdefault(key, adapter)
        for key, adapter in adapters["artifact_adapters"].items():
            self._artifact_adapters.setdefault(key, adapter)
        if self._capability_validator is None:
            self._capability_validator = adapters["capability_validator"]

    def _register_builtin_adapters(self) -> None:
        from quant_platform.models.adapters.defaults import build_default_adapters

        adapters = build_default_adapters()
        for key, adapter in adapters["input_adapters"].items():
            self.register_input_adapter(key, adapter)
        for key, adapter in adapters["prediction_adapters"].items():
            self.register_prediction_adapter(key, adapter)
        for key, adapter in adapters["artifact_adapters"].items():
            self.register_artifact_adapter(key, adapter)
        self.set_capability_validator(adapters["capability_validator"])
