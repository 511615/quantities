from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from quant_platform.agents.orchestration import AgentOrchestrationService, ToolAuthorizationService
from quant_platform.agents.services.providers import (
    BacktestQueryService,
    ExecutionProposalService,
    RiskQueryService,
    StrategyProposalService,
    TrainingQueryService,
)
from quant_platform.agents.services.execution_service import ExecutionAgentService
from quant_platform.agents.services.research_service import ResearchAgentService
from quant_platform.agents.services.risk_service import RiskAgentService
from quant_platform.agents.services.strategy_service import StrategyAgentService
from quant_platform.agents.tool_registry.adapters import register_default_tools
from quant_platform.agents.tool_registry.registry import ToolRegistry
from quant_platform.backtest.facade import BacktestFacade
from quant_platform.common.config.loader import load_app_config
from quant_platform.common.config.models import ModelConfig
from quant_platform.common.io.files import LocalArtifactStore
from quant_platform.data.catalog.catalog import DataCatalog
from quant_platform.data.ingestion.service import DomainIngestionCoordinator
from quant_platform.datasets.contracts.dataset import DatasetSample
from quant_platform.datasets.labeling.forward_return import ForwardReturnLabeler
from quant_platform.features.transforms.market_features import MarketFeatureBuilder
from quant_platform.models.registry.default_models import register_default_models
from quant_platform.models.registry.model_registry import ModelRegistry
from quant_platform.training.runners import LocalTrainingRunner, PredictionRunner


@dataclass
class WorkflowRuntime:
    artifact_root: Path
    dataset_store: dict[str, list[DatasetSample]]
    model_registry: ModelRegistry
    training_runner: LocalTrainingRunner
    prediction_runner: PredictionRunner
    backtest_facade: BacktestFacade
    agent_service: AgentOrchestrationService
    store: LocalArtifactStore
    data_catalog: DataCatalog
    ingestion_service: DomainIngestionCoordinator
    feature_builder: MarketFeatureBuilder
    labeler: ForwardReturnLabeler
    tool_registry: ToolRegistry

    @classmethod
    def build(
        cls,
        artifact_root: Path,
        model_config: ModelConfig | None = None,
    ) -> "WorkflowRuntime":
        dataset_store: dict[str, list[DatasetSample]] = {}
        resolved_model_config = model_config or load_app_config().model
        model_registry = ModelRegistry()
        register_default_models(model_registry, resolved_model_config)
        store = LocalArtifactStore(artifact_root)
        backtest_facade = BacktestFacade(artifact_root)
        tool_registry = ToolRegistry()
        register_default_tools(
            tool_registry,
            training_queries=TrainingQueryService(artifact_root),
            backtest_queries=BacktestQueryService(artifact_root),
            risk_queries=RiskQueryService(artifact_root),
            strategy_proposals=StrategyProposalService(artifact_root),
            execution_proposals=ExecutionProposalService(artifact_root),
        )
        research_service = ResearchAgentService(artifact_root, tool_registry)
        strategy_service = StrategyAgentService(artifact_root, tool_registry)
        risk_service = RiskAgentService(artifact_root, tool_registry)
        execution_service = ExecutionAgentService(artifact_root, tool_registry)
        return cls(
            artifact_root=artifact_root,
            dataset_store=dataset_store,
            model_registry=model_registry,
            training_runner=LocalTrainingRunner(
                model_registry=model_registry,
                dataset_store=dataset_store,
                artifact_root=artifact_root,
            ),
            prediction_runner=PredictionRunner(
                model_registry=model_registry,
                dataset_store=dataset_store,
                artifact_root=artifact_root,
            ),
            backtest_facade=backtest_facade,
            agent_service=AgentOrchestrationService(
                artifact_root,
                research_service=research_service,
                strategy_service=strategy_service,
                risk_service=risk_service,
                execution_service=execution_service,
                authorization_service=ToolAuthorizationService(tool_registry),
            ),
            store=store,
            data_catalog=DataCatalog(artifact_root),
            ingestion_service=DomainIngestionCoordinator(artifact_root),
            feature_builder=MarketFeatureBuilder(),
            labeler=ForwardReturnLabeler(),
            tool_registry=tool_registry,
        )
