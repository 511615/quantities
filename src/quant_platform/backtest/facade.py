from __future__ import annotations

from pathlib import Path

from quant_platform.backtest.contracts.backtest import BacktestRequest, BacktestResult
from quant_platform.backtest.contracts.signal import SignalFrame
from quant_platform.backtest.engines.event_driven import EventDrivenSimulationEngine
from quant_platform.backtest.engines.vectorbt_adapter import VectorbtResearchAdapter
from quant_platform.backtest.engines.vectorized import ResearchBacktestEngine
from quant_platform.data.contracts.market import NormalizedMarketBar
from quant_platform.training.contracts.training import PredictionFrame


class BacktestFacade:
    def __init__(self, artifact_root: Path) -> None:
        self.research_engine = ResearchBacktestEngine(artifact_root)
        self.vectorbt_research_engine = VectorbtResearchAdapter(artifact_root)
        self.simulation_engine = EventDrivenSimulationEngine(artifact_root)

    def run_research(
        self,
        request: BacktestRequest,
        prediction_frame: PredictionFrame | None = None,
        market_bars: list[NormalizedMarketBar] | None = None,
        signal_frame: SignalFrame | None = None,
    ) -> BacktestResult:
        engine = (
            self.vectorbt_research_engine
            if request.research_backend == "vectorbt"
            else self.research_engine
        )
        return engine.run(
            request=request,
            prediction_frame=prediction_frame,
            market_bars=market_bars,
            signal_frame=signal_frame,
        )

    def run_simulation(
        self,
        request: BacktestRequest,
        prediction_frame: PredictionFrame | None = None,
        market_bars: list[NormalizedMarketBar] | None = None,
        signal_frame: SignalFrame | None = None,
    ) -> BacktestResult:
        return self.simulation_engine.run(
            request=request,
            prediction_frame=prediction_frame,
            market_bars=market_bars,
            signal_frame=signal_frame,
        )
