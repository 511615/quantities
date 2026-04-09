from __future__ import annotations

from pathlib import Path

from quant_platform.api.facade import QuantPlatformFacade


def test_vectorized_backtest_writes_all_expected_artifacts(artifact_root) -> None:
    facade = QuantPlatformFacade(artifact_root)
    result = facade.backtest_smoke()
    assert Path(result.orders_uri).exists()
    assert Path(result.fills_uri).exists()
    assert Path(result.positions_uri).exists()
    assert Path(result.pnl_uri).exists()
    assert Path(result.report_uri).exists()
    assert Path(result.diagnostics_uri).exists()
    assert Path(result.leakage_audit_uri).exists()
    assert result.engine_type == "research"
    assert result.risk_metrics["position_count"] >= 1.0
    assert result.risk_metrics["gross_exposure"] >= 0.0
