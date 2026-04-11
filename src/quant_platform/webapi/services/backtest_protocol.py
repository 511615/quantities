from __future__ import annotations

from datetime import datetime
from typing import Any

from quant_platform.backtest.contracts.backtest import (
    BacktestRequest,
    BenchmarkSpec,
    CalendarSpec,
    CostModel,
    PortfolioConfig,
    StrategyConfig,
)
from quant_platform.backtest.contracts.scenario import ScenarioSpec
from quant_platform.backtest.metrics.promotion import (
    DEFAULT_MAX_DRAWDOWN,
    DEFAULT_MAX_SIMULATION_OUTPERFORMANCE,
    DEFAULT_MAX_STRESS_FAILS,
    DEFAULT_MAX_TURNOVER,
    DEFAULT_MIN_WORST_SCENARIO_RETURN_DELTA,
)
from quant_platform.common.hashing.digest import stable_digest
from quant_platform.webapi.schemas.views import (
    BacktestProtocolResultView,
    BacktestTemplateView,
    GateResultView,
    RankComponentView,
)

OFFICIAL_BACKTEST_TEMPLATE_ID = "system::official_backtest_protocol_v1"
OFFICIAL_PROTOCOL_VERSION = "v1"
OFFICIAL_OUTPUT_CONTRACT_VERSION = "prediction_frame_v1"
OFFICIAL_SCENARIO_BUNDLE = (
    "BASELINE",
    "COST_X2",
    "COST_X5",
    "LATENCY_SHOCK",
    "LIQUIDITY_DROUGHT",
    "LONG_ONLY_FALLBACK",
    "STALE_SIGNAL",
    "BROKEN_DATA_GUARD",
)


def custom_backtest_template() -> BacktestTemplateView:
    return BacktestTemplateView(
        template_id="custom::interactive",
        name="Custom Interactive Backtest",
        description="保留当前 workbench 自由回测能力，允许用户自行选择预测范围和数据预置。",
        source="custom",
        read_only=False,
        official=False,
        protocol_version="interactive",
        output_contract_version=OFFICIAL_OUTPUT_CONTRACT_VERSION,
        ranking_policy="不做官方 slice/rank 约束，按用户当前自定义配置返回结果。",
        slice_policy="custom",
        scenario_bundle=["BASELINE"],
        eligibility_rules=["任意兼容 run 都可以发起自定义回测。"],
        required_metadata=[],
        notes=["custom 模式不进入官方榜单，但结果仍可查看和复盘。"],
    )


def official_backtest_template(*, default_benchmark_symbol: str = "BTCUSDT") -> BacktestTemplateView:
    return BacktestTemplateView(
        template_id=OFFICIAL_BACKTEST_TEMPLATE_ID,
        name="Official Backtest Protocol v1",
        description=(
            "平台级官方回测模板。采用统一预测输出比较、固定压力场景、固定披露要求，"
            "用于跨模型做更公平的能力与鲁棒性评估。"
        ),
        source="system",
        read_only=True,
        official=True,
        protocol_version=OFFICIAL_PROTOCOL_VERSION,
        output_contract_version=OFFICIAL_OUTPUT_CONTRACT_VERSION,
        fixed_prediction_scope="test",
        ranking_policy=(
            "先做合规 gate，再在相同 benchmark slice 内比较能力、经济性与鲁棒性指标；"
            "单次回测详情页展示原始组件，跨模型排名在 comparison 中计算。"
        ),
        slice_policy=(
            "同一 dataset_snapshot × frequency × target_name × horizon × prediction_scope "
            "内比较，避免不同时间范围和不同任务尺度直接裸比。"
        ),
        scenario_bundle=list(OFFICIAL_SCENARIO_BUNDLE),
        eligibility_rules=[
            "模型必须能输出标准 prediction frame，才能进入官方模板。",
            "训练数据范围不设硬门槛，但必须披露训练窗口与关键训练元数据。",
            "未完成披露的模型允许跑出官方回测结果，但 gate 会标记为 metadata_incomplete。",
        ],
        required_metadata=[
            "训练数据起止时间",
            "lookback window / context length",
            "label horizon",
            "模态列表与融合方式摘要",
            "随机种子",
            "调参试验次数",
            "是否使用外部预训练",
            "是否使用合成数据",
        ],
        notes=[
            "官方模板不可删除，和自定义回测并存。",
            f"官方模板默认预测范围固定为 test，基准符号默认回落到 {default_benchmark_symbol}。",
            "custom 模式仍保留 dataset preset、prediction scope、strategy、portfolio、cost 等自由参数。",
        ],
    )


def build_official_backtest_request(
    *,
    prediction_frame_uri: str,
    benchmark_symbol: str,
) -> BacktestRequest:
    return BacktestRequest(
        prediction_frame_uri=prediction_frame_uri,
        strategy_config=StrategyConfig(name="sign_strategy"),
        portfolio_config=PortfolioConfig(
            initial_cash=100000.0,
            max_gross_leverage=1.0,
            max_position_weight=1.0,
        ),
        cost_model=CostModel(fee_bps=5.0, slippage_bps=2.0),
        benchmark_spec=BenchmarkSpec(name="buy_and_hold", symbol=benchmark_symbol),
        calendar_spec=CalendarSpec(timezone="UTC", frequency="1h"),
        scenario_specs=[
            ScenarioSpec(name=name, description=f"official protocol scenario: {name}")
            for name in OFFICIAL_SCENARIO_BUNDLE
        ],
    )


def build_custom_backtest_request(
    *,
    prediction_frame_uri: str,
    benchmark_symbol: str,
) -> BacktestRequest:
    return BacktestRequest(
        prediction_frame_uri=prediction_frame_uri,
        strategy_config=StrategyConfig(name="sign_strategy"),
        portfolio_config=PortfolioConfig(
            initial_cash=100000.0,
            max_gross_leverage=1.0,
            max_position_weight=1.0,
        ),
        cost_model=CostModel(fee_bps=5.0, slippage_bps=2.0),
        benchmark_spec=BenchmarkSpec(name="buy_and_hold", symbol=benchmark_symbol),
        calendar_spec=CalendarSpec(timezone="UTC", frequency="1h"),
        scenario_specs=[
            ScenarioSpec(
                name="BASELINE",
                description="default interactive workbench backtest configuration",
            )
        ],
    )


def build_protocol_metadata(
    *,
    template: BacktestTemplateView,
    launch_mode: str,
    prediction_scope: str,
    dataset_id: str | None,
    dataset_frequency: str | None,
    target_name: str | None,
    label_horizon: int | None,
    lookback_bucket: str | None,
    metadata_summary: dict[str, str | None],
) -> dict[str, object]:
    slice_coverage = [
        part
        for part in [
            dataset_id,
            dataset_frequency,
            target_name,
            str(label_horizon) if label_horizon is not None else None,
            prediction_scope,
        ]
        if part
    ]
    return {
        "template_id": template.template_id,
        "template_name": template.name,
        "official": template.official,
        "launch_mode": launch_mode,
        "protocol_version": template.protocol_version,
        "output_contract_version": template.output_contract_version,
        "fixed_prediction_scope": template.fixed_prediction_scope,
        "ranking_policy": template.ranking_policy,
        "slice_policy": template.slice_policy,
        "scenario_bundle": list(template.scenario_bundle),
        "eligibility_rules": list(template.eligibility_rules),
        "required_metadata": list(template.required_metadata),
        "notes": list(template.notes),
        "metadata_summary": metadata_summary,
        "lookback_bucket": lookback_bucket,
        "slice_coverage": slice_coverage,
        "slice_id": stable_digest({"template_id": template.template_id, "slice": slice_coverage}),
    }


def compute_protocol_result(
    *,
    protocol_metadata: dict[str, Any] | None,
    simulation_metrics: dict[str, float],
    divergence_metrics: dict[str, float],
    scenario_metrics: dict[str, float],
    comparison_warnings: list[str],
    passed_consistency_checks: bool | None,
) -> BacktestProtocolResultView | None:
    if not protocol_metadata:
        return None
    template = BacktestTemplateView(
        template_id=str(protocol_metadata.get("template_id", "custom::interactive")),
        name=str(protocol_metadata.get("template_name", "Custom Interactive Backtest")),
        description="",
        source="system" if bool(protocol_metadata.get("official")) else "custom",
        read_only=bool(protocol_metadata.get("official")),
        official=bool(protocol_metadata.get("official")),
        protocol_version=_str(protocol_metadata.get("protocol_version")),
        output_contract_version=_str(protocol_metadata.get("output_contract_version")),
        fixed_prediction_scope=_str(protocol_metadata.get("fixed_prediction_scope")),
        ranking_policy=_str(protocol_metadata.get("ranking_policy")),
        slice_policy=_str(protocol_metadata.get("slice_policy")),
        scenario_bundle=_str_list(protocol_metadata.get("scenario_bundle")),
        eligibility_rules=_str_list(protocol_metadata.get("eligibility_rules")),
        required_metadata=_str_list(protocol_metadata.get("required_metadata")),
        notes=_str_list(protocol_metadata.get("notes")),
    )
    metadata_summary = {
        str(key): (_str(value) if value is not None else None)
        for key, value in dict(protocol_metadata.get("metadata_summary") or {}).items()
    }
    metadata_complete = all(metadata_summary.get(key) for key in metadata_summary)
    gate_results = [
        GateResultView(
            key="metadata_complete",
            label="披露信息完整",
            passed=metadata_complete,
            severity="warning",
            detail="官方模板要求披露训练时间范围、lookback、模态、种子和预训练信息。",
        ),
        GateResultView(
            key="consistency_checks",
            label="研究/仿真一致性",
            passed=bool(passed_consistency_checks),
            severity="error",
            detail="要求研究引擎与仿真引擎不存在异常优于关系。",
        ),
        GateResultView(
            key="stress_bundle_complete",
            label="官方压力场景齐备",
            passed=_stress_bundle_complete(protocol_metadata, scenario_metrics),
            severity="error",
            detail="官方模板至少要求完整跑完固定 stress bundle。",
        ),
        GateResultView(
            key="risk_limits",
            label="基础风险阈值",
            passed=_risk_limits_passed(simulation_metrics, divergence_metrics, scenario_metrics),
            severity="error",
            detail=(
                f"要求 max_drawdown <= {DEFAULT_MAX_DRAWDOWN:.2f}, "
                f"turnover_total <= {DEFAULT_MAX_TURNOVER:.2f}, "
                f"stress_fail_count <= {DEFAULT_MAX_STRESS_FAILS:.0f}。"
            ),
        ),
    ]
    gate_status = (
        "passed"
        if all(item.passed is not False for item in gate_results) and not comparison_warnings
        else "warning"
    )
    rank_components = [
        RankComponentView(
            key="annual_return",
            label="Annual Return",
            value=_float(simulation_metrics.get("annual_return")),
            detail="单次结果展示原始指标；跨模型百分位在 comparison 中再算。",
        ),
        RankComponentView(
            key="information_ratio",
            label="Information Ratio",
            value=_float(simulation_metrics.get("information_ratio")),
            detail="能力组件原始值。",
        ),
        RankComponentView(
            key="max_drawdown",
            label="Max Drawdown",
            value=_float(simulation_metrics.get("max_drawdown")),
            detail="经济与风险组件原始值。",
        ),
        RankComponentView(
            key="implementation_shortfall",
            label="Implementation Shortfall",
            value=_float(simulation_metrics.get("implementation_shortfall")),
            detail="执行成本组件原始值。",
        ),
        RankComponentView(
            key="worst_scenario_return_delta",
            label="Worst Scenario Delta",
            value=_float(scenario_metrics.get("worst_scenario_return_delta")),
            detail="鲁棒性组件原始值。",
        ),
        RankComponentView(
            key="simulation_minus_research_cumulative_return",
            label="Research/Simulation Divergence",
            value=_float(divergence_metrics.get("simulation_minus_research_cumulative_return")),
            detail="越接近 0 越可信。",
        ),
    ]
    return BacktestProtocolResultView(
        template=template,
        gate_status=gate_status,
        gate_results=gate_results,
        rank_components=rank_components,
        slice_id=_str(protocol_metadata.get("slice_id")),
        slice_coverage=_str_list(protocol_metadata.get("slice_coverage")),
        lookback_bucket=_str(protocol_metadata.get("lookback_bucket")),
        metadata_summary=metadata_summary,
    )


def derive_lookback_bucket(start_time: datetime | None, end_time: datetime | None) -> str | None:
    if start_time is None or end_time is None:
        return None
    span_days = max((end_time - start_time).days, 0)
    if span_days <= 90:
        return "<=90d"
    if span_days <= 365:
        return "90d-365d"
    return ">365d"


def _stress_bundle_complete(protocol_metadata: dict[str, Any], scenario_metrics: dict[str, float]) -> bool:
    required = {
        name for name in _str_list(protocol_metadata.get("scenario_bundle")) if name != "BASELINE"
    }
    available_map = {
        "COST_X2": "cost_x2_return_delta",
        "COST_X5": "cost_x5_return_delta",
        "LATENCY_SHOCK": "latency_shock_return_delta",
        "LIQUIDITY_DROUGHT": "liquidity_drought_return_delta",
        "LONG_ONLY_FALLBACK": "long_only_fallback_return_delta",
    }
    for scenario_name in required:
        metric_name = available_map.get(scenario_name)
        if metric_name is None:
            continue
        if metric_name not in scenario_metrics:
            return False
    return True


def _risk_limits_passed(
    simulation_metrics: dict[str, float],
    divergence_metrics: dict[str, float],
    scenario_metrics: dict[str, float],
) -> bool:
    if _float(simulation_metrics.get("max_drawdown")) > DEFAULT_MAX_DRAWDOWN:
        return False
    if _float(simulation_metrics.get("turnover_total")) > DEFAULT_MAX_TURNOVER:
        return False
    if _float(scenario_metrics.get("worst_scenario_return_delta")) < DEFAULT_MIN_WORST_SCENARIO_RETURN_DELTA:
        return False
    if _float(scenario_metrics.get("stress_fail_count")) > DEFAULT_MAX_STRESS_FAILS:
        return False
    if _float(divergence_metrics.get("simulation_minus_research_cumulative_return")) > DEFAULT_MAX_SIMULATION_OUTPERFORMANCE:
        return False
    return True


def _float(value: Any) -> float | None:
    return float(value) if isinstance(value, (int, float)) else None


def _str(value: Any) -> str | None:
    return str(value) if isinstance(value, (int, float, str)) else None


def _str_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if isinstance(item, (int, float, str))]
