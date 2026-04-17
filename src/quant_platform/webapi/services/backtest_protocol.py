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
OFFICIAL_DEFAULT_WINDOW_DAYS = 180
OFFICIAL_WINDOW_OPTIONS = (30, 90, 180, 365)
OFFICIAL_MARKET_BENCHMARK_DATASET_ID = "baseline_real_benchmark_dataset"
OFFICIAL_MULTIMODAL_BENCHMARK_DATASET_ID = "official_reddit_pullpush_multimodal_v2_fusion"
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
OFFICIAL_NLP_MIN_TEST_COVERAGE_RATIO = 0.60
OFFICIAL_NLP_MAX_TEST_EMPTY_BARS = 168
OFFICIAL_NLP_MAX_DUPLICATE_RATIO = 0.05
OFFICIAL_NLP_MIN_ENTITY_LINK_COVERAGE_RATIO = 0.95
OFFICIAL_NLP_ARCHIVAL_VENDORS = (
    "news_archive",
    "reddit_archive",
    "gdelt",
)

CUSTOM_TEMPLATE_REQUIREMENTS = (
    ("custom_any_compatible_run", "Any compatible run can be launched in custom mode."),
)

CUSTOM_TEMPLATE_NOTES = (
    ("custom_visible_but_unranked", "Custom mode stays visible for inspection but is excluded from official ranking."),
)

OFFICIAL_TEMPLATE_REQUIREMENTS = (
    ("prediction_frame_contract", "Model output must follow the prediction_frame_v1 contract."),
    ("training_disclosure_required", "Training-time disclosure fields must be populated before official comparison is trusted."),
    ("official_latest_benchmark_binding", "Official mode binds to the newest official rolling benchmark and ignores dataset overrides."),
    ("official_fixed_window_options", "Official mode allows only fixed window presets: 30, 90, 180, and 365 days."),
    ("official_same_slice_ranking", "Official ranking compares runs only when the official benchmark version and window size match."),
    ("official_multimodal_bundle_binding", "If non-market modalities are used, official mode binds evaluation to the official multimodal benchmark bundle."),
    ("official_nlp_window_alignment", "If NLP is used, the requested NLP collection window must match the market template window."),
    ("official_source_asset_alignment", "Official source checks focus on benchmark asset alignment and platform-compatible schema, not the training vendor label."),
    (
        "official_nlp_quality_thresholds",
        (
            "If NLP is used, the official gate requires test-window coverage >= "
            f"{OFFICIAL_NLP_MIN_TEST_COVERAGE_RATIO:.0%}, max empty gap <= "
            f"{OFFICIAL_NLP_MAX_TEST_EMPTY_BARS} bars, duplicate ratio <= "
            f"{OFFICIAL_NLP_MAX_DUPLICATE_RATIO:.0%}, and entity link coverage >= "
            f"{OFFICIAL_NLP_MIN_ENTITY_LINK_COVERAGE_RATIO:.0%}."
        ),
    ),
)

OFFICIAL_REQUIRED_METADATA = (
    ("train_dataset_window", "Training dataset start/end time"),
    ("lookback_window", "Lookback window / context length"),
    ("label_horizon", "Label horizon"),
    ("modalities_and_fusion_summary", "Modalities and fusion summary"),
    ("random_seed", "Random seed"),
    ("tuning_trial_count", "Tuning trial count"),
    ("external_pretraining_flag", "External pretraining flag"),
    ("synthetic_data_flag", "Synthetic data flag"),
    ("actual_market_dataset_window", "Actual market dataset window"),
    ("actual_official_backtest_window", "Actual official backtest test window"),
    ("actual_nlp_coverage_window", "Actual NLP coverage window and official NLP gate result when NLP is present"),
    ("required_modalities_resolved", "Required modalities resolved for the official run"),
    ("official_rolling_benchmark_version", "Official rolling benchmark version"),
    ("official_rolling_window_size", "Official rolling window size and actual window start/end time"),
    ("official_market_benchmark_dataset_id", "Official market benchmark dataset id"),
    ("official_multimodal_benchmark_dataset_id", "Official multimodal benchmark dataset id when non-market signals are used"),
)

OFFICIAL_TEMPLATE_NOTES = (
    ("official_template_read_only", "The official template is read-only and cannot be deleted."),
    ("official_template_latest_market_environment", "The official template always uses the newest available market environment instead of the training dataset window."),
    ("official_window_comparison_scope", "Window size is user-selectable, but official rankings only compare results that use the same window preset."),
    ("custom_mode_flexible_controls", "Custom mode keeps dataset preset, scope, strategy, portfolio, and cost controls flexible."),
)


def _nlp_gate_reason_key(value: str) -> str | None:
    if value == "Official NLP gate passed: archival source, aligned time window, and quality thresholds satisfied.":
        return "official_nlp_gate_passed"
    if value.startswith("Official backtest template is blocked by the NLP quality gate:"):
        return "official_nlp_quality_gate_failed"
    if value == "NLP gate did not report details.":
        return "official_nlp_gate_missing_detail"
    return None


def custom_backtest_template() -> BacktestTemplateView:
    return BacktestTemplateView(
        template_id="custom::interactive",
        name="Custom Interactive Backtest",
        description=(
            "Flexible backtest mode. It preserves the existing workbench freedom to choose the "
            "dataset, prediction scope, and portfolio controls interactively."
        ),
        source="custom",
        read_only=False,
        official=False,
        protocol_version="interactive",
        output_contract_version=OFFICIAL_OUTPUT_CONTRACT_VERSION,
        ranking_policy="No same-template ranking contract is enforced in custom mode.",
        slice_policy="custom",
        scenario_bundle=["BASELINE"],
        eligibility_rules=[item[1] for item in CUSTOM_TEMPLATE_REQUIREMENTS],
        eligibility_rule_keys=[item[0] for item in CUSTOM_TEMPLATE_REQUIREMENTS],
        required_metadata=[],
        required_metadata_keys=[],
        notes=[item[1] for item in CUSTOM_TEMPLATE_NOTES],
        note_keys=[item[0] for item in CUSTOM_TEMPLATE_NOTES],
    )


def official_backtest_template(*, default_benchmark_symbol: str = "BTCUSDT") -> BacktestTemplateView:
    return BacktestTemplateView(
        template_id=OFFICIAL_BACKTEST_TEMPLATE_ID,
        name="Official Backtest Protocol v1",
        description=(
            "System template for current-market evaluation. It fixes prediction scope, stress "
            "bundle, and disclosure requirements while binding each launch to the newest "
            "official rolling benchmark version."
        ),
        source="system",
        read_only=True,
        official=True,
        protocol_version=OFFICIAL_PROTOCOL_VERSION,
        output_contract_version=OFFICIAL_OUTPUT_CONTRACT_VERSION,
        fixed_prediction_scope="test",
        ranking_policy=(
            "Run gate checks first, then compare only inside the same official rolling "
            "benchmark version and window size."
        ),
        slice_policy=(
            "Compare only within the same official rolling benchmark version, window size, "
            "frequency, target, horizon, and prediction scope."
        ),
        scenario_bundle=list(OFFICIAL_SCENARIO_BUNDLE),
        eligibility_rules=[item[1] for item in OFFICIAL_TEMPLATE_REQUIREMENTS],
        eligibility_rule_keys=[item[0] for item in OFFICIAL_TEMPLATE_REQUIREMENTS],
        required_metadata=[item[1] for item in OFFICIAL_REQUIRED_METADATA],
        required_metadata_keys=[item[0] for item in OFFICIAL_REQUIRED_METADATA],
        notes=[
            OFFICIAL_TEMPLATE_NOTES[0][1],
            f"The official template locks prediction scope to test and defaults the benchmark to {default_benchmark_symbol}.",
            OFFICIAL_TEMPLATE_NOTES[1][1],
            OFFICIAL_TEMPLATE_NOTES[2][1],
            OFFICIAL_TEMPLATE_NOTES[3][1],
        ],
        note_keys=[
            OFFICIAL_TEMPLATE_NOTES[0][0],
            "official_template_fixed_prediction_scope",
            OFFICIAL_TEMPLATE_NOTES[1][0],
            OFFICIAL_TEMPLATE_NOTES[2][0],
            OFFICIAL_TEMPLATE_NOTES[3][0],
        ],
    )


def build_official_backtest_request(
    *,
    prediction_frame_uri: str,
    benchmark_symbol: str,
    research_backend: str = "native",
    portfolio_method: str = "proportional",
) -> BacktestRequest:
    return BacktestRequest(
        prediction_frame_uri=prediction_frame_uri,
        research_backend=research_backend,
        strategy_config=StrategyConfig(
            name="sign_strategy",
            portfolio_method=portfolio_method,
        ),
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
    research_backend: str = "native",
    portfolio_method: str = "proportional",
) -> BacktestRequest:
    return BacktestRequest(
        prediction_frame_uri=prediction_frame_uri,
        research_backend=research_backend,
        strategy_config=StrategyConfig(
            name="sign_strategy",
            portfolio_method=portfolio_method,
        ),
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
    required_modalities: list[str] | None = None,
    official_benchmark_version: str | None = None,
    official_window_days: int | None = None,
    official_window_start_time: str | None = None,
    official_window_end_time: str | None = None,
    official_market_dataset_id: str | None = None,
    official_multimodal_dataset_id: str | None = None,
    official_dataset_ids: list[str] | None = None,
) -> dict[str, object]:
    slice_candidates = (
        [
            official_benchmark_version,
            str(official_window_days) if official_window_days is not None else None,
            dataset_frequency,
            target_name,
            str(label_horizon) if label_horizon is not None else None,
            prediction_scope,
        ]
        if template.official
        else [
            dataset_id,
            dataset_frequency,
            target_name,
            str(label_horizon) if label_horizon is not None else None,
            prediction_scope,
        ]
    )
    slice_coverage = [part for part in slice_candidates if part]
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
        "eligibility_rule_keys": list(template.eligibility_rule_keys),
        "required_metadata": list(template.required_metadata),
        "required_metadata_keys": list(template.required_metadata_keys),
        "notes": list(template.notes),
        "note_keys": list(template.note_keys),
        "metadata_summary": metadata_summary,
        "required_modalities": list(required_modalities or []),
        "lookback_bucket": lookback_bucket,
        "slice_coverage": slice_coverage,
        "official_benchmark_version": official_benchmark_version,
        "official_window_days": official_window_days,
        "official_window_start_time": official_window_start_time,
        "official_window_end_time": official_window_end_time,
        "official_market_dataset_id": official_market_dataset_id,
        "official_multimodal_dataset_id": official_multimodal_dataset_id,
        "official_dataset_ids": list(official_dataset_ids or []),
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
        eligibility_rule_keys=_str_list(protocol_metadata.get("eligibility_rule_keys")),
        required_metadata=_str_list(protocol_metadata.get("required_metadata")),
        required_metadata_keys=_str_list(protocol_metadata.get("required_metadata_keys")),
        notes=_str_list(protocol_metadata.get("notes")),
        note_keys=_str_list(protocol_metadata.get("note_keys")),
    )
    metadata_summary = {
        str(key): (_str(value) if value is not None else None)
        for key, value in dict(protocol_metadata.get("metadata_summary") or {}).items()
    }
    metadata_complete = all(metadata_summary.get(key) for key in metadata_summary)
    nlp_gate_status = _str(protocol_metadata.get("nlp_gate_status"))
    nlp_gate_reasons = _str_list(protocol_metadata.get("nlp_gate_reasons"))
    gate_results = [
        GateResultView(
            key="metadata_complete",
            label="Metadata Complete",
            label_key="metadata_complete",
            passed=metadata_complete,
            severity="warning",
            detail="Official comparison requires the training and backtest disclosure fields to be populated.",
            detail_key="training_and_backtest_disclosure_required",
        ),
        GateResultView(
            key="consistency_checks",
            label="Research / Simulation Consistency",
            label_key="research_simulation_consistency",
            passed=bool(passed_consistency_checks),
            severity="error",
            detail="Research and simulation outputs must not show an abnormal inversion relationship.",
            detail_key="research_and_simulation_no_abnormal_inversion",
        ),
        GateResultView(
            key="stress_bundle_complete",
            label="Stress Bundle Complete",
            label_key="stress_bundle_complete",
            passed=_stress_bundle_complete(protocol_metadata, scenario_metrics),
            severity="error",
            detail="Official comparison requires the fixed stress bundle to be present.",
            detail_key="fixed_stress_bundle_required",
        ),
        GateResultView(
            key="risk_limits",
            label="Risk Limits",
            label_key="risk_limits",
            passed=_risk_limits_passed(simulation_metrics, divergence_metrics, scenario_metrics),
            severity="error",
            detail=(
                f"Requires max_drawdown <= {DEFAULT_MAX_DRAWDOWN:.2f}, "
                f"turnover_total <= {DEFAULT_MAX_TURNOVER:.2f}, "
                f"stress_fail_count <= {DEFAULT_MAX_STRESS_FAILS:.0f}."
            ),
            detail_key="official_risk_limit_thresholds",
        ),
    ]
    if nlp_gate_status:
        gate_results.append(
            GateResultView(
                key="official_nlp_quality_gate",
                label="Official NLP Quality Gate",
                label_key="official_nlp_quality_gate",
                passed=(nlp_gate_status == "passed"),
                severity="error" if nlp_gate_status == "failed" else "warning",
                detail="; ".join(nlp_gate_reasons) if nlp_gate_reasons else "NLP gate did not report details.",
                detail_key=(
                    _nlp_gate_reason_key(nlp_gate_reasons[0])
                    if len(nlp_gate_reasons) == 1
                    else None
                )
                or "official_nlp_gate_missing_detail",
            )
        )
    has_error_failure = any(item.passed is False and item.severity == "error" for item in gate_results)
    has_warning = bool(comparison_warnings) or any(item.passed is False for item in gate_results)
    gate_status = "failed" if has_error_failure else ("warning" if has_warning else "passed")
    rank_components = [
        RankComponentView(
            key="annual_return",
            label="Annual Return",
            value=_float(simulation_metrics.get("annual_return")),
            detail="Raw component shown before same-template ranking.",
        ),
        RankComponentView(
            key="information_ratio",
            label="Information Ratio",
            value=_float(simulation_metrics.get("information_ratio")),
            detail="Capability component.",
        ),
        RankComponentView(
            key="max_drawdown",
            label="Max Drawdown",
            value=_float(simulation_metrics.get("max_drawdown")),
            detail="Risk component.",
        ),
        RankComponentView(
            key="implementation_shortfall",
            label="Implementation Shortfall",
            value=_float(simulation_metrics.get("implementation_shortfall")),
            detail="Execution cost component.",
        ),
        RankComponentView(
            key="worst_scenario_return_delta",
            label="Worst Scenario Delta",
            value=_float(scenario_metrics.get("worst_scenario_return_delta")),
            detail="Robustness component.",
        ),
        RankComponentView(
            key="simulation_minus_research_cumulative_return",
            label="Research/Simulation Divergence",
            value=_float(divergence_metrics.get("simulation_minus_research_cumulative_return")),
            detail="Closer to 0 is more trustworthy.",
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
        required_modalities=_str_list(protocol_metadata.get("required_modalities")),
        official_dataset_ids=_str_list(protocol_metadata.get("official_dataset_ids")),
        actual_market_start_time=_dt(protocol_metadata.get("actual_market_start_time")),
        actual_market_end_time=_dt(protocol_metadata.get("actual_market_end_time")),
        actual_backtest_start_time=_dt(protocol_metadata.get("actual_backtest_start_time")),
        actual_backtest_end_time=_dt(protocol_metadata.get("actual_backtest_end_time")),
        actual_nlp_start_time=_dt(protocol_metadata.get("actual_nlp_start_time")),
        actual_nlp_end_time=_dt(protocol_metadata.get("actual_nlp_end_time")),
        nlp_gate_status=nlp_gate_status,
        nlp_gate_reasons=nlp_gate_reasons,
        nlp_gate_reason_keys=[
            key for item in nlp_gate_reasons if (key := _nlp_gate_reason_key(item)) is not None
        ],
        official_benchmark_version=_str(protocol_metadata.get("official_benchmark_version")),
        official_window_days=_int(protocol_metadata.get("official_window_days")),
        official_window_start_time=_dt(protocol_metadata.get("official_window_start_time")),
        official_window_end_time=_dt(protocol_metadata.get("official_window_end_time")),
        official_market_dataset_id=_str(protocol_metadata.get("official_market_dataset_id")),
        official_multimodal_dataset_id=_str(protocol_metadata.get("official_multimodal_dataset_id")),
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


def _int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return int(text)
        except ValueError:
            return None
    return None


def _dt(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _str_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if isinstance(item, (int, float, str))]
