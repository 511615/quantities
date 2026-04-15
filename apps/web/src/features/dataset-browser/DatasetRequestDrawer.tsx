import { useMutation, useQueryClient } from "@tanstack/react-query";
import { type FormEvent, useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";

import { api } from "../../shared/api/client";
import { useDatasetRequestOptions, useJobStatus } from "../../shared/api/hooks";
import type {
  DatasetAcquisitionRequest,
  DatasetAcquisitionSourceRequest,
  DatasetDomainCapabilityView,
  DatasetOptionValueView,
  DatasetRequestOptionsView,
} from "../../shared/api/types";
import { I18N } from "../../shared/lib/i18n";
import { formatStageNameLabel } from "../../shared/lib/labels";
import { GlossaryHint } from "../../shared/ui/GlossaryHint";
import { StatusPill } from "../../shared/ui/StatusPill";

type DatasetRequestDrawerProps = {
  title?: string;
  description?: string;
  triggerTone?: "primary" | "secondary" | string;
  initialValues?: {
    dataDomain?: string;
    exchange?: string;
    frequency?: string;
    sourceVendor?: string;
    symbol?: string;
  };
};

type SourceDraft = {
  key: string;
  dataDomain: string;
  sourceVendor: string;
  exchange: string;
  frequency: string;
  symbolType: string;
  selectionMode: string;
  symbols: string;
  identifier: string;
};

const DEFAULT_DOMAIN_ORDER = ["market", "macro", "on_chain", "derivatives", "sentiment_events"];
const DEFAULT_VENDOR_BY_DOMAIN: Record<string, string> = {
  market: "binance",
  macro: "fred",
  on_chain: "defillama",
  derivatives: "binance_futures",
  sentiment_events: "reddit_archive",
};
const DEFAULT_EXCHANGE_BY_DOMAIN: Record<string, string> = {
  market: "binance",
};
const DEFAULT_IDENTIFIER_BY_DOMAIN: Record<string, string> = {
  macro: "DFF",
  on_chain: "ethereum",
  derivatives: "BTCUSDT",
  sentiment_events: "btc_news",
};
const DEFAULT_SYMBOL = "BTCUSDT";
const DOMAIN_LABELS: Record<string, string> = {
  market: "市场数据",
  macro: "宏观数据",
  on_chain: "链上数据",
  derivatives: "衍生品数据",
  sentiment_events: "情绪 / 事件数据",
};

function dateInputValue(date: Date) {
  return date.toISOString().slice(0, 10);
}

function defaultDateRange() {
  const end = new Date();
  const start = new Date(end);
  start.setDate(start.getDate() - 30);
  return {
    startDate: dateInputValue(start),
    endDate: dateInputValue(end),
  };
}

function createDraft(domain = "market"): SourceDraft {
  return {
    key: `${domain}-${Date.now()}-${Math.random().toString(16).slice(2)}`,
    dataDomain: domain,
    sourceVendor: "",
    exchange: "",
    frequency: "",
    symbolType: "",
    selectionMode: "",
    symbols: "",
    identifier: "",
  };
}

function domainLabel(domain: string) {
  return DOMAIN_LABELS[domain] ?? domain;
}

function localizeRequestOptionLabel(value: string, label?: string | null) {
  const normalized = (label ?? value).trim().toLowerCase();
  if (normalized === "news_archive") {
    return "新闻归档";
  }
  if (normalized === "gnews") {
    return "Google 新闻";
  }
  if (normalized === "reddit_archive") {
    return "Reddit 历史归档";
  }
  if (normalized === "manual_list") {
    return "手动列表";
  }
  if (normalized === "top_n") {
    return "前 N 个标的";
  }
  if (normalized === "binance") {
    return "币安";
  }
  if (normalized === "ccxt") {
    return "CCXT 聚合接入";
  }
  if (normalized === "okx") {
    return "OKX";
  }
  if (normalized === "baseline_market_features") {
    return "基础市场特征";
  }
  if (normalized === "time_series") {
    return "时间序列切分";
  }
  if (normalized === "available_time_safe_asof") {
    return "按可用时间安全对齐";
  }
  if (normalized === "single_domain") {
    return "单域产出";
  }
  return label ?? value;
}

function isCcxtMarketDraft(draft: SourceDraft) {
  return draft.dataDomain === "market" && draft.sourceVendor === "ccxt";
}

function marketSymbolPlaceholder(draft: SourceDraft) {
  return isCcxtMarketDraft(draft) ? "BTC/USDT, ETH/USDT" : "BTCUSDT";
}

function marketDraftHint(draft: SourceDraft) {
  if (!isCcxtMarketDraft(draft)) {
    return null;
  }
  const exchange = draft.exchange ? localizeRequestOptionLabel(draft.exchange) : "交易所";
  return `CCXT 会按 ${exchange} 的现货/合约市场去拉取 K 线；标的建议写成 BTC/USDT 这类交易所原生格式，切到 OKX 也可以直接复用。`;
}

function normalizeOptionValue(
  rawValue: string | undefined,
  options: DatasetOptionValueView[] | undefined,
  fallback = "",
) {
  const value = rawValue?.trim();
  if (value && options?.some((option) => option.value === value)) {
    return value;
  }
  if (value) {
    const lowered = value.toLowerCase();
    const matched = options?.find(
      (option) =>
        option.value.toLowerCase() === lowered || option.label.toLowerCase() === lowered,
    );
    if (matched) {
      return matched.value;
    }
  }
  return options?.find((option) => option.recommended)?.value ?? options?.[0]?.value ?? fallback;
}

function capabilityForDomain(
  options: DatasetRequestOptionsView | undefined,
  domain: string,
): DatasetDomainCapabilityView {
  const domainSpecific = options?.domain_capabilities?.[domain];
  const resolveOptions = (
    explicit: DatasetOptionValueView[] | undefined,
    supported: string[] | undefined,
    fallback: DatasetOptionValueView[] | undefined,
  ) => {
    if (explicit && explicit.length > 0) {
      return explicit;
    }
    if (supported && supported.length > 0) {
      return supported.map((value, index) => {
        const matched = fallback?.find((option) => option.value === value);
        return {
          value,
          label: matched?.label ?? value,
          description: matched?.description ?? null,
          recommended: matched?.recommended ?? index === 0,
        };
      });
    }
    return fallback ?? [];
  };
  return {
    source_vendors: resolveOptions(
      domainSpecific?.source_vendors,
      domainSpecific?.supported_vendors,
      options?.source_vendors,
    ),
    exchanges: resolveOptions(
      domainSpecific?.exchanges,
      domainSpecific?.supported_exchanges,
      options?.exchanges,
    ),
    frequencies: resolveOptions(
      domainSpecific?.frequencies,
      domainSpecific?.supported_frequencies,
      options?.frequencies,
    ),
    symbol_types: resolveOptions(
      domainSpecific?.symbol_types,
      domainSpecific?.supported_symbol_types,
      options?.symbol_types,
    ),
    selection_modes: resolveOptions(
      domainSpecific?.selection_modes,
      domainSpecific?.supported_selection_modes,
      options?.selection_modes,
    ),
  };
}

function hydrateDraft(
  draft: SourceDraft,
  options: DatasetRequestOptionsView | undefined,
  initialValues?: DatasetRequestDrawerProps["initialValues"],
) {
  const domain = draft.dataDomain || initialValues?.dataDomain || "market";
  const capability = capabilityForDomain(options, domain);
  const preferredVendor =
    draft.sourceVendor || (domain === "market" ? initialValues?.sourceVendor : undefined);
  const preferredExchange =
    draft.exchange || (domain === "market" ? initialValues?.exchange : undefined);
  const preferredFrequency = draft.frequency || initialValues?.frequency;
  const preferredSymbols =
    draft.symbols || (domain === "market" ? initialValues?.symbol : undefined) || "";

  return {
    ...draft,
    dataDomain: domain,
    sourceVendor: normalizeOptionValue(
      preferredVendor,
      capability.source_vendors,
      DEFAULT_VENDOR_BY_DOMAIN[domain] ?? "",
    ),
    exchange:
      domain === "market"
        ? normalizeOptionValue(
            preferredExchange,
            capability.exchanges,
            DEFAULT_EXCHANGE_BY_DOMAIN[domain] ?? "",
          )
        : "",
    frequency: normalizeOptionValue(preferredFrequency, capability.frequencies, "1h"),
    symbolType: normalizeOptionValue(draft.symbolType, capability.symbol_types, "spot"),
    selectionMode: normalizeOptionValue(
      draft.selectionMode,
      capability.selection_modes,
      "manual_list",
    ),
    symbols:
      domain === "market"
        ? preferredSymbols || DEFAULT_SYMBOL
        : "",
    identifier:
      domain === "market"
        ? ""
        : draft.identifier || DEFAULT_IDENTIFIER_BY_DOMAIN[domain] || "",
  };
}

function recommendedValue(options: DatasetOptionValueView[] | undefined, fallback: string) {
  return options?.find((option) => option.recommended)?.value ?? options?.[0]?.value ?? fallback;
}

function buildSymbolSelector(draft: SourceDraft) {
  const symbols = draft.symbols
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
  return {
    symbol_type: draft.symbolType || "spot",
    selection_mode: draft.selectionMode || "manual_list",
    symbols,
    symbol_count: symbols.length > 0 ? symbols.length : null,
    tags: [],
  };
}

function buildSourceRequest(draft: SourceDraft): DatasetAcquisitionSourceRequest {
  if (draft.dataDomain === "market") {
    return {
      data_domain: "market",
      source_vendor: draft.sourceVendor,
      exchange: draft.exchange || null,
      frequency: draft.frequency,
      symbol_selector: buildSymbolSelector(draft),
      filters: {},
    };
  }
  return {
    data_domain: draft.dataDomain,
    source_vendor: draft.sourceVendor,
    frequency: draft.frequency,
    identifier: draft.identifier.trim(),
    filters: {},
  };
}

function alignDraftFrequencies(
  drafts: SourceDraft[],
  options: DatasetRequestOptionsView | undefined,
): SourceDraft[] {
  if (drafts.length <= 1) {
    return drafts;
  }
  const supportedSets = drafts.map((draft) => {
    const values = capabilityForDomain(options, draft.dataDomain).frequencies
      .map((option) => option.value)
      .filter(Boolean);
    return new Set(values);
  });
  const commonFrequencies = drafts
    .map((draft) => capabilityForDomain(options, draft.dataDomain).frequencies.map((option) => option.value))
    .find((values) => values.length > 0)
    ?.filter((value) => supportedSets.every((supported) => supported.has(value))) ?? [];
  if (commonFrequencies.length === 0) {
    return drafts;
  }
  const currentFrequencies = new Set(drafts.map((draft) => draft.frequency).filter(Boolean));
  if (currentFrequencies.size === 1 && commonFrequencies.includes(drafts[0]?.frequency ?? "")) {
    return drafts;
  }
  const nextFrequency = commonFrequencies[0];
  return drafts.map((draft) => ({ ...draft, frequency: nextFrequency }));
}

export function DatasetRequestDrawer({
  title = "\u7533\u8bf7\u6570\u636e\u96c6",
  description = "\u4e00\u6b21\u9009\u62e9 1..n \u4e2a\u57df\uff0c\u591a\u57df\u65f6\u76f4\u63a5\u4ea7\u51fa\u5408\u5e76\u6570\u636e\u96c6\uff0c\u7136\u540e\u7528\u6570\u636e\u96c6 ID \u8fde\u5230\u8bad\u7ec3\u548c\u56de\u6d4b\u3002",
  triggerTone = "primary",
  initialValues,
}: DatasetRequestDrawerProps) {
  const initialDateRange = useMemo(() => defaultDateRange(), []);
  const queryClient = useQueryClient();
  const optionsQuery = useDatasetRequestOptions();
  const [open, setOpen] = useState(false);
  const [jobId, setJobId] = useState<string | null>(null);
  const [formError, setFormError] = useState<string | null>(null);
  const [startDate, setStartDate] = useState(initialDateRange.startDate);
  const [endDate, setEndDate] = useState(initialDateRange.endDate);
  const [sourceDrafts, setSourceDrafts] = useState<SourceDraft[]>([
    createDraft(initialValues?.dataDomain || "market"),
  ]);
  const jobQuery = useJobStatus(jobId);

  useEffect(() => {
    setSourceDrafts((current) =>
      current.map((draft) => hydrateDraft(draft, optionsQuery.data, initialValues)),
    );
  }, [initialValues, optionsQuery.data]);

  const domainOptions = useMemo(() => {
    const apiValues = optionsQuery.data?.domains?.map((option) => option.value) ?? [];
    return Array.from(new Set([...DEFAULT_DOMAIN_ORDER, ...apiValues].filter(Boolean)));
  }, [optionsQuery.data?.domains]);

  const availableDomainsToAdd = domainOptions.filter(
    (domain) => !sourceDrafts.some((draft) => draft.dataDomain === domain),
  );

  const requestMutation = useMutation({
    mutationFn: (payload: DatasetAcquisitionRequest) => api.requestDataset(payload),
    onSuccess: (result) => {
      setJobId(result.job_id);
      setFormError(null);
      void queryClient.invalidateQueries({ queryKey: ["jobs"] });
      void queryClient.invalidateQueries({ queryKey: ["workbench-overview"] });
    },
  });

  const datasetId = jobQuery.data?.result?.dataset_id ?? null;
  const datasetDetailHref = jobQuery.data?.result?.deeplinks?.dataset_detail ?? null;
  const runDetailHref = jobQuery.data?.result?.deeplinks?.run_detail ?? null;
  const trainHref = datasetId
    ? `/models?launchTrain=1&datasetId=${encodeURIComponent(datasetId)}`
    : null;

  function updateDraft(key: string, updater: (draft: SourceDraft) => SourceDraft) {
    setSourceDrafts((current) =>
      current.map((draft) =>
        draft.key === key
          ? hydrateDraft(updater(draft), optionsQuery.data, initialValues)
          : draft,
      ),
    );
  }

  function addDomain() {
    const nextDomain = availableDomainsToAdd[0];
    if (!nextDomain) {
      return;
    }
    setSourceDrafts((current) =>
      alignDraftFrequencies(
        [
          ...current,
          hydrateDraft(createDraft(nextDomain), optionsQuery.data, initialValues),
        ],
        optionsQuery.data,
      ),
    );
  }

  function removeDomain(key: string) {
    setSourceDrafts((current) =>
      current.length > 1 ? current.filter((draft) => draft.key !== key) : current,
    );
  }

  function validateRequest(drafts: SourceDraft[]) {
    if (!startDate || !endDate) {
      return "\u8bf7\u5148\u9009\u62e9\u5b8c\u6574\u7684\u65f6\u95f4\u7a97\u53e3\u3002";
    }
    if (startDate > endDate) {
      return "\u5f00\u59cb\u65e5\u671f\u4e0d\u80fd\u665a\u4e8e\u7ed3\u675f\u65e5\u671f\u3002";
    }
    if (new Set(drafts.map((draft) => draft.dataDomain)).size !== drafts.length) {
      return "\u540c\u4e00\u57df\u4e0d\u9700\u8981\u91cd\u590d\u6dfb\u52a0\uff0c\u8bf7\u79fb\u9664\u91cd\u590d\u914d\u7f6e\u3002";
    }
    const multiDomain = drafts.length > 1;
    const marketCount = drafts.filter((draft) => draft.dataDomain === "market").length;
    if (multiDomain && marketCount !== 1) {
      return "\u53ef\u8bad\u7ec3 / \u53ef\u56de\u6d4b\u7684\u5408\u5e76\u8bf7\u6c42\u5fc5\u987b\u5305\u542b\u4e14\u4ec5\u5305\u542b\u4e00\u4e2a\u5e02\u573a\u951a\u70b9\u3002";
    }
    if (multiDomain && new Set(drafts.map((draft) => draft.frequency)).size !== 1) {
      return "\u591a\u57df\u76f4\u63a5\u5408\u5e76\u8981\u6c42\u6240\u6709\u6570\u636e\u6e90\u7684\u9891\u7387\u5b8c\u5168\u4e00\u81f4\u3002";
    }
    const marketDraft = drafts.find((draft) => draft.dataDomain === "market");
    if (marketDraft && !marketDraft.symbols.trim()) {
      return "请至少为市场数据域填写一个标的。";
    }
    const missingIdentifier = drafts.find(
      (draft) => draft.dataDomain !== "market" && !draft.identifier.trim(),
    );
    if (missingIdentifier) {
      return `请为${domainLabel(missingIdentifier.dataDomain)}填写标识符。`;
    }
    return null;
  }

  function submitRequest() {
    const normalizedDrafts = alignDraftFrequencies(
      sourceDrafts.map((draft) => hydrateDraft(draft, optionsQuery.data, initialValues)),
      optionsQuery.data,
    );
    setSourceDrafts(normalizedDrafts);

    const validationError = validateRequest(normalizedDrafts);
    if (validationError) {
      setFormError(validationError);
      return;
    }

    const marketDraft =
      normalizedDrafts.find((draft) => draft.dataDomain === "market") ?? normalizedDrafts[0];
    const singleDraft = normalizedDrafts[0];
    const isSingleDomain = normalizedDrafts.length === 1;
    const isSingleMarket = isSingleDomain && singleDraft?.dataDomain === "market";
    const singleDomain = isSingleDomain ? singleDraft.dataDomain : "market";
    const singleSelectionMode =
      isSingleMarket
        ? singleDraft.selectionMode || "manual_list"
        : "explicit";

    const payload: DatasetAcquisitionRequest = {
      request_name: `workbench-${normalizedDrafts.length > 1 ? "merged" : singleDomain}-${Date.now()}`,
      data_domain: isSingleDomain ? singleDomain : "market",
      dataset_type: "training_panel",
      asset_mode: "single_asset",
      time_window: {
        start_time: `${startDate}T00:00:00Z`,
        end_time: `${endDate}T23:59:59Z`,
      },
      symbol_selector: isSingleMarket ? buildSymbolSelector(marketDraft) : undefined,
      selection_mode: singleSelectionMode,
      source_vendor: isSingleDomain ? singleDraft.sourceVendor : undefined,
      exchange: isSingleMarket ? marketDraft.exchange : undefined,
      frequency: isSingleDomain ? singleDraft.frequency : undefined,
      filters: {},
      sources: normalizedDrafts.map(buildSourceRequest),
      merge_policy_name: normalizedDrafts.length > 1 ? "available_time_safe_asof" : undefined,
      build_config: {
        feature_set_id: recommendedValue(
          optionsQuery.data?.feature_sets,
          "baseline_market_features",
        ),
        label_horizon: Number(
          recommendedValue(optionsQuery.data?.label_horizons, "1"),
        ),
        label_kind: "regression",
        split_strategy: recommendedValue(optionsQuery.data?.split_strategies, "time_series"),
        sample_policy_name: recommendedValue(
          optionsQuery.data?.sample_policies,
          "training_panel_strict",
        ),
        alignment_policy_name: recommendedValue(
          optionsQuery.data?.alignment_policies,
          normalizedDrafts.length > 1 ? "available_time_safe_asof" : "event_time_inner",
        ),
        missing_feature_policy_name: recommendedValue(
          optionsQuery.data?.missing_feature_policies,
          "drop_if_missing",
        ),
        sample_policy: {},
        alignment_policy: {},
        missing_feature_policy: {},
      },
    };

    setFormError(null);
    requestMutation.mutate(payload);
  }

  return (
    <div className="drawer-wrap">
      <button
        data-testid="dataset-request-trigger"
        className={triggerTone === "secondary" ? "action-button secondary" : "action-button"}
        onClick={() => setOpen((value) => !value)}
        type="button"
      >
        {"\u7533\u8bf7\u65b0\u6570\u636e\u96c6"}
      </button>
      {open ? (
        <div className="drawer-panel">
          <h3>{title}</h3>
          <p className="drawer-copy">{description}</p>

          <div className="dataset-callout">
            <strong>
              {sourceDrafts.length > 1
                ? "\u591a\u57df\u76f4\u63a5\u5408\u5e76"
                : "\u5355\u57df\u8bad\u7ec3\u6570\u636e\u96c6"}
            </strong>
            <span>
              {sourceDrafts.length > 1
                ? "\u591a\u57df\u65f6\u4f1a\u7528\u5e02\u573a\u57df\u4f5c\u4e3a\u552f\u4e00\u951a\u70b9\uff0c\u540e\u7aef\u6309\u201c\u6309\u53ef\u7528\u65f6\u95f4\u5b89\u5168\u5bf9\u9f50\u201d\u7b56\u7565\u76f4\u63a5\u5408\u5e76\u4e3a\u6700\u7ec8\u6570\u636e\u96c6\u3002"
                : "\u5355\u57df\u65f6\u4f1a\u76f4\u63a5\u4ea7\u51fa\u53ef\u7ee7\u7eed\u4f7f\u7528\u6570\u636e\u96c6 ID \u53d1\u8d77\u8bad\u7ec3\u7684\u6570\u636e\u96c6\u3002"}
            </span>
          </div>

          <form
            className="page-stack"
            onSubmit={(event: FormEvent<HTMLFormElement>) => {
              event.preventDefault();
              submitRequest();
            }}
          >
            <div className="form-section-grid">
              <label>
                <span>{"\u5f00\u59cb\u65e5\u671f"}</span>
                <input
                  className="field"
                  onChange={(event) => setStartDate(event.target.value)}
                  type="date"
                  value={startDate}
                />
              </label>
              <label>
                <span>{"\u7ed3\u675f\u65e5\u671f"}</span>
                <input
                  className="field"
                  onChange={(event) => setEndDate(event.target.value)}
                  type="date"
                  value={endDate}
                />
              </label>
            </div>

            <section className="page-stack compact-gap">
              <div className="split-line">
                <span className="form-label-row">
                  <strong>{"\u57df\u914d\u7f6e"}</strong>
                  <GlossaryHint hintKey="data_domain" iconOnly />
                </span>
                <button
                  className="link-button"
                  disabled={availableDomainsToAdd.length === 0}
                  onClick={addDomain}
                  type="button"
                >
                  添加数据域
                </button>
              </div>

              {sourceDrafts.map((draft, index) => {
                const capability = capabilityForDomain(optionsQuery.data, draft.dataDomain);
                return (
                  <section className="panel form-shell" key={draft.key}>
                    <div className="split-line">
                      <strong>
                        {draft.dataDomain === "market"
                          ? "\u5e02\u573a\u951a\u70b9"
                          : `\u8f85\u52a9\u57df ${index}`}
                      </strong>
                      {sourceDrafts.length > 1 ? (
                        <button
                          className="link-button danger-link"
                          onClick={() => removeDomain(draft.key)}
                          type="button"
                        >
                          {I18N.action.delete}
                        </button>
                      ) : null}
                    </div>

                    <div className="form-section-grid">
                      <label>
                        <span>{"\u57df"}</span>
                        <select
                          className="field"
                          onChange={(event) =>
                            updateDraft(draft.key, (current) => ({
                              ...current,
                              dataDomain: event.target.value,
                              sourceVendor: "",
                              exchange: "",
                              identifier: "",
                            }))
                          }
                          value={draft.dataDomain}
                        >
                          {domainOptions.map((domain) => (
                            <option key={domain} value={domain}>
                              {domainLabel(domain)}
                            </option>
                          ))}
                        </select>
                      </label>

                      <label>
                        <span>{"鏉ユ簮"}</span>
                        <select
                          className="field"
                          onChange={(event) =>
                            updateDraft(draft.key, (current) => ({
                              ...current,
                              sourceVendor: event.target.value,
                            }))
                          }
                          value={draft.sourceVendor}
                        >
                          {capability.source_vendors.map((option) => (
                            <option key={option.value} value={option.value}>
                              {localizeRequestOptionLabel(option.value, option.label)}
                            </option>
                          ))}
                        </select>
                      </label>

                      <label>
                        <span>{"\u9891\u7387"}</span>
                        <select
                          className="field"
                          onChange={(event) =>
                            updateDraft(draft.key, (current) => ({
                              ...current,
                              frequency: event.target.value,
                            }))
                          }
                          value={draft.frequency}
                        >
                          {capability.frequencies.map((option) => (
                            <option key={option.value} value={option.value}>
                              {option.label}
                            </option>
                          ))}
                        </select>
                      </label>

                      {draft.dataDomain === "market" ? (
                        <>
                          <label>
                            <span>{"浜ゆ槗鎵€"}</span>
                            <select
                              className="field"
                              onChange={(event) =>
                                updateDraft(draft.key, (current) => ({
                                  ...current,
                                  exchange: event.target.value,
                                }))
                              }
                              value={draft.exchange}
                            >
                              {(capability.exchanges ?? []).map((option) => (
                                <option key={option.value} value={option.value}>
                                  {localizeRequestOptionLabel(option.value, option.label)}
                                </option>
                              ))}
                            </select>
                          </label>

                          <label>
                            <span className="form-label-row">
                              <span>{"\u9009\u62e9\u65b9\u5f0f"}</span>
                              <GlossaryHint hintKey="selection_mode" iconOnly />
                            </span>
                            <select
                              className="field"
                              onChange={(event) =>
                                updateDraft(draft.key, (current) => ({
                                  ...current,
                                  selectionMode: event.target.value,
                                }))
                              }
                              value={draft.selectionMode}
                            >
                              {(capability.selection_modes ?? []).map((option) => (
                                <option key={option.value} value={option.value}>
                                  {localizeRequestOptionLabel(option.value, option.label)}
                                </option>
                              ))}
                            </select>
                          </label>

                          <label>
                            <span>{"鏍囩殑"}</span>
                            <input
                              className="field"
                              onChange={(event) =>
                                updateDraft(draft.key, (current) => ({
                                  ...current,
                                  symbols: event.target.value,
                                }))
                              }
                              placeholder={marketSymbolPlaceholder(draft)}
                              value={draft.symbols}
                            />
                          </label>
                          {marketDraftHint(draft) ? (
                            <div className="dataset-callout">
                              <strong>CCXT 市场提示</strong>
                              <span>{marketDraftHint(draft)}</span>
                            </div>
                          ) : null}
                        </>
                      ) : (
                        <label>
                          <span>{"标识符"}</span>
                          <input
                            className="field"
                            onChange={(event) =>
                              updateDraft(draft.key, (current) => ({
                                ...current,
                                identifier: event.target.value,
                              }))
                            }
                            placeholder={DEFAULT_IDENTIFIER_BY_DOMAIN[draft.dataDomain] ?? "璇疯緭鍏ユ爣璇嗙"}
                            value={draft.identifier}
                          />
                        </label>
                      )}
                    </div>
                  </section>
                );
              })}
            </section>

            <section className="dataset-callout">
              <strong className="form-label-row">
                <span>{"\u6784\u5efa\u89c4\u5219"}</span>
                <GlossaryHint hintKey="sample_policy" iconOnly />
              </strong>
              <span>
                {`\u7279\u5f81\uff1a${localizeRequestOptionLabel(recommendedValue(optionsQuery.data?.feature_sets, "baseline_market_features"))} / \u5207\u5206\uff1a${localizeRequestOptionLabel(recommendedValue(optionsQuery.data?.split_strategies, "time_series"))} / \u5408\u5e76\uff1a${localizeRequestOptionLabel(sourceDrafts.length > 1 ? "available_time_safe_asof" : "single_domain")}`}
              </span>
            </section>

            {formError ? <p className="form-error">{formError}</p> : null}
            {requestMutation.isError ? (
              <p className="form-error">{(requestMutation.error as Error).message}</p>
            ) : null}

            <button
              className="action-button secondary"
              disabled={requestMutation.isPending}
              onClick={submitRequest}
              type="button"
            >
              {requestMutation.isPending
                ? "\u63d0\u4ea4\u4e2d..."
                : "\u63d0\u4ea4\u6570\u636e\u8bf7\u6c42"}
            </button>
          </form>

          {jobQuery.data ? (
            <div className="job-box">
              <div className="split-line">
                <strong>{jobQuery.data.job_id}</strong>
                <StatusPill status={jobQuery.data.status} />
              </div>
              {jobQuery.data.stages.map((stage) => (
                <div className="job-stage" key={stage.name}>
                  <span>{formatStageNameLabel(stage.name)}</span>
                  <span>{stage.summary}</span>
                </div>
              ))}
              {jobQuery.data.error_message ? (
                <p className="form-error">{jobQuery.data.error_message}</p>
              ) : null}
              {jobQuery.data.status === "success" ? (
                <div className="table-actions">
                  {datasetDetailHref ? (
                    <Link className="link-button" to={datasetDetailHref}>
                      {"\u67e5\u770b\u6570\u636e\u96c6\u8be6\u60c5"}
                    </Link>
                  ) : null}
                  {runDetailHref ? (
                    <Link className="link-button" to={runDetailHref}>
                      {"\u67e5\u770b\u8fd0\u884c\u8be6\u60c5"}
                    </Link>
                  ) : null}
                  {!runDetailHref && trainHref ? (
                    <Link className="link-button" to={trainHref}>
                      {"\u7ee7\u7eed\u8fd9\u4efd\u6570\u636e\u96c6\u8bad\u7ec3"}
                    </Link>
                  ) : null}
                </div>
              ) : null}
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}

