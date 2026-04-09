import { useState } from "react";
import { Link, useSearchParams } from "react-router-dom";

import { DatasetDeleteDialog } from "../features/dataset-browser/DatasetDeleteDialog";
import { DatasetRequestDrawer } from "../features/dataset-browser/DatasetRequestDrawer";
import { DatasetWorkspaceNav } from "../features/dataset-browser/DatasetWorkspaceNav";
import {
  adaptDatasetSummary,
  createApiNotReadyMessage,
  createDatasetFacets,
  filterDatasetSummaries,
  isApiNotReadyError,
  type DatasetBrowserFilters,
} from "../features/dataset-browser/workbench";
import { useDatasetRequestOptions, useDatasets } from "../shared/api/hooks";
import type { DatasetOptionValueView } from "../shared/api/types";
import { formatDate } from "../shared/lib/format";
import type { GlossaryKey } from "../shared/lib/i18n";
import { GlossaryHint } from "../shared/ui/GlossaryHint";
import { PanelHeader } from "../shared/ui/PanelHeader";
import { EmptyState, ErrorState, LoadingState } from "../shared/ui/StateViews";

function TermLabel({ label, hintKey }: { label: string; hintKey: GlossaryKey }) {
  return (
    <span className="dataset-label-with-hint">
      <span>{label}</span>
      <GlossaryHint hintKey={hintKey} iconOnly />
    </span>
  );
}

function readFilters(searchParams: URLSearchParams): DatasetBrowserFilters {
  return {
    data_domain: searchParams.get("data_domain") ?? "",
    dataset_type: searchParams.get("dataset_type") ?? "",
    source: searchParams.get("source") ?? "",
    exchange: searchParams.get("exchange") ?? "",
    symbol: searchParams.get("symbol") ?? "",
    frequency: searchParams.get("frequency") ?? "",
    version: searchParams.get("version") ?? "",
    time_from: searchParams.get("time_from") ?? "",
    time_to: searchParams.get("time_to") ?? "",
  };
}

function filterLabel(value: string) {
  if (!value) {
    return "全部";
  }

  const labels: Record<string, string> = {
    market: "市场数据",
    derivatives: "衍生品数据",
    on_chain: "链上数据",
    macro: "宏观数据",
    sentiment_events: "情绪 / 事件数据",
    training_panel: "训练面板",
    fusion_training_panel: "融合训练面板",
    display_slice: "展示切片",
    feature_snapshot: "特征快照",
  };

  return labels[value] ?? value;
}

function mergeFacetValues(fallback: string[], backend: string[]) {
  return Array.from(new Set([...backend, ...fallback].filter(Boolean))).sort((a, b) =>
    a.localeCompare(b, "zh-CN"),
  );
}

function optionValues(
  options: DatasetOptionValueView[] | undefined,
  strategy: "value" | "label" = "value",
) {
  return (options ?? [])
    .map((item) => (strategy === "label" ? item.label || item.value : item.value))
    .filter(Boolean);
}

export function DatasetsBrowserPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [deleteTarget, setDeleteTarget] = useState<{ datasetId: string; label: string } | null>(
    null,
  );
  const [deleteNotice, setDeleteNotice] = useState<string | null>(null);

  const filters = readFilters(searchParams);
  const datasetsQuery = useDatasets(1, 100);
  const requestOptionsQuery = useDatasetRequestOptions();

  if (datasetsQuery.isLoading) {
    return <LoadingState />;
  }

  if (datasetsQuery.isError) {
    return <ErrorState message={(datasetsQuery.error as Error).message} />;
  }

  const adapted = (datasetsQuery.data?.items ?? []).map(adaptDatasetSummary);
  const fallbackFacets = createDatasetFacets(datasetsQuery.data?.items ?? []);
  const facets = {
    domains: mergeFacetValues(
      fallbackFacets.domains,
      optionValues(requestOptionsQuery.data?.domains, "value"),
    ),
    types: fallbackFacets.types,
    sources: mergeFacetValues(
      fallbackFacets.sources,
      optionValues(requestOptionsQuery.data?.source_vendors, "label"),
    ),
    exchanges: mergeFacetValues(
      fallbackFacets.exchanges,
      optionValues(requestOptionsQuery.data?.exchanges, "label"),
    ),
    symbols: fallbackFacets.symbols,
    frequencies: mergeFacetValues(
      fallbackFacets.frequencies,
      optionValues(requestOptionsQuery.data?.frequencies, "value"),
    ),
    versions: fallbackFacets.versions,
  };
  const filteredItems = filterDatasetSummaries(adapted, filters);

  const updateFilter = (key: keyof DatasetBrowserFilters, value: string) => {
    const next = new URLSearchParams(searchParams);
    if (value) {
      next.set(key, value);
    } else {
      next.delete(key);
    }
    setSearchParams(next, { replace: true });
  };

  const activeSummary = Object.entries(filters)
    .filter(([, value]) => value)
    .map(([key, value]) => `${key}: ${filterLabel(value)}`)
    .join(" / ");

  return (
    <div className="page-stack">
      <section className="hero-strip compact-hero">
        <div>
          <div className="eyebrow">数据浏览器</div>
          <h1>按数据域、来源与版本浏览</h1>
          <p>
            先按真实后端能力筛出数据域、来源、交易所、频率和版本，再决定进入详情、查看依赖，还是直接发起新的数据申请。
          </p>
        </div>
        <div className="hero-actions">
          <DatasetRequestDrawer
            description="按照当前筛选条件发起新的数据集申请，提交后继续沿用现有任务追踪链路。"
            initialValues={{
              dataDomain: filters.data_domain || undefined,
              exchange: filters.exchange || undefined,
              frequency: filters.frequency || undefined,
              sourceVendor: filters.source || undefined,
              symbol: filters.symbol || undefined,
            }}
            title="按当前筛选申请数据集"
            triggerTone="secondary"
          />
          <Link className="comparison-link" to="/datasets">
            返回总览
          </Link>
          <Link className="comparison-link" to="/datasets/training">
            查看训练面板
          </Link>
        </div>
      </section>

      <DatasetWorkspaceNav />

      {deleteNotice ? (
        <section className="dataset-callout">
          <strong>数据集已处理</strong>
          <span>{deleteNotice}</span>
        </section>
      ) : null}

      <section className="panel">
        <PanelHeader
          eyebrow="筛选条件"
          title="后端驱动的真实筛选项"
          description="优先使用后端返回的 options；如果某个维度暂未开放，前端才退回到目录观察值。"
        />

        {requestOptionsQuery.isError ? (
          <div className="dataset-callout">
            <strong>
              {isApiNotReadyError(requestOptionsQuery.error)
                ? "筛选项接口暂未就绪"
                : "筛选项加载失败"}
            </strong>
            <span>
              {isApiNotReadyError(requestOptionsQuery.error)
                ? `${createApiNotReadyMessage("数据筛选项")} 当前先用目录里的真实值继续浏览。`
                : (requestOptionsQuery.error as Error).message}
            </span>
          </div>
        ) : null}

        <div className="form-section-grid dataset-filter-grid">
          <label>
            <span>
              <TermLabel hintKey="data_domain" label="数据域" />
            </span>
            <select
              className="field"
              onChange={(event) => updateFilter("data_domain", event.target.value)}
              value={filters.data_domain}
            >
              <option value="">全部</option>
              {facets.domains.map((item) => (
                <option key={item} value={item}>
                  {filterLabel(item)}
                </option>
              ))}
            </select>
          </label>

          <label>
            <span>
              <TermLabel hintKey="dataset_type" label="数据类型" />
            </span>
            <select
              className="field"
              onChange={(event) => updateFilter("dataset_type", event.target.value)}
              value={filters.dataset_type}
            >
              <option value="">全部</option>
              {facets.types.map((item) => (
                <option key={item} value={item}>
                  {filterLabel(item)}
                </option>
              ))}
            </select>
          </label>

          <label>
            <span>来源</span>
            <select
              className="field"
              onChange={(event) => updateFilter("source", event.target.value)}
              value={filters.source}
            >
              <option value="">全部</option>
              {facets.sources.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            </select>
          </label>

          <label>
            <span>交易所</span>
            <select
              className="field"
              onChange={(event) => updateFilter("exchange", event.target.value)}
              value={filters.exchange}
            >
              <option value="">全部</option>
              {facets.exchanges.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            </select>
          </label>

          <label>
            <span>资产 / Symbol</span>
            <select
              className="field"
              onChange={(event) => updateFilter("symbol", event.target.value)}
              value={filters.symbol}
            >
              <option value="">全部</option>
              {facets.symbols.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            </select>
          </label>

          <label>
            <span>频率</span>
            <select
              className="field"
              onChange={(event) => updateFilter("frequency", event.target.value)}
              value={filters.frequency}
            >
              <option value="">全部</option>
              {facets.frequencies.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            </select>
          </label>

          <label>
            <span>
              <TermLabel hintKey="snapshot_version" label="版本" />
            </span>
            <select
              className="field"
              onChange={(event) => updateFilter("version", event.target.value)}
              value={filters.version}
            >
              <option value="">全部</option>
              {facets.versions.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            </select>
          </label>

          <label>
            <span>开始时间</span>
            <input
              className="field"
              onChange={(event) =>
                updateFilter("time_from", event.target.value ? `${event.target.value}T00:00:00Z` : "")
              }
              type="date"
              value={filters.time_from ? filters.time_from.slice(0, 10) : ""}
            />
          </label>

          <label>
            <span>结束时间</span>
            <input
              className="field"
              onChange={(event) =>
                updateFilter("time_to", event.target.value ? `${event.target.value}T23:59:59Z` : "")
              }
              type="date"
              value={filters.time_to ? filters.time_to.slice(0, 10) : ""}
            />
          </label>
        </div>

        <div className="dataset-filter-summary">
          <strong>当前结果：</strong>
          <span>{filteredItems.length} 份数据集</span>
          <span>{activeSummary || "还没有启用筛选条件"}</span>
        </div>
      </section>

      {filteredItems.length === 0 ? (
        <EmptyState
          title="没有匹配的数据集"
          body="放宽一个或多个筛选条件后再试，通常先从数据域、来源和频率开始回退最有效。"
        />
      ) : (
        <section className="panel">
          <PanelHeader
            eyebrow="结果目录"
            title="数据集目录"
            description="这里展示的是当前目录中真实存在的数据集，而不是前端根据本地文件猜出来的列表。"
          />

          <div className="dataset-browser-table-shell">
            <table className="data-table">
              <thead>
                <tr>
                  <th>名称</th>
                  <th>
                    <TermLabel hintKey="data_domain" label="数据域" />
                  </th>
                  <th>
                    <TermLabel hintKey="dataset_type" label="类型" />
                  </th>
                  <th>来源 / 交易所</th>
                  <th>覆盖范围</th>
                  <th>频率</th>
                  <th>
                    <TermLabel hintKey="snapshot_version" label="版本" />
                  </th>
                  <th>
                    <TermLabel hintKey="freshness" label="新鲜度" />
                  </th>
                  <th>健康状态</th>
                  <th>操作</th>
                </tr>
              </thead>
              <tbody>
                {filteredItems.map((item) => (
                  <tr key={item.datasetId}>
                    <td>
                      <Link to={`/datasets/${item.datasetId}`}>{item.title}</Link>
                      <div className="dataset-row-subcopy">
                        <span>{item.subtitle}</span>
                        <span>技术标识：{item.technicalId}</span>
                      </div>
                    </td>
                    <td>{item.dataDomainLabel}</td>
                    <td>{item.datasetTypeLabel}</td>
                    <td>
                      {item.sourceVendor}
                      <div className="dataset-row-subcopy">
                        <span>{item.exchangeLabel}</span>
                        <span>{item.symbolLabel}</span>
                      </div>
                    </td>
                    <td>{item.coverageLabel}</td>
                    <td>{item.frequencyLabel}</td>
                    <td>
                      {item.snapshotVersion}
                      <div className="dataset-row-subcopy">
                        <span>{formatDate(item.asOfTime)}</span>
                      </div>
                    </td>
                    <td>{item.freshnessLabel}</td>
                    <td>{item.healthLabel}</td>
                    <td>
                      <div className="table-actions">
                        <Link className="link-button" to={`/datasets/${item.datasetId}`}>
                          查看详情
                        </Link>
                        <button
                          className="link-button danger-link"
                          onClick={() => {
                            setDeleteNotice(null);
                            setDeleteTarget({ datasetId: item.datasetId, label: item.title });
                          }}
                          type="button"
                        >
                          删除
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      <DatasetDeleteDialog
        datasetId={deleteTarget?.datasetId ?? null}
        datasetLabel={deleteTarget?.label ?? "数据集"}
        onClose={() => setDeleteTarget(null)}
        onDeleted={(result) => {
          setDeleteNotice(`${result.dataset_id} 已从本地目录与缓存中移除。`);
        }}
        open={Boolean(deleteTarget)}
      />
    </div>
  );
}
