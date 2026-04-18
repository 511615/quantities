import { useState } from "react";
import { Link, useSearchParams } from "react-router-dom";

import { DatasetDeleteDialog } from "../features/dataset-browser/DatasetDeleteDialog";
import { DatasetRequestDrawer } from "../features/dataset-browser/DatasetRequestDrawer";
import { DatasetWorkspaceNav } from "../features/dataset-browser/DatasetWorkspaceNav";
import {
  buildDatasetCard,
  createDatasetFacets,
  describeDatasetType,
  filterDatasetCards,
  type DatasetBrowserFilters,
} from "../features/dataset-browser/presentation";
import { useDatasets } from "../shared/api/hooks";
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

function filterKeyLabel(key: keyof DatasetBrowserFilters) {
  const labels: Record<keyof DatasetBrowserFilters, string> = {
    data_domain: "数据域",
    dataset_type: "数据集类型",
    source: "来源",
    exchange: "交易所",
    symbol: "资产 / 标的",
    frequency: "频率",
    version: "快照版本",
    time_from: "开始时间",
    time_to: "结束时间",
  };
  return labels[key];
}

export function DatasetsBrowserPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [deleteTarget, setDeleteTarget] = useState<{ datasetId: string; label: string } | null>(null);
  const [deleteNotice, setDeleteNotice] = useState<string | null>(null);

  const filters = readFilters(searchParams);
  const datasetsQuery = useDatasets(1, 100);

  if (datasetsQuery.isLoading) {
    return <LoadingState />;
  }

  if (datasetsQuery.isError) {
    return <ErrorState message={(datasetsQuery.error as Error).message} />;
  }

  const cards = (datasetsQuery.data?.items ?? []).map(buildDatasetCard);
  const facets = createDatasetFacets(cards);
  const filteredItems = filterDatasetCards(cards, filters);

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
    .map(([key, value]) => `${filterKeyLabel(key as keyof DatasetBrowserFilters)}：${value}`)
    .join(" / ");

  const explainers = [
    {
      title: "展示切片",
      summary: "适合先看覆盖范围、来源和样本结构，用来判断值不值得继续追踪。",
    },
    {
      title: "训练面板",
      summary: "已经具备标签与切分语义，适合直接进入训练或多模态融合。",
    },
    {
      title: "特征快照",
      summary: "更适合作为上游信号资产，被后续模型或融合流程复用。",
    },
  ];

  return (
    <div className="page-stack">
      <section className="page-header-shell">
        <div className="page-header-main">
          <div className="eyebrow">数据浏览器</div>
          <h1>按数据域、来源与版本浏览</h1>
          <p>先看来源、覆盖范围和数据角色，再决定是否进入详情页、训练流程或继续追踪某个版本。</p>
        </div>
        <div className="page-header-actions">
          <DatasetRequestDrawer
            description="从浏览页直接发起数据申请，保持当前筛选上下文。"
            initialValues={{
              dataDomain: filters.data_domain || undefined,
              exchange: filters.exchange || undefined,
              frequency: filters.frequency || undefined,
              sourceVendor: filters.source || undefined,
              symbol: filters.symbol || undefined,
            }}
            title="申请新数据集"
            triggerTone="secondary"
          />
          <Link className="comparison-link" to="/datasets">
            返回数据集总览
          </Link>
          <Link className="comparison-link" to="/datasets/training">
            查看训练面板
          </Link>
        </div>
      </section>

      <DatasetWorkspaceNav />

      {deleteNotice ? (
        <section className="dataset-callout">
          <strong>数据集更新</strong>
          <span>{deleteNotice}</span>
        </section>
      ) : null}

      <section className="panel">
        <PanelHeader
          eyebrow="筛选条件"
          title="缩小数据范围"
          description="通过数据域、类型、来源、交易所、频率和版本快速收敛结果。"
        />
        <div className="form-section-grid dataset-filter-grid">
          <label>
            <TermLabel hintKey="data_domain" label="数据域" />
            <select className="field" onChange={(event) => updateFilter("data_domain", event.target.value)} value={filters.data_domain}>
              <option value="">全部</option>
              {facets.domains.map((value) => (
                <option key={value} value={value}>
                  {value}
                </option>
              ))}
            </select>
          </label>
          <label>
            <TermLabel hintKey="dataset_type" label="数据集类型" />
            <select className="field" onChange={(event) => updateFilter("dataset_type", event.target.value)} value={filters.dataset_type}>
              <option value="">全部</option>
              {facets.types.map((value) => (
                <option key={value} value={value}>
                  {value}
                </option>
              ))}
            </select>
          </label>
          <label>
            <span>来源</span>
            <select className="field" onChange={(event) => updateFilter("source", event.target.value)} value={filters.source}>
              <option value="">全部</option>
              {facets.sources.map((value) => (
                <option key={value} value={value}>
                  {value}
                </option>
              ))}
            </select>
          </label>
          <label>
            <span>交易所</span>
            <select className="field" onChange={(event) => updateFilter("exchange", event.target.value)} value={filters.exchange}>
              <option value="">全部</option>
              {facets.exchanges.map((value) => (
                <option key={value} value={value}>
                  {value}
                </option>
              ))}
            </select>
          </label>
          <label>
            <span>资产 / 标的</span>
            <select className="field" onChange={(event) => updateFilter("symbol", event.target.value)} value={filters.symbol}>
              <option value="">全部</option>
              {facets.symbols.map((value) => (
                <option key={value} value={value}>
                  {value}
                </option>
              ))}
            </select>
          </label>
          <label>
            <span>频率</span>
            <select className="field" onChange={(event) => updateFilter("frequency", event.target.value)} value={filters.frequency}>
              <option value="">全部</option>
              {facets.frequencies.map((value) => (
                <option key={value} value={value}>
                  {value}
                </option>
              ))}
            </select>
          </label>
          <label>
            <TermLabel hintKey="snapshot_version" label="快照版本" />
            <select className="field" onChange={(event) => updateFilter("version", event.target.value)} value={filters.version}>
              <option value="">全部</option>
              {facets.versions.map((value) => (
                <option key={value} value={value}>
                  {value}
                </option>
              ))}
            </select>
          </label>
          <label>
            <span>开始时间</span>
            <input className="field" onChange={(event) => updateFilter("time_from", event.target.value)} type="date" value={filters.time_from} />
          </label>
          <label>
            <span>结束时间</span>
            <input className="field" onChange={(event) => updateFilter("time_to", event.target.value)} type="date" value={filters.time_to} />
          </label>
        </div>

        <div className="dataset-filter-summary">
          <strong>当前结果</strong>
          <span>{filteredItems.length} 个数据集</span>
          <span>{activeSummary || "未应用筛选条件"}</span>
        </div>
      </section>

      <section className="panel">
        <PanelHeader
          eyebrow="结果目录"
          title="当前可用数据集"
          description="目录页优先回答“有哪些数据可看”“哪些适合训练”“哪些版本值得继续追踪”。"
        />
        {filteredItems.length === 0 ? (
          <EmptyState title="没有匹配的数据集" body="放宽一个或多个筛选条件后再试，例如数据域、来源或频率。" />
        ) : (
          <div className="dataset-browser-table-shell">
            <table className="data-table dataset-browser-table">
              <thead>
                <tr>
                  <th>名称</th>
                  <th><TermLabel hintKey="data_domain" label="数据域" /></th>
                  <th><TermLabel hintKey="dataset_type" label="类型" /></th>
                  <th>来源 / 交易所</th>
                  <th>覆盖范围</th>
                  <th>频率</th>
                  <th><TermLabel hintKey="snapshot_version" label="快照版本" /></th>
                  <th><TermLabel hintKey="freshness" label="新鲜度" /></th>
                  <th>质量</th>
                  <th>操作</th>
                </tr>
              </thead>
              <tbody>
                {filteredItems.map((item) => (
                  <tr key={item.datasetId}>
                    <td>
                      <div className="table-title-cell">
                        <Link to={`/datasets/${encodeURIComponent(item.datasetId)}`}>{item.title}</Link>
                        <span>{item.subtitle}</span>
                        <span>更新时间：{formatDate(item.asOfTime)}</span>
                      </div>
                    </td>
                    <td>{item.domainLabel}</td>
                    <td>{item.datasetTypeLabel}</td>
                    <td>
                      <div className="dataset-row-subcopy">
                        <span>{item.sourceLabel}</span>
                        <span>{item.exchangeLabel}</span>
                      </div>
                    </td>
                    <td>{item.coverageLabel}</td>
                    <td>{item.frequencyLabel}</td>
                    <td>{item.snapshotVersion}</td>
                    <td>{item.freshnessLabel}</td>
                    <td>{item.qualityLabel}</td>
                    <td>
                      <div className="table-actions">
                        <Link className="link-button" to={`/datasets/${encodeURIComponent(item.datasetId)}`}>
                          查看详情
                        </Link>
                        <button
                          className="link-button danger-link"
                          onClick={() => setDeleteTarget({ datasetId: item.datasetId, label: item.title })}
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
        )}
      </section>

      <section className="panel">
        <PanelHeader
          eyebrow="使用建议"
          title="先浏览，再决定是否训练"
          description="把目录页和训练页职责拆开，避免把可浏览数据误当成可训练面板。"
        />
        <div className="dataset-roles-grid">
          {explainers.map((item) => (
            <article className="details-panel dataset-role-card" key={item.title}>
              <strong>{item.title}</strong>
              <p>{item.summary}</p>
            </article>
          ))}
        </div>
        <div className="stack-list">
          {filteredItems.slice(0, 3).map((item) => (
            <div className="stack-item align-start" key={item.datasetId}>
              <strong>{item.title}</strong>
              <span>{describeDatasetType(item.datasetType)}</span>
            </div>
          ))}
        </div>
      </section>

      <DatasetDeleteDialog
        datasetId={deleteTarget?.datasetId ?? null}
        datasetLabel={deleteTarget?.label ?? "数据集"}
        onClose={() => setDeleteTarget(null)}
        onDeleted={(result) => {
          setDeleteTarget(null);
          setDeleteNotice(`${result.dataset_id} 已从注册表中移除。`);
        }}
        open={deleteTarget !== null}
      />
    </div>
  );
}
