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
    return "All";
  }

  const labels: Record<string, string> = {
    market: "Market data",
    derivatives: "Derivatives data",
    on_chain: "On-chain data",
    macro: "Macro data",
    sentiment_events: "Sentiment / event data",
    training_panel: "Training panel",
    fusion_training_panel: "Fusion training panel",
    display_slice: "Display slice",
    feature_snapshot: "Feature snapshot",
  };

  return labels[value] ?? value;
}

const datasetTypeExplainers = [
  {
    title: "Raw event library",
    summary: "Streams of cleaned NLP events preserved with timestamps and metadata.",
    detail: "This is the source table that feeds downstream feature snapshots and training panels.",
  },
  {
    title: "Training panel",
    summary: "Labeled datasets aligned to market history for benchmarking or model training.",
    detail: "It bundles features, labels, and splits so training workflows stay dataset_id-first.",
  },
  {
    title: "Feature snapshot",
    summary: "Aggregated signal collections ready for fusion without rebuilding from raw text.",
    detail: "Feature snapshots are structured assets, not raw multimodal text inputs.",
  },
];

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
          <div className="eyebrow">Dataset Explorer</div>
          <h1>Build signal-ready NLP datasets with clarity</h1>
          <p>
            Choose sources, inspect coverage, and understand roles before requesting new assets. The workflow stays dataset_id-first and aligned with training/backtest chains.
          </p>
        </div>
        <div className="hero-actions">
          <DatasetRequestDrawer
            description="Request a curated dataset built from cleaned NLP and market sources. Pick sources, exchange, frequency, and filters, and we materialize the merged dataset for training."
            initialValues={{
              dataDomain: filters.data_domain || undefined,
              exchange: filters.exchange || undefined,
              frequency: filters.frequency || undefined,
              sourceVendor: filters.source || undefined,
              symbol: filters.symbol || undefined,
            }}
            title="Request dataset"
            triggerTone="secondary"
          />
          <Link className="comparison-link" to="/datasets">
            Back to datasets
          </Link>
          <Link className="comparison-link" to="/datasets/training">
            View training panels
          </Link>
        </div>
      </section>

      <section className="panel dataset-education-panel">
        <PanelHeader
          eyebrow="Why it matters"
          title="Dataset roles explained"
          description="Raw event libraries, training panels, and feature snapshots have distinct duties - choose the product that matches your goal."
        />
        <div className="dataset-education-grid">
          {datasetTypeExplainers.map((item) => (
            <article className="details-panel dataset-education-card" key={item.title}>
              <h3>{item.title}</h3>
              <p>{item.summary}</p>
              <small>{item.detail}</small>
            </article>
          ))}
        </div>
        <div className="dataset-callout">
          <strong>Structured NLP signals, not raw multimodal blobs.</strong>
          <span>
            These assets capture aggregated sentiment and event metrics aligned by event_time, ready to fuse with market features without reprocessing raw text.
          </span>
        </div>
      </section>

      <DatasetWorkspaceNav />

      {deleteNotice ? (
        <section className="dataset-callout">
          <strong>Dataset update</strong>
          <span>{deleteNotice}</span>
        </section>
      ) : null}

      <section className="panel">
        <PanelHeader
          eyebrow="Dataset filters"
          title="Filter datasets"
          description="Use the selectors below to focus on raw event libraries, training panels, or feature snapshots."
        />

        {requestOptionsQuery.isError ? (
          <div className="dataset-callout">
            <strong>
              {isApiNotReadyError(requestOptionsQuery.error)
                ? "Filter options are not ready"
                : "Failed to load filters"}
            </strong>
            <span>
              {isApiNotReadyError(requestOptionsQuery.error)
                ? `${createApiNotReadyMessage("dataset filters")} Please try again once the service is available.`
                : (requestOptionsQuery.error as Error).message}
            </span>
          </div>
        ) : null}

        <div className="form-section-grid dataset-filter-grid">
          <label>
            <span>
              <TermLabel hintKey="data_domain" label="Data domain" />
            </span>
            <select
              className="field"
              onChange={(event) => updateFilter("data_domain", event.target.value)}
              value={filters.data_domain}
            >
              <option value="">All</option>
              {facets.domains.map((item) => (
                <option key={item} value={item}>
                  {filterLabel(item)}
                </option>
              ))}
            </select>
          </label>

          <label>
            <span>
              <TermLabel hintKey="dataset_type" label="Dataset type" />
            </span>
            <select
              className="field"
              onChange={(event) => updateFilter("dataset_type", event.target.value)}
              value={filters.dataset_type}
            >
              <option value="">All</option>
              {facets.types.map((item) => (
                <option key={item} value={item}>
                  {filterLabel(item)}
                </option>
              ))}
            </select>
          </label>

          <label>
            <span>Source</span>
            <select
              className="field"
              onChange={(event) => updateFilter("source", event.target.value)}
              value={filters.source}
            >
              <option value="">All</option>
              {facets.sources.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            </select>
          </label>

          <label>
            <span>Exchange</span>
            <select
              className="field"
              onChange={(event) => updateFilter("exchange", event.target.value)}
              value={filters.exchange}
            >
              <option value="">All</option>
              {facets.exchanges.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            </select>
          </label>

          <label>
            <span>Asset / symbol</span>
            <select
              className="field"
              onChange={(event) => updateFilter("symbol", event.target.value)}
              value={filters.symbol}
            >
              <option value="">All</option>
              {facets.symbols.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            </select>
          </label>

          <label>
            <span>Frequency</span>
            <select
              className="field"
              onChange={(event) => updateFilter("frequency", event.target.value)}
              value={filters.frequency}
            >
              <option value="">All</option>
              {facets.frequencies.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            </select>
          </label>

          <label>
            <span>
              <TermLabel hintKey="snapshot_version" label="Snapshot version" />
            </span>
            <select
              className="field"
              onChange={(event) => updateFilter("version", event.target.value)}
              value={filters.version}
            >
              <option value="">All</option>
              {facets.versions.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            </select>
          </label>

          <label>
            <span>Start time</span>
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
            <span>End time</span>
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
          <strong>Current results:</strong>
          <span>{filteredItems.length} datasets</span>
          <span>{activeSummary || "No filters applied"}</span>
        </div>
      </section>

      {filteredItems.length === 0 ? (
        <EmptyState
          title="No matching datasets"
          body="Relax one or more filters (data domain, source, or frequency) and try again."
        />
      ) : (
      <section className="panel">
        <PanelHeader
          eyebrow="Results"
          title="Dataset catalog"
          description="The table below lists the datasets currently available in the registry, not guesses from local files."
        />
        <div className="dataset-browser-table-shell">
          <table className="data-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>
                  <TermLabel hintKey="data_domain" label="Data domain" />
                </th>
                <th>
                  <TermLabel hintKey="dataset_type" label="Type" />
                </th>
                <th>Source / Exchange</th>
                <th>Coverage</th>
                <th>Frequency</th>
                <th>
                  <TermLabel hintKey="snapshot_version" label="Snapshot version" />
                </th>
                <th>
                  <TermLabel hintKey="freshness" label="Freshness" />
                </th>
                <th>Health</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {filteredItems.map((item) => (
                <tr key={item.datasetId}>
                  <td>
                    <Link to={`/datasets/${item.datasetId}`}>{item.title}</Link>
                    <div className="dataset-row-subcopy">
                      <span>{item.subtitle}</span>
                      <span>Tech ID: {item.technicalId}</span>
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
                        View details
                      </Link>
                      <button
                        className="link-button danger-link"
                        onClick={() => {
                          setDeleteNotice(null);
                          setDeleteTarget({ datasetId: item.datasetId, label: item.title });
                        }}
                        type="button"
                      >
                        Delete
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
        datasetLabel={deleteTarget?.label ?? "dataset"}
        onClose={() => setDeleteTarget(null)}
        onDeleted={(result) => {
          setDeleteNotice(`${result.dataset_id} has been removed from the registry.`);
        }}
        open={Boolean(deleteTarget)}
      />
    </div>
  );
}
