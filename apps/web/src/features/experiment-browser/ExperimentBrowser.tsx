import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  useReactTable,
} from "@tanstack/react-table";
import type { UseQueryResult } from "@tanstack/react-query";
import { startTransition, useMemo, useState } from "react";
import { Link, useNavigate } from "react-router-dom";

import type { ExperimentListItem, ExperimentsResponse } from "../../shared/api/types";
import { formatDate, formatNumber } from "../../shared/lib/format";
import { I18N } from "../../shared/lib/i18n";
import { EmptyState, ErrorState, LoadingState } from "../../shared/ui/StateViews";
import { PanelHeader } from "../../shared/ui/PanelHeader";
import { StatusPill } from "../../shared/ui/StatusPill";

const columnHelper = createColumnHelper<ExperimentListItem>();

type ExperimentBrowserProps = {
  query: UseQueryResult<ExperimentsResponse, Error>;
  search: string;
  onSearchChange: (value: string) => void;
  modelFilter: string;
  onModelFilterChange: (value: string) => void;
  datasetFilter: string;
  onDatasetFilterChange: (value: string) => void;
  onLaunchBacktest?: (runId: string) => void;
  title?: string;
};

export function ExperimentBrowser({
  query,
  search,
  onSearchChange,
  modelFilter,
  onModelFilterChange,
  datasetFilter,
  onDatasetFilterChange,
  onLaunchBacktest,
  title = "\u5b9e\u9a8c\u4e0e\u8fd0\u884c",
}: ExperimentBrowserProps) {
  const navigate = useNavigate();
  const [selectedRuns, setSelectedRuns] = useState<string[]>([]);

  const columns = useMemo(
    () => [
      columnHelper.display({
        id: "select",
        header: "",
        cell: ({ row }) => (
          <input
            checked={selectedRuns.includes(row.original.run_id)}
            onChange={(event) => {
              const checked = event.target.checked;
              startTransition(() => {
                setSelectedRuns((current) =>
                  checked
                    ? [...current, row.original.run_id]
                    : current.filter((value) => value !== row.original.run_id),
                );
              });
            }}
            type="checkbox"
          />
        ),
      }),
      columnHelper.accessor("run_id", {
        header: "\u8bad\u7ec3\u5b9e\u4f8b ID",
        cell: ({ row }) => <Link to={`/runs/${row.original.run_id}`}>{row.original.run_id}</Link>,
      }),
      columnHelper.accessor("model_name", { header: "\u6a21\u578b" }),
      columnHelper.accessor("dataset_id", {
        header: "\u6570\u636e\u96c6",
        cell: ({ getValue }) => getValue() ?? "--",
      }),
      columnHelper.accessor("primary_metric_value", {
        header: "MAE",
        cell: ({ getValue }) => formatNumber(getValue()),
      }),
      columnHelper.accessor("created_at", {
        header: "\u521b\u5efa\u65f6\u95f4",
        cell: ({ getValue }) => formatDate(getValue()),
      }),
      columnHelper.accessor("status", {
        header: "\u72b6\u6001",
        cell: ({ getValue }) => <StatusPill status={getValue()} />,
      }),
      columnHelper.display({
        id: "actions",
        header: "",
        cell: ({ row }) =>
          onLaunchBacktest ? (
            <button
              className="link-button"
              onClick={() => onLaunchBacktest(row.original.run_id)}
              type="button"
            >
              {I18N.action.launchBacktest}
            </button>
          ) : null,
      }),
    ],
    [onLaunchBacktest, selectedRuns],
  );

  const table = useReactTable({
    columns,
    data: query.data?.items ?? [],
    getCoreRowModel: getCoreRowModel(),
  });

  return (
    <section className="panel">
      <PanelHeader
        eyebrow={I18N.nav.runs}
        title={title}
        description={
          "\u652f\u6301\u641c\u7d22\u3001\u7b5b\u9009\u3001\u6392\u5e8f\u4e0e\u6279\u91cf\u52fe\u9009\u8fdb\u884c\u5bf9\u6bd4\u3002"
        }
        action={
          <button
            className="action-button secondary"
            disabled={selectedRuns.length < 2}
            onClick={() => navigate(`/comparison?runs=${encodeURIComponent(selectedRuns.join(","))}`)}
            type="button"
          >
            {I18N.action.openComparison}
          </button>
        }
      />

      <div className="toolbar">
        <input
          className="field"
          onChange={(event) => onSearchChange(event.target.value)}
          placeholder={"\u641c\u7d22 run_id / \u6a21\u578b / \u6570\u636e\u96c6"}
          value={search}
        />
        <select
          className="field"
          onChange={(event) => onModelFilterChange(event.target.value)}
          value={modelFilter}
        >
          <option value="">{"\u5168\u90e8\u6a21\u578b"}</option>
          {query.data?.available_models.map((modelName) => (
            <option key={modelName} value={modelName}>
              {modelName}
            </option>
          ))}
        </select>
        <select
          className="field"
          onChange={(event) => onDatasetFilterChange(event.target.value)}
          value={datasetFilter}
        >
          <option value="">{"\u5168\u90e8\u6570\u636e\u96c6"}</option>
          {query.data?.available_datasets.map((dataset) => (
            <option key={dataset} value={dataset}>
              {dataset}
            </option>
          ))}
        </select>
      </div>

      {query.isLoading ? <LoadingState label={I18N.state.loading} /> : null}
      {query.isError ? <ErrorState message={query.error.message} /> : null}
      {!query.isLoading && !query.isError ? (
        query.data && query.data.items.length > 0 ? (
          <table className="data-table">
            <thead>
              {table.getHeaderGroups().map((headerGroup) => (
                <tr key={headerGroup.id}>
                  {headerGroup.headers.map((header) => (
                    <th key={header.id}>
                      {header.isPlaceholder
                        ? null
                        : flexRender(header.column.columnDef.header, header.getContext())}
                    </th>
                  ))}
                </tr>
              ))}
            </thead>
            <tbody>
              {table.getRowModel().rows.map((row) => (
                <tr key={row.id}>
                  {row.getVisibleCells().map((cell) => (
                    <td key={cell.id}>
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <EmptyState
            title={I18N.state.empty}
            body={"\u5f53\u524d\u7b5b\u9009\u6761\u4ef6\u4e0b\u6ca1\u6709\u8fd0\u884c\u8bb0\u5f55\u3002"}
          />
        )
      ) : null}
    </section>
  );
}
