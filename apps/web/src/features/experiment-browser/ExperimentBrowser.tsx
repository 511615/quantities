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
import { I18N, translateText } from "../../shared/lib/i18n";
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
  title = translateText("实验与运行"),
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
                  checked ? [...current, row.original.run_id] : current.filter((value) => value !== row.original.run_id),
                );
              });
            }}
            type="checkbox"
          />
        ),
      }),
      columnHelper.accessor("run_id", {
        header: translateText("训练实例 ID"),
        cell: ({ row }) => (
          <Link to={`/runs/${encodeURIComponent(row.original.run_id)}`}>{row.original.run_id}</Link>
        ),
      }),
      columnHelper.accessor("model_name", { header: translateText("模型") }),
      columnHelper.accessor("dataset_id", {
        header: translateText("数据集"),
        cell: ({ getValue }) => getValue() ?? "--",
      }),
      columnHelper.accessor("primary_metric_value", {
        header: "MAE",
        cell: ({ getValue }) => formatNumber(getValue()),
      }),
      columnHelper.accessor("created_at", {
        header: translateText("创建时间"),
        cell: ({ getValue }) => formatDate(getValue()),
      }),
      columnHelper.accessor("status", {
        header: translateText("状态"),
        cell: ({ getValue }) => <StatusPill status={getValue()} />,
      }),
      columnHelper.display({
        id: "actions",
        header: "",
        cell: ({ row }) =>
          onLaunchBacktest ? (
            <button className="link-button" onClick={() => onLaunchBacktest(row.original.run_id)} type="button">
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
        description={translateText("支持搜索、筛选、排序与批量勾选进行对比。")}
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
          placeholder={translateText("搜索训练实例 ID / 模型 / 数据集")}
          value={search}
        />
        <select className="field" onChange={(event) => onModelFilterChange(event.target.value)} value={modelFilter}>
          <option value="">{translateText("全部模型")}</option>
          {query.data?.available_models.map((modelName) => (
            <option key={modelName} value={modelName}>
              {modelName}
            </option>
          ))}
        </select>
        <select className="field" onChange={(event) => onDatasetFilterChange(event.target.value)} value={datasetFilter}>
          <option value="">{translateText("全部数据集")}</option>
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
                      {header.isPlaceholder ? null : flexRender(header.column.columnDef.header, header.getContext())}
                    </th>
                  ))}
                </tr>
              ))}
            </thead>
            <tbody>
              {table.getRowModel().rows.map((row) => (
                <tr key={row.id}>
                  {row.getVisibleCells().map((cell) => (
                    <td key={cell.id}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <EmptyState
            title={I18N.state.empty}
            body={translateText("当前筛选条件下没有运行记录。")}
          />
        )
      ) : null}
    </section>
  );
}
