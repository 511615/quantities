import { useMemo, useState } from "react";
import { Link } from "react-router-dom";

import type { BacktestListItemView } from "../shared/api/types";
import { useBacktests, useDeleteBacktestMutation } from "../shared/api/hooks";
import { formatDate, formatNumber } from "../shared/lib/format";
import { I18N, translateText } from "../shared/lib/i18n";
import { ConfirmDialog } from "../shared/ui/ConfirmDialog";
import { GlossaryHint } from "../shared/ui/GlossaryHint";
import { PanelHeader } from "../shared/ui/PanelHeader";
import { EmptyState, ErrorState, LoadingState } from "../shared/ui/StateViews";
import { StatusPill } from "../shared/ui/StatusPill";

export function BacktestsPage() {
  const [search, setSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState("");
  const [pendingDelete, setPendingDelete] = useState<BacktestListItemView | null>(null);

  const params = useMemo(() => {
    const next = new URLSearchParams({
      page: "1",
      per_page: "50",
    });
    if (search) {
      next.set("search", search);
    }
    if (statusFilter) {
      next.set("status", statusFilter);
    }
    return next;
  }, [search, statusFilter]);

  const query = useBacktests(params);
  const deleteMutation = useDeleteBacktestMutation();

  async function handleDeleteConfirm() {
    if (!pendingDelete) {
      return;
    }
    await deleteMutation.mutateAsync(pendingDelete.backtest_id);
    setPendingDelete(null);
  }

  return (
    <div className="page-stack">
      <section className="panel">
        <PanelHeader
          eyebrow={I18N.nav.backtests}
          title={translateText("回测列表")}
          description={translateText("按状态筛选并进入回测报告详情。")}
        />
        <div className="toolbar">
          <input
            className="field"
            onChange={(event) => setSearch(event.target.value)}
            placeholder={translateText("搜索回测 ID / 训练实例 ID")}
            value={search}
          />
          <select className="field" onChange={(event) => setStatusFilter(event.target.value)} value={statusFilter}>
            <option value="">{translateText("全部状态")}</option>
            {query.data?.available_statuses.map((status) => (
              <option key={status} value={status}>
                {status}
              </option>
            ))}
          </select>
        </div>
        {query.isLoading ? <LoadingState label={I18N.state.loading} /> : null}
        {query.isError ? <ErrorState message={(query.error as Error).message} /> : null}
        {!query.isLoading && !query.isError ? (
          query.data && query.data.items.length > 0 ? (
            <table className="data-table">
              <thead>
                <tr>
                  <th>{translateText("回测 ID")}</th>
                  <th>{translateText("训练实例 ID")}</th>
                  <th><GlossaryHint hintKey="max_drawdown" /></th>
                  <th>{translateText("年化收益")}</th>
                  <th>{translateText("告警")}</th>
                  <th>{translateText("状态")}</th>
                  <th>{translateText("操作")}</th>
                </tr>
              </thead>
              <tbody>
                {query.data.items.map((item) => (
                  <tr key={item.backtest_id}>
                    <td>
                      <Link to={`/backtests/${encodeURIComponent(item.backtest_id)}`}>{item.backtest_id}</Link>
                    </td>
                    <td>{item.run_id ?? "--"}</td>
                    <td>{formatNumber(item.max_drawdown)}</td>
                    <td>{formatNumber(item.annual_return)}</td>
                    <td>{Math.max(item.warning_count, item.official && item.gate_status === "failed" ? 1 : 0)}</td>
                    <td
                      title={
                        item.official && item.gate_status === "failed"
                          ? translateText("官方协议门禁未通过，结果仅可用于排错，不参与官方比较。")
                          : undefined
                      }
                    >
                      <StatusPill status={item.official && item.gate_status === "failed" ? "failed" : item.status} />
                    </td>
                    <td>
                      <div className="table-actions">
                        <Link className="link-button" to={`/backtests/${encodeURIComponent(item.backtest_id)}`}>
                          {translateText("详情")}
                        </Link>
                        <button
                          className="link-button danger-link"
                          onClick={() => {
                            deleteMutation.reset();
                            setPendingDelete(item);
                          }}
                          type="button"
                        >
                          {I18N.action.delete}
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <EmptyState
              title={I18N.state.empty}
              body={translateText("当前过滤条件下没有回测记录。")}
            />
          )
        ) : null}
      </section>

      <section className="panel">
        <PanelHeader eyebrow={translateText("数据更新")} title={translateText("列表信息")} />
        <div className="stack-list">
          <div className="stack-item">
            <strong>{query.data?.total ?? 0}</strong>
            <span>{translateText("回测记录")}</span>
            <span>{formatDate(query.data?.items[0]?.updated_at ?? null)}</span>
          </div>
        </div>
      </section>

      <ConfirmDialog
        cancelLabel={I18N.action.cancel}
        confirmDisabled={!pendingDelete || deleteMutation.isPending}
        confirmLabel={deleteMutation.isPending ? translateText("删除中...") : I18N.action.confirmDelete}
        message={
          pendingDelete
            ? translateText("将永久删除回测记录，并清理对应报告产物。训练实例会保留，但这条回测不会继续显示。")
            : ""
        }
        onCancel={() => {
          deleteMutation.reset();
          setPendingDelete(null);
        }}
        onConfirm={handleDeleteConfirm}
        open={pendingDelete !== null}
        title={translateText("删除回测")}
        tone="danger"
      >
        {deleteMutation.isError ? (
          <div className="dialog-section">
            <strong>{translateText("删除失败")}</strong>
            <p>{(deleteMutation.error as Error).message}</p>
          </div>
        ) : null}
      </ConfirmDialog>
    </div>
  );
}
