import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";

import { useBacktests, useDeleteBacktestMutation } from "../shared/api/hooks";
import { formatDate, formatNumber } from "../shared/lib/format";
import { I18N } from "../shared/lib/i18n";
import { ConfirmDialog } from "../shared/ui/ConfirmDialog";
import { GlossaryHint } from "../shared/ui/GlossaryHint";
import { PanelHeader } from "../shared/ui/PanelHeader";
import { EmptyState, ErrorState, LoadingState } from "../shared/ui/StateViews";
import { StatusPill } from "../shared/ui/StatusPill";

function filterValidIds(current: string[], validIds: Set<string>) {
  const next = current.filter((id) => validIds.has(id));
  return next.length === current.length && next.every((id, index) => id === current[index])
    ? current
    : next;
}

export function BacktestsPage() {
  const [search, setSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState("");
  const [selectedBacktestIds, setSelectedBacktestIds] = useState<string[]>([]);
  const [pendingDeleteIds, setPendingDeleteIds] = useState<string[]>([]);

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
  const visibleBacktests = useMemo(() => query.data?.items ?? [], [query.data?.items]);
  const visibleBacktestIdsKey = useMemo(
    () => visibleBacktests.map((item) => item.backtest_id).join("|"),
    [visibleBacktests],
  );
  const pendingDeleteBacktests = visibleBacktests.filter((item) => pendingDeleteIds.includes(item.backtest_id));
  const allVisibleSelected =
    visibleBacktests.length > 0 && visibleBacktests.every((item) => selectedBacktestIds.includes(item.backtest_id));

  useEffect(() => {
    const validIds = new Set(visibleBacktests.map((item) => item.backtest_id));
    setSelectedBacktestIds((current) => filterValidIds(current, validIds));
    setPendingDeleteIds((current) => filterValidIds(current, validIds));
  }, [visibleBacktestIdsKey, visibleBacktests]);

  async function handleDeleteConfirm() {
    if (pendingDeleteIds.length === 0) {
      return;
    }
    for (const backtestId of pendingDeleteIds) {
      await deleteMutation.mutateAsync(backtestId);
    }
    setSelectedBacktestIds((current) => current.filter((id) => !pendingDeleteIds.includes(id)));
    setPendingDeleteIds([]);
  }

  function toggleBacktestSelection(backtestId: string, checked: boolean) {
    setSelectedBacktestIds((current) =>
      checked ? Array.from(new Set([...current, backtestId])) : current.filter((id) => id !== backtestId),
    );
  }

  function toggleSelectAllVisible(checked: boolean) {
    setSelectedBacktestIds((current) => {
      if (!checked) {
        return current.filter((id) => !visibleBacktests.some((item) => item.backtest_id === id));
      }
      return Array.from(new Set([...current, ...visibleBacktests.map((item) => item.backtest_id)]));
    });
  }

  return (
    <div className="page-stack backtests-page">
      <section className="panel">
        <PanelHeader
          eyebrow={I18N.nav.backtests}
          title="回测列表"
          description="按状态筛选并进入回测报告详情。"
        />
        <div className="toolbar">
          <input
            className="field"
            onChange={(event) => setSearch(event.target.value)}
            placeholder="搜索回测 ID / 训练实例 ID"
            value={search}
          />
          <select className="field" onChange={(event) => setStatusFilter(event.target.value)} value={statusFilter}>
            <option value="">全部状态</option>
            {query.data?.available_statuses.map((status) => (
              <option key={status} value={status}>
                {status}
              </option>
            ))}
          </select>
          <button
            className="link-button danger-link"
            disabled={selectedBacktestIds.length === 0 || deleteMutation.isPending}
            onClick={() => {
              deleteMutation.reset();
              setPendingDeleteIds(selectedBacktestIds);
            }}
            type="button"
          >
            {selectedBacktestIds.length > 0 ? `批量删除 (${selectedBacktestIds.length})` : "批量删除"}
          </button>
        </div>
        {query.isLoading ? <LoadingState label={I18N.state.loading} /> : null}
        {query.isError ? <ErrorState message={(query.error as Error).message} /> : null}
        {!query.isLoading && !query.isError ? (
          query.data && query.data.items.length > 0 ? (
            <table className="data-table">
              <thead>
                <tr>
                  <th>
                    <input
                      aria-label="全选当前回测"
                      checked={allVisibleSelected}
                      onChange={(event) => toggleSelectAllVisible(event.target.checked)}
                      type="checkbox"
                    />
                  </th>
                  <th>回测 ID</th>
                  <th>训练实例 ID</th>
                  <th>回测时间</th>
                  <th>
                    <GlossaryHint hintKey="max_drawdown" />
                  </th>
                  <th>年化收益</th>
                  <th>告警</th>
                  <th>状态</th>
                  <th>操作</th>
                </tr>
              </thead>
              <tbody>
                {query.data.items.map((item) => (
                  <tr key={item.backtest_id}>
                    <td>
                      <input
                        aria-label={`选择回测 ${item.backtest_id}`}
                        checked={selectedBacktestIds.includes(item.backtest_id)}
                        onChange={(event) => toggleBacktestSelection(item.backtest_id, event.target.checked)}
                        type="checkbox"
                      />
                    </td>
                    <td>
                      <Link to={`/backtests/${encodeURIComponent(item.backtest_id)}`}>{item.backtest_id}</Link>
                    </td>
                    <td>{item.run_id ?? "--"}</td>
                    <td>{formatDate(item.updated_at)}</td>
                    <td>{formatNumber(item.max_drawdown)}</td>
                    <td>{formatNumber(item.annual_return)}</td>
                    <td>{Math.max(item.warning_count, item.official && item.gate_status === "failed" ? 1 : 0)}</td>
                    <td
                      title={
                        item.official && item.gate_status === "failed"
                          ? "官方协议门禁未通过，结果仅可用于排错，不参与官方比较。"
                          : undefined
                      }
                    >
                      <StatusPill status={item.official && item.gate_status === "failed" ? "failed" : item.status} />
                    </td>
                    <td>
                      <div className="table-actions">
                        <Link className="link-button" to={`/backtests/${encodeURIComponent(item.backtest_id)}`}>
                          详情
                        </Link>
                        <button
                          className="link-button danger-link"
                          onClick={() => {
                            deleteMutation.reset();
                            setPendingDeleteIds([item.backtest_id]);
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
            <EmptyState title={I18N.state.empty} body="当前过滤条件下没有回测记录。" />
          )
        ) : null}
      </section>

      <section className="panel">
        <PanelHeader eyebrow="数据更新" title="列表信息" />
        <div className="stack-list">
          <div className="stack-item">
            <strong>{query.data?.total ?? 0}</strong>
            <span>回测记录</span>
            <span>{formatDate(query.data?.items[0]?.updated_at ?? null)}</span>
          </div>
        </div>
      </section>

      <ConfirmDialog
        cancelLabel={I18N.action.cancel}
        confirmDisabled={pendingDeleteIds.length === 0 || deleteMutation.isPending}
        confirmLabel={deleteMutation.isPending ? "删除中..." : I18N.action.confirmDelete}
        message={
          pendingDeleteIds.length > 1
            ? "将永久删除所选回测记录，并清理对应报告产物。训练实例会保留，但这些回测不会继续显示。"
            : pendingDeleteIds.length === 1
              ? "将永久删除回测记录，并清理对应报告产物。训练实例会保留，但这条回测不会继续显示。"
              : ""
        }
        onCancel={() => {
          deleteMutation.reset();
          setPendingDeleteIds([]);
        }}
        onConfirm={handleDeleteConfirm}
        open={pendingDeleteIds.length > 0}
        title={pendingDeleteIds.length > 1 ? "批量删除回测" : "删除回测"}
        tone="danger"
      >
        {pendingDeleteBacktests.length > 1 ? (
          <div className="dialog-section">
            <strong>即将删除以下回测</strong>
            <div className="stack-list">
              {pendingDeleteBacktests.map((item) => (
                <div className="stack-item align-start" key={item.backtest_id}>
                  <strong>{item.backtest_id}</strong>
                  <span>{item.run_id ?? "--"}</span>
                </div>
              ))}
            </div>
          </div>
        ) : null}
        {deleteMutation.isError ? (
          <div className="dialog-section">
            <strong>删除失败</strong>
            <p>{(deleteMutation.error as Error).message}</p>
          </div>
        ) : null}
      </ConfirmDialog>
    </div>
  );
}
