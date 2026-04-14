import { useMemo, useState } from "react";
import { Link } from "react-router-dom";

import type { BacktestListItemView } from "../shared/api/types";
import { useBacktests, useDeleteBacktestMutation } from "../shared/api/hooks";
import { formatDate, formatNumber } from "../shared/lib/format";
import { I18N } from "../shared/lib/i18n";
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
          title={"\u56de\u6d4b\u5217\u8868"}
          description={"\u6309\u72b6\u6001\u7b5b\u9009\u5e76\u8fdb\u5165\u56de\u6d4b\u62a5\u544a\u8be6\u60c5\u3002"}
        />
        <div className="toolbar">
          <input
            className="field"
            onChange={(event) => setSearch(event.target.value)}
            placeholder={"\u641c\u7d22\u56de\u6d4b ID / \u8bad\u7ec3\u5b9e\u4f8b ID"}
            value={search}
          />
          <select
            className="field"
            onChange={(event) => setStatusFilter(event.target.value)}
            value={statusFilter}
          >
            <option value="">{"\u5168\u90e8\u72b6\u6001"}</option>
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
                  <th>{"\u56de\u6d4b ID"}</th>
                  <th>{"\u8bad\u7ec3\u5b9e\u4f8b ID"}</th>
                  <th><GlossaryHint hintKey="max_drawdown" /></th>
                  <th>{"\u5e74\u5316\u6536\u76ca"}</th>
                  <th>{"\u544a\u8b66"}</th>
                  <th>{"\u72b6\u6001"}</th>
                  <th>{"\u64cd\u4f5c"}</th>
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
                    <td title={item.official && item.gate_status === "failed" ? "官方协议门禁未通过，结果仅可用于排错，不参与官方比较。" : undefined}>
                      <StatusPill
                        status={item.official && item.gate_status === "failed" ? "failed" : item.status}
                      />
                    </td>
                    <td>
                      <div className="table-actions">
                        <Link className="link-button" to={`/backtests/${encodeURIComponent(item.backtest_id)}`}>
                          {"\u8be6\u60c5"}
                        </Link>
                        <button
                          className="link-button danger-link"
                          onClick={() => {
                            deleteMutation.reset();
                            setPendingDelete(item);
                          }}
                          type="button"
                        >
                          {"\u5220\u9664"}
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
              body={"\u5f53\u524d\u8fc7\u6ee4\u6761\u4ef6\u4e0b\u6ca1\u6709\u56de\u6d4b\u8bb0\u5f55\u3002"}
            />
          )
        ) : null}
      </section>

      <section className="panel">
        <PanelHeader eyebrow={"\u6570\u636e\u66f4\u65b0"} title={"\u5217\u8868\u4fe1\u606f"} />
        <div className="stack-list">
          <div className="stack-item">
            <strong>{query.data?.total ?? 0}</strong>
            <span>{"\u56de\u6d4b\u8bb0\u5f55"}</span>
            <span>{formatDate(query.data?.items[0]?.updated_at ?? null)}</span>
          </div>
        </div>
      </section>

      <ConfirmDialog
        cancelLabel={"\u53d6\u6d88"}
        confirmDisabled={!pendingDelete || deleteMutation.isPending}
        confirmLabel={deleteMutation.isPending ? "\u5220\u9664\u4e2d..." : "\u786e\u8ba4\u5220\u9664"}
        message={
          pendingDelete
            ? `\u5c06\u6c38\u4e45\u5220\u9664\u56de\u6d4b ${pendingDelete.backtest_id}\uff0c\u5e76\u6e05\u7406\u5bf9\u5e94\u62a5\u544a\u4ea7\u7269\u3002\u5bf9\u5e94\u7684\u8bad\u7ec3\u5b9e\u4f8b\u4f1a\u4fdd\u7559\uff0c\u4f46\u4e0d\u518d\u663e\u793a\u8fd9\u6761\u56de\u6d4b\u8bb0\u5f55\u3002`
            : ""
        }
        onCancel={() => {
          deleteMutation.reset();
          setPendingDelete(null);
        }}
        onConfirm={handleDeleteConfirm}
        open={pendingDelete !== null}
        title={"\u5220\u9664\u56de\u6d4b"}
        tone="danger"
      >
        {deleteMutation.isError ? (
          <div className="dialog-section">
            <strong>{"\u5220\u9664\u5931\u8d25"}</strong>
            <p>{(deleteMutation.error as Error).message}</p>
          </div>
        ) : null}
      </ConfirmDialog>
    </div>
  );
}
