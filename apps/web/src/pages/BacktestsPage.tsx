import { useMemo, useState } from "react";
import { Link } from "react-router-dom";

import { useBacktests } from "../shared/api/hooks";
import { formatDate, formatNumber } from "../shared/lib/format";
import { I18N } from "../shared/lib/i18n";
import { GlossaryHint } from "../shared/ui/GlossaryHint";
import { PanelHeader } from "../shared/ui/PanelHeader";
import { EmptyState, ErrorState, LoadingState } from "../shared/ui/StateViews";
import { StatusPill } from "../shared/ui/StatusPill";

export function BacktestsPage() {
  const [search, setSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState("");

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
            placeholder={"\u641c\u7d22 backtest_id / \u8bad\u7ec3\u5b9e\u4f8b ID"}
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
                </tr>
              </thead>
              <tbody>
                {query.data.items.map((item) => (
                  <tr key={item.backtest_id}>
                    <td>
                      <Link to={`/backtests/${item.backtest_id}`}>{item.backtest_id}</Link>
                    </td>
                    <td>{item.run_id ?? "--"}</td>
                    <td>{formatNumber(item.max_drawdown)}</td>
                    <td>{formatNumber(item.annual_return)}</td>
                    <td>{item.warning_count}</td>
                    <td>
                      <StatusPill status={item.status} />
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
    </div>
  );
}
