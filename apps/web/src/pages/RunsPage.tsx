import { useMemo, useState } from "react";

import { ExperimentBrowser } from "../features/experiment-browser/ExperimentBrowser";
import { LaunchBacktestDrawer } from "../features/launch-backtest/LaunchBacktestDrawer";
import { useRuns } from "../shared/api/hooks";
import { I18N } from "../shared/lib/i18n";

export function RunsPage() {
  const [search, setSearch] = useState("");
  const [modelFilter, setModelFilter] = useState("");
  const [datasetFilter, setDatasetFilter] = useState("");
  const [selectedRunIdForBacktest, setSelectedRunIdForBacktest] = useState<string | null>(null);

  const params = useMemo(() => {
    const next = new URLSearchParams({
      page: "1",
      per_page: "50",
      sort_by: "created_at",
      sort_order: "desc",
    });
    if (search) {
      next.set("search", search);
    }
    if (modelFilter) {
      next.set("model_name", modelFilter);
    }
    if (datasetFilter) {
      next.set("dataset_id", datasetFilter);
    }
    return next;
  }, [datasetFilter, modelFilter, search]);

  const runsQuery = useRuns(params);

  return (
    <div className="page-stack">
      <section className="panel">
        <div className="split-line">
          <div>
            <div className="eyebrow">{I18N.nav.runs}</div>
            <h2>{"\u8fd0\u884c\u5217\u8868"}</h2>
            <p>
              {
                "\u8fd9\u4e2a\u9875\u9762\u7528\u4e8e\u5b9e\u9a8c\u6d4f\u89c8\u3001\u7b5b\u9009\u548c\u53d1\u8d77\u56de\u6d4b\u3002"
              }
            </p>
          </div>
          <LaunchBacktestDrawer initialRunId={selectedRunIdForBacktest} />
        </div>
      </section>
      <ExperimentBrowser
        datasetFilter={datasetFilter}
        modelFilter={modelFilter}
        onDatasetFilterChange={setDatasetFilter}
        onLaunchBacktest={setSelectedRunIdForBacktest}
        onModelFilterChange={setModelFilter}
        onSearchChange={setSearch}
        query={runsQuery}
        search={search}
        title={"\u8fd0\u884c\u4e0e\u5b9e\u9a8c\u6d4f\u89c8"}
      />
    </div>
  );
}
