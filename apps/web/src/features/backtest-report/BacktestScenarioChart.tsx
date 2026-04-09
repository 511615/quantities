import type { BacktestReportView } from "../../shared/api/types";
import { WorkbenchChart } from "../../shared/ui/WorkbenchChart";

export function BacktestScenarioChart({ detail }: { detail: BacktestReportView }) {
  return (
    <WorkbenchChart
      loadingLabel={"\u52a0\u8f7d\u573a\u666f\u56fe\u8868..."}
      style={{ height: 300 }}
      option={{
        tooltip: { trigger: "axis" },
        xAxis: {
          type: "category",
          data: Object.keys(detail.scenario_metrics),
          axisLabel: { rotate: 28, color: "#b9b0a0" },
        },
        yAxis: { type: "value", axisLabel: { color: "#b9b0a0" } },
        series: [
          {
            type: "bar",
            data: Object.values(detail.scenario_metrics),
          },
        ],
      }}
    />
  );
}
