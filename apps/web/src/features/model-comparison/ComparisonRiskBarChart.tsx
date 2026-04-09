import type { ComparisonRowView } from "../../shared/api/types";
import { WorkbenchChart } from "../../shared/ui/WorkbenchChart";

export function ComparisonRiskBarChart({ rows }: { rows: ComparisonRowView[] }) {
  return (
    <WorkbenchChart
      loadingLabel={"\u52a0\u8f7d\u6761\u5f62\u56fe..."}
      style={{ height: 320 }}
      option={{
        tooltip: { trigger: "axis" },
        legend: { textStyle: { color: "#d6d2c4" } },
        xAxis: {
          type: "category",
          data: rows.map((row) => row.model_name),
          axisLabel: { color: "#b9b0a0" },
        },
        yAxis: { type: "value", axisLabel: { color: "#b9b0a0" } },
        series: [
          {
            name: "\u6700\u5927\u56de\u64a4",
            type: "bar",
            data: rows.map((row) => row.max_drawdown ?? 0),
          },
          {
            name: "\u6362\u624b\u7387",
            type: "bar",
            data: rows.map((row) => row.turnover_total ?? 0),
          },
        ],
      }}
    />
  );
}
