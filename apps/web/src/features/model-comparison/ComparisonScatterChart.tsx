import type { ComparisonRowView } from "../../shared/api/types";
import { WorkbenchChart } from "../../shared/ui/WorkbenchChart";

export function ComparisonScatterChart({ rows }: { rows: ComparisonRowView[] }) {
  return (
    <WorkbenchChart
      loadingLabel={"\u52a0\u8f7d\u6563\u70b9\u56fe..."}
      style={{ height: 320 }}
      option={{
        tooltip: { trigger: "item" },
        xAxis: {
          type: "value",
          name: "测试集平均绝对误差",
          axisLabel: { color: "#b9b0a0" },
        },
        yAxis: {
          type: "value",
          name: "\u5e74\u5316\u6536\u76ca",
          axisLabel: { color: "#b9b0a0" },
        },
        series: [
          {
            type: "scatter",
            symbolSize: 16,
            data: rows.map((row) => [
              row.mean_test_mae ?? row.train_mae ?? 0,
              row.annual_return ?? 0,
              row.label,
            ]),
          },
        ],
      }}
    />
  );
}
