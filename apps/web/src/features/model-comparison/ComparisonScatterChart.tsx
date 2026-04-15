import type { ComparisonRowView } from "../../shared/api/types";
import { translateText } from "../../shared/lib/i18n";
import { useChartTheme } from "../../shared/lib/chartTheme";
import { WorkbenchChart } from "../../shared/ui/WorkbenchChart";

export function ComparisonScatterChart({ rows }: { rows: ComparisonRowView[] }) {
  const chartTheme = useChartTheme();

  return (
    <WorkbenchChart
      loadingLabel={translateText("加载散点图...")}
      style={{ height: 320 }}
      option={{
        tooltip: { trigger: "item" },
        xAxis: {
          type: "value",
          name: translateText("测试集平均绝对误差"),
          nameTextStyle: { color: chartTheme.axisText },
          axisLine: { lineStyle: { color: chartTheme.axisLine } },
          axisLabel: { color: chartTheme.axisText },
        },
        yAxis: {
          type: "value",
          name: translateText("年化收益"),
          nameTextStyle: { color: chartTheme.axisText },
          axisLine: { lineStyle: { color: chartTheme.axisLine } },
          axisLabel: { color: chartTheme.axisText },
          splitLine: { lineStyle: { color: chartTheme.splitLine } },
        },
        series: [
          {
            type: "scatter",
            symbolSize: 16,
            itemStyle: { color: chartTheme.accent },
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
