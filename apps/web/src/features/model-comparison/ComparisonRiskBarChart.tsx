import type { ComparisonRowView } from "../../shared/api/types";
import { translateText } from "../../shared/lib/i18n";
import { useChartTheme } from "../../shared/lib/chartTheme";
import { WorkbenchChart } from "../../shared/ui/WorkbenchChart";

export function ComparisonRiskBarChart({ rows }: { rows: ComparisonRowView[] }) {
  const chartTheme = useChartTheme();

  return (
    <WorkbenchChart
      loadingLabel={translateText("加载条形图...")}
      style={{ height: 320 }}
      option={{
        tooltip: { trigger: "axis" },
        legend: { textStyle: { color: chartTheme.legendText } },
        xAxis: {
          type: "category",
          data: rows.map((row) => row.model_name),
          axisLine: { lineStyle: { color: chartTheme.axisLine } },
          axisLabel: { color: chartTheme.axisText },
        },
        yAxis: {
          type: "value",
          axisLabel: { color: chartTheme.axisText },
          splitLine: { lineStyle: { color: chartTheme.splitLine } },
        },
        series: [
          {
            name: translateText("最大回撤"),
            type: "bar",
            itemStyle: { color: chartTheme.warning },
            data: rows.map((row) => row.max_drawdown ?? 0),
          },
          {
            name: translateText("换手率"),
            type: "bar",
            itemStyle: { color: chartTheme.accentAlt },
            data: rows.map((row) => row.turnover_total ?? 0),
          },
        ],
      }}
    />
  );
}
