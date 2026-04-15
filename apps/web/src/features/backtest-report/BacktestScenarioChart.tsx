import type { BacktestReportView } from "../../shared/api/types";
import { translateText } from "../../shared/lib/i18n";
import { useChartTheme } from "../../shared/lib/chartTheme";
import { WorkbenchChart } from "../../shared/ui/WorkbenchChart";

export function BacktestScenarioChart({ detail }: { detail: BacktestReportView }) {
  const chartTheme = useChartTheme();

  return (
    <WorkbenchChart
      loadingLabel={translateText("加载场景图表...")}
      style={{ height: 300 }}
      option={{
        tooltip: { trigger: "axis" },
        xAxis: {
          type: "category",
          data: Object.keys(detail.scenario_metrics),
          axisLine: { lineStyle: { color: chartTheme.axisLine } },
          axisLabel: { rotate: 28, color: chartTheme.axisText },
        },
        yAxis: {
          type: "value",
          axisLabel: { color: chartTheme.axisText },
          splitLine: { lineStyle: { color: chartTheme.splitLine } },
        },
        series: [
          {
            type: "bar",
            itemStyle: { color: chartTheme.warning },
            data: Object.values(detail.scenario_metrics),
          },
        ],
      }}
    />
  );
}
