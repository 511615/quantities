import type { BacktestReportView } from "../../shared/api/types";
import { translateText } from "../../shared/lib/i18n";
import { useChartTheme } from "../../shared/lib/chartTheme";
import { WorkbenchChart } from "../../shared/ui/WorkbenchChart";

export function BacktestPerformanceChart({ detail }: { detail: BacktestReportView }) {
  const chartTheme = useChartTheme();
  const researchMetrics = detail.research?.metrics ?? {};
  const simulationMetrics = detail.simulation?.metrics ?? {};

  return (
    <WorkbenchChart
      loadingLabel={translateText("加载对比图表...")}
      style={{ height: 300 }}
      option={{
        tooltip: { trigger: "axis" },
        legend: { textStyle: { color: chartTheme.legendText } },
        xAxis: {
          type: "category",
          data: [
            translateText("年化收益"),
            translateText("最大回撤"),
            translateText("换手率"),
            translateText("实现短缺"),
          ],
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
            name: translateText("研究引擎"),
            type: "bar",
            itemStyle: { color: chartTheme.accentAlt },
            data: [
              researchMetrics.annual_return ?? 0,
              researchMetrics.max_drawdown ?? 0,
              researchMetrics.turnover_total ?? 0,
              researchMetrics.implementation_shortfall ?? 0,
            ],
          },
          {
            name: translateText("模拟引擎"),
            type: "bar",
            itemStyle: { color: chartTheme.accent },
            data: [
              simulationMetrics.annual_return ?? 0,
              simulationMetrics.max_drawdown ?? 0,
              simulationMetrics.turnover_total ?? 0,
              simulationMetrics.implementation_shortfall ?? 0,
            ],
          },
        ],
      }}
    />
  );
}
