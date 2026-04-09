import type { BacktestReportView } from "../../shared/api/types";
import { WorkbenchChart } from "../../shared/ui/WorkbenchChart";

export function BacktestPerformanceChart({ detail }: { detail: BacktestReportView }) {
  const researchMetrics = detail.research?.metrics ?? {};
  const simulationMetrics = detail.simulation?.metrics ?? {};

  return (
    <WorkbenchChart
      loadingLabel={"\u52a0\u8f7d\u5bf9\u6bd4\u56fe\u8868..."}
      style={{ height: 300 }}
      option={{
        tooltip: { trigger: "axis" },
        legend: { textStyle: { color: "#d6d2c4" } },
        xAxis: {
          type: "category",
          data: [
            "\u5e74\u5316\u6536\u76ca",
            "\u6700\u5927\u56de\u64a4",
            "\u6362\u624b\u7387",
            "\u5b9e\u73b0\u77ed\u7f3a",
          ],
          axisLabel: { color: "#b9b0a0" },
        },
        yAxis: { type: "value", axisLabel: { color: "#b9b0a0" } },
        series: [
          {
            name: "\u7814\u7a76\u5f15\u64ce",
            type: "bar",
            data: [
              researchMetrics.annual_return ?? 0,
              researchMetrics.max_drawdown ?? 0,
              researchMetrics.turnover_total ?? 0,
              researchMetrics.implementation_shortfall ?? 0,
            ],
          },
          {
            name: "\u6a21\u62df\u5f15\u64ce",
            type: "bar",
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
