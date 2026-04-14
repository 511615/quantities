import type { EChartsOption } from "echarts";

import { WorkbenchChart } from "../../shared/ui/WorkbenchChart";

export type CandlePoint = {
  time: string;
  open: number;
  close: number;
  low: number;
  high: number;
  volume: number;
};

type DatasetCandlestickChartProps = {
  candles: CandlePoint[];
  showMA5: boolean;
  showMA10: boolean;
  showVolume: boolean;
};

export function DatasetCandlestickChart({
  candles,
  showMA5,
  showMA10,
  showVolume,
}: DatasetCandlestickChartProps) {
  const categories = candles.map((item) => item.time);
  const values = candles.map((item) => [item.open, item.close, item.low, item.high]);
  const volumes = candles.map((item) => item.volume);
  const closes = candles.map((item) => item.close);

  const option: EChartsOption = {
    animationDuration: 300,
    tooltip: { trigger: "axis" },
    legend: {
      top: 0,
      textStyle: { color: "#b8b09e" },
      data: [
        "\u4ef7\u683c",
        ...(showMA5 ? ["5日均线"] : []),
        ...(showMA10 ? ["10日均线"] : []),
        ...(showVolume ? ["成交量"] : []),
      ],
    },
    grid: [
      { left: 44, right: 24, top: 34, height: showVolume ? "52%" : "70%" },
      ...(showVolume ? [{ left: 44, right: 24, top: "72%", height: "16%" }] : []),
    ],
    xAxis: [
      {
        type: "category" as const,
        data: categories,
        boundaryGap: true,
        axisLine: { lineStyle: { color: "rgba(213, 207, 193, 0.2)" } },
        axisLabel: { color: "#b8b09e", hideOverlap: true },
      },
      ...(showVolume
        ? [
            {
              type: "category" as const,
              gridIndex: 1,
              data: categories,
              boundaryGap: true,
              axisLine: { lineStyle: { color: "rgba(213, 207, 193, 0.2)" } },
              axisLabel: { show: false },
            },
          ]
        : []),
    ],
    yAxis: [
      {
        scale: true,
        axisLabel: { color: "#b8b09e" },
        splitLine: { lineStyle: { color: "rgba(213, 207, 193, 0.08)" } },
      },
      ...(showVolume
        ? [
            {
              gridIndex: 1,
              scale: true,
              axisLabel: { color: "#b8b09e" },
              splitLine: { show: false },
            },
          ]
        : []),
    ],
    dataZoom: [
      { type: "inside", xAxisIndex: showVolume ? [0, 1] : [0] },
      { type: "slider", bottom: 10, xAxisIndex: showVolume ? [0, 1] : [0] },
    ],
    series: [
      {
        name: "\u4ef7\u683c",
        type: "candlestick" as const,
        data: values,
        itemStyle: {
          color: "#c7ff73",
          color0: "#ff8d70",
          borderColor: "#c7ff73",
          borderColor0: "#ff8d70",
        },
      },
      ...(showMA5
        ? [
            {
              name: "5日均线",
              type: "line" as const,
              data: movingAverage(closes, 5),
              smooth: true,
              showSymbol: false,
              lineStyle: { width: 1.6, color: "#7fe3ff" },
            },
          ]
        : []),
      ...(showMA10
        ? [
            {
              name: "10日均线",
              type: "line" as const,
              data: movingAverage(closes, 10),
              smooth: true,
              showSymbol: false,
              lineStyle: { width: 1.6, color: "#ffc37d" },
            },
          ]
        : []),
      ...(showVolume
        ? [
            {
              name: "成交量",
              type: "bar" as const,
              xAxisIndex: 1,
              yAxisIndex: 1,
              data: volumes,
              itemStyle: { color: "rgba(199, 255, 115, 0.4)" },
            },
          ]
        : []),
    ],
  };

  return <WorkbenchChart loadingLabel="\u52a0\u8f7d K \u7ebf\u4e2d..." option={option} style={{ height: 420 }} />;
}

function movingAverage(values: number[], windowSize: number): Array<number | "-"> {
  return values.map((_, index) => {
    if (index < windowSize - 1) {
      return "-";
    }
    const slice = values.slice(index - windowSize + 1, index + 1);
    const mean = slice.reduce((sum, item) => sum + item, 0) / slice.length;
    return Number(mean.toFixed(2));
  });
}
