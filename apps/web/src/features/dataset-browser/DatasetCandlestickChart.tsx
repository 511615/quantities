import type { EChartsOption } from "echarts";

import { useChartTheme } from "../../shared/lib/chartTheme";
import { translateText } from "../../shared/lib/i18n";
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
  const chartTheme = useChartTheme();
  const categories = candles.map((item) => item.time);
  const values = candles.map((item) => [item.open, item.close, item.low, item.high]);
  const volumes = candles.map((item) => item.volume);
  const closes = candles.map((item) => item.close);

  const option: EChartsOption = {
    animationDuration: 300,
    tooltip: { trigger: "axis" },
    legend: {
      top: 0,
      textStyle: { color: chartTheme.legendText },
      data: [
        translateText("价格"),
        ...(showMA5 ? [translateText("5日均线")] : []),
        ...(showMA10 ? [translateText("10日均线")] : []),
        ...(showVolume ? [translateText("成交量")] : []),
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
        axisLine: { lineStyle: { color: chartTheme.axisLine } },
        axisLabel: { color: chartTheme.axisText, hideOverlap: true },
      },
      ...(showVolume
        ? [
            {
              type: "category" as const,
              gridIndex: 1,
              data: categories,
              boundaryGap: true,
              axisLine: { lineStyle: { color: chartTheme.axisLine } },
              axisLabel: { show: false },
            },
          ]
        : []),
    ],
    yAxis: [
      {
        scale: true,
        axisLabel: { color: chartTheme.axisText },
        splitLine: { lineStyle: { color: chartTheme.splitLine } },
      },
      ...(showVolume
        ? [
            {
              gridIndex: 1,
              scale: true,
              axisLabel: { color: chartTheme.axisText },
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
        name: translateText("价格"),
        type: "candlestick" as const,
        data: values,
        itemStyle: {
          color: chartTheme.accent,
          color0: chartTheme.danger,
          borderColor: chartTheme.accent,
          borderColor0: chartTheme.danger,
        },
      },
      ...(showMA5
        ? [
            {
              name: translateText("5日均线"),
              type: "line" as const,
              data: movingAverage(closes, 5),
              smooth: true,
              showSymbol: false,
              lineStyle: { width: 1.6, color: chartTheme.accentAlt },
            },
          ]
        : []),
      ...(showMA10
        ? [
            {
              name: translateText("10日均线"),
              type: "line" as const,
              data: movingAverage(closes, 10),
              smooth: true,
              showSymbol: false,
              lineStyle: { width: 1.6, color: chartTheme.warning },
            },
          ]
        : []),
      ...(showVolume
        ? [
            {
              name: translateText("成交量"),
              type: "bar" as const,
              xAxisIndex: 1,
              yAxisIndex: 1,
              data: volumes,
              itemStyle: { color: `${chartTheme.accent}88` },
            },
          ]
        : []),
    ],
  };

  return <WorkbenchChart loadingLabel={translateText("加载 K 线中...")} option={option} style={{ height: 420 }} />;
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
