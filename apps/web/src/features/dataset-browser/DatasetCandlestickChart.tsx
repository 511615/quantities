import type { EChartsOption } from "echarts";

import { useChartTheme } from "../../shared/lib/chartTheme";
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

const PRICE_LABEL = "价格";
const MA5_LABEL = "5 bar 均线";
const MA10_LABEL = "10 bar 均线";
const VOLUME_LABEL = "成交量";

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
  const ma5 = showMA5 ? movingAverage(closes, 5) : [];
  const ma10 = showMA10 ? movingAverage(closes, 10) : [];

  const option: EChartsOption = {
    animationDuration: 250,
    tooltip: {
      trigger: "axis",
      formatter: (params: unknown) => formatTooltip(params, candles, ma5, ma10),
    },
    legend: {
      top: 0,
      textStyle: { color: chartTheme.legendText },
      data: [
        PRICE_LABEL,
        ...(showMA5 ? [MA5_LABEL] : []),
        ...(showMA10 ? [MA10_LABEL] : []),
        ...(showVolume ? [VOLUME_LABEL] : []),
      ],
    },
    grid: [
      { left: 44, right: 24, top: 34, height: showVolume ? "52%" : "70%" },
      ...(showVolume ? [{ left: 44, right: 24, top: "72%", height: "16%" }] : []),
    ],
    xAxis: [
      {
        type: "category",
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
        name: PRICE_LABEL,
        type: "candlestick",
        data: values,
        z: 1,
        itemStyle: {
          color: chartTheme.accent,
          color0: chartTheme.danger,
          borderColor: chartTheme.accent,
          borderColor0: chartTheme.danger,
          opacity: 0.42,
        },
      },
      ...(showMA5
        ? [
            {
              name: MA5_LABEL,
              type: "line" as const,
              data: ma5,
              smooth: false,
              connectNulls: false,
              showSymbol: false,
              z: 10,
              lineStyle: { width: 3.2, color: chartTheme.accentAlt, opacity: 1, type: "solid" as const },
            },
          ]
        : []),
      ...(showMA10
        ? [
            {
              name: MA10_LABEL,
              type: "line" as const,
              data: ma10,
              smooth: false,
              connectNulls: false,
              showSymbol: false,
              z: 11,
              lineStyle: { width: 3.2, color: chartTheme.warning, opacity: 1, type: "dashed" as const },
            },
          ]
        : []),
      ...(showVolume
        ? [
            {
              name: VOLUME_LABEL,
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

  return <WorkbenchChart loadingLabel="加载 K 线中..." option={option} style={{ height: 420 }} />;
}

function movingAverage(values: number[], windowSize: number): Array<number | null> {
  return values.map((_, index) => {
    if (index < windowSize - 1) {
      return null;
    }
    const slice = values.slice(index - windowSize + 1, index + 1);
    const mean = slice.reduce((sum, item) => sum + item, 0) / slice.length;
    return Number(mean.toFixed(2));
  });
}

function formatTooltip(
  params: unknown,
  candles: CandlePoint[],
  ma5: Array<number | null>,
  ma10: Array<number | null>,
) {
  const items = Array.isArray(params) ? params : [];
  const dataIndex = typeof items[0]?.dataIndex === "number" ? items[0].dataIndex : -1;
  const candle = dataIndex >= 0 ? candles[dataIndex] : null;
  if (!candle) {
    return "";
  }
  const rows = [
    candle.time,
    `${PRICE_LABEL} open ${formatValue(candle.open)} close ${formatValue(candle.close)} lowest ${formatValue(candle.low)} highest ${formatValue(candle.high)}`,
    `${MA5_LABEL} ${formatNullableValue(ma5[dataIndex] ?? null)}`,
    `${MA10_LABEL} ${formatNullableValue(ma10[dataIndex] ?? null)}`,
    `${VOLUME_LABEL} ${formatValue(candle.volume)}`,
  ];
  return rows.join("<br/>");
}

function formatValue(value: number) {
  return value.toLocaleString("zh-CN", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

function formatNullableValue(value: number | null) {
  return value === null ? "--" : formatValue(value);
}
