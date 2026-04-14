import { lazy, Suspense } from "react";
import type { ComponentType, CSSProperties } from "react";
import type { EChartsOption } from "echarts";
import * as echarts from "echarts/core";
import { BarChart, CandlestickChart, LineChart, ScatterChart } from "echarts/charts";
import {
  DataZoomComponent,
  GridComponent,
  LegendComponent,
  TitleComponent,
  TooltipComponent,
} from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";

import { I18N } from "../lib/i18n";
import { LoadingState } from "./StateViews";

echarts.use([
  TitleComponent,
  TooltipComponent,
  LegendComponent,
  GridComponent,
  DataZoomComponent,
  BarChart,
  CandlestickChart,
  LineChart,
  ScatterChart,
  CanvasRenderer,
]);

type WorkbenchChartProps = {
  option: EChartsOption;
  style?: CSSProperties;
  className?: string;
  loadingLabel?: string;
};

type ReactEChartsCoreProps = {
  option: EChartsOption;
  echarts: typeof echarts;
  style?: CSSProperties;
  className?: string;
  lazyUpdate?: boolean;
  notMerge?: boolean;
};

type ReactEChartsCoreModule = {
  default: ComponentType<ReactEChartsCoreProps> | { default: ComponentType<ReactEChartsCoreProps> };
};

const ReactEChartsCore = lazy(async () => {
  const module = (await import("echarts-for-react/lib/core")) as ReactEChartsCoreModule;
  const component =
    typeof module.default === "function" ? module.default : module.default.default;
  return { default: component };
});

export function WorkbenchChart({
  option,
  style,
  className,
  loadingLabel = I18N.state.loading,
}: WorkbenchChartProps) {
  return (
    <Suspense fallback={<LoadingState label={loadingLabel} />}>
      <ReactEChartsCore
        className={className}
        echarts={echarts}
        lazyUpdate
        notMerge
        option={option}
        style={style}
      />
    </Suspense>
  );
}
