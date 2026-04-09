import { lazy, Suspense } from "react";
import type { CSSProperties } from "react";
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

const ReactEChartsCore = lazy(() => import("echarts-for-react/lib/core"));

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
