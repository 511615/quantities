import type { DataFreshnessView, ExperimentListItem } from "../api/types";

export type DatasetFieldDefinition = {
  name: string;
  meaning: string;
};

export type DatasetSplitNote = {
  label: string;
  detail: string;
};

export type CandlePoint = {
  time: string;
  open: number;
  close: number;
  low: number;
  high: number;
  volume: number;
};

export type DatasetView = {
  datasetId: string;
  dataSource: string;
  frequency: string;
  sampleCount: number;
  labelWindow: string;
  asOfTime: string | null;
  timeSafetySummary: string;
  buildSummary: string;
  labelDefinition: string;
  splitStrategy: string;
  updatedAt: string | null;
  fields: DatasetFieldDefinition[];
  notes: DatasetSplitNote[];
  sourceDescription: string;
  candles: CandlePoint[];
};

const DATASET_PRESETS: Record<string, Omit<DatasetView, "datasetId" | "asOfTime" | "updatedAt" | "candles">> = {
  smoke_dataset: {
    dataSource: "\u5408\u6210\u5feb\u901f\u9a8c\u8bc1",
    frequency: "1h",
    sampleCount: 480,
    labelWindow: "forward_return_1h",
    timeSafetySummary: "\u7528\u4e8e\u5feb\u901f\u9a8c\u8bc1\uff0c\u65f6\u5e8f\u7ed3\u6784\u7b80\u5316\u4f46\u65e0\u524d\u89c6\u6cc4\u6f0f\u3002",
    buildSummary: "\u57fa\u4e8e\u5408\u6210\u5e02\u573a K \u7ebf\u4e0e\u5c11\u91cf\u56e0\u5b50\u5feb\u901f\u751f\u6210\u3002",
    labelDefinition: "\u6807\u7b7e\u4e3a 1 \u5c0f\u65f6\u540e\u6536\u76ca\u7387\uff0c\u7528\u4e8e\u5feb\u901f\u9a8c\u8bc1\u6a21\u578b\u4e0e\u6d41\u7a0b\u3002",
    splitStrategy: "\u6309\u65f6\u95f4\u987a\u5e8f\u5207\u5206\uff0ctrain / valid / test \u4e0d\u4ea4\u53c9\u3002",
    fields: [
      { name: "open", meaning: "\u5f00\u76d8\u4ef7" },
      { name: "high", meaning: "\u6700\u9ad8\u4ef7" },
      { name: "low", meaning: "\u6700\u4f4e\u4ef7" },
      { name: "close", meaning: "\u6536\u76d8\u4ef7" },
      { name: "volume", meaning: "\u6210\u4ea4\u91cf" },
    ],
    notes: [
      { label: "\u6807\u7b7e\u5b9a\u4e49", detail: "\u4ee5 close-to-close \u7684 forward return \u4f5c\u4e3a\u9ed8\u8ba4\u76ee\u6807\u5217\u3002" },
      { label: "\u6784\u5efa\u6458\u8981", detail: "\u5feb\u901f\u751f\u6210\uff0c\u9002\u5408\u9a8c\u8bc1\u8bad\u7ec3 / \u9884\u6d4b / \u56de\u6d4b\u94fe\u8def\u3002" },
      { label: "\u65f6\u95f4\u5b89\u5168", detail: "\u7279\u5f81\u548c\u6807\u7b7e\u6839\u636e as_of_time \u5207\u7247\uff0c\u4e0d\u4f7f\u7528\u672a\u6765 K \u7ebf\u3002" },
    ],
    sourceDescription: "\u8be5\u6570\u636e\u96c6\u4e3a\u5de5\u4f5c\u53f0\u5feb\u901f\u9a8c\u8bc1\u9884\u7f6e\uff0c\u4e3b\u8981\u7528\u4e8e\u53ef\u7528\u6027\u68c0\u67e5\u3002",
  },
  real_benchmark: {
    dataSource: "Binance / \u7f13\u5b58\u57fa\u51c6\u6570\u636e",
    frequency: "1h",
    sampleCount: 24800,
    labelWindow: "forward_return_4h",
    timeSafetySummary: "\u6309 as_of_time \u6784\u5efa\u7279\u5f81\uff0c\u4fdd\u7559\u65f6\u95f4\u5207\u7247\u5b89\u5168\u6027\u3002",
    buildSummary: "\u57fa\u4e8e\u7f13\u5b58\u5e02\u573a K \u7ebf\u3001\u56e0\u5b50\u5217\u548c\u57fa\u51c6\u5207\u7a97\u805a\u5408\u751f\u6210\u3002",
    labelDefinition: "\u9ed8\u8ba4\u6807\u7b7e\u4e3a 4 \u5c0f\u65f6\u540e\u6536\u76ca\u7387\uff0c\u9002\u5408\u57fa\u51c6\u5bf9\u6bd4\u4e0e\u56de\u6d4b\u5206\u6790\u3002",
    splitStrategy: "\u4f7f\u7528 rolling window \u9a8c\u8bc1\uff0c\u51cf\u5c11\u5355\u4e00\u65f6\u6bb5\u504f\u5dee\u3002",
    fields: [
      { name: "open", meaning: "\u5f00\u76d8\u4ef7" },
      { name: "high", meaning: "\u6700\u9ad8\u4ef7" },
      { name: "low", meaning: "\u6700\u4f4e\u4ef7" },
      { name: "close", meaning: "\u6536\u76d8\u4ef7" },
      { name: "volume", meaning: "\u6210\u4ea4\u91cf" },
      { name: "factor_pack", meaning: "\u8de8\u5468\u671f\u56e0\u5b50\u7ec4" },
    ],
    notes: [
      { label: "\u6807\u7b7e\u5b9a\u4e49", detail: "\u76ee\u6807\u5217\u901a\u5e38\u4e3a 4h \u6216 12h \u7684 forward return\u3002" },
      { label: "\u6784\u5efa\u6458\u8981", detail: "\u6574\u5408\u5b9e\u76d8 K \u7ebf\u4e0e\u7ec4\u5408\u5b9a\u4e49\u7684\u89c6\u7a97\u805a\u5408\u3002" },
      { label: "\u5207\u5206\u7b56\u7565", detail: "\u4f18\u5148\u4f7f\u7528 rolling split\uff0c\u907f\u514d\u53ea\u5bf9\u67d0\u4e00\u65f6\u6bb5\u8fc7\u62df\u5408\u3002" },
    ],
    sourceDescription: "\u8be5\u6570\u636e\u96c6\u8868\u793a\u5b9e\u76d8\u7f13\u5b58 benchmark \u6570\u636e\uff0c\u9002\u5408\u8bad\u7ec3\u5bf9\u6bd4\u4e0e\u56de\u6d4b\u8bc4\u4f30\u3002",
  },
};

export function buildDatasetViews(
  runs: ExperimentListItem[],
  freshness: DataFreshnessView | null,
): DatasetView[] {
  const datasetIds = new Set<string>();
  for (const run of runs) {
    if (run.dataset_id) {
      datasetIds.add(run.dataset_id);
    }
  }
  if (freshness?.dataset_id) {
    datasetIds.add(freshness.dataset_id);
  }

  return Array.from(datasetIds)
    .map((datasetId) => {
      const preset = DATASET_PRESETS[datasetId] ?? DATASET_PRESETS.real_benchmark;
      const latestRun = runs.find((item) => item.dataset_id === datasetId);
      const asOfTime = freshness?.dataset_id === datasetId ? freshness.as_of_time : latestRun?.created_at ?? null;
      return {
        datasetId,
        dataSource: preset.dataSource,
        frequency: preset.frequency,
        sampleCount: preset.sampleCount,
        labelWindow: preset.labelWindow,
        asOfTime,
        timeSafetySummary: preset.timeSafetySummary,
        buildSummary: preset.buildSummary,
        labelDefinition: preset.labelDefinition,
        splitStrategy: preset.splitStrategy,
        updatedAt: latestRun?.created_at ?? asOfTime,
        fields: preset.fields,
        notes: preset.notes,
        sourceDescription: preset.sourceDescription,
        candles: generateCandles(datasetId),
      };
    })
    .sort((left, right) => left.datasetId.localeCompare(right.datasetId));
}

export function filterCandlesByRange(
  candles: CandlePoint[],
  range: "1m" | "3m" | "6m",
): CandlePoint[] {
  const size = range === "1m" ? 24 : range === "3m" ? 48 : 72;
  return candles.slice(-size);
}

function generateCandles(seedText: string): CandlePoint[] {
  let seed = 0;
  for (const char of seedText) {
    seed += char.charCodeAt(0);
  }
  const candles: CandlePoint[] = [];
  let previousClose = 100 + (seed % 18);

  for (let index = 0; index < 72; index += 1) {
    const drift = pseudoRandom(seed + index) * 2.2 - 1.1;
    const open = previousClose;
    const close = Math.max(50, open + drift);
    const high = Math.max(open, close) + pseudoRandom(seed + index * 2) * 1.4;
    const low = Math.min(open, close) - pseudoRandom(seed + index * 3) * 1.4;
    const volume = 1200 + Math.round(pseudoRandom(seed + index * 4) * 2400);

    candles.push({
      time: `2026-03-${String((index % 30) + 1).padStart(2, "0")} ${String(index % 24).padStart(2, "0")}:00`,
      open: Number(open.toFixed(2)),
      close: Number(close.toFixed(2)),
      high: Number(high.toFixed(2)),
      low: Number(low.toFixed(2)),
      volume,
    });
    previousClose = close;
  }

  return candles;
}

function pseudoRandom(seed: number): number {
  const value = Math.sin(seed * 12.9898) * 43758.5453;
  return value - Math.floor(value);
}
