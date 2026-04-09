import { useEffect, useMemo, useState } from "react";
import { Link, useNavigate, useParams, useSearchParams } from "react-router-dom";

import { DatasetCandlestickChart } from "../features/dataset-browser/DatasetCandlestickChart";
import type { CandlePoint } from "../features/dataset-browser/DatasetCandlestickChart";
import { DatasetDeleteDialog } from "../features/dataset-browser/DatasetDeleteDialog";
import { DatasetWorkspaceNav } from "../features/dataset-browser/DatasetWorkspaceNav";
import { LaunchTrainDrawer } from "../features/launch-training/LaunchTrainDrawer";
import {
  adaptDatasetDetail,
  createApiNotReadyMessage,
  isApiNotReadyError,
  normalizeDatasetDetail,
} from "../features/dataset-browser/workbench";
import type { DatasetDependencyView } from "../shared/api/types";
import {
  useDatasetDependencies,
  useDatasetDetail,
  useDatasetOhlcv,
  useDatasetReadiness,
} from "../shared/api/hooks";
import { formatDate, formatNumber, formatPercent } from "../shared/lib/format";
import type { GlossaryKey } from "../shared/lib/i18n";
import { GlossaryHint } from "../shared/ui/GlossaryHint";
import { PanelHeader } from "../shared/ui/PanelHeader";
import { EmptyState, ErrorState, LoadingState } from "../shared/ui/StateViews";
import { StatusPill } from "../shared/ui/StatusPill";

const RANGE_PRESETS = ["7d", "30d", "90d", "180d", "all", "custom"] as const;
type RangePreset = (typeof RANGE_PRESETS)[number];

function TermLabel({ label, hintKey }: { label: string; hintKey: GlossaryKey }) {
  return (
    <span className="dataset-label-with-hint">
      <span>{label}</span>
      <GlossaryHint hintKey={hintKey} iconOnly />
    </span>
  );
}

function toDateInputValue(value: string | null | undefined) {
  if (!value) {
    return "";
  }
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "" : date.toISOString().slice(0, 10);
}

function fromDateInputValue(value: string, endOfDay = false) {
  if (!value) {
    return null;
  }
  return endOfDay ? `${value}T23:59:59Z` : `${value}T00:00:00Z`;
}

function startOfPreset(endIso: string | null, preset: Exclude<RangePreset, "custom">) {
  if (preset === "all") {
    return null;
  }
  const end = endIso ? new Date(endIso) : new Date();
  const days = { "7d": 7, "30d": 30, "90d": 90, "180d": 180 }[preset];
  if (!days || Number.isNaN(end.getTime())) {
    return null;
  }
  end.setUTCDate(end.getUTCDate() - days);
  return end.toISOString();
}

function clampRange(dataStart: string | null, dataEnd: string | null, preset: RangePreset) {
  if (preset === "custom" || preset === "all") {
    return { start: dataStart, end: dataEnd };
  }
  const presetStart = startOfPreset(dataEnd, preset);
  if (!presetStart) {
    return { start: dataStart, end: dataEnd };
  }
  if (!dataStart) {
    return { start: presetStart, end: dataEnd };
  }
  return {
    start: new Date(presetStart) < new Date(dataStart) ? dataStart : presetStart,
    end: dataEnd,
  };
}

function toCandles(
  items: Array<{ event_time: string; open: number; high: number; low: number; close: number; volume: number }>,
): CandlePoint[] {
  return items.map((item) => ({
    time: formatDate(item.event_time),
    open: item.open,
    high: item.high,
    low: item.low,
    close: item.close,
    volume: item.volume,
  }));
}

function presetLabel(preset: RangePreset) {
  return {
    "7d": "近 7 天",
    "30d": "近 30 天",
    "90d": "近 90 天",
    "180d": "近 180 天",
    all: "全部",
    custom: "自定义",
  }[preset];
}

function dependencyKindLabel(kind: string) {
  return (
    {
      run: "训练运行",
      backtest: "回测结果",
      dataset: "派生数据集",
      data_asset: "上游数据资产",
    }[kind.toLowerCase()] ?? kind
  );
}

function dependencyDirectionLabel(direction?: string) {
  if (direction === "depends_on") {
    return "上游依赖";
  }
  if (direction === "referenced_by") {
    return "下游引用";
  }
  return "关联关系";
}

function dependencyText(item: DatasetDependencyView) {
  return item.dependency_label || item.dependency_id;
}

export function DatasetDetailPage() {
  const navigate = useNavigate();
  const { datasetId = "" } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();
  const [deleteOpen, setDeleteOpen] = useState(false);

  const detailQuery = useDatasetDetail(datasetId);
  const readinessQuery = useDatasetReadiness(datasetId);
  const dependenciesQuery = useDatasetDependencies(datasetId);

  const normalized = normalizeDatasetDetail(detailQuery.data);
  const detail = normalized ? adaptDatasetDetail(normalized, readinessQuery.data ?? null) : null;

  const rangePreset = (RANGE_PRESETS.includes((searchParams.get("range_preset") ?? "") as RangePreset)
    ? searchParams.get("range_preset")
    : "30d") as RangePreset;

  useEffect(() => {
    if (!searchParams.get("range_preset")) {
      const next = new URLSearchParams(searchParams);
      next.set("range_preset", "30d");
      setSearchParams(next, { replace: true });
    }
  }, [searchParams, setSearchParams]);

  const dataStart = detail?.summary.raw.freshness.data_start_time ?? null;
  const dataEnd = detail?.summary.raw.freshness.data_end_time ?? detail?.summary.asOfTime ?? null;
  const clampedRange = clampRange(dataStart, dataEnd, rangePreset);
  const startTime =
    rangePreset === "custom" ? searchParams.get("start_time") ?? dataStart : clampedRange.start;
  const endTime =
    rangePreset === "custom" ? searchParams.get("end_time") ?? dataEnd : clampedRange.end;

  const canRenderMarketSlice =
    detail?.summary.dataDomain === "market" && Boolean(detail.summary.symbol);

  const barsQuery = useDatasetOhlcv(canRenderMarketSlice ? datasetId : null, {
    page: 1,
    per_page: 240,
    start_time: startTime,
    end_time: endTime,
  });
  const bars = barsQuery.data?.items ?? [];
  const candles = toCandles(bars);

  const dependencyItems = dependenciesQuery.data?.items ?? [];
  const blockingDependencies = dependenciesQuery.data?.blocking_items ?? [];
  const canDelete = dependenciesQuery.data?.can_delete ?? blockingDependencies.length === 0;
  const dependencySummary = useMemo(() => {
    if (dependenciesQuery.isLoading) {
      return "正在检查这份数据集的上游来源和下游引用。";
    }
    if (dependenciesQuery.isError) {
      return "依赖信息暂时读取失败。";
    }
    if (blockingDependencies.length > 0) {
      return `当前有 ${blockingDependencies.length} 个下游引用阻止直接删除。`;
    }
    if (dependencyItems.length > 0) {
      return `当前共发现 ${dependencyItems.length} 条关联依赖。`;
    }
    return "当前没有检测到额外依赖。";
  }, [blockingDependencies.length, dependenciesQuery.isError, dependenciesQuery.isLoading, dependencyItems.length]);

  const updateQuery = (updates: Record<string, string | null>) => {
    const next = new URLSearchParams(searchParams);
    Object.entries(updates).forEach(([key, value]) => {
      if (!value) {
        next.delete(key);
      } else {
        next.set(key, value);
      }
    });
    setSearchParams(next, { replace: true });
  };

  if (detailQuery.isLoading) {
    return <LoadingState />;
  }
  if (detailQuery.isError) {
    return <ErrorState message={(detailQuery.error as Error).message} />;
  }
  if (!detail) {
    return <EmptyState title="数据集不存在" body="当前没有找到这份数据集详情。" />;
  }

  const quality = detail.qualitySummary;
  const readinessErrorMessage = readinessQuery.isError
    ? isApiNotReadyError(readinessQuery.error)
      ? createApiNotReadyMessage("训练就绪度")
      : (readinessQuery.error as Error).message
    : null;

  return (
    <div className="page-stack">
      <section className="hero-strip compact-hero">
        <div>
          <div className="eyebrow">数据集详情</div>
          <h1>{detail.summary.title}</h1>
          <p>{detail.heroSummary}</p>
        </div>
        <div className="hero-actions">
          {detail.readiness.rawStatus !== "not_ready" ? (
            <LaunchTrainDrawer
              datasetId={datasetId}
              datasetLabel={detail.summary.title}
              description="直接基于当前数据集发起训练，训练入口会优先使用 dataset_id。"
              title="基于当前数据集发起训练"
              triggerLabel="用这份数据集训练"
            />
          ) : null}
          <button className="link-button danger-link" onClick={() => setDeleteOpen(true)} type="button">
            删除数据集
          </button>
          <Link className="comparison-link" to="/datasets/browser">
            返回浏览器
          </Link>
          <Link className="comparison-link" to="/datasets/training">
            查看训练面板
          </Link>
        </div>
      </section>

      <DatasetWorkspaceNav detailLabel="详情" />

      <div className="metric-grid">
        <div className="metric-tile">
          <span>
            <TermLabel hintKey="data_domain" label="数据域" />
          </span>
          <strong>{detail.summary.dataDomainLabel}</strong>
        </div>
        <div className="metric-tile">
          <span>
            <TermLabel hintKey="dataset_type" label="数据类型" />
          </span>
          <strong>{detail.summary.datasetTypeLabel}</strong>
        </div>
        <div className="metric-tile">
          <span>覆盖范围</span>
          <strong>{detail.summary.coverageLabel}</strong>
        </div>
        <div className="metric-tile">
          <span>
            <TermLabel hintKey="snapshot_version" label="快照版本" />
          </span>
          <strong>{detail.summary.snapshotVersion}</strong>
        </div>
      </div>

      <section className="panel">
        <PanelHeader
          eyebrow="一眼看懂"
          title="这是什么数据"
          description="先解释这份数据是什么、适合做什么，再展开构建细节与训练信息。"
        />
        <div className="dataset-hero-grid">
          <section className="details-panel">
            <PanelHeader eyebrow="用途说明" title="能拿它做什么" description={detail.intendedUse} />
            <div className="story-list">
              <div className="story-item">
                <p>{detail.heroSummary}</p>
              </div>
              <div className="story-item">
                <p>{detail.riskNote}</p>
              </div>
            </div>
          </section>

          <section className="details-panel">
            <PanelHeader eyebrow="常用指标" title="样本规模与覆盖范围" description="先看规模，再决定值不值得继续深入。" />
            <div className="definition-grid">
              <div className="definition-item">
                <span>总条数</span>
                <strong>{detail.summary.rowCountLabel}</strong>
              </div>
              <div className="definition-item">
                <span>
                  <TermLabel hintKey="feature_dimensions" label="特征维度" />
                </span>
                <strong>{detail.summary.featureCountLabel}</strong>
              </div>
              <div className="definition-item">
                <span>
                  <TermLabel hintKey="label_columns" label="标签列" />
                </span>
                <strong>{detail.summary.labelCountLabel}</strong>
              </div>
              <div className="definition-item">
                <span>
                  <TermLabel hintKey="label_horizon" label="标签窗口" />
                </span>
                <strong>{detail.summary.labelHorizonLabel}</strong>
              </div>
              <div className="definition-item">
                <span>
                  <TermLabel hintKey="entity_scope" label="实体范围" />
                </span>
                <strong>{detail.summary.entityScopeLabel}</strong>
              </div>
              <div className="definition-item">
                <span>
                  <TermLabel hintKey="data_coverage" label="时间覆盖范围" />
                </span>
                <strong>{detail.summary.coverageLabel}</strong>
              </div>
            </div>
          </section>
        </div>
      </section>

      <section className="panel">
        <PanelHeader eyebrow="采集与训练" title="来源、构建与训练就绪度" description="把来源、构建过程和训练就绪度拆开看。" />
        <div className="dataset-lifecycle-grid">
          <section className="details-panel">
            <PanelHeader eyebrow="采集来源" title="数据从哪里来" description="优先看来源、交易所和请求方式。" />
            <div className="kv-list compact">
              {detail.acquisitionEntries.map((row) => (
                <div className="kv-row" key={row.key}>
                  <span>{row.label}</span>
                  <strong>{row.value}</strong>
                </div>
              ))}
            </div>
          </section>

          <section className="details-panel">
            <PanelHeader eyebrow="构建结果" title="构建出了什么" description="看版本、Schema、样本规模与字段稳定性。" />
            <div className="kv-list compact">
              {detail.buildEntries.concat(detail.schemaEntries).map((row) => (
                <div className="kv-row" key={`${row.key}-${row.value}`}>
                  <span>{row.label}</span>
                  <strong>{row.value}</strong>
                </div>
              ))}
            </div>
          </section>

          <section className="details-panel">
            <PanelHeader eyebrow="训练就绪度" title="现在能不能训练" description="优先使用后端 readiness 结论。" />
            <div className="page-stack">
              <div className="split-line">
                <strong>{detail.readiness.statusLabel}</strong>
                <StatusPill status={detail.readiness.rawStatus} />
              </div>
              <div className="dataset-callout">
                <strong>{detail.readiness.summary}</strong>
                <span>{readinessErrorMessage ?? "训练入口会以这份 readiness 结论为准。"}</span>
              </div>
              <div className="kv-list compact">
                {detail.readiness.checklist.map((item) => (
                  <div className="kv-row" key={item.key}>
                    <span>{item.label}</span>
                    <strong>{item.value}</strong>
                  </div>
                ))}
              </div>
            </div>
          </section>
        </div>
      </section>

      <section className="panel">
        <PanelHeader eyebrow="依赖关系" title="上游来源、下游引用与删除影响" description="这里展示真实 dependencies 接口结果。" />
        <div className="metric-grid">
          <div className="metric-tile">
            <span>总依赖数</span>
            <strong>{dependencyItems.length}</strong>
          </div>
          <div className="metric-tile">
            <span>阻塞删除</span>
            <strong>{blockingDependencies.length}</strong>
          </div>
          <div className="metric-tile">
            <span>当前状态</span>
            <strong>{canDelete ? "可删除" : "暂不可删除"}</strong>
          </div>
        </div>
        <div className="dataset-callout">
          <strong>{canDelete ? "当前允许删除" : "当前不允许删除"}</strong>
          <span>{dependencySummary}</span>
        </div>
        {dependenciesQuery.isLoading ? <LoadingState /> : null}
        {dependenciesQuery.isError ? <ErrorState message={(dependenciesQuery.error as Error).message} /> : null}
        {!dependenciesQuery.isLoading && !dependenciesQuery.isError ? (
          dependencyItems.length > 0 ? (
            <div className="dataset-domain-grid">
              {dependencyItems.map((item) =>
                item.href ? (
                  <Link className="dataset-card" key={`${item.dependency_kind}-${item.dependency_id}`} to={item.href}>
                    <div className="dataset-domain-top">
                      <div>
                        <strong>{dependencyText(item)}</strong>
                        <span>{dependencyKindLabel(item.dependency_kind)}</span>
                      </div>
                      <span className="dataset-card-tag">
                        {item.blocking ? "阻塞删除" : dependencyDirectionLabel(item.direction)}
                      </span>
                    </div>
                    <div className="dataset-domain-stats">
                      <span>技术标识：{item.dependency_id}</span>
                    </div>
                  </Link>
                ) : (
                  <div className="dataset-card" key={`${item.dependency_kind}-${item.dependency_id}`}>
                    <div className="dataset-domain-top">
                      <div>
                        <strong>{dependencyText(item)}</strong>
                        <span>{dependencyKindLabel(item.dependency_kind)}</span>
                      </div>
                      <span className="dataset-card-tag">
                        {item.blocking ? "阻塞删除" : dependencyDirectionLabel(item.direction)}
                      </span>
                    </div>
                    <div className="dataset-domain-stats">
                      <span>技术标识：{item.dependency_id}</span>
                    </div>
                  </div>
                ),
              )}
            </div>
          ) : (
            <EmptyState title="暂无依赖" body="当前没有检测到这份数据集的额外依赖关系。" />
          )
        ) : null}
      </section>

      <section className="panel">
        <PanelHeader eyebrow="字段与质量" title="字段分组、标签定义与质量摘要" description="兼顾新手阅读和高级排查。" />
        <div className="dataset-field-layout">
          <section className="details-panel">
            <PanelHeader eyebrow="字段分组" title="字段构成" description="字段先按用途分组，再看示例字段。" />
            <div className="feature-group-list">
              {detail.featureGroups.length > 0 ? (
                detail.featureGroups.map((group) => (
                  <div className="feature-group-card" key={group.key}>
                    <strong>{group.label}</strong>
                    <p>{group.description}</p>
                    <span>{group.columns.join(" / ") || "暂无字段示例"}</span>
                  </div>
                ))
              ) : (
                <EmptyState title="暂无字段分组" body="当前还没有可展示的字段分组。" />
              )}
            </div>
          </section>

          <section className="details-panel">
            <PanelHeader eyebrow="质量摘要" title="标签、切分与质量" description="这里展示训练面板最常看的技术指标。" />
            <div className="kv-list compact">
              <div className="kv-row">
                <span>
                  <TermLabel hintKey="label_columns" label="标签列" />
                </span>
                <strong>{detail.labelColumns.join(" / ") || "--"}</strong>
              </div>
              <div className="kv-row">
                <span>
                  <TermLabel hintKey="split_strategy" label="切分方式" />
                </span>
                <strong>{detail.summary.raw.split_strategy ?? "--"}</strong>
              </div>
              <div className="kv-row">
                <span>
                  <TermLabel hintKey="freshness" label="新鲜度" />
                </span>
                <strong>{detail.summary.freshnessLabel}</strong>
              </div>
              <div className="kv-row">
                <span>
                  <TermLabel hintKey="quality_status" label="健康状态" />
                </span>
                <strong>{detail.summary.healthLabel}</strong>
              </div>
              <div className="kv-row">
                <span>
                  <TermLabel hintKey="missing_ratio" label="缺失率" />
                </span>
                <strong>{formatPercent(quality?.missing_ratio, 2)}</strong>
              </div>
              <div className="kv-row">
                <span>
                  <TermLabel hintKey="duplicate_rows" label="重复率" />
                </span>
                <strong>{formatPercent(quality?.duplicate_ratio, 2)}</strong>
              </div>
            </div>
          </section>
        </div>
      </section>

      <section className="panel">
        <PanelHeader eyebrow="可视化切片" title="图表浏览" description="市场型数据继续支持 K 线切片，其他域不伪造图表。" />
        {canRenderMarketSlice ? (
          <div className="page-stack">
            <div className="range-preset-row" role="group" aria-label="时间窗口">
              {RANGE_PRESETS.map((preset) => (
                <button
                  className={`range-chip${rangePreset === preset ? " active" : ""}`}
                  key={preset}
                  onClick={() => updateQuery({ range_preset: preset })}
                  type="button"
                >
                  {presetLabel(preset)}
                </button>
              ))}
            </div>
            <div className="form-section-grid dataset-date-grid">
              <label>
                <span>开始日期</span>
                <input
                  className="field"
                  onChange={(event) =>
                    updateQuery({ range_preset: "custom", start_time: fromDateInputValue(event.target.value) })
                  }
                  type="date"
                  value={toDateInputValue(startTime)}
                />
              </label>
              <label>
                <span>结束日期</span>
                <input
                  className="field"
                  onChange={(event) =>
                    updateQuery({ range_preset: "custom", end_time: fromDateInputValue(event.target.value, true) })
                  }
                  type="date"
                  value={toDateInputValue(endTime)}
                />
              </label>
            </div>
            {barsQuery.isLoading ? <LoadingState /> : null}
            {barsQuery.isError ? <ErrorState message={(barsQuery.error as Error).message} /> : null}
            {!barsQuery.isLoading && !barsQuery.isError && bars.length === 0 ? (
              <EmptyState title="当前窗口没有样本" body="可以扩大时间窗口，或切换到全部查看。" />
            ) : null}
            {bars.length > 0 ? (
              <>
                <div className="metric-grid">
                  <div className="metric-tile">
                    <span>已加载 K 线</span>
                    <strong>{bars.length}</strong>
                  </div>
                  <div className="metric-tile">
                    <span>最近收盘价</span>
                    <strong>{formatNumber(bars[bars.length - 1]?.close, 2)}</strong>
                  </div>
                  <div className="metric-tile">
                    <span>当前切片</span>
                    <strong>{detail.summary.symbolLabel}</strong>
                  </div>
                  <div className="metric-tile">
                    <span>频率</span>
                    <strong>{detail.summary.frequencyLabel}</strong>
                  </div>
                </div>
                <section className="panel">
                  <DatasetCandlestickChart candles={candles} showMA10={true} showMA5={true} showVolume={true} />
                </section>
                <section className="panel">
                  <PanelHeader eyebrow="最近样本" title="最近 OHLCV 记录" description="既能看图，也能直接核对原始样本。" />
                  <div className="dataset-browser-table-shell">
                    <table className="data-table compact-table">
                      <thead>
                        <tr>
                          <th>事件时间</th>
                          <th>可用时间</th>
                          <th>Symbol</th>
                          <th>Open</th>
                          <th>High</th>
                          <th>Low</th>
                          <th>Close</th>
                          <th>Volume</th>
                        </tr>
                      </thead>
                      <tbody>
                        {bars.slice(-12).reverse().map((row) => (
                          <tr key={`${row.event_time}-${row.symbol}`}>
                            <td>{formatDate(row.event_time)}</td>
                            <td>{formatDate(row.available_time)}</td>
                            <td>{row.symbol}</td>
                            <td>{formatNumber(row.open, 2)}</td>
                            <td>{formatNumber(row.high, 2)}</td>
                            <td>{formatNumber(row.low, 2)}</td>
                            <td>{formatNumber(row.close, 2)}</td>
                            <td>{formatNumber(row.volume, 0)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </section>
              </>
            ) : null}
          </div>
        ) : (
          <EmptyState title="当前切片接口未就绪" body="这份数据不是单资产市场切片，或后端尚未提供统一 series 接口。" />
        )}
      </section>

      <DatasetDeleteDialog
        datasetId={datasetId}
        datasetLabel={detail.summary.title}
        onClose={() => setDeleteOpen(false)}
        onDeleted={() => navigate("/datasets/browser")}
        open={deleteOpen}
      />
    </div>
  );
}
