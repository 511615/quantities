import { Link } from "react-router-dom";

import { DatasetWorkspaceNav } from "../features/dataset-browser/DatasetWorkspaceNav";
import {
  buildDatasetCard,
  buildTrainingCardsFromApi,
  buildTrainingCardsFromDatasets,
} from "../features/dataset-browser/presentation";
import { useDatasets, useTrainingDatasets } from "../shared/api/hooks";
import { translateText } from "../shared/lib/i18n";
import { GlossaryHint } from "../shared/ui/GlossaryHint";
import { PanelHeader } from "../shared/ui/PanelHeader";
import { EmptyState, ErrorState, LoadingState } from "../shared/ui/StateViews";

function TermLabel({
  label,
  hintKey,
}: {
  label: string;
  hintKey:
    | "label_horizon"
    | "split_strategy"
    | "feature_dimensions"
    | "dataset_type"
    | "freshness"
    | "training_panel";
}) {
  return (
    <span className="dataset-label-with-hint">
      <span>{label}</span>
      <GlossaryHint hintKey={hintKey} iconOnly />
    </span>
  );
}

export function TrainingDatasetsPage() {
  const trainingQuery = useTrainingDatasets();
  const shouldUseFallback = trainingQuery.isError || (!trainingQuery.isLoading && (trainingQuery.data?.items?.length ?? 0) === 0);
  const datasetsQuery = useDatasets(1, 100, shouldUseFallback);

  if (trainingQuery.isLoading || (shouldUseFallback && datasetsQuery.isLoading)) {
    return <LoadingState />;
  }

  if (shouldUseFallback && datasetsQuery.isError) {
    return <ErrorState message={(datasetsQuery.error as Error).message} />;
  }

  const items = shouldUseFallback
    ? buildTrainingCardsFromDatasets((datasetsQuery.data?.items ?? []).map(buildDatasetCard))
    : buildTrainingCardsFromApi(trainingQuery.data?.items ?? []);

  const visibleItems = items.filter((item) => item.readinessStatus !== "not_ready");
  const blockedItems = items.filter((item) => item.readinessStatus === "not_ready");

  return (
    <div className="page-stack">
      <section className="page-header-shell">
        <div className="page-header-main">
          <div className="eyebrow">{translateText("训练数据集")}</div>
          <h1>{translateText("训练面板总览")}</h1>
          <p>{translateText("这里只保留真正适合进入训练比较的面板，帮助快速判断哪些数据值得继续推进。")}</p>
        </div>
        <div className="page-header-actions">
          <Link className="comparison-link" to="/datasets/browser">
            {translateText("打开数据浏览器")}
          </Link>
          <Link className="comparison-link" to="/models">
            {translateText("前往模型页")}
          </Link>
        </div>
      </section>

      <DatasetWorkspaceNav />

      {visibleItems.length === 0 ? (
        <EmptyState
          title={translateText("暂无训练面板")}
          body={translateText("当前目录里还没有明确可训练的数据集。")}
        />
      ) : (
        <>
          <div className="summary-grid">
            <div className="summary-card">
              <span>{translateText("训练面板数量")}</span>
              <strong>{visibleItems.length}</strong>
            </div>
            <div className="summary-card">
              <span>{translateText("多资产面板")}</span>
              <strong>{visibleItems.filter((item) => item.universeSummary.includes("多资产") || item.universeSummary.includes(" / ")).length}</strong>
            </div>
            <div className="summary-card">
              <span>{translateText("新鲜数据")}</span>
              <strong>{visibleItems.filter((item) => item.freshnessLabel === "新鲜").length}</strong>
            </div>
            <div className="summary-card">
              <span>{translateText("需留意")}</span>
              <strong>{visibleItems.filter((item) => item.readinessStatus === "warning").length}</strong>
            </div>
          </div>

          <section className="panel">
            <PanelHeader
              eyebrow={translateText("训练比较")}
              title={translateText("训练面板目录")}
              description={translateText("先比较样本量、特征维度、标签窗口和切分方式，再决定用哪份数据发起训练。")}
            />
            <div className="dataset-browser-table-shell">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>{translateText("名称")}</th>
                    <th><TermLabel hintKey="dataset_type" label={translateText("数据集类型")} /></th>
                    <th>{translateText("研究范围")}</th>
                    <th>{translateText("样本量")}</th>
                    <th><TermLabel hintKey="feature_dimensions" label={translateText("特征维度")} /></th>
                    <th>{translateText("标签列")}</th>
                    <th><TermLabel hintKey="label_horizon" label={translateText("标签窗口")} /></th>
                    <th><TermLabel hintKey="split_strategy" label={translateText("切分方式")} /></th>
                    <th><TermLabel hintKey="freshness" label={translateText("新鲜度")} /></th>
                    <th>{translateText("质量")}</th>
                    <th><TermLabel hintKey="training_panel" label={translateText("训练就绪度")} /></th>
                    <th>{translateText("操作")}</th>
                  </tr>
                </thead>
                <tbody>
                  {visibleItems.map((item) => (
                    <tr key={item.datasetId}>
                      <td>
                        <div className="table-title-cell">
                          <Link to={`/datasets/${encodeURIComponent(item.datasetId)}`}>{item.title}</Link>
                          <span>{item.subtitle}</span>
                          <span>{`${translateText("版本")}：${item.snapshotVersion}`}</span>
                        </div>
                      </td>
                      <td>
                        <div className="dataset-row-subcopy">
                          <strong>{item.datasetTypeLabel}</strong>
                          <span>{item.domainLabel}</span>
                        </div>
                      </td>
                      <td>{item.universeSummary}</td>
                      <td>{item.sampleCountLabel}</td>
                      <td>{item.featureCountLabel}</td>
                      <td>{item.labelCountLabel}</td>
                      <td>{item.labelHorizonLabel}</td>
                      <td>{item.splitStrategyLabel}</td>
                      <td>{item.freshnessLabel}</td>
                      <td>{item.qualityLabel}</td>
                      <td>
                        <div className="dataset-row-subcopy">
                          <strong>{item.readinessLabel}</strong>
                          <span>{item.readinessReason}</span>
                        </div>
                      </td>
                      <td>
                        <Link className="link-button" to={`/models?launchTrain=1&datasetId=${encodeURIComponent(item.datasetId)}`}>
                          {translateText("发起训练")}
                        </Link>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          {blockedItems.length > 0 ? (
            <section className="panel">
              <PanelHeader
                eyebrow={translateText("暂不可训练")}
                title={translateText("当前不建议直接推进的面板")}
                description={translateText("这些数据先不进入主比较表，避免把“能浏览”误认成“能训练”。")}
              />
              <div className="stack-list">
                {blockedItems.map((item) => (
                  <div className="stack-item align-start" key={item.datasetId}>
                    <div className="page-stack compact-gap">
                      <Link to={`/datasets/${encodeURIComponent(item.datasetId)}`}>{item.title}</Link>
                      <span>{item.subtitle}</span>
                      <span>{item.readinessReason}</span>
                    </div>
                    <strong>{item.readinessLabel}</strong>
                  </div>
                ))}
              </div>
            </section>
          ) : null}
        </>
      )}
    </div>
  );
}
