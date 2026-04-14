import { Link } from "react-router-dom";

import { DatasetWorkspaceNav } from "../features/dataset-browser/DatasetWorkspaceNav";
import { useDatasets, useTrainingDatasets } from "../shared/api/hooks";
import { PanelHeader } from "../shared/ui/PanelHeader";
import { EmptyState, ErrorState, LoadingState } from "../shared/ui/StateViews";
import { GlossaryHint } from "../shared/ui/GlossaryHint";
import {
  adaptDatasetSummary,
  adaptTrainingDatasetSummary,
  createApiNotReadyMessage,
  createFallbackTrainingDatasetItems,
  groupTrainingDatasets,
  isApiNotReadyError,
} from "../features/dataset-browser/workbench";

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
  const backendItems = (trainingQuery.data?.items ?? []).map(adaptTrainingDatasetSummary);
  const shouldUseFallback =
    trainingQuery.isError || (!trainingQuery.isLoading && backendItems.length === 0);
  const datasetsQuery = useDatasets(1, 100, shouldUseFallback);

  if (trainingQuery.isLoading || (shouldUseFallback && datasetsQuery.isLoading)) {
    return <LoadingState />;
  }

  if (shouldUseFallback && datasetsQuery.isError) {
    return <ErrorState message={(datasetsQuery.error as Error).message} />;
  }

  const fallbackItems = shouldUseFallback
    ? createFallbackTrainingDatasetItems(
        groupTrainingDatasets((datasetsQuery.data?.items ?? []).map(adaptDatasetSummary)),
      )
    : [];
  const usingBackend = !trainingQuery.isError && backendItems.length > 0;
  const items = (usingBackend ? backendItems : fallbackItems).filter(
    (item) => item.readinessStatus !== "not_ready",
  );
  const blockedItems = usingBackend
    ? backendItems.filter((item) => item.readinessStatus === "not_ready")
    : [];
  const fallbackMessage = trainingQuery.isError
    ? isApiNotReadyError(trainingQuery.error)
      ? createApiNotReadyMessage("训练数据集摘要")
      : (trainingQuery.error as Error).message
    : null;

  return (
    <div className="page-stack">
      <section className="hero-strip compact-hero">
        <div>
          <div className="eyebrow">训练数据集</div>
          <h1>训练面板汇总</h1>
          <p>
            这里只展示真正适合拿去训练的面板，不把纯浏览切片和技术快照混在一起，方便直接比较样本量、维度、切分方式和训练就绪度。
          </p>
        </div>
        <div className="hero-actions">
          <Link className="comparison-link" to="/datasets/browser">
            打开数据浏览器
          </Link>
          <Link className="comparison-link" to="/models">
            前往模型页
          </Link>
        </div>
      </section>

      <DatasetWorkspaceNav />

      {fallbackMessage ? (
        <div className="panel panel-empty panel-warn">
          <h3>训练摘要暂未接管</h3>
          <p>{fallbackMessage}</p>
          <p>当前先按已有数据集元数据做兼容展示，等后端 `/api/datasets/training` 稳定后会自动切换。</p>
        </div>
      ) : null}

      {items.length === 0 ? (
        <EmptyState
          title="暂无训练面板"
          body="当前目录里还没有足够元数据让前端确认哪些数据集属于训练面板。"
        />
      ) : (
        <>
          <div className="metric-grid">
            <div className="metric-tile">
              <span>训练面板数量</span>
              <strong>{items.length}</strong>
            </div>
            <div className="metric-tile">
              <span>多资产面板</span>
              <strong>{items.filter((item) => item.entityScope === "multi_asset").length}</strong>
            </div>
            <div className="metric-tile">
              <span>新鲜数据</span>
              <strong>{items.filter((item) => item.freshnessLabel === "新鲜").length}</strong>
            </div>
            <div className="metric-tile">
              <span>需留意</span>
              <strong>{items.filter((item) => item.readinessStatus === "warning").length}</strong>
            </div>
          </div>

          <section className="panel">
            <PanelHeader
              eyebrow="比较表"
              title="训练面板对比"
              description="优先展示后端确认过的训练摘要；如果接口暂未就绪，才会退回到前端兼容推断。"
            />

            <div className="dataset-browser-table-shell">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>名称</th>
                    <th>
                      <TermLabel hintKey="dataset_type" label="数据类型" />
                    </th>
                    <th>实体范围</th>
                    <th>标的池</th>
                    <th>样本量</th>
                    <th>
                      <TermLabel hintKey="feature_dimensions" label="特征维度" />
                    </th>
                    <th>标签列</th>
                    <th>
                      <TermLabel hintKey="label_horizon" label="标签窗口" />
                    </th>
                    <th>
                      <TermLabel hintKey="split_strategy" label="切分方式" />
                    </th>
                    <th>
                      <TermLabel hintKey="freshness" label="新鲜度" />
                    </th>
                    <th>质量状态</th>
                    <th>
                      <TermLabel hintKey="training_panel" label="训练就绪度" />
                    </th>
                    <th>操作</th>
                  </tr>
                </thead>
                <tbody>
                  {items.map((item) => (
                    <tr key={item.datasetId}>
                      <td>
                        <Link to={`/datasets/${encodeURIComponent(item.datasetId)}`}>{item.title}</Link>
                        <div className="dataset-row-subcopy">
                          <span>{item.subtitle}</span>
                          <span>技术标识：{item.technicalId}</span>
                        </div>
                      </td>
                      <td>
                        <div className="dataset-row-subcopy">
                          <strong>{item.datasetTypeLabel}</strong>
                          <span>{item.dataDomainLabel}</span>
                        </div>
                      </td>
                      <td>{item.entityScopeLabel}</td>
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
                        <Link
                          className="link-button"
                          to={`/models?launchTrain=1&datasetId=${encodeURIComponent(item.datasetId)}`}
                        >
                          发起训练
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
                eyebrow="暂不可训练"
                title="当前被后端判定为暂不可训练的数据集"
                description="这些数据集先不进入主比较表，避免把“能看”误认成“能训练”。"
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
