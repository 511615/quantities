import { Link } from "react-router-dom";

import { DatasetRequestActivityPanel } from "../features/dataset-browser/DatasetRequestActivityPanel";
import { DatasetRequestDrawer } from "../features/dataset-browser/DatasetRequestDrawer";
import { DatasetWorkspaceNav } from "../features/dataset-browser/DatasetWorkspaceNav";
import {
  buildDatasetCard,
  buildTrainingCardsFromDatasets,
  describeDatasetType,
  filterDatasetRequestJobs,
  groupDatasetDomains,
} from "../features/dataset-browser/presentation";
import { useDatasets, useJobs } from "../shared/api/hooks";
import { formatDate } from "../shared/lib/format";
import { translateText } from "../shared/lib/i18n";
import { EmptyState, ErrorState, LoadingState } from "../shared/ui/StateViews";
import { PanelHeader } from "../shared/ui/PanelHeader";

export function DatasetsOverviewPage() {
  const datasetsQuery = useDatasets(1, 100);
  const jobsQuery = useJobs();

  if (datasetsQuery.isLoading) {
    return <LoadingState />;
  }

  if (datasetsQuery.isError) {
    return <ErrorState message={(datasetsQuery.error as Error).message} />;
  }

  const items = (datasetsQuery.data?.items ?? []).map(buildDatasetCard);

  if (items.length === 0) {
    return (
      <EmptyState
        title={translateText("暂无数据集")}
        body={translateText(
          "当前目录里还没有可展示的数据集。后端生成新的数据元信息后，这里会自动汇总成数据工作区总览。",
        )}
      />
    );
  }

  const domainCards = groupDatasetDomains(items).filter((item) => item.total > 0);
  const trainingItems = buildTrainingCardsFromDatasets(items);
  const newestDataset = [...items]
    .sort((a, b) => (b.asOfTime ?? "").localeCompare(a.asOfTime ?? "", "zh-CN"))[0];
  const healthWatchCount = items.filter((item) => item.qualityLabel !== "健康").length;
  const datasetJobs = filterDatasetRequestJobs(jobsQuery.data?.items ?? []);

  return (
    <div className="page-stack">
      <section className="page-header-shell">
        <div className="page-header-main">
          <div className="eyebrow">{translateText("数据工作区")}</div>
          <h1>{translateText("数据集总览")}</h1>
          <p>
            {translateText(
              "先按数据域建立全局地图，再决定进入浏览器、查看详情，还是直接去训练面板比较可训练数据集。",
            )}
          </p>
        </div>
        <div className="page-header-actions">
          <DatasetRequestDrawer
            description={translateText("从总览页直接发起新的采集与构建请求，提交后会自动进入现有任务跟踪链路。")}
            title={translateText("从总览页申请数据集")}
          />
          <Link className="comparison-link" to="/datasets/browser">
            {translateText("进入数据浏览器")}
          </Link>
          <Link className="comparison-link" to="/datasets/training">
            {translateText("查看训练面板")}
          </Link>
        </div>
      </section>

      <DatasetWorkspaceNav />

      <div className="summary-grid">
        <div className="summary-card">
          <span>{translateText("数据域")}</span>
          <strong>{domainCards.length}</strong>
        </div>
        <div className="summary-card">
          <span>{translateText("数据集总数")}</span>
          <strong>{items.length}</strong>
        </div>
        <div className="summary-card">
          <span>{translateText("训练面板")}</span>
          <strong>{trainingItems.length}</strong>
        </div>
        <div className="summary-card">
          <span>{translateText("需留意项")}</span>
          <strong>{healthWatchCount}</strong>
        </div>
      </div>

      <div className="workspace-grid workspace-grid-balanced">
        <section className="workspace-primary page-stack">
          <section className="panel">
            <PanelHeader
              eyebrow={translateText("申请与构建")}
              title={translateText("申请新数据集")}
              description={translateText("在总览页内把数据申请、构建跟踪和目录回写收敛到一条主链路。")}
            />
            <div className="dataset-callout">
              <strong>
                {translateText("建议先明确时间窗口、资产模式、来源和频率，再提交请求。")}
              </strong>
              <span>
                {translateText(
                  "如果后端接口尚未开放，这里会明确提示未就绪，不会伪造成功态或演示数据。",
                )}
              </span>
            </div>
            <DatasetRequestDrawer
              description={translateText("适合从总览页发起新的采集与构建请求。")}
              title={translateText("工作台数据申请")}
            />
          </section>

          <section className="panel">
            <PanelHeader
              eyebrow={translateText("域级入口")}
              title={translateText("按数据域进入")}
              description={translateText(
                "先看数据属于哪一类，再往下看来源、频率、覆盖范围和训练适配性。",
              )}
            />
            <div className="dataset-domain-grid">
              {domainCards.map((card) => (
                <Link className="dataset-domain-card" key={card.key} to={`/datasets/browser?data_domain=${card.key}`}>
                  <div className="dataset-domain-top">
                    <div>
                      <strong>{translateText(card.label)}</strong>
                      <span>{translateText(card.summary)}</span>
                    </div>
                    <span className="dataset-card-tag">
                      {card.total} {translateText("份")}
                    </span>
                  </div>
                  <div className="dataset-domain-stats">
                    <span>
                      {translateText("训练面板")} {card.trainingCount} {translateText("份")}
                    </span>
                    <span>
                      {translateText("新鲜数据")} {card.freshCount} {translateText("份")}
                    </span>
                  </div>
                </Link>
              ))}
            </div>
          </section>

          <section className="panel">
            <PanelHeader
              eyebrow={translateText("推荐浏览顺序")}
              title={translateText("先看什么，再决定下一步")}
              description={translateText(
                "把“浏览数据”和“训练数据”明确区分，减少误把展示切片当训练面板的情况。",
              )}
            />
            <div className="timeline-list">
              <div className="timeline-row">
                <div className="timeline-badge">1</div>
                <div className="timeline-copy">
                  <strong>{translateText("先去浏览器按数据域筛选")}</strong>
                  <span>
                    {translateText(
                      "适合建立全局地图，也适合按来源、频率和版本快速定位目标数据集。",
                    )}
                  </span>
                </div>
              </div>
              <div className="timeline-row">
                <div className="timeline-badge">2</div>
                <div className="timeline-copy">
                  <strong>{translateText("进入详情页看覆盖、质量与依赖")}</strong>
                  <span>
                    {translateText(
                      "详情页优先回答这是什么数据、覆盖多久、样本有多少，以及它和上下游对象的关系。",
                    )}
                  </span>
                </div>
              </div>
              <div className="timeline-row">
                <div className="timeline-badge">3</div>
                <div className="timeline-copy">
                  <strong>{translateText("需要训练时再进入训练面板页")}</strong>
                  <span>
                    {translateText(
                      "训练页聚焦比较样本量、特征维度、标签窗口、切分方式和训练就绪度。",
                    )}
                  </span>
                </div>
              </div>
            </div>
          </section>
        </section>

        <aside className="workspace-sidebar page-stack">
          <section className="panel">
            <PanelHeader
              eyebrow={translateText("最近更新")}
              title={translateText("当前最新数据")}
              description={translateText(
                "优先展示工作区里最近更新的一份数据，帮助快速判断当前看到的是不是旧版本。",
              )}
            />
            {newestDataset ? (
              <div className="stack-list">
                <div className="stack-item align-start">
                  <Link to={`/datasets/${encodeURIComponent(newestDataset.datasetId)}`}>{newestDataset.title}</Link>
                  <span>{newestDataset.subtitle}</span>
                  <span>
                    {translateText("更新时间")}：{formatDate(newestDataset.asOfTime)}
                  </span>
                </div>
              </div>
            ) : (
              <EmptyState
                title={translateText("暂无更新")}
                body={translateText("当前还没有可用的更新时间信息。")}
              />
            )}
          </section>

          <section className="panel">
            <PanelHeader
              eyebrow={translateText("最近申请")}
              title={translateText("数据申请任务")}
              description={translateText(
                "优先展示数据申请、准备和构建任务，方便追踪最近一次请求是否已经写回目录。",
              )}
            />
            <DatasetRequestActivityPanel
              jobs={datasetJobs}
              emptyBody={translateText(
                "当前还没有检测到数据申请或构建任务。提交一次请求后，这里会出现追踪记录。",
              )}
            />
          </section>

          <section className="panel">
            <PanelHeader
              eyebrow={translateText("训练入口")}
              title={translateText("训练面板速览")}
              description={translateText(
                "只展示真正适合拿去训练的面板，不把所有浏览切片混在一起。",
              )}
            />
            {trainingItems.length > 0 ? (
              <div className="stack-list">
                {trainingItems.slice(0, 4).map((item) => (
                  <div className="stack-item align-start" key={item.datasetId}>
                    <Link to={`/datasets/${encodeURIComponent(item.datasetId)}`}>{item.title}</Link>
                    <span>{translateText(describeDatasetType(item.datasetTypeLabel === "融合训练面板" ? "fusion_training_panel" : "training_panel"))}</span>
                    <span>
                      {item.sampleCountLabel} {translateText("行")} / {item.featureCountLabel} {translateText("维")} /{" "}
                      {translateText(item.labelHorizonLabel)}
                    </span>
                  </div>
                ))}
              </div>
            ) : (
              <EmptyState
                title={translateText("暂无训练面板")}
                body={translateText("当前目录里还没有明确可训练的数据集。")}
              />
            )}
          </section>
        </aside>
      </div>
    </div>
  );
}
