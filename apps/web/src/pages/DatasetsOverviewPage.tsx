import { Link } from "react-router-dom";

import { DatasetRequestActivityPanel } from "../features/dataset-browser/DatasetRequestActivityPanel";
import { DatasetRequestDrawer } from "../features/dataset-browser/DatasetRequestDrawer";
import { DatasetWorkspaceNav } from "../features/dataset-browser/DatasetWorkspaceNav";
import { useDatasets, useJobs } from "../shared/api/hooks";
import { formatDate } from "../shared/lib/format";
import { EmptyState, ErrorState, LoadingState } from "../shared/ui/StateViews";
import { PanelHeader } from "../shared/ui/PanelHeader";
import {
  adaptDatasetSummary,
  describeDatasetType,
  filterDatasetRequestJobs,
  groupDatasetDomains,
  groupTrainingDatasets,
} from "../features/dataset-browser/workbench";

export function DatasetsOverviewPage() {
  const datasetsQuery = useDatasets(1, 100);
  const jobsQuery = useJobs();

  if (datasetsQuery.isLoading) {
    return <LoadingState />;
  }

  if (datasetsQuery.isError) {
    return <ErrorState message={(datasetsQuery.error as Error).message} />;
  }

  const items = (datasetsQuery.data?.items ?? []).map(adaptDatasetSummary);

  if (items.length === 0) {
    return (
      <EmptyState
        title="暂无数据集"
        body="当前目录里还没有可展示的数据集。等后端产生新的数据元信息后，这里会自动汇总成数据工作台总览。"
      />
    );
  }

  const domainCards = groupDatasetDomains(items).filter((item) => item.total > 0);
  const trainingItems = groupTrainingDatasets(items);
  const newestDataset = [...items].sort((a, b) =>
    (b.asOfTime ?? "").localeCompare(a.asOfTime ?? "", "zh-CN"),
  )[0];
  const healthWatchCount = items.filter((item) => item.healthLabel !== "健康").length;
  const datasetJobs = filterDatasetRequestJobs(jobsQuery.data?.items ?? []);

  return (
    <div className="page-stack">
      <section className="hero-strip compact-hero">
        <div>
          <div className="eyebrow">数据工作区</div>
          <h1>数据集总览</h1>
          <p>
            先按数据域建立全局地图，再决定进入浏览器、查看详情，还是直接去训练面板比较可训练数据集。
          </p>
        </div>
        <div className="hero-actions">
          <DatasetRequestDrawer
            description="从总览页直接发起新的采集与构建请求，提交后会自动进入现有任务追踪链路。"
            title="从总览页申请数据集"
          />
          <Link className="comparison-link" to="/datasets/browser">
            进入数据浏览器
          </Link>
          <Link className="comparison-link" to="/datasets/training">
            查看训练面板
          </Link>
        </div>
      </section>

      <DatasetWorkspaceNav />

      <div className="metric-grid">
        <div className="metric-tile">
          <span>数据域</span>
          <strong>{domainCards.length}</strong>
        </div>
        <div className="metric-tile">
          <span>数据集总数</span>
          <strong>{items.length}</strong>
        </div>
        <div className="metric-tile">
          <span>训练面板</span>
          <strong>{trainingItems.length}</strong>
        </div>
        <div className="metric-tile">
          <span>待留意项</span>
          <strong>{healthWatchCount}</strong>
        </div>
      </div>

      <div className="workspace-grid">
        <section className="workspace-primary page-stack">
          <section className="panel">
            <PanelHeader
              eyebrow="采集与构建"
              title="申请新数据集"
              description="不新增页面，只在当前工作台内补齐数据申请、构建追踪和目录回写这条链路。"
            />
            <div className="page-stack">
              <div className="dataset-callout">
                <strong>建议先明确时间窗口、资产模式、来源和频率，再提交申请。</strong>
                <span>
                  如果后端接口尚未开放，这里会明确提示未就绪，不会伪造成功态或演示数据。
                </span>
              </div>
              <DatasetRequestDrawer
                description="适合从总览页发起新的采集与构建请求。"
                title="工作台数据申请"
              />
            </div>
          </section>

          <section className="panel">
            <PanelHeader
              eyebrow="域级入口"
              title="按数据域进入"
              description="不再把币种当一级入口。先理解自己在看哪一类数据，再往下钻 symbol、频率和版本。"
            />
            <div className="dataset-domain-grid">
              {domainCards.map((card) => (
                <Link
                  className="dataset-domain-card"
                  key={card.key}
                  to={`/datasets/browser?data_domain=${card.key}`}
                >
                  <div className="dataset-domain-top">
                    <div>
                      <strong>{card.label}</strong>
                      <span>{card.summary}</span>
                    </div>
                    <span className="dataset-card-tag">{card.total} 份</span>
                  </div>
                  <div className="dataset-domain-stats">
                    <span>训练面板 {card.trainingCount} 份</span>
                    <span>新鲜数据 {card.freshCount} 份</span>
                  </div>
                </Link>
              ))}
            </div>
          </section>

          <section className="panel">
            <PanelHeader
              eyebrow="怎么使用"
              title="推荐浏览顺序"
              description="先看是什么数据，再看覆盖范围和样本规模，最后才进入切片图表或训练兼容性。"
            />
            <div className="timeline-list">
              <div className="timeline-row">
                <div className="timeline-badge">1</div>
                <div className="timeline-copy">
                  <strong>先去浏览器按数据域筛选</strong>
                  <span>适合先建立全局地图，也适合快速按来源、频率和版本定位目标数据集。</span>
                </div>
              </div>
              <div className="timeline-row">
                <div className="timeline-badge">2</div>
                <div className="timeline-copy">
                  <strong>进入详情页看覆盖与质量</strong>
                  <span>详情页会先回答这是什么数据、覆盖多久、有多少条、字段和标签是什么。</span>
                </div>
              </div>
              <div className="timeline-row">
                <div className="timeline-badge">3</div>
                <div className="timeline-copy">
                  <strong>如果要训练，再去训练面板页比较</strong>
                  <span>重点比较样本量、特征数、标签窗口、切分方式和训练就绪度。</span>
                </div>
              </div>
            </div>
          </section>
        </section>

        <aside className="workspace-sidebar page-stack">
          <section className="panel">
            <PanelHeader
              eyebrow="最近更新"
              title="当前最新数据"
              description="优先展示工作台里最近更新的一份数据，帮助判断现在看到的是不是旧版本。"
            />
            {newestDataset ? (
              <div className="stack-list">
                <div className="stack-item align-start">
                  <Link to={`/datasets/${newestDataset.datasetId}`}>{newestDataset.title}</Link>
                  <span>{newestDataset.subtitle}</span>
                  <span>更新时间：{formatDate(newestDataset.asOfTime)}</span>
                </div>
              </div>
            ) : (
              <EmptyState title="暂无更新" body="当前还没有可用的更新时间信息。" />
            )}
          </section>

          <section className="panel">
            <PanelHeader
              eyebrow="最近申请"
              title="数据申请任务"
              description="这里优先展示与数据申请、准备和构建相关的任务，方便回看最近一次请求是否已经写回目录。"
            />
            <DatasetRequestActivityPanel
              emptyBody="当前还没有检测到数据申请或构建任务。提交一次申请后，这里会出现追踪记录。"
              jobs={datasetJobs}
            />
          </section>

          <section className="panel">
            <PanelHeader
              eyebrow="训练入口"
              title="训练面板速览"
              description="优先关注真正可拿去训练的数据集，而不是把所有浏览切片都混在一起。"
            />
            {trainingItems.length > 0 ? (
              <div className="stack-list">
                {trainingItems.slice(0, 4).map((item) => (
                  <div className="stack-item align-start" key={item.datasetId}>
                    <Link to={`/datasets/${item.datasetId}`}>{item.title}</Link>
                    <span>{describeDatasetType(item.datasetType)}</span>
                    <span>
                      {item.rowCountLabel} 条 / {item.featureCountLabel} 维 / {item.labelHorizonLabel}
                    </span>
                  </div>
                ))}
              </div>
            ) : (
              <EmptyState
                title="暂无训练面板"
                body="当前目录里还没有能明确识别成训练面板的数据集。"
              />
            )}
          </section>
        </aside>
      </div>
    </div>
  );
}
