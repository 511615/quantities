import { Link } from "react-router-dom";

import { useBacktestOptions, useBenchmarks } from "../shared/api/hooks";
import { formatDate, formatNumber } from "../shared/lib/format";
import { I18N, translateText } from "../shared/lib/i18n";
import { GlossaryHint } from "../shared/ui/GlossaryHint";
import { PanelHeader } from "../shared/ui/PanelHeader";
import { EmptyState, ErrorState, LoadingState } from "../shared/ui/StateViews";

function localizeTemplateName(name?: string | null, templateId?: string | null) {
  if (templateId === "system::official_backtest_protocol_v1") {
    return translateText("官方回测协议 v1");
  }
  return name ?? "--";
}

function localizeTemplateDescription(description?: string | null) {
  if (!description || description.trim().toLowerCase() === "official protocol") {
    return translateText("平台统一回测模板。");
  }
  return description;
}

export function BenchmarksPage() {
  const query = useBenchmarks();
  const optionsQuery = useBacktestOptions();
  const officialTemplate = optionsQuery.data?.template_options?.find(
    (item) => item.template_id === optionsQuery.data?.official_template_id,
  );

  return (
    <div className="page-stack">
      {officialTemplate ? (
        <section className="panel">
          <PanelHeader
            eyebrow={translateText("官方协议")}
            title={localizeTemplateName(officialTemplate.name, officialTemplate.template_id)}
            description={localizeTemplateDescription(officialTemplate.description)}
            action={
              <Link className="link-button" to={`/comparison?official_only=1&template_id=${encodeURIComponent(officialTemplate.template_id)}`}>
                {translateText("查看对比")}
              </Link>
            }
          />
          <div className="metric-grid detail-metric-grid">
            <div className="metric-tile">
              <span>{translateText("模板 ID")}</span>
              <strong>{officialTemplate.template_id}</strong>
            </div>
            <div className="metric-tile">
              <span>{translateText("协议版本")}</span>
              <strong>{officialTemplate.protocol_version ?? "--"}</strong>
            </div>
            <div className="metric-tile">
              <span>{translateText("状态")}</span>
              <strong>{translateText("不可删除")}</strong>
            </div>
          </div>
          <div className="stack-list">
            {officialTemplate.scenario_bundle.slice(0, 4).map((item) => (
              <div className="stack-item" key={item}>
                <strong>{item}</strong>
                <span>{translateText("官方压力场景包的固定组成部分")}</span>
              </div>
            ))}
          </div>
        </section>
      ) : null}
      <section className="panel">
        <PanelHeader
          eyebrow={I18N.nav.benchmarks}
          title={I18N.nav.benchmarks}
          description={translateText("将基准榜单与模型对比入口收敛到同一个工作面。")}
          action={
            <Link className="link-button" to="/comparison">
              {I18N.nav.comparison}
            </Link>
          }
        />
        {query.isLoading ? <LoadingState label={I18N.state.loading} /> : null}
        {query.isError ? <ErrorState message={(query.error as Error).message} /> : null}
        {!query.isLoading && !query.isError ? (
          query.data && query.data.length > 0 ? (
            <table className="data-table">
              <thead>
                <tr>
                  <th>{translateText("基准名称")}</th>
                  <th>{translateText("数据集")}</th>
                  <th>{translateText("当前领先")}</th>
                  <th><GlossaryHint hintKey="benchmark" termOverride={translateText("得分")} /></th>
                  <th>{translateText("更新时间")}</th>
                </tr>
              </thead>
              <tbody>
                {query.data.map((item) => (
                  <tr key={item.benchmark_name}>
                    <td>
                      <Link to={`/benchmarks/${encodeURIComponent(item.benchmark_name)}`}>{item.benchmark_name}</Link>
                    </td>
                    <td>{item.dataset_id}</td>
                    <td>{item.top_model_name ?? "--"}</td>
                    <td>{formatNumber(item.top_model_score)}</td>
                    <td>{formatDate(item.updated_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <EmptyState title={I18N.state.empty} body={translateText("当前没有可展示的基准结果。")} />
          )
        ) : null}
      </section>
    </div>
  );
}
