import { useState } from "react";
import { Link, useParams } from "react-router-dom";

import { LaunchBacktestDrawer } from "../features/launch-backtest/LaunchBacktestDrawer";
import { useArtifactPreview, useRunDetail } from "../shared/api/hooks";
import { formatDate, formatNumber } from "../shared/lib/format";
import { I18N } from "../shared/lib/i18n";
import { formatArtifactLabel } from "../shared/lib/labels";
import { mapRunDetail } from "../shared/view-model/mappers";
import { GlossaryHint } from "../shared/ui/GlossaryHint";
import { MetricGrid } from "../shared/ui/MetricGrid";
import { PanelHeader } from "../shared/ui/PanelHeader";
import { EmptyState, ErrorState, LoadingState } from "../shared/ui/StateViews";
import { StatusPill } from "../shared/ui/StatusPill";

export function RunDetailPage() {
  const { runId = "" } = useParams();
  const [previewUri, setPreviewUri] = useState<string | null>(null);
  const runQuery = useRunDetail(runId);
  const previewQuery = useArtifactPreview(previewUri);

  if (runQuery.isLoading) {
    return <LoadingState label={I18N.state.loading} />;
  }
  if (runQuery.isError) {
    return <ErrorState message={(runQuery.error as Error).message} />;
  }
  if (!runQuery.data) {
    return <EmptyState body={"\u6ca1\u6709\u627e\u5230\u5bf9\u5e94 run \u8be6\u60c5\u3002"} title={I18N.state.empty} />;
  }

  const detail = mapRunDetail(runQuery.data);

  return (
    <div className="page-stack">
      <section className="panel">
        <PanelHeader
          eyebrow={I18N.nav.trainedModels}
          title={detail.run_id}
          description={
            "\u628a Run Detail \u6536\u8fdb\u5df2\u8bad\u7ec3\u6a21\u578b\u89c6\u56fe\uff0c\u7edf\u4e00\u67e5\u770b\u8bad\u7ec3\u6307\u6807\u3001\u4ea7\u7269\u3001\u5173\u8054\u56de\u6d4b\u548c\u8bf4\u660e\u5907\u6ce8\u3002"
          }
          action={
            <div className="table-actions">
              <Link className="link-button" to="/models?tab=trained">
                {I18N.nav.trainedModels}
              </Link>
              <StatusPill status={detail.status} />
              <LaunchBacktestDrawer initialRunId={detail.run_id} initialDatasetId={detail.dataset_id} />
            </div>
          }
        />
        <MetricGrid
          items={[
            { label: "\u6a21\u578b", value: detail.model_name },
            { label: "\u7b97\u6cd5\u7c7b\u578b", value: detail.family ?? "--" },
            { label: "\u6570\u636e\u96c6", value: detail.dataset_id ?? "--" },
            { label: "\u521b\u5efa\u65f6\u95f4", value: formatDate(detail.created_at) },
            { label: "\u540e\u7aef", value: detail.backend ?? "--" },
            { label: "MAE", value: formatNumber(detail.metrics.mae) },
          ]}
        />
      </section>

      <div className="detail-grid wide-secondary">
        <section className="panel">
          <PanelHeader
            eyebrow={"\u8bad\u7ec3\u6307\u6807"}
            title={"\u6307\u6807\u8be6\u60c5"}
            description={"\u7edf\u4e00\u5c55\u793a manifest \u548c tracking \u91cc\u7684\u53ef\u7528\u6307\u6807\u3002"}
          />
          <table className="data-table compact-table">
            <thead>
              <tr>
                <th>{"\u6307\u6807"}</th>
                <th>{"\u503c"}</th>
              </tr>
            </thead>
            <tbody>
              {Object.entries(detail.metrics).map(([key, value]) => (
                <tr key={key}>
                  <td>{key === "mae" ? <GlossaryHint hintKey="mae" /> : key}</td>
                  <td>{formatNumber(value)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>

        <section className="panel">
          <PanelHeader eyebrow={"\u914d\u7f6e\u7ebf\u7d22"} title={"\u8bad\u7ec3\u53c2\u6570\u7ebf\u7d22"} />
          <div className="stack-list">
            <div className="stack-item align-start">
              <strong>
                <GlossaryHint hintKey="epochs" />
              </strong>
              <span>
                {detail.tracking_params.epochs ??
                  detail.tracking_params.trainer_epochs ??
                  "--"}
              </span>
            </div>
            <div className="stack-item align-start">
              <strong>
                <GlossaryHint hintKey="learning_rate" />
              </strong>
              <span>{detail.tracking_params.learning_rate ?? "--"}</span>
            </div>
            <div className="stack-item align-start">
              <strong>
                <GlossaryHint hintKey="regularization" />
              </strong>
              <span>
                {detail.tracking_params.regularization ??
                  detail.tracking_params.l2 ??
                  detail.tracking_params.alpha ??
                  "--"}
              </span>
            </div>
            <div className="stack-item align-start">
              <strong>
                <GlossaryHint hintKey="prediction_scope" />
              </strong>
              <span>{detail.predictions.map((item) => item.scope).join(", ") || "--"}</span>
            </div>
            <div className="stack-item align-start">
              <strong>{"\u91cd\u73b0\u4e0a\u4e0b\u6587"}</strong>
              <span>{JSON.stringify(detail.repro_context)}</span>
            </div>
          </div>
        </section>
      </div>

      <div className="detail-grid wide-secondary">
        <section className="panel">
          <PanelHeader eyebrow={"\u7279\u5f81"} title={"\u7279\u5f81\u91cd\u8981\u6027"} />
          {Object.keys(detail.feature_importance).length > 0 ? (
            <div className="stack-list">
              {Object.entries(detail.feature_importance).map(([name, value]) => (
                <div className="stack-item" key={name}>
                  <strong>{name}</strong>
                  <span>{formatNumber(value)}</span>
                </div>
              ))}
            </div>
          ) : (
            <EmptyState body={"\u5f53\u524d run \u6ca1\u6709 feature importance \u5de5\u4ef6\u3002"} title={I18N.state.empty} />
          )}
        </section>

        <section className="panel">
          <PanelHeader eyebrow={I18N.nav.backtests} title={I18N.nav.backtests} />
          {detail.related_backtests.length > 0 ? (
            <div className="stack-list">
              {detail.related_backtests.map((backtest) => (
                <div className="stack-item align-start" key={backtest.backtest_id}>
                  <div className="split-line">
                    <Link to={`/backtests/${backtest.backtest_id}`}>{backtest.backtest_id}</Link>
                    <StatusPill status={backtest.passed_consistency_checks === false ? "failed" : "success"} />
                  </div>
                  <span>
                    <GlossaryHint hintKey="max_drawdown" /> {formatNumber(backtest.max_drawdown)}
                  </span>
                  <span>{`\u5e74\u5316\u6536\u76ca ${formatNumber(backtest.annual_return)}`}</span>
                </div>
              ))}
            </div>
          ) : (
            <EmptyState body={"\u5f53\u524d run \u8fd8\u6ca1\u6709\u5173\u8054 backtest\u3002"} title={I18N.state.empty} />
          )}
        </section>
      </div>

      <section className="panel">
        <PanelHeader eyebrow={"\u9884\u6d4b\u4ea7\u7269"} title={"\u9884\u6d4b\u4ea7\u7269"} />
        {detail.predictions.length > 0 ? (
          <div className="stack-list">
            {detail.predictions.map((prediction) => (
              <div className="stack-item" key={prediction.uri}>
                <strong>{prediction.scope}</strong>
                <span>{`${prediction.sample_count} \u6761\u6837\u672c`}</span>
                <button className="link-button" onClick={() => setPreviewUri(prediction.uri)} type="button">
                  {I18N.action.preview}
                </button>
              </div>
            ))}
          </div>
        ) : (
          <EmptyState body={"\u5f53\u524d run \u8fd8\u6ca1\u6709\u9884\u6d4b\u7ed3\u679c\u3002"} title={I18N.state.empty} />
        )}
      </section>

      <section className="panel">
        <PanelHeader eyebrow={"\u5de5\u4ef6\u6d4f\u89c8"} title={"\u5de5\u4ef6\u6d4f\u89c8"} />
        <div className="artifact-grid">
          <div className="artifact-list">
            {detail.artifacts.map((artifact) => (
              <button
                className="artifact-row"
                key={artifact.uri}
                onClick={() => setPreviewUri(artifact.uri)}
                type="button"
              >
                <strong>{formatArtifactLabel(artifact.kind, artifact.label)}</strong>
                <span>{artifact.uri}</span>
              </button>
            ))}
          </div>
          <div className="artifact-preview">
            {previewQuery.isLoading ? <LoadingState label={I18N.state.loading} /> : null}
            {previewQuery.isError ? <ErrorState message={(previewQuery.error as Error).message} /> : null}
            {!previewQuery.isLoading && !previewQuery.isError ? (
              previewQuery.data ? (
                <pre>{JSON.stringify(previewQuery.data.content, null, 2)}</pre>
              ) : (
                <EmptyState body={"\u5de6\u4fa7\u70b9\u51fb\u5de5\u4ef6\u8fdb\u884c\u9884\u89c8\u3002"} title={I18N.state.selectArtifact} />
              )
            ) : null}
          </div>
        </div>
      </section>

      {detail.notes.length > 0 ? (
        <section className="panel">
          <PanelHeader eyebrow={"\u6570\u636e\u63d0\u793a"} title={"\u90e8\u5206\u6570\u636e\u8bf4\u660e"} />
          <div className="stack-list">
            {detail.notes.map((note) => (
              <div className="stack-item align-start" key={note}>
                <strong>{note}</strong>
              </div>
            ))}
          </div>
        </section>
      ) : null}
    </div>
  );
}
