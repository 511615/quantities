import { Link } from "react-router-dom";

import type { JobStatusView } from "../../shared/api/types";
import { formatDate } from "../../shared/lib/format";
import { formatJobTypeLabel, formatStageNameLabel } from "../../shared/lib/labels";
import { translateText } from "../../shared/lib/i18n";
import { EmptyState } from "../../shared/ui/StateViews";
import { StatusPill } from "../../shared/ui/StatusPill";
import { datasetJobDetailPath } from "./presentation";

type DatasetRequestActivityPanelProps = {
  jobs: JobStatusView[];
  emptyTitle?: string;
  emptyBody?: string;
};

export function DatasetRequestActivityPanel({
  jobs,
  emptyTitle = translateText("暂无数据申请任务"),
  emptyBody = translateText("这里会展示最近发起的数据申请、构建或准备任务。"),
}: DatasetRequestActivityPanelProps) {
  if (jobs.length === 0) {
    return <EmptyState title={emptyTitle} body={emptyBody} />;
  }

  return (
    <div className="stack-list">
      {jobs.slice(0, 5).map((job) => {
        const detailPath = datasetJobDetailPath(job);
        const currentStage = job.stages[job.stages.length - 1];
        return (
          <div className="stack-item align-start" key={job.job_id}>
            <div className="page-stack compact-gap">
              <strong>{job.job_id}</strong>
              <span>{formatJobTypeLabel(job.job_type)}</span>
              <span>
                {currentStage
                  ? `${formatStageNameLabel(currentStage.name)} · ${currentStage.summary || "--"}`
                  : translateText("阶段待更新")}
              </span>
              <span>
                {translateText("更新时间")}：{formatDate(job.updated_at)}
              </span>
            </div>
            <div className="page-stack compact-gap align-end">
              <StatusPill status={job.status} />
              {detailPath ? <Link to={detailPath}>{translateText("查看数据集详情")}</Link> : null}
            </div>
          </div>
        );
      })}
    </div>
  );
}
