import type { ReactNode } from "react";

import type { JobStageView, JobStatusView } from "../api/types";
import { formatDate } from "../lib/format";
import { translateText } from "../lib/i18n";
import { formatStageNameLabel } from "../lib/labels";
import { StatusPill } from "./StatusPill";

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function asStringArray(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((item): item is string => typeof item === "string") : [];
}

function currentJobStage(job: JobStatusView) {
  return (
    job.stages.find((stage) => stage.status === "running") ??
    job.stages.find((stage) => stage.status === "queued") ??
    job.stages[job.stages.length - 1] ??
    null
  );
}

function requestedStageNames(job: JobStatusView) {
  const pipelineSummary = asRecord(job.result.pipeline_summary);
  const requested = asStringArray(pipelineSummary.requested_stages);
  if (requested.length > 0) {
    return requested;
  }
  return job.stages.map((stage) => stage.name);
}

function stageProgress(job: JobStatusView) {
  const requested = requestedStageNames(job);
  const total = Math.max(requested.length, job.stages.length, 1);
  const completed = job.stages.filter((stage) => stage.status === "success").length;
  if (job.status === "success") {
    return { value: 100, completed: total, total };
  }
  if (job.status === "failed") {
    return {
      value: Math.max(8, Math.round((completed / total) * 100)),
      completed,
      total,
    };
  }
  const hasRunningStage = job.stages.some((stage) => stage.status === "running");
  const fractional = completed + (hasRunningStage ? 0.5 : job.status === "queued" ? 0.15 : 0);
  return {
    value: Math.max(5, Math.min(96, Math.round((fractional / total) * 100))),
    completed,
    total,
  };
}

function summaryBlock(job: JobStatusView) {
  const summary = asRecord(job.result.summary);
  const headline = typeof summary.headline === "string" ? summary.headline : null;
  const detail = typeof summary.detail === "string" ? summary.detail : null;
  const highlights = asStringArray(summary.highlights);
  return { headline, detail, highlights };
}

function stageTimestampLabel(stage: JobStageView) {
  if (stage.finished_at) {
    return `${translateText("完成于")} ${formatDate(stage.finished_at)}`;
  }
  if (stage.started_at) {
    return `${translateText("开始于")} ${formatDate(stage.started_at)}`;
  }
  return null;
}

type JobProgressCardProps = {
  job: JobStatusView;
  footer?: ReactNode;
};

export function JobProgressCard({ job, footer }: JobProgressCardProps) {
  const progress = stageProgress(job);
  const activeStage = currentJobStage(job);
  const summary = summaryBlock(job);

  return (
    <div className="job-box">
      <div className="split-line">
        <strong>{job.job_id}</strong>
        <StatusPill status={job.status} />
      </div>
      <div className="job-progress-meta">
        <strong>{translateText("任务进度")}</strong>
        <span>{`${progress.value}%`}</span>
      </div>
      <div
        aria-valuemax={100}
        aria-valuemin={0}
        aria-valuenow={progress.value}
        className="job-progress-track"
        role="progressbar"
      >
        <div className="job-progress-fill" style={{ width: `${progress.value}%` }} />
      </div>
      <div className="job-progress-meta subdued">
        <span>
          {activeStage
            ? `${translateText("当前阶段")}：${formatStageNameLabel(activeStage.name)}`
            : translateText("等待任务开始")}
        </span>
        <span>{`${progress.completed}/${progress.total} ${translateText("个阶段完成")}`}</span>
      </div>
      {summary.headline ? (
        <div className="dataset-callout compact">
          <strong>{summary.headline}</strong>
          {summary.detail ? <span>{summary.detail}</span> : null}
          {summary.highlights.length > 0 ? <span>{summary.highlights.join(" / ")}</span> : null}
        </div>
      ) : null}
      {job.stages.map((stage) => (
        <div className="job-stage detailed" key={stage.name}>
          <div className="job-stage-copy">
            <strong>{formatStageNameLabel(stage.name)}</strong>
            <span>{stage.summary || "--"}</span>
            {stageTimestampLabel(stage) ? (
              <span className="job-stage-time">{stageTimestampLabel(stage)}</span>
            ) : null}
          </div>
          <StatusPill status={stage.status} />
        </div>
      ))}
      {job.error_message ? <p className="form-error">{job.error_message}</p> : null}
      {footer ? <div className="table-actions">{footer}</div> : null}
    </div>
  );
}
