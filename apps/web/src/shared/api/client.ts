import type {
  ArtifactPreviewResponse,
  BacktestDeleteResponse,
  BacktestLaunchPreflightView,
  BacktestLaunchOptionsView,
  BacktestReportView,
  BacktestsResponse,
  BenchmarkDetailView,
  BenchmarkListItemView,
  DatasetAcquisitionRequest,
  DatasetDeleteResponse,
  DatasetDependenciesResponse,
  DatasetDetailView,
  DatasetFusionBuildResponse,
  DatasetFusionRequest,
  DatasetListResponse,
  DatasetPipelinePlanView,
  DatasetPipelineRequest,
  DatasetReadinessSummaryView,
  DatasetRequestOptionsView,
  ExperimentsResponse,
  JobListResponse,
  JobStatusView,
  LaunchBacktestRequest,
  LaunchBacktestPreflightRequest,
  LaunchDatasetMultimodalTrainRequest,
  LaunchJobResponse,
  LaunchModelCompositionRequest,
  LaunchTrainRequest,
  ModelTemplateCreateRequest,
  ModelTemplateView,
  ModelTemplateListResponse,
  ModelTemplateUpdateRequest,
  ModelComparisonView,
  OhlcvBarsResponse,
  RunDetailView,
  TrainedModelDetailView,
  TrainingDatasetListResponse,
  TrainingDatasetSummaryView,
  TrainLaunchOptionsView,
  DatasetNlpInspectionView,
  WorkbenchOverviewView,
} from "./types";

const API_BASE = (import.meta.env.VITE_API_BASE as string | undefined) ?? "";

function flattenErrorDetail(detail: unknown): string[] {
  if (typeof detail === "string") {
    return [detail];
  }
  if (Array.isArray(detail)) {
    return detail.flatMap((item) => flattenErrorDetail(item));
  }
  if (detail && typeof detail === "object") {
    const record = detail as Record<string, unknown>;
    const location = Array.isArray(record.loc)
      ? record.loc.map((item) => String(item)).join(".")
      : null;
    const message =
      typeof record.msg === "string"
        ? record.msg
        : typeof record.message === "string"
          ? record.message
          : typeof record.detail === "string"
            ? record.detail
            : null;
    if (location && message) {
      return [`${location}: ${message}`];
    }
    if (message) {
      return [message];
    }
    return Object.values(record).flatMap((value) => flattenErrorDetail(value));
  }
  return [];
}

async function readErrorMessage(response: Response): Promise<string> {
  const contentType = response.headers.get("content-type") ?? "";
  if (contentType.includes("application/json")) {
    const payload = (await response.json()) as { detail?: unknown; message?: unknown };
    const messages = flattenErrorDetail(payload.detail ?? payload.message);
    if (messages.length > 0) {
      return messages.join("\n");
    }
    return `Request failed: ${response.status}`;
  }
  const detail = await response.text();
  return detail || `Request failed: ${response.status}`;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {}),
    },
    ...init,
  });
  if (!response.ok) {
    throw new Error(await readErrorMessage(response));
  }
  if (response.status === 204) {
    return undefined as T;
  }
  return (await response.json()) as T;
}

export const api = {
  overview() {
    return request<WorkbenchOverviewView>("/api/workbench/overview");
  },
  experiments(params: URLSearchParams) {
    return request<ExperimentsResponse>(`/api/experiments?${params.toString()}`);
  },
  runs(params: URLSearchParams) {
    return request<ExperimentsResponse>(`/api/runs?${params.toString()}`);
  },
  run(runId: string) {
    return request<RunDetailView>(`/api/runs/${encodeURIComponent(runId)}`);
  },
  backtests(params: URLSearchParams) {
    return request<BacktestsResponse>(`/api/backtests?${params.toString()}`);
  },
  backtest(backtestId: string) {
    return request<BacktestReportView>(`/api/backtests/${encodeURIComponent(backtestId)}`);
  },
  deleteBacktest(backtestId: string) {
    return request<BacktestDeleteResponse>(`/api/backtests/${encodeURIComponent(backtestId)}`, {
      method: "DELETE",
    });
  },
  benchmarks() {
    return request<BenchmarkListItemView[]>("/api/benchmarks");
  },
  benchmark(name: string) {
    return request<BenchmarkDetailView>(`/api/benchmarks/${encodeURIComponent(name)}`);
  },
  datasets(page = 1, perPage = 20) {
    return request<DatasetListResponse>(`/api/datasets?page=${page}&per_page=${perPage}`);
  },
  datasetRequestOptions() {
    return request<DatasetRequestOptionsView>("/api/datasets/request-options");
  },
  requestDataset(body: DatasetAcquisitionRequest) {
    return request<LaunchJobResponse>("/api/datasets/requests", {
      method: "POST",
      body: JSON.stringify(body),
    });
  },
  createDatasetPipeline(body: DatasetPipelineRequest) {
    return request<DatasetPipelinePlanView>("/api/datasets/pipelines", {
      method: "POST",
      body: JSON.stringify(body),
    });
  },
  createDatasetFusion(body: DatasetFusionRequest) {
    return request<DatasetFusionBuildResponse>("/api/datasets/fusions", {
      method: "POST",
      body: JSON.stringify(body),
    });
  },
  trainingDatasets() {
    return request<TrainingDatasetListResponse | TrainingDatasetSummaryView[]>(
      "/api/datasets/training",
    ).then((payload) => (Array.isArray(payload) ? { items: payload } : payload));
  },
  dataset(datasetId: string) {
    return request<DatasetDetailView>(`/api/datasets/${encodeURIComponent(datasetId)}`);
  },
  datasetDownloadUrl(datasetId: string) {
    return `${API_BASE}/api/datasets/${encodeURIComponent(datasetId)}/download`;
  },
  datasetDependencies(datasetId: string) {
    return request<DatasetDependenciesResponse>(
      `/api/datasets/${encodeURIComponent(datasetId)}/dependencies`,
    );
  },
  deleteDataset(datasetId: string) {
    return request<DatasetDeleteResponse>(`/api/datasets/${encodeURIComponent(datasetId)}`, {
      method: "DELETE",
    });
  },
  datasetReadiness(datasetId: string) {
    return request<DatasetReadinessSummaryView>(
      `/api/datasets/${encodeURIComponent(datasetId)}/readiness`,
    );
  },
  datasetNlpInspection(datasetId: string) {
    return request<DatasetNlpInspectionView>(
      `/api/datasets/${encodeURIComponent(datasetId)}/nlp-inspection`,
    );
  },
  datasetOhlcv(
    datasetId: string,
    params: {
      page?: number;
      per_page?: number;
      start_time?: string | null;
      end_time?: string | null;
    },
  ) {
    const query = new URLSearchParams();
    query.set("page", String(params.page ?? 1));
    query.set("per_page", String(params.per_page ?? 300));
    if (params.start_time) {
      query.set("start_time", params.start_time);
    }
    if (params.end_time) {
      query.set("end_time", params.end_time);
    }
    return request<OhlcvBarsResponse>(
      `/api/datasets/${encodeURIComponent(datasetId)}/ohlcv?${query.toString()}`,
    );
  },
  compare(body: {
    run_ids: string[];
    benchmark_selections: Array<{ benchmark_name: string; model_names: string[] }>;
    template_id?: string;
    official_only?: boolean;
  }) {
    return request<ModelComparisonView>("/api/comparisons/models", {
      method: "POST",
      body: JSON.stringify(body),
    });
  },
  trainOptions() {
    return request<TrainLaunchOptionsView>("/api/launch/train/options");
  },
  modelTemplates(includeDeleted = false) {
    const query = includeDeleted ? "?include_deleted=true" : "";
    return request<ModelTemplateListResponse>(`/api/models/templates${query}`);
  },
  createModelTemplate(body: ModelTemplateCreateRequest) {
    return request<ModelTemplateView>("/api/models/templates", {
      method: "POST",
      body: JSON.stringify(body),
    });
  },
  updateModelTemplate(templateId: string, body: ModelTemplateUpdateRequest) {
    return request<ModelTemplateView>(`/api/models/templates/${encodeURIComponent(templateId)}`, {
      method: "PATCH",
      body: JSON.stringify(body),
    });
  },
  deleteModelTemplate(templateId: string) {
    return request<void>(`/api/models/templates/${encodeURIComponent(templateId)}`, {
      method: "DELETE",
    });
  },
  deleteTrainedModel(runId: string) {
    return request<TrainedModelDetailView>(`/api/models/trained/${encodeURIComponent(runId)}`, {
      method: "DELETE",
    });
  },
  backtestOptions() {
    return request<BacktestLaunchOptionsView>("/api/launch/backtest/options");
  },
  backtestPreflight(body: LaunchBacktestPreflightRequest) {
    return request<BacktestLaunchPreflightView>("/api/launch/backtest/preflight", {
      method: "POST",
      body: JSON.stringify(body),
    });
  },
  launchTrain(body: LaunchTrainRequest) {
    return request<LaunchJobResponse>("/api/launch/train", {
      method: "POST",
      body: JSON.stringify(body),
    });
  },
  launchDatasetMultimodalTrain(body: LaunchDatasetMultimodalTrainRequest) {
    return request<LaunchJobResponse>("/api/launch/dataset-multimodal-train", {
      method: "POST",
      body: JSON.stringify(body),
    });
  },
  launchModelComposition(body: LaunchModelCompositionRequest) {
    return request<LaunchJobResponse>("/api/launch/model-composition", {
      method: "POST",
      body: JSON.stringify(body),
    });
  },
  launchBacktest(body: LaunchBacktestRequest) {
    return request<LaunchJobResponse>("/api/launch/backtest", {
      method: "POST",
      body: JSON.stringify(body),
    });
  },
  jobs() {
    return request<JobListResponse>("/api/jobs");
  },
  job(jobId: string) {
    return request<JobStatusView>(`/api/jobs/${encodeURIComponent(jobId)}`);
  },
  previewArtifact(uri: string) {
    return request<ArtifactPreviewResponse>(
      `/api/artifacts/preview?uri=${encodeURIComponent(uri)}`,
    );
  },
};
