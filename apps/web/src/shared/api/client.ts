import type {
  ArtifactPreviewResponse,
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
  LaunchJobResponse,
  LaunchTrainRequest,
  ModelComparisonView,
  OhlcvBarsResponse,
  RunDetailView,
  TrainingDatasetListResponse,
  TrainingDatasetSummaryView,
  TrainLaunchOptionsView,
  WorkbenchOverviewView,
} from "./types";

const API_BASE = (import.meta.env.VITE_API_BASE as string | undefined) ?? "";

async function readErrorMessage(response: Response): Promise<string> {
  const contentType = response.headers.get("content-type") ?? "";
  if (contentType.includes("application/json")) {
    const payload = (await response.json()) as { detail?: string };
    return payload.detail || `Request failed: ${response.status}`;
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
    return request<RunDetailView>(`/api/runs/${runId}`);
  },
  backtests(params: URLSearchParams) {
    return request<BacktestsResponse>(`/api/backtests?${params.toString()}`);
  },
  backtest(backtestId: string) {
    return request<BacktestReportView>(`/api/backtests/${backtestId}`);
  },
  benchmarks() {
    return request<BenchmarkListItemView[]>("/api/benchmarks");
  },
  benchmark(name: string) {
    return request<BenchmarkDetailView>(`/api/benchmarks/${name}`);
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
  }) {
    return request<ModelComparisonView>("/api/comparisons/models", {
      method: "POST",
      body: JSON.stringify(body),
    });
  },
  trainOptions() {
    return request<TrainLaunchOptionsView>("/api/launch/train/options");
  },
  backtestOptions() {
    return request<BacktestLaunchOptionsView>("/api/launch/backtest/options");
  },
  launchTrain(body: LaunchTrainRequest) {
    return request<LaunchJobResponse>("/api/launch/train", {
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
    return request<JobStatusView>(`/api/jobs/${jobId}`);
  },
  previewArtifact(uri: string) {
    return request<ArtifactPreviewResponse>(
      `/api/artifacts/preview?uri=${encodeURIComponent(uri)}`,
    );
  },
};
