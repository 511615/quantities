import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { api } from "./client";
import type {
  DatasetAcquisitionRequest,
  DatasetFusionRequest,
  DatasetNlpInspectionView,
  DatasetPipelineRequest,
} from "./types";

export function useWorkbenchOverview() {
  return useQuery({
    queryKey: ["workbench-overview"],
    queryFn: () => api.overview(),
  });
}

export function useExperiments(params: URLSearchParams) {
  return useQuery({
    queryKey: ["experiments", params.toString()],
    queryFn: () => api.experiments(params),
  });
}

export function useRuns(params: URLSearchParams) {
  return useQuery({
    queryKey: ["runs", params.toString()],
    queryFn: () => api.runs(params),
  });
}

export function useRunDetail(runId: string) {
  return useQuery({
    queryKey: ["run", runId],
    queryFn: () => api.run(runId),
    enabled: Boolean(runId),
  });
}

export function useBacktests(params: URLSearchParams) {
  return useQuery({
    queryKey: ["backtests", params.toString()],
    queryFn: () => api.backtests(params),
  });
}

export function useBacktestDetail(backtestId: string) {
  return useQuery({
    queryKey: ["backtest", backtestId],
    queryFn: () => api.backtest(backtestId),
    enabled: Boolean(backtestId),
  });
}

export function useDeleteBacktestMutation() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (backtestId: string) => api.deleteBacktest(backtestId),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["backtests"] }),
        queryClient.invalidateQueries({ queryKey: ["backtest"] }),
        queryClient.invalidateQueries({ queryKey: ["run"] }),
        queryClient.invalidateQueries({ queryKey: ["jobs"] }),
        queryClient.invalidateQueries({ queryKey: ["workbench-overview"] }),
      ]);
    },
  });
}

export function useBenchmarks() {
  return useQuery({
    queryKey: ["benchmarks"],
    queryFn: () => api.benchmarks(),
  });
}

export function useBenchmarkDetail(name: string) {
  return useQuery({
    queryKey: ["benchmark", name],
    queryFn: () => api.benchmark(name),
    enabled: Boolean(name),
  });
}

export function useDatasets(page = 1, perPage = 20, enabled = true) {
  return useQuery({
    queryKey: ["datasets", page, perPage],
    queryFn: () => api.datasets(page, perPage),
    enabled,
  });
}

export function useDatasetRequestOptions(enabled = true) {
  return useQuery({
    queryKey: ["datasets", "request-options"],
    queryFn: () => api.datasetRequestOptions(),
    enabled,
  });
}

export function useTrainingDatasets(enabled = true) {
  return useQuery({
    queryKey: ["datasets", "training"],
    queryFn: () => api.trainingDatasets(),
    enabled,
  });
}

export function useDatasetDetail(datasetId: string | null) {
  return useQuery({
    queryKey: ["dataset", datasetId],
    queryFn: () => api.dataset(datasetId ?? ""),
    enabled: Boolean(datasetId),
  });
}

export function useDatasetDependencies(datasetId: string | null, enabled = true) {
  return useQuery({
    queryKey: ["dataset", datasetId, "dependencies"],
    queryFn: () => api.datasetDependencies(datasetId ?? ""),
    enabled: enabled && Boolean(datasetId),
  });
}

export function useDatasetReadiness(datasetId: string | null, enabled = true) {
  return useQuery({
    queryKey: ["dataset", datasetId, "readiness"],
    queryFn: () => api.datasetReadiness(datasetId ?? ""),
    enabled: enabled && Boolean(datasetId),
  });
}

export function useDatasetNlpInspection(datasetId: string | null, enabled = true) {
  return useQuery({
    queryKey: ["dataset", datasetId, "nlp-inspection"],
    queryFn: () => api.datasetNlpInspection(datasetId ?? ""),
    enabled: enabled && Boolean(datasetId),
  });
}

export function useDatasetOhlcv(
  datasetId: string | null,
  params: {
    page?: number;
    per_page?: number;
    start_time?: string | null;
    end_time?: string | null;
  },
) {
  return useQuery({
    queryKey: ["dataset-ohlcv", datasetId, params],
    queryFn: () => api.datasetOhlcv(datasetId ?? "", params),
    enabled: Boolean(datasetId),
  });
}

export function useComparison(
  query: {
    runIds: string[];
    benchmarkSelections: Array<{ benchmark_name: string; model_names: string[] }>;
    templateId?: string;
    officialOnly?: boolean;
  },
) {
  return useQuery({
    queryKey: [
      "comparison",
      query.runIds,
      query.benchmarkSelections,
      query.templateId ?? null,
      query.officialOnly ?? false,
    ],
    queryFn: () =>
      api.compare({
        run_ids: query.runIds,
        benchmark_selections: query.benchmarkSelections,
        template_id: query.templateId,
        official_only: query.officialOnly,
      }),
    enabled: query.runIds.length > 0 || query.benchmarkSelections.length > 0,
  });
}

export function useTrainOptions() {
  return useQuery({
    queryKey: ["launch-options", "train"],
    queryFn: () => api.trainOptions(),
  });
}

export function useModelTemplates(includeDeleted = false) {
  return useQuery({
    queryKey: ["model-templates", includeDeleted],
    queryFn: () => api.modelTemplates(includeDeleted),
  });
}

export function useCreateModelTemplateMutation() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (body: Parameters<typeof api.createModelTemplate>[0]) => api.createModelTemplate(body),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["model-templates"] }),
        queryClient.invalidateQueries({ queryKey: ["launch-options", "train"] }),
      ]);
    },
  });
}

export function useUpdateModelTemplateMutation() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      templateId,
      body,
    }: {
      templateId: string;
      body: Parameters<typeof api.updateModelTemplate>[1];
    }) => api.updateModelTemplate(templateId, body),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["model-templates"] }),
        queryClient.invalidateQueries({ queryKey: ["launch-options", "train"] }),
      ]);
    },
  });
}

export function useDeleteModelTemplateMutation() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (templateId: string) => api.deleteModelTemplate(templateId),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["model-templates"] }),
        queryClient.invalidateQueries({ queryKey: ["launch-options", "train"] }),
      ]);
    },
  });
}

export function useBacktestOptions() {
  return useQuery({
    queryKey: ["launch-options", "backtest"],
    queryFn: () => api.backtestOptions(),
  });
}

export function useJobs() {
  return useQuery({
    queryKey: ["jobs"],
    queryFn: () => api.jobs(),
    refetchInterval: 4000,
  });
}

export function useJobStatus(jobId: string | null) {
  return useQuery({
    queryKey: ["job", jobId],
    queryFn: () => api.job(jobId ?? ""),
    enabled: Boolean(jobId),
    refetchInterval: (query) => {
      const status = query.state.data?.status;
      return status === "success" || status === "failed" ? false : 1500;
    },
  });
}

export function useArtifactPreview(uri: string | null) {
  return useQuery({
    queryKey: ["artifact-preview", uri],
    queryFn: () => api.previewArtifact(uri ?? ""),
    enabled: Boolean(uri),
  });
}

export function useRequestDatasetMutation() {
  return useMutation({
    mutationFn: (body: DatasetAcquisitionRequest) => api.requestDataset(body),
  });
}

export function useRequestDatasetPipelineMutation() {
  return useMutation({
    mutationFn: (body: DatasetPipelineRequest) => api.createDatasetPipeline(body),
  });
}

export function useCreateDatasetFusionMutation() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (body: DatasetFusionRequest) => api.createDatasetFusion(body),
    onSuccess: async (result) => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["datasets"] }),
        queryClient.invalidateQueries({ queryKey: ["datasets", "training"] }),
        queryClient.invalidateQueries({ queryKey: ["dataset", result.dataset_id] }),
        queryClient.invalidateQueries({ queryKey: ["dataset", result.dataset_id, "series"] }),
        queryClient.invalidateQueries({ queryKey: ["workbench-overview"] }),
      ]);
    },
  });
}

export function useDeleteDatasetMutation() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (datasetId: string) => api.deleteDataset(datasetId),
    onSuccess: async (result) => {
      if (result.status !== "deleted") {
        return;
      }
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["datasets"] }),
        queryClient.invalidateQueries({ queryKey: ["datasets", "training"] }),
        queryClient.invalidateQueries({ queryKey: ["workbench-overview"] }),
      ]);
    },
  });
}
