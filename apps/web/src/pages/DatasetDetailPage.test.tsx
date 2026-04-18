import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { DatasetDetailPage } from "./DatasetDetailPage";
import {
  datasetDeleteFixture,
  datasetDependenciesFixture,
  datasetDetailFixture,
  datasetNlpInspectionFixture,
  datasetOhlcvFixture,
  datasetReadinessFixture,
} from "../test/fixtures";
import type { DatasetDependenciesResponse } from "../shared/api/types";
import { createFetchMock, jsonResponse } from "../test/mockApi";

vi.mock("../features/dataset-browser/DatasetCandlestickChart", () => ({
  DatasetCandlestickChart: () => <div data-testid="dataset-candles-chart">chart</div>,
}));

vi.mock("../shared/ui/WorkbenchChart", () => ({
  WorkbenchChart: ({ loadingLabel }: { loadingLabel?: string }) => (
    <div data-testid="workbench-chart">{loadingLabel ?? "chart"}</div>
  ),
}));

let datasetDependenciesResponse: DatasetDependenciesResponse = {
  ...datasetDependenciesFixture,
  can_delete: true,
  blocking_items: [],
  items: [],
};
let datasetDetailResponse = datasetDetailFixture;
let datasetReadinessResponse = datasetReadinessFixture;

const fetchMock = vi.fn(
  createFetchMock([
    (url, init) =>
      url.endsWith("/api/datasets/smoke_dataset") && init?.method === "DELETE"
        ? jsonResponse(datasetDeleteFixture)
        : undefined,
    (url, init) =>
      url.endsWith("/api/datasets/smoke_dataset") && (!init?.method || init.method === "GET")
        ? jsonResponse(datasetDetailResponse)
        : undefined,
    (url) =>
      url.endsWith("/api/datasets/smoke_dataset/readiness")
        ? jsonResponse(datasetReadinessResponse)
        : undefined,
    (url) =>
      url.endsWith("/api/datasets/smoke_dataset/nlp-inspection")
        ? jsonResponse(datasetNlpInspectionFixture)
        : undefined,
    (url) =>
      url.endsWith("/api/datasets/smoke_dataset/dependencies")
        ? jsonResponse(datasetDependenciesResponse)
        : undefined,
    (url) =>
      url.includes("/api/datasets/smoke_dataset/ohlcv")
        ? jsonResponse(datasetOhlcvFixture)
        : undefined,
    (url) =>
      url.endsWith("/api/launch/train/options")
        ? jsonResponse({
            dataset_presets: [],
            model_options: [],
            template_options: [
              {
                value: "registry::elastic_net",
                label: "Elastic Net default",
                description: "Template sourced from model registry.",
                recommended: true,
              },
            ],
            trainer_presets: [{ value: "fast", label: "Fast", description: null, recommended: true }],
            feature_scope_modalities: [
              { value: "market", label: "Market", description: null, recommended: true },
              { value: "macro", label: "Macro", description: null, recommended: false },
            ],
            default_seed: 7,
            constraints: {},
          })
        : undefined,
    (url) =>
      url.endsWith("/api/launch/backtest/options")
        ? jsonResponse({
            official_template_id: "system::official_backtest_protocol_v1",
            official_window_options: [
              { value: "30", label: "Recent 30d", description: null, recommended: true },
              { value: "180", label: "Recent 180d", description: null, recommended: false },
            ],
            dataset_presets: [],
            prediction_scopes: [{ value: "test", label: "test", description: null, recommended: true }],
            strategy_presets: [{ value: "sign", label: "sign", description: null, recommended: true }],
            portfolio_presets: [
              { value: "research_default", label: "research_default", description: null, recommended: true },
            ],
            cost_presets: [{ value: "standard", label: "standard", description: null, recommended: true }],
            research_backends: [{ value: "native", label: "native", description: null, recommended: true }],
            portfolio_methods: [{ value: "proportional", label: "proportional", description: null, recommended: true }],
            default_benchmark_symbol: "BTCUSDT",
            constraints: {},
          })
        : undefined,
    (url, init) =>
      url.endsWith("/api/launch/dataset-multimodal-train") && init?.method === "POST"
        ? jsonResponse({
            job_id: "job-dataset-multimodal-loop",
            status: "queued",
            job_api_path: "/api/jobs/job-dataset-multimodal-loop",
            tracking_token: "job-dataset-multimodal-loop",
            submitted_at: "2026-04-18T00:00:00Z",
          })
        : undefined,
    (url) =>
      url.endsWith("/api/jobs/job-dataset-multimodal-loop")
        ? jsonResponse({
            job_id: "job-dataset-multimodal-loop",
            job_type: "dataset_multimodal_train",
            status: "success",
            created_at: "2026-04-18T00:00:00Z",
            updated_at: "2026-04-18T00:00:04Z",
            stages: [
              {
                name: "train_market",
                status: "success",
                summary: "Completed market training",
                started_at: "2026-04-18T00:00:00Z",
                finished_at: "2026-04-18T00:00:01Z",
              },
              {
                name: "compose",
                status: "success",
                summary: "Composed run is ready",
                started_at: "2026-04-18T00:00:01Z",
                finished_at: "2026-04-18T00:00:02Z",
              },
              {
                name: "backtest",
                status: "success",
                summary: "Completed 1 official backtest run",
                started_at: "2026-04-18T00:00:02Z",
                finished_at: "2026-04-18T00:00:04Z",
              },
            ],
            result: {
              dataset_id: "smoke_dataset",
              dataset_ids: ["smoke_dataset", "baseline_real_benchmark_dataset"],
              run_ids: ["multimodal-compose-123", "market-run-1", "macro-run-1"],
              backtest_ids: ["backtest-123"],
              benchmark_names: [],
              fit_result_uris: [],
              summary_artifacts: [],
              deeplinks: {
                dataset_detail: "/datasets/smoke_dataset",
                run_detail: "/runs/multimodal-compose-123",
                backtest_detail: "/backtests/backtest-123",
                review_detail: null,
              },
              result_links: [],
              summary: null,
              pipeline_summary: null,
            },
            error_message: null,
          })
        : undefined,
  ]),
);

beforeEach(() => {
  datasetDependenciesResponse = {
    ...datasetDependenciesFixture,
    can_delete: true,
    blocking_items: [],
    items: [],
  };
  datasetDetailResponse = datasetDetailFixture;
  datasetReadinessResponse = datasetReadinessFixture;
  vi.stubGlobal("fetch", fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
  fetchMock.mockClear();
});

function renderPage() {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
        refetchOnWindowFocus: false,
      },
    },
  });

  render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={["/datasets/smoke_dataset?range_preset=30d"]}>
        <Routes>
          <Route element={<DatasetDetailPage />} path="/datasets/:datasetId" />
          <Route element={<div>browser-page</div>} path="/datasets/browser" />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

test("renders dataset detail with modality quality and official NLP gate details", async () => {
  renderPage();

  await waitFor(() =>
    expect(screen.getAllByRole("heading", { name: "Smoke Dataset" }).length).toBeGreaterThan(0),
  );
  await waitFor(() => expect(screen.getByTestId("dataset-candles-chart")).toBeInTheDocument());

  expect(screen.getByText("Modality Quality Summary")).toBeInTheDocument();
  expect(screen.getByText("Aligned Multimodal Window")).toBeInTheDocument();
  expect(screen.getByText("NLP 质量门禁")).toBeInTheDocument();
  expect(screen.getByText("实际市场窗口")).toBeInTheDocument();
  expect(screen.getByText("官方测试窗口")).toBeInTheDocument();
  expect(screen.getByText("实际文本信号窗口")).toBeInTheDocument();
  expect(screen.getAllByText(/NLP/i).length).toBeGreaterThan(0);
  expect(screen.getByRole("link", { name: "获取该数据集" })).toHaveAttribute(
    "href",
    "/api/datasets/smoke_dataset/download",
  );
});

test("opens delete dialog and confirms deletable datasets", async () => {
  renderPage();

  await waitFor(() =>
    expect(screen.getAllByRole("heading", { name: "Smoke Dataset" }).length).toBeGreaterThan(0),
  );

  fireEvent.click(screen.getByRole("button", { name: /删除数据集/i }));

  const dialog = await screen.findByRole("dialog");
  expect(within(dialog).getByText(/当前已启用硬删除/i)).toBeInTheDocument();
  await waitFor(() =>
    expect(within(dialog).getByRole("button", { name: /硬删除数据集/i })).toBeEnabled(),
  );

  fireEvent.click(within(dialog).getByRole("button", { name: /硬删除数据集/i }));
  await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
});

test("shows clear reason when recommended dataset cannot be deleted", async () => {
  datasetDependenciesResponse = datasetDependenciesFixture;

  renderPage();

  await waitFor(() =>
    expect(screen.getAllByRole("heading", { name: "Smoke Dataset" }).length).toBeGreaterThan(0),
  );

  fireEvent.click(screen.getByRole("button", { name: /删除数据集/i }));

  const dialog = await screen.findByRole("dialog");
  await waitFor(() => {
    expect(within(dialog).getByText("当前不能删除的原因")).toBeInTheDocument();
    expect(
      within(dialog).getByText("该数据集属于系统推荐集，前端不允许发起删除。"),
    ).toBeInTheDocument();
  });
  await waitFor(() =>
    expect(within(dialog).getByRole("button", { name: /当前不可删除/i })).toBeDisabled(),
  );
});
