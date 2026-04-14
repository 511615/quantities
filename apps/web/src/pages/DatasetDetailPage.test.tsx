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
import { createFetchMock, jsonResponse } from "../test/mockApi";

vi.mock("../features/dataset-browser/DatasetCandlestickChart", () => ({
  DatasetCandlestickChart: () => <div data-testid="dataset-candles-chart">chart</div>,
}));

vi.mock("../shared/ui/WorkbenchChart", () => ({
  WorkbenchChart: ({ loadingLabel }: { loadingLabel?: string }) => (
    <div data-testid="workbench-chart">{loadingLabel ?? "chart"}</div>
  ),
}));

const fetchMock = vi.fn(
  createFetchMock([
    (url, init) =>
      url.endsWith("/api/datasets/smoke_dataset") && init?.method === "DELETE"
        ? jsonResponse(datasetDeleteFixture)
        : undefined,
    (url, init) =>
      url.endsWith("/api/datasets/smoke_dataset") && (!init?.method || init.method === "GET")
        ? jsonResponse(datasetDetailFixture)
        : undefined,
    (url) =>
      url.endsWith("/api/datasets/smoke_dataset/readiness")
        ? jsonResponse(datasetReadinessFixture)
        : undefined,
    (url) =>
      url.endsWith("/api/datasets/smoke_dataset/nlp-inspection")
        ? jsonResponse(datasetNlpInspectionFixture)
        : undefined,
    (url) =>
      url.endsWith("/api/datasets/smoke_dataset/dependencies")
        ? jsonResponse({
            ...datasetDependenciesFixture,
            can_delete: true,
            blocking_items: [],
            items: [],
          })
        : undefined,
    (url) =>
      url.includes("/api/datasets/smoke_dataset/ohlcv")
        ? jsonResponse(datasetOhlcvFixture)
        : undefined,
  ]),
);

beforeEach(() => {
  vi.stubGlobal("fetch", fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
  fetchMock.mockClear();
});

test("renders dataset detail and exposes official NLP gate details", async () => {
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

  await waitFor(() =>
    expect(screen.getByRole("heading", { name: "Smoke Dataset" })).toBeInTheDocument(),
  );
  await waitFor(() => expect(screen.getByTestId("dataset-candles-chart")).toBeInTheDocument());

  expect(screen.getByRole("heading", { name: "NLP 质量门禁" })).toBeInTheDocument();
  expect(screen.getByText("实际市场窗口")).toBeInTheDocument();
  expect(screen.getByText("官方测试窗口")).toBeInTheDocument();
  expect(screen.getByText("实际文本信号窗口")).toBeInTheDocument();
  expect(screen.getAllByText(/仅归档型 NLP 数据源/i).length).toBeGreaterThan(0);
  expect(screen.getByRole("link", { name: "获取该数据集" })).toHaveAttribute(
    "href",
    "/api/datasets/smoke_dataset/download",
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
  fetchMock.mockImplementation(
    createFetchMock([
      (url, init) =>
        url.endsWith("/api/datasets/smoke_dataset") && init?.method === "DELETE"
          ? jsonResponse(datasetDeleteFixture)
          : undefined,
      (url, init) =>
        url.endsWith("/api/datasets/smoke_dataset") && (!init?.method || init.method === "GET")
          ? jsonResponse(datasetDetailFixture)
          : undefined,
      (url) =>
        url.endsWith("/api/datasets/smoke_dataset/readiness")
          ? jsonResponse(datasetReadinessFixture)
          : undefined,
      (url) =>
        url.endsWith("/api/datasets/smoke_dataset/nlp-inspection")
          ? jsonResponse(datasetNlpInspectionFixture)
          : undefined,
      (url) =>
        url.endsWith("/api/datasets/smoke_dataset/dependencies")
          ? jsonResponse(datasetDependenciesFixture)
          : undefined,
      (url) =>
        url.includes("/api/datasets/smoke_dataset/ohlcv")
          ? jsonResponse(datasetOhlcvFixture)
          : undefined,
    ]),
  );

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
      <MemoryRouter initialEntries={["/datasets/smoke_dataset"]}>
        <Routes>
          <Route element={<DatasetDetailPage />} path="/datasets/:datasetId" />
          <Route element={<div>browser-page</div>} path="/datasets/browser" />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );

  await waitFor(() =>
    expect(screen.getByRole("heading", { name: "Smoke Dataset" })).toBeInTheDocument(),
  );

  fireEvent.click(screen.getByRole("button", { name: /删除数据集/i }));

  const dialog = await screen.findByRole("dialog");
  expect(within(dialog).getByText("当前不能删除的原因")).toBeInTheDocument();
  expect(within(dialog).getByText("该数据集属于系统推荐集，前端不允许发起删除。")).toBeInTheDocument();
  expect(within(dialog).getByRole("button", { name: /当前不可删除/i })).toBeDisabled();
});
