import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

import { DatasetDetailPage } from "./DatasetDetailPage";
import {
  datasetDeleteFixture,
  datasetDependenciesFixture,
  datasetDetailFixture,
  datasetOhlcvFixture,
  datasetReadinessFixture,
} from "../test/fixtures";
import { createFetchMock, jsonResponse } from "../test/mockApi";

vi.mock("../features/dataset-browser/DatasetCandlestickChart", () => ({
  DatasetCandlestickChart: () => <div data-testid="dataset-candles-chart">chart</div>,
}));

const fetchMock = vi.fn(
  createFetchMock([
    (url, init) =>
      url.endsWith("/api/datasets/smoke_dataset") && init?.method === "DELETE"
        ? jsonResponse(datasetDeleteFixture)
        : undefined,
    (url, init) =>
      url.endsWith("/api/datasets/smoke_dataset")
        && (!init?.method || init.method === "GET")
        ? jsonResponse(datasetDetailFixture)
        : undefined,
    (url) =>
      url.endsWith("/api/datasets/smoke_dataset/readiness")
        ? jsonResponse(datasetReadinessFixture)
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

test("renders dataset detail and exposes delete action in hero area", async () => {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
        refetchOnWindowFocus: false,
      },
    },
  });

  const view = (
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={["/datasets/smoke_dataset?range_preset=30d"]}>
        <Routes>
          <Route element={<DatasetDetailPage />} path="/datasets/:datasetId" />
          <Route element={<div>browser-page</div>} path="/datasets/browser" />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>
  );

  render(view);

  await waitFor(() =>
    expect(screen.getByRole("heading", { name: "Smoke Dataset" })).toBeInTheDocument(),
  );
  await waitFor(() => expect(screen.getByTestId("dataset-candles-chart")).toBeInTheDocument());
  expect(screen.getAllByRole("button", { name: "关于 数据域" }).length).toBeGreaterThan(0);
  expect(screen.getAllByRole("button", { name: "关于 快照版本" }).length).toBeGreaterThan(0);

  fireEvent.click(screen.getByRole("button", { name: "删除数据集" }));

  const dialog = await screen.findByRole("dialog");
  expect(within(dialog).getByText("当前允许删除")).toBeInTheDocument();
  await waitFor(() =>
    expect(within(dialog).getByRole("button", { name: "确认永久删除" })).toBeEnabled(),
  );

  fireEvent.click(within(dialog).getByRole("button", { name: "确认永久删除" }));

  await waitFor(() =>
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument(),
  );
});
