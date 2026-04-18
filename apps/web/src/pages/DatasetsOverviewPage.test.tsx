import { screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { DatasetsOverviewPage } from "./DatasetsOverviewPage";
import { datasetsFixture, jobsFixture } from "../test/fixtures";
import { createFetchMock, jsonResponse } from "../test/mockApi";
import { renderWithProviders } from "../test/renderWithProviders";

const fetchMock = vi.fn(
  createFetchMock([
    (url) =>
      url.includes("/api/datasets?page=1&per_page=100")
        ? jsonResponse(datasetsFixture)
        : url.endsWith("/api/jobs")
          ? jsonResponse(jobsFixture)
        : undefined,
  ]),
);

beforeEach(() => {
  vi.stubGlobal("fetch", fetchMock);
  window.localStorage.clear();
});

afterEach(() => {
  vi.unstubAllGlobals();
  fetchMock.mockClear();
  window.localStorage.clear();
});

test("renders dataset overview with domain-first information architecture", async () => {
  renderWithProviders(<DatasetsOverviewPage />, "/datasets");

  await waitFor(() =>
    expect(screen.getByRole("heading", { name: "数据集总览" })).toBeInTheDocument(),
  );

  expect(screen.getByText("按数据域进入")).toBeInTheDocument();
  expect(screen.getByRole("link", { name: "进入数据浏览器" })).toBeInTheDocument();
  expect(screen.getAllByRole("button", { name: "申请新数据集" }).length).toBeGreaterThan(0);
  expect(screen.getByText("市场数据")).toBeInTheDocument();
  expect(screen.getByText("宏观数据")).toBeInTheDocument();
  expect(screen.getByText("训练面板速览")).toBeInTheDocument();
  expect(screen.getByText("数据申请任务")).toBeInTheDocument();
  expect(screen.getByText("job-dataset-1")).toBeInTheDocument();
});

test("renders dataset overview in English when locale is en-US", async () => {
  window.localStorage.setItem("qp.ui.locale", "en-US");

  renderWithProviders(<DatasetsOverviewPage />, "/datasets");

  await waitFor(() =>
    expect(screen.getByRole("heading", { name: "Dataset Overview" })).toBeInTheDocument(),
  );

  expect(screen.getByText("Browse by Domain")).toBeInTheDocument();
  expect(screen.getByRole("link", { name: "Open Dataset Browser" })).toBeInTheDocument();
  expect(screen.getAllByRole("button", { name: "Request New Dataset" }).length).toBeGreaterThan(0);
  expect(screen.getByText("Market Data")).toBeInTheDocument();
  expect(screen.getByText("Macro Data")).toBeInTheDocument();
  expect(screen.getByText("Training Panel Snapshot")).toBeInTheDocument();
  expect(screen.getByText("Dataset Request Jobs")).toBeInTheDocument();
});
