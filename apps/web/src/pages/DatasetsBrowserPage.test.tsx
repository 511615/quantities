import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { DatasetsBrowserPage } from "./DatasetsBrowserPage";
import {
  datasetDependenciesFixture,
  datasetRequestOptionsFixture,
  datasetsFixture,
} from "../test/fixtures";
import { createFetchMock, jsonResponse } from "../test/mockApi";
import { renderWithProviders } from "../test/renderWithProviders";

const fetchMock = vi.fn(
  createFetchMock([
    (url, init) =>
      url.includes("/api/datasets?page=1&per_page=100")
        ? jsonResponse(datasetsFixture)
        : url.endsWith("/api/datasets/request-options")
          ? jsonResponse(datasetRequestOptionsFixture)
          : url.endsWith("/api/datasets/smoke_dataset/dependencies")
            ? jsonResponse(datasetDependenciesFixture)
            : url.endsWith("/api/datasets/smoke_dataset") && init?.method === "DELETE"
              ? jsonResponse({
                  dataset_id: "smoke_dataset",
                  status: "blocked",
                  message: "Dataset cannot be deleted because dependent resources still reference it.",
                  blocking_items: datasetDependenciesFixture.blocking_items,
                  deleted_files: [],
                })
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

test("renders browser results and shows blocked delete dependencies", async () => {
  renderWithProviders(<DatasetsBrowserPage />, "/datasets/browser?data_domain=market");

  await waitFor(() =>
    expect(screen.getByRole("heading", { name: "按数据域、来源与版本浏览" })).toBeInTheDocument(),
  );

  expect(screen.getAllByText("Smoke Dataset").length).toBeGreaterThan(0);
  expect(screen.queryByText("Macro Liquidity Snapshot")).not.toBeInTheDocument();
  expect(screen.getByText(/当前结果/)).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "申请新数据集" })).toBeInTheDocument();
  expect(screen.getByRole("heading", { name: "当前可用数据集" })).toBeInTheDocument();
  expect(screen.getByRole("heading", { name: "先浏览，再决定是否训练" })).toBeInTheDocument();

  fireEvent.click(screen.getByRole("button", { name: "申请新数据集" }));
  await waitFor(() => expect(screen.getAllByText("申请新数据集").length).toBeGreaterThan(1));

  fireEvent.click(screen.getAllByRole("button", { name: "删除" })[0]);

  await waitFor(() => expect(screen.getByText("删除数据集")).toBeInTheDocument());
  const dialog = screen.getByRole("dialog");
  expect(within(dialog).getByText("删除范围")).toBeInTheDocument();
  expect(within(dialog).getByText("正在扫描依赖关系图")).toBeInTheDocument();
  expect(within(dialog).getByText("当前已启用硬删除")).toBeInTheDocument();
});
