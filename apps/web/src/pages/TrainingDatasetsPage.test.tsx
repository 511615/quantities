import { screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { TrainingDatasetsPage } from "./TrainingDatasetsPage";
import { trainingDatasetsFixture } from "../test/fixtures";
import { createFetchMock, jsonResponse } from "../test/mockApi";
import { renderWithProviders } from "../test/renderWithProviders";

const fetchMock = vi.fn(
  createFetchMock([
    (url) =>
      url.endsWith("/api/datasets/training")
        ? jsonResponse(trainingDatasetsFixture)
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

test("shows train CTA for ready and warning datasets but not for not_ready datasets", async () => {
  renderWithProviders(<TrainingDatasetsPage />, "/datasets/training");

  await waitFor(() =>
    expect(screen.getByRole("heading", { name: "训练面板汇总" })).toBeInTheDocument(),
  );

  const readyRow = screen
    .getByRole("link", { name: "跨资产收益训练面板 / Cross Asset Training Panel" })
    .closest("tr");
  expect(readyRow).not.toBeNull();
  expect(
    within(readyRow as HTMLElement).getByRole("link", { name: "发起训练" }),
  ).toHaveAttribute(
    "href",
    "/models?launchTrain=1&datasetId=cross_asset_training_panel_v2",
  );

  const fusionRow = screen
    .getByRole("link", { name: "多域融合训练面板 / Multi-domain Fusion Training Panel" })
    .closest("tr");
  expect(fusionRow).not.toBeNull();
  expect(within(fusionRow as HTMLElement).getAllByText("融合训练面板").length).toBeGreaterThan(0);
  expect(within(fusionRow as HTMLElement).getAllByText("市场数据").length).toBeGreaterThan(0);
  expect(
    within(fusionRow as HTMLElement).getByRole("link", { name: "发起训练" }),
  ).toHaveAttribute("href", "/models?launchTrain=1&datasetId=market_macro_onchain_fusion_v1");

  const warningRow = screen
    .getByRole("link", { name: "链路联调训练样本 / Smoke Training Sample" })
    .closest("tr");
  expect(warningRow).not.toBeNull();
  expect(
    within(warningRow as HTMLElement).getByRole("link", { name: "发起训练" }),
  ).toHaveAttribute("href", "/models?launchTrain=1&datasetId=smoke_dataset");

  const blockedItem = screen
    .getByRole("link", { name: "宏观流动性快照 / Macro Liquidity Snapshot" })
    .closest(".stack-item");
  expect(blockedItem).not.toBeNull();
  expect(
    within(blockedItem as HTMLElement).queryByRole("link", { name: "发起训练" }),
  ).not.toBeInTheDocument();
  expect(screen.getByText("当前被后端判定为暂不可训练的数据集")).toBeInTheDocument();
});
