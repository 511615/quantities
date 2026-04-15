import { screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { TrainingDatasetsPage } from "./TrainingDatasetsPage";
import { trainingDatasetsFixture } from "../test/fixtures";
import { createFetchMock, jsonResponse } from "../test/mockApi";
import { renderWithProviders } from "../test/renderWithProviders";

const fetchMock = vi.fn(
  createFetchMock([
    (url) => (url.endsWith("/api/datasets/training") ? jsonResponse(trainingDatasetsFixture) : undefined),
  ]),
);

beforeEach(() => {
  vi.stubGlobal("fetch", fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
  fetchMock.mockClear();
});

test("shows train CTA for ready and warning datasets but not for blocked datasets", async () => {
  renderWithProviders(<TrainingDatasetsPage />, "/datasets/training");

  await waitFor(() => expect(screen.getByRole("heading", { name: "训练面板总览" })).toBeInTheDocument());

  const readyRow = screen.getByRole("link", { name: /Cross Asset Training Panel/i }).closest("tr");
  expect(readyRow).not.toBeNull();
  expect(within(readyRow as HTMLElement).getByRole("link", { name: "发起训练" })).toHaveAttribute(
    "href",
    "/models?launchTrain=1&datasetId=cross_asset_training_panel_v2",
  );

  const fusionRow = screen.getByRole("link", { name: /Multi-domain Fusion Training Panel/i }).closest("tr");
  expect(fusionRow).not.toBeNull();
  expect(within(fusionRow as HTMLElement).getByRole("link", { name: "发起训练" })).toHaveAttribute(
    "href",
    "/models?launchTrain=1&datasetId=market_macro_onchain_fusion_v1",
  );

  const warningRow = screen.getByRole("link", { name: /Smoke Training Sample/i }).closest("tr");
  expect(warningRow).not.toBeNull();
  expect(within(warningRow as HTMLElement).getByRole("link", { name: "发起训练" })).toHaveAttribute(
    "href",
    "/models?launchTrain=1&datasetId=smoke_dataset",
  );

  const blockedItem = screen.getByRole("link", { name: /Macro Liquidity Snapshot/i }).closest(".stack-item");
  expect(blockedItem).not.toBeNull();
  expect(within(blockedItem as HTMLElement).queryByRole("link", { name: "发起训练" })).not.toBeInTheDocument();
  expect(screen.getByRole("heading", { name: "当前不建议直接推进的面板" })).toBeInTheDocument();
});
