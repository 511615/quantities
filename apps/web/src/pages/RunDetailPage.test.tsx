import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { App } from "../app/App";
import { artifactPreviewFixture, runDetailFixture } from "../test/fixtures";
import { createFetchMock, jsonResponse } from "../test/mockApi";

vi.mock("../shared/ui/WorkbenchChart", () => ({
  WorkbenchChart: ({ loadingLabel }: { loadingLabel?: string }) => (
    <div data-testid="workbench-chart">{loadingLabel ?? "chart"}</div>
  ),
}));

const fetchMock = vi.fn(
  createFetchMock([
    (url) =>
      url.endsWith("/api/runs/smoke-train-run") ? jsonResponse(runDetailFixture) : undefined,
    (url) =>
      url.includes("/api/artifacts/preview") ? jsonResponse(artifactPreviewFixture) : undefined,
    (url) =>
      url.includes("/api/launch/backtest/options")
        ? jsonResponse({
            dataset_presets: [{ value: "smoke", label: "Smoke", description: null, recommended: true }],
            prediction_scopes: [{ value: "full", label: "full", description: null, recommended: true }],
            strategy_presets: [{ value: "sign", label: "sign", description: null, recommended: true }],
            portfolio_presets: [{ value: "research_default", label: "default", description: null, recommended: true }],
            cost_presets: [{ value: "standard", label: "standard", description: null, recommended: true }],
            default_benchmark_symbol: "BTCUSDT",
            constraints: {},
          })
        : undefined,
  ]),
);

beforeEach(() => {
  vi.stubGlobal("fetch", fetchMock);
  window.history.pushState({}, "", "/runs/smoke-train-run");
});

afterEach(() => {
  vi.unstubAllGlobals();
  fetchMock.mockClear();
});

test("renders run detail and previews artifacts", async () => {
  render(<App />);

  await waitFor(() => expect(screen.getByText("回归指标总览")).toBeInTheDocument());
  expect(screen.getByText("特征重要性")).toBeInTheDocument();
  expect(screen.getByText("预测 vs 真实")).toBeInTheDocument();
  expect(screen.getAllByTestId("workbench-chart").length).toBeGreaterThan(0);
  expect(screen.getAllByText("\u56de\u6d4b\u5206\u6790").length).toBeGreaterThan(0);
  fireEvent.click(
    screen.getByRole("button", {
      name: /评估摘要 .*evaluation_summary\.json/,
    }),
  );
  await waitFor(() =>
    expect(screen.getByText(/"selected_scope": "test"/)).toBeInTheDocument(),
  );
});
