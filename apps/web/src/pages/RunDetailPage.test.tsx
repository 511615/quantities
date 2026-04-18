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
            dataset_presets: [
              { value: "smoke", label: "Smoke", description: null, recommended: true },
            ],
            prediction_scopes: [
              { value: "full", label: "full", description: null, recommended: true },
            ],
            strategy_presets: [
              { value: "sign", label: "sign", description: null, recommended: true },
            ],
            portfolio_presets: [
              {
                value: "research_default",
                label: "default",
                description: null,
                recommended: true,
              },
            ],
            cost_presets: [
              { value: "standard", label: "standard", description: null, recommended: true },
            ],
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

  await waitFor(() =>
    expect(screen.getByRole("heading", { name: "smoke-train-run" })).toBeInTheDocument(),
  );
  expect(screen.getAllByTestId("workbench-chart").length).toBeGreaterThan(0);
  expect(screen.getByRole("link", { name: "smoke_dataset" })).toHaveAttribute(
    "href",
    "/datasets/smoke_dataset",
  );
  expect(screen.getByRole("link", { name: "macro_liquidity_snapshot" })).toHaveAttribute(
    "href",
    "/datasets/macro_liquidity_snapshot",
  );
  expect(screen.getAllByText("市场").length).toBeGreaterThan(0);
  expect(screen.getByText(/lag_return_1, lag_return_2, volume_zscore/)).toBeInTheDocument();
  expect(screen.getAllByText("可训练").length).toBeGreaterThan(0);

  fireEvent.click(
    screen.getByRole("button", {
      name: /evaluation_summary\.json/,
    }),
  );

  await waitFor(() =>
    expect(screen.getByText(/"selected_scope": "test"/)).toBeInTheDocument(),
  );
});
