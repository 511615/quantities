import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { App } from "../app/App";
import { artifactPreviewFixture, runDetailFixture } from "../test/fixtures";
import { createFetchMock, jsonResponse } from "../test/mockApi";

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

  await waitFor(() => expect(screen.getByText("\u6307\u6807\u8be6\u60c5")).toBeInTheDocument());
  expect(screen.getByText("\u7279\u5f81\u91cd\u8981\u6027")).toBeInTheDocument();
  expect(screen.getAllByText("\u56de\u6d4b\u5206\u6790").length).toBeGreaterThan(0);
  fireEvent.click(screen.getByText("\u8bad\u7ec3\u6e05\u5355"));
  await waitFor(() =>
    expect(screen.getByText(/"run_id": "smoke-train-run"/)).toBeInTheDocument(),
  );
});
