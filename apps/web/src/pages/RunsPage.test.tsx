import { screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { RunsPage } from "./RunsPage";
import { experimentsFixture } from "../test/fixtures";
import { createFetchMock, jsonResponse } from "../test/mockApi";
import { renderWithProviders } from "../test/renderWithProviders";

const fetchMock = vi.fn(
  createFetchMock([
    (url) => (url.includes("/api/runs?") ? jsonResponse(experimentsFixture) : undefined),
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
});

afterEach(() => {
  vi.unstubAllGlobals();
  fetchMock.mockClear();
});

test("renders runs page list", async () => {
  renderWithProviders(<RunsPage />);
  await waitFor(() => expect(screen.getByText("\u8fd0\u884c\u4e0e\u5b9e\u9a8c\u6d4f\u89c8")).toBeInTheDocument());
  await waitFor(() => expect(screen.getByText("smoke-train-run")).toBeInTheDocument());
});
