import { render, screen } from "@testing-library/react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { App } from "./App";
import { overviewFixture } from "../test/fixtures";
import { createFetchMock, jsonResponse } from "../test/mockApi";

const fetchMock = vi.fn(
  createFetchMock([
    (url) =>
      url.includes("/api/workbench/overview") ? jsonResponse(overviewFixture) : undefined,
    (url) =>
      url.includes("/api/launch/train/options")
        ? jsonResponse({
            dataset_presets: [{ value: "smoke", label: "Smoke", description: null, recommended: true }],
            model_options: [{ value: "elastic_net", label: "elastic_net", description: null, recommended: true }],
            trainer_presets: [{ value: "fast", label: "fast", description: null, recommended: true }],
            default_seed: 7,
            constraints: {},
          })
        : undefined,
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
  window.history.pushState({}, "", "/");
});

afterEach(() => {
  vi.unstubAllGlobals();
  fetchMock.mockClear();
});

test("renders app shell and navigation in Chinese", async () => {
  render(<App />);

  expect(await screen.findByText("\u91cf\u5316\u7814\u7a76\u5de5\u4f5c\u53f0")).toBeInTheDocument();
  expect(screen.getByText("\u5de5\u4f5c\u53f0")).toBeInTheDocument();
  expect(screen.getByText("\u6a21\u578b\u7ba1\u7406")).toBeInTheDocument();
  expect(screen.getByText("\u6570\u636e\u96c6")).toBeInTheDocument();
  expect(screen.getByText("\u56de\u6d4b\u5206\u6790")).toBeInTheDocument();
  expect(screen.getByText("\u57fa\u51c6\u5bf9\u6bd4")).toBeInTheDocument();
  expect(screen.getByText("\u4efb\u52a1\u4e2d\u5fc3")).toBeInTheDocument();
});
