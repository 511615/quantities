import { screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { HomePage } from "./HomePage";
import { overviewFixture } from "../test/fixtures";
import { createFetchMock, jsonResponse } from "../test/mockApi";
import { renderWithProviders } from "../test/renderWithProviders";

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
});

afterEach(() => {
  vi.unstubAllGlobals();
  fetchMock.mockClear();
});

test("renders overview sections and recent entities", async () => {
  renderWithProviders(<HomePage />);

  await waitFor(() =>
    expect(screen.getByRole("heading", { name: "\u6700\u8fd1\u6d3b\u52a8" })).toBeInTheDocument(),
  );
  expect(screen.getAllByText("smoke-train-run").length).toBeGreaterThan(0);
  expect(screen.getByRole("heading", { name: "\u57fa\u51c6\u5feb\u7167" })).toBeInTheDocument();
  expect(
    screen.getByRole("link", { name: "baseline_family_walk_forward" }),
  ).toBeInTheDocument();
  expect(screen.getByRole("heading", { name: "\u5feb\u901f\u5165\u53e3" })).toBeInTheDocument();
  expect(
    screen.getByRole("link", { name: /\u6570\u636e\u96c6.*smoke_dataset.*\u65b0\u9c9c/i }),
  ).toBeInTheDocument();
});
