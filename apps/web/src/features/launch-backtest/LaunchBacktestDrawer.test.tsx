import { fireEvent, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { LaunchBacktestDrawer } from "./LaunchBacktestDrawer";
import { createFetchMock, jsonResponse } from "../../test/mockApi";
import { renderWithProviders } from "../../test/renderWithProviders";

const fetchMock = vi.fn(
  createFetchMock([
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
    (url, init) =>
      url.endsWith("/api/launch/backtest") && init?.method === "POST"
        ? jsonResponse({
            job_id: "job-backtest-1",
            status: "queued",
            tracking_token: "job:job-backtest-1",
            submitted_at: "2026-04-08T00:00:00Z",
          })
        : undefined,
    (url) =>
      url.endsWith("/api/jobs/job-backtest-1")
        ? jsonResponse({
            job_id: "job-backtest-1",
            job_type: "backtest",
            status: "success",
            created_at: "2026-04-08T00:00:00Z",
            updated_at: "2026-04-08T00:00:01Z",
            stages: [{ name: "backtest", status: "success", summary: "ok", started_at: null, finished_at: null }],
            result: {
              dataset_id: "smoke_dataset",
              run_ids: [],
              backtest_ids: ["smoke-backtest"],
              fit_result_uris: [],
              summary_artifacts: [],
              deeplinks: { run_detail: null, backtest_detail: "/backtests/smoke-backtest", review_detail: null },
            },
            error_message: null,
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

test("submits backtest launch and shows detail deeplink button", async () => {
  renderWithProviders(<LaunchBacktestDrawer initialRunId="smoke-train-run" />);
  await waitFor(() => expect(screen.getByText("full")).toBeInTheDocument());
  fireEvent.click(screen.getByText("\u63d0\u4ea4"));
  await waitFor(() =>
    expect(
      fetchMock.mock.calls
        .map(([input]) =>
          typeof input === "string"
            ? input
            : input instanceof URL
              ? input.toString()
              : input.url,
        )
        .some((url) => url.endsWith("/api/launch/backtest")),
    ).toBe(true),
  );
  await waitFor(() =>
    expect(
      fetchMock.mock.calls
        .map(([input]) =>
          typeof input === "string"
            ? input
            : input instanceof URL
              ? input.toString()
              : input.url,
        )
        .some((url) => url.endsWith("/api/jobs/job-backtest-1")),
    ).toBe(true),
  );
});
