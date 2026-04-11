import { screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { LaunchBacktestDrawer } from "./LaunchBacktestDrawer";
import { createFetchMock, jsonResponse } from "../../test/mockApi";
import { renderWithProviders } from "../../test/renderWithProviders";

const fetchMock = vi.fn(
  createFetchMock([
    (url) =>
      url.includes("/api/launch/backtest/options")
        ? jsonResponse({
            default_mode: "official",
            official_template_id: "system::official_backtest_protocol_v1",
            template_options: [
              {
                template_id: "system::official_backtest_protocol_v1",
                name: "Official Backtest Protocol v1",
                description: "official protocol",
                source: "system",
                read_only: true,
                official: true,
                protocol_version: "v1",
                output_contract_version: "prediction_frame_v1",
                fixed_prediction_scope: "test",
                ranking_policy: null,
                slice_policy: null,
                scenario_bundle: ["BASELINE"],
                eligibility_rules: [],
                required_metadata: [],
                notes: [],
              },
            ],
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

test("renders official protocol as default mode", async () => {
  renderWithProviders(<LaunchBacktestDrawer initialRunId="smoke-train-run" />);
  await waitFor(() => expect(screen.getByText("test")).toBeInTheDocument());
  expect(screen.getByText("\u5b98\u65b9\u6a21\u677f")).toBeInTheDocument();
  expect(screen.getByText("\u5b98\u65b9\u56de\u6d4b\u534f\u8bae")).toBeInTheDocument();
  expect(screen.getByText("\u81ea\u5b9a\u4e49\u56de\u6d4b")).toBeInTheDocument();
});
