import { fireEvent, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { LaunchBacktestDrawer } from "./LaunchBacktestDrawer";
import { createFetchMock, jsonResponse } from "../../test/mockApi";
import { renderWithProviders } from "../../test/renderWithProviders";

function delay(ms: number) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

const fetchMock = vi.fn(
  createFetchMock([
    (url) =>
      url.includes("/api/launch/backtest/options")
        ? jsonResponse({
            default_mode: "official",
            official_template_id: "system::official_backtest_protocol_v1",
            official_multimodal_schema_version: "official_multimodal_standard_v1",
            official_multimodal_feature_names: ["lag_return_1", "sentiment_score"],
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
            official_window_options: [
              { value: "90", label: "Recent 90d", description: null, recommended: true },
              { value: "365", label: "Recent 365d", description: null, recommended: false },
            ],
            dataset_presets: [{ value: "smoke", label: "Smoke", description: null, recommended: true }],
            prediction_scopes: [{ value: "full", label: "full", description: null, recommended: true }],
            strategy_presets: [{ value: "sign", label: "sign", description: null, recommended: true }],
            portfolio_presets: [
              {
                value: "research_default",
                label: "default",
                description: null,
                recommended: true,
              },
            ],
            cost_presets: [{ value: "standard", label: "standard", description: null, recommended: true }],
            default_benchmark_symbol: "BTCUSDT",
            default_official_window_days: 90,
            constraints: {},
          })
        : undefined,
    (url) =>
      url.endsWith("/api/runs/smoke-train-run")
        ? jsonResponse({
            run_id: "smoke-train-run",
            model_name: "mean_baseline",
            dataset_id: "smoke_dataset",
            dataset_ids: ["smoke_dataset"],
            datasets: [{ dataset_id: "smoke_dataset", modality: "market" }],
            family: "baseline",
            backend: "native",
            status: "success",
            created_at: "2026-04-08T00:00:00Z",
            metrics: {},
            tracking_params: {},
            manifest_metrics: {},
            repro_context: {},
            dataset_summary: {},
            evaluation_summary: {},
            feature_importance: {},
            predictions: [],
            related_backtests: [],
            artifacts: [],
            notes: [],
            glossary_hints: [],
          })
        : undefined,
    (url) =>
      url.endsWith("/api/datasets/smoke_dataset/readiness")
        ? jsonResponse({
            dataset_id: "smoke_dataset",
            build_status: "success",
            readiness_status: "ready",
            blocking_issues: [],
            warnings: [],
            raw_row_count: 100,
            usable_row_count: 100,
            dropped_row_count: 0,
            feature_count: 8,
            feature_schema_hash: "hash",
            feature_dimension_consistent: true,
            entity_scope: "single_asset",
            entity_count: 1,
            alignment_status: "aligned",
            missing_feature_status: "pass",
            label_alignment_status: "pass",
            split_integrity_status: "pass",
            temporal_safety_status: "pass",
            freshness_status: "fresh",
            recommended_next_actions: [],
            official_template_eligible: true,
            official_nlp_gate_status: "passed",
            official_nlp_gate_reasons: [],
            archival_nlp_source_only: true,
            market_window_start_time: "2024-01-01T00:00:00Z",
            market_window_end_time: "2026-04-11T02:00:00Z",
            official_backtest_start_time: "2025-09-15T09:00:00Z",
            official_backtest_end_time: "2026-04-11T02:00:00Z",
            nlp_actual_start_time: null,
            nlp_actual_end_time: null,
          })
        : undefined,
    (url, init) => {
      if (!url.endsWith("/api/launch/backtest/preflight") || init?.method !== "POST") {
        return undefined;
      }
      const body = JSON.parse(String(init.body ?? "{}")) as {
        official_window_days?: number;
      };
      const response = jsonResponse({
        compatible: true,
        mode: "official",
        template_id: "system::official_backtest_protocol_v1",
        official_window_days: body.official_window_days ?? 90,
        official_benchmark_version: "official:benchmark-v2",
        official_market_dataset_id: "baseline_real_benchmark_dataset",
        official_multimodal_dataset_id: "official_reddit_pullpush_multimodal_v2_fusion",
        official_window_start_time: "2026-01-11T02:00:00Z",
        official_window_end_time: "2026-04-11T02:00:00Z",
        requires_text_features: false,
        required_feature_names: ["lag_return_1"],
        available_official_feature_names: ["lag_return_1"],
        missing_official_feature_names: [],
        blocking_reasons: [],
        nlp_gate_status: "not_required",
        nlp_gate_reasons: [],
      });
      if (body.official_window_days === 365) {
        return delay(150).then(() => response);
      }
      return response;
    },
    (url, init) =>
      url.endsWith("/api/launch/backtest") && init?.method === "POST"
        ? jsonResponse({
            job_id: "job-backtest-1",
            status: "queued",
            tracking_token: "job:job-backtest-1",
            submitted_at: "2026-04-08T00:00:00Z",
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

test("shows checking progress and re-enables submit after an official preflight refetch", async () => {
  renderWithProviders(<LaunchBacktestDrawer initialRunId="smoke-train-run" />);

  await waitFor(() => {
    const match = document.querySelector<HTMLButtonElement>(
      '.backtest-launch-submit .action-button.secondary[type="button"]',
    );
    expect(match).toBeTruthy();
    expect(match?.disabled).toBe(false);
  });

  fireEvent.change(screen.getByLabelText("官方窗口"), {
    target: { value: "365" },
  });

  await waitFor(() => {
    expect(
      fetchMock.mock.calls.some(
        ([url, init]) =>
          String(url).includes("/api/launch/backtest/preflight") &&
          init &&
          typeof init === "object" &&
          "body" in init &&
          String((init as { body?: string }).body).includes('"official_window_days":365'),
      ),
    ).toBe(true);
  });

  expect(screen.getByRole("button", { name: "兼容性检查中..." })).toBeDisabled();

  const readySubmitButton = await screen.findByRole("button", { name: "提交" });
  expect(readySubmitButton).toBeEnabled();
  fireEvent.click(readySubmitButton);

  await waitFor(() => {
    expect(
      fetchMock.mock.calls.some(
        ([url, init]) =>
          String(url).endsWith("/api/launch/backtest") &&
          init &&
          typeof init === "object" &&
          "body" in init &&
          String((init as { body?: string }).body).includes('"official_window_days":365'),
      ),
    ).toBe(true);
  });
});
