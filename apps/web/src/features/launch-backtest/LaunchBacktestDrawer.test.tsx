import { cleanup, fireEvent, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { LaunchBacktestDrawer } from "./LaunchBacktestDrawer";
import { createFetchMock, jsonResponse } from "../../test/mockApi";
import { renderWithProviders } from "../../test/renderWithProviders";
import { I18N } from "../../shared/lib/i18n";

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
            official_multimodal_feature_names: [
              "lag_return_1",
              "lag_return_2",
              "sentiment_score",
              "text_reddit_comment_count_1h",
            ],
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
              { value: "30", label: "Recent 30d", description: null, recommended: false },
              { value: "90", label: "Recent 90d", description: null, recommended: true },
              { value: "180", label: "Recent 180d", description: null, recommended: false },
              { value: "365", label: "Recent 365d", description: null, recommended: false },
            ],
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
            dataset_ids: ["smoke_dataset", "macro_liquidity_snapshot"],
            datasets: [
              {
                dataset_id: "smoke_dataset",
                modality: "market",
              },
            ],
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
      url.endsWith("/api/runs/unsupported-run")
        ? jsonResponse({
            run_id: "unsupported-run",
            model_name: "mean_baseline",
            dataset_id: "smoke_dataset",
            dataset_ids: ["smoke_dataset"],
            datasets: [
              {
                dataset_id: "smoke_dataset",
                modality: "market",
              },
            ],
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
      url.endsWith("/api/runs/legacy-composed-run")
        ? jsonResponse({
            run_id: "legacy-composed-run",
            model_name: "multimodal_compose",
            dataset_id: "official_reddit_pullpush_multimodal_v2_fusion",
            dataset_ids: [
              "baseline_real_benchmark_dataset",
              "official_reddit_pullpush_multimodal_v2_fusion",
            ],
            datasets: [
              {
                dataset_id: "baseline_real_benchmark_dataset",
                modality: "market",
              },
              {
                dataset_id: "official_reddit_pullpush_multimodal_v2_fusion",
                modality: "sentiment_events",
              },
            ],
            composition: {
              source_runs: [
                { run_id: "market-run", modality: "market" },
                { run_id: "nlp-run", modality: "sentiment_events" },
              ],
            },
            official_template_eligible: false,
            official_blocking_reasons: [
              "Legacy composed run is not eligible for official backtest.",
            ],
            family: "ensemble",
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
            nlp_actual_start_time: "2024-01-01T00:00:00Z",
            nlp_actual_end_time: "2026-04-11T01:35:00Z",
          })
        : undefined,
    (url, init) => {
      if (!url.endsWith("/api/launch/backtest/preflight") || init?.method !== "POST") {
        return undefined;
      }
      const body = JSON.parse(String(init.body ?? "{}")) as {
        run_id?: string;
        official_window_days?: number;
      };
      if (body.official_window_days === 365) {
        return delay(150).then(() =>
          jsonResponse({
            compatible: true,
            mode: "official",
            template_id: "system::official_backtest_protocol_v1",
            official_window_days: 365,
            official_benchmark_version:
              "official_reddit_pullpush_multimodal_v2_fusion:benchmark-v2",
            official_market_dataset_id: "baseline_real_benchmark_dataset",
            official_multimodal_dataset_id: "official_reddit_pullpush_multimodal_v2_fusion",
            official_window_start_time: "2025-04-11T02:00:00Z",
            official_window_end_time: "2026-04-11T02:00:00Z",
            requires_text_features: true,
            required_feature_names: [
              "lag_return_1",
              "lag_return_2",
              "sentiment_score",
              "text_reddit_comment_count_1h",
            ],
            available_official_feature_names: [
              "lag_return_1",
              "lag_return_2",
              "sentiment_score",
              "text_reddit_comment_count_1h",
            ],
            missing_official_feature_names: [],
            blocking_reasons: [],
            nlp_gate_status: "passed",
            nlp_gate_reasons: [],
          }),
        );
      }
      if (body.run_id === "unsupported-run") {
        return jsonResponse({
          compatible: false,
          mode: "official",
          template_id: "system::official_backtest_protocol_v1",
          official_window_days: body.official_window_days ?? 90,
          official_benchmark_version:
            "official_reddit_pullpush_multimodal_v2_fusion:benchmark-v2",
          official_market_dataset_id: "baseline_real_benchmark_dataset",
          official_multimodal_dataset_id: "official_reddit_pullpush_multimodal_v2_fusion",
          official_window_start_time: "2026-01-11T02:00:00Z",
          official_window_end_time: "2026-04-11T02:00:00Z",
          requires_text_features: true,
          required_feature_names: [
            "lag_return_1",
            "sentiment_score",
            "text_reddit_embedding_768",
          ],
          available_official_feature_names: [
            "lag_return_1",
            "lag_return_2",
            "sentiment_score",
            "text_reddit_comment_count_1h",
          ],
          missing_official_feature_names: ["text_reddit_embedding_768"],
          blocking_reasons: [
            "Official benchmark dataset is missing features: text_reddit_embedding_768",
          ],
          nlp_gate_status: "passed",
          nlp_gate_reasons: [],
        });
      }
      return jsonResponse({
        compatible: true,
        mode: "official",
        template_id: "system::official_backtest_protocol_v1",
        official_window_days: body.official_window_days ?? 90,
        official_benchmark_version:
          "official_reddit_pullpush_multimodal_v2_fusion:benchmark-v2",
        official_market_dataset_id: "baseline_real_benchmark_dataset",
        official_multimodal_dataset_id: "official_reddit_pullpush_multimodal_v2_fusion",
        official_window_start_time: "2026-01-11T02:00:00Z",
        official_window_end_time: "2026-04-11T02:00:00Z",
        requires_text_features: true,
        required_feature_names: [
          "lag_return_1",
          "lag_return_2",
          "sentiment_score",
          "text_reddit_comment_count_1h",
        ],
        available_official_feature_names: [
          "lag_return_1",
          "lag_return_2",
          "sentiment_score",
          "text_reddit_comment_count_1h",
        ],
        missing_official_feature_names: [],
        blocking_reasons: [],
        nlp_gate_status: "passed",
        nlp_gate_reasons: [],
      });
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
    (url) =>
      url.endsWith("/api/jobs/job-backtest-1")
        ? jsonResponse({
            job_id: "job-backtest-1",
            job_type: "backtest",
            status: "success",
            created_at: "2026-04-08T00:00:00Z",
            updated_at: "2026-04-08T00:00:01Z",
            stages: [
              {
                name: "backtest",
                status: "success",
                summary: "ok",
                started_at: null,
                finished_at: null,
              },
            ],
            result: {
              dataset_id: "smoke_dataset",
              run_ids: [],
              backtest_ids: ["smoke-backtest"],
              fit_result_uris: [],
              summary_artifacts: [],
              deeplinks: {
                run_detail: null,
                backtest_detail: "/backtests/smoke-backtest",
                review_detail: null,
              },
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
  cleanup();
  vi.unstubAllGlobals();
  fetchMock.mockClear();
});

test("renders official protocol as default mode", async () => {
  renderWithProviders(<LaunchBacktestDrawer initialRunId="smoke-train-run" />);

  await waitFor(() => expect(screen.getByText("官方回测协议 v1")).toBeInTheDocument());

  const [officialTab, customTab] = within(screen.getByRole("tablist")).getAllByRole("button");
  expect(officialTab).toHaveClass("active");
  expect(customTab).not.toHaveClass("active");
  expect(officialTab).toBeEnabled();
  expect(screen.getByText(/Model output must follow prediction_frame_v1/i)).toBeInTheDocument();
  expect(screen.getByText("Official Market Dataset ID")).toBeInTheDocument();
  expect(screen.getByText("baseline_real_benchmark_dataset")).toBeInTheDocument();
  expect(screen.getByText("Official Multimodal Dataset ID")).toBeInTheDocument();
  expect(screen.getByText("official_reddit_pullpush_multimodal_v2_fusion")).toBeInTheDocument();
  expect(screen.getByLabelText("Official Window")).toHaveValue("90");
  expect(screen.getByText("Latest Official Window")).toBeInTheDocument();
  expect(screen.getByText("Actual Market Window")).toBeInTheDocument();
  expect(screen.getByText("Official Test Window")).toBeInTheDocument();
  expect(screen.getByText("Actual NLP Window")).toBeInTheDocument();
  expect(screen.getByText("Official Compatibility")).toBeInTheDocument();
  expect(screen.getByText("Official Schema Version")).toBeInTheDocument();
  expect(screen.getByText("official_multimodal_standard_v1")).toBeInTheDocument();
  expect(screen.getAllByText("瀹樻柟鏍囧噯鐗瑰緛").length).toBeGreaterThan(0);
  expect(screen.getByText("text_reddit_comment_count_1h")).toBeInTheDocument();
  expect(screen.getByText("Archival NLP Only")).toBeInTheDocument();
  expect(
    screen.getByText(/If NLP quality gates fail, the official template is blocked/i),
  ).toBeInTheDocument();
  await waitFor(() => expect(screen.getByText("Compatible")).toBeInTheDocument());
  await waitFor(() =>
    expect(screen.getByRole("button", { name: I18N.action.submit })).toBeEnabled(),
  );
});

test("submits official window days for official backtest", async () => {
  renderWithProviders(<LaunchBacktestDrawer initialRunId="smoke-train-run" />);

  await waitFor(() => expect(screen.getByText("官方回测协议 v1")).toBeInTheDocument());
  await waitFor(() =>
    expect(screen.getByRole("button", { name: I18N.action.submit })).toBeEnabled(),
  );

  fireEvent.change(screen.getByLabelText("Official Window"), {
    target: { value: "365" },
  });
  await waitFor(() =>
    expect(screen.getByRole("button", { name: I18N.action.submit })).toBeEnabled(),
  );
  fireEvent.click(screen.getByRole("button", { name: I18N.action.submit }));

  await waitFor(() => {
    expect(
      fetchMock.mock.calls.some(
        ([url, init]) =>
          String(url).includes("/api/launch/backtest") &&
          !String(url).includes("/api/launch/backtest/preflight") &&
          init &&
          typeof init === "object" &&
          "body" in init &&
          String((init as { body?: string }).body).includes('"official_window_days":365'),
      ),
    ).toBe(true);
  });
});

test("blocks official submit in the drawer when preflight reports schema incompatibility", async () => {
  renderWithProviders(<LaunchBacktestDrawer initialRunId="unsupported-run" />);

  await waitFor(() => expect(screen.getByText("官方回测协议 v1")).toBeInTheDocument());
  await waitFor(() =>
    expect(
      screen.getByText(
        "Official benchmark dataset is missing features: text_reddit_embedding_768",
      ),
    ).toBeInTheDocument(),
  );

  const submitButton = screen.getByRole("button", { name: I18N.action.submit });
  expect(submitButton).toBeDisabled();
  expect(screen.getByText("Incompatible")).toBeInTheDocument();
  expect(screen.getByText("Official Compatibility")).toBeInTheDocument();
  expect(screen.getByText("Blocking Summary")).toBeInTheDocument();
  expect(screen.getAllByText("text_reddit_embedding_768").length).toBeGreaterThan(0);

  fireEvent.click(submitButton);

  expect(
    fetchMock.mock.calls.some(
      ([url, init]) =>
        String(url).endsWith("/api/launch/backtest") &&
        init &&
        typeof init === "object" &&
        "method" in init &&
        (init as { method?: string }).method === "POST",
    ),
  ).toBe(false);
});

test("keeps official mode available for composed runs and relies on preflight", async () => {
  renderWithProviders(<LaunchBacktestDrawer initialRunId="legacy-composed-run" />);

  const [officialTab, customTab] = within(screen.getByRole("tablist")).getAllByRole("button");

  expect(officialTab).toBeEnabled();
  expect(officialTab).toHaveClass("active");
  expect(customTab).not.toHaveClass("active");
  await waitFor(() =>
    expect(screen.getByRole("button", { name: I18N.action.submit })).toBeEnabled(),
  );
});

test("submits dataset_ids for multimodal custom backtest", async () => {
  renderWithProviders(
    <LaunchBacktestDrawer
      initialRunId="smoke-train-run"
      initialDatasetIds={["smoke_dataset", "macro_liquidity_snapshot"]}
    />,
  );

  await waitFor(() => expect(screen.getByText("官方回测协议 v1")).toBeInTheDocument());
  const [, customTab] = within(screen.getByRole("tablist")).getAllByRole("button");
  fireEvent.click(customTab);
  fireEvent.change(screen.getByLabelText("Dataset IDs"), {
    target: { value: "smoke_dataset\nmacro_liquidity_snapshot" },
  });
  fireEvent.click(screen.getByRole("button", { name: I18N.action.submit }));

  await waitFor(() =>
    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining("/api/launch/backtest"),
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({
          run_id: "smoke-train-run",
          mode: "custom",
          dataset_ids: ["smoke_dataset", "macro_liquidity_snapshot"],
          prediction_scope: "full",
          strategy_preset: "sign",
          portfolio_preset: "research_default",
          cost_preset: "standard",
          benchmark_symbol: "BTCUSDT",
        }),
      }),
    ),
  );
});
