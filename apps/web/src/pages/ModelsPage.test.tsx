import { cleanup, fireEvent, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { ModelsPage } from "./ModelsPage";
import { experimentsFixture, modelTemplatesFixture, trainOptionsFixture } from "../test/fixtures";
import { I18N } from "../shared/lib/i18n";
import { createFetchMock, jsonResponse } from "../test/mockApi";
import { renderWithProviders } from "../test/renderWithProviders";

let activeExperimentsFixture = experimentsFixture;

const fetchMock = vi.fn(
  createFetchMock([
    (url) => (url.includes("/api/runs?") ? jsonResponse(activeExperimentsFixture) : undefined),
    (url) =>
      url.endsWith("/api/models/templates") ? jsonResponse(modelTemplatesFixture) : undefined,
    (url) =>
      url.endsWith("/api/launch/train/options") ? jsonResponse(trainOptionsFixture) : undefined,
    (url) =>
      url.endsWith("/api/datasets/cross_asset_training_panel_v2")
        ? jsonResponse({
            dataset: {
              dataset_id: "cross_asset_training_panel_v2",
              display_name: "Cross Asset Training Panel",
              subtitle: "Multi-asset training sample",
              dataset_category: "Training Panel",
              asset_id: null,
              data_source: "binance",
              frequency: "4h",
              as_of_time: "2026-04-07T12:00:00Z",
              sample_count: 100,
              row_count: 100,
              feature_count: 8,
              label_count: 1,
              label_horizon: 1,
              split_strategy: "time_series",
              time_range_label: "2026-03-01 to 2026-04-07",
              source_vendor: "binance",
              exchange: "binance",
              entity_scope: "multi_asset",
              entity_count: 8,
              symbols_preview: ["BTCUSDT", "ETHUSDT"],
              snapshot_version: "v1",
              quality_status: "warning",
              readiness_status: "warning",
              build_status: "success",
              request_origin: "dataset_request",
              is_smoke: false,
              freshness: {
                as_of_time: "2026-04-07T12:00:00Z",
                data_start_time: "2026-03-01T00:00:00Z",
                data_end_time: "2026-04-07T12:00:00Z",
                lag_seconds: 0,
                status: "fresh",
                summary: "fresh",
              },
              temporal_safety_summary: "ok",
              links: [],
            },
            display_name: "Cross Asset Training Panel",
            subtitle: "Multi-asset training sample",
            summary: "summary",
            intended_use: "use",
            risk_note: "risk",
            row_count: 100,
            feature_count: 8,
            label_count: 1,
            feature_columns_preview: [],
            label_columns: ["label"],
            feature_groups: [],
            quality_summary: null,
            glossary_hints: [],
            label_spec: {},
            split_manifest: {},
            sample_policy: {},
            quality: {},
            acquisition_profile: {},
            build_profile: {},
            schema_profile: {},
            readiness_profile: {},
            training_profile: {},
            links: [],
          })
        : undefined,
    (url) =>
      url.endsWith("/api/datasets/cross_asset_training_panel_v2/readiness")
        ? jsonResponse({
            dataset_id: "cross_asset_training_panel_v2",
            build_status: "success",
            readiness_status: "warning",
            blocking_issues: [],
            warnings: ["quality_warning"],
            raw_row_count: 100,
            usable_row_count: 98,
            dropped_row_count: 2,
            feature_count: 8,
            feature_schema_hash: "hash",
            feature_dimension_consistent: true,
            entity_scope: "multi_asset",
            entity_count: 8,
            alignment_status: "aligned",
            missing_feature_status: "clean",
            label_alignment_status: "aligned",
            split_integrity_status: "valid",
            temporal_safety_status: "passed",
            freshness_status: "fresh",
            recommended_next_actions: [],
          })
        : undefined,
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
    (url, init) =>
      url.endsWith("/api/launch/model-composition") && init?.method === "POST"
        ? jsonResponse({
            job_id: "job-compose-1",
            status: "queued",
            tracking_token: "job:job-compose-1",
            submitted_at: "2026-04-08T12:00:00Z",
          })
        : undefined,
    (url) =>
      url.endsWith("/api/jobs/job-compose-1")
        ? jsonResponse({
            job_id: "job-compose-1",
            job_type: "model_composition",
            status: "success",
            created_at: "2026-04-08T12:00:00Z",
            updated_at: "2026-04-08T12:00:01Z",
            stages: [
              {
                name: "compose",
                status: "success",
                summary: "ok",
                started_at: null,
                finished_at: null,
              },
            ],
            result: {
              dataset_id: null,
              run_ids: ["multimodal-run-1"],
              backtest_ids: [],
              fit_result_uris: [],
              summary_artifacts: [],
              deeplinks: {
                run_detail: "/runs/multimodal-run-1",
                backtest_detail: null,
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
  activeExperimentsFixture = experimentsFixture;
  window.localStorage.clear();
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  fetchMock.mockClear();
  window.localStorage.clear();
});

test("renders backend model templates and trained model tabs", async () => {
  renderWithProviders(<ModelsPage />, "/models");

  await waitFor(() =>
    expect(screen.getByRole("heading", { name: "模型模板" })).toBeInTheDocument(),
  );
  await waitFor(() => expect(screen.getByText("Elastic Net Default")).toBeInTheDocument());
  expect(screen.getByText("Custom Elastic Net")).toBeInTheDocument();
  expect(screen.getAllByText("使用此模板训练").length).toBeGreaterThan(0);

  fireEvent.click(screen.getByRole("button", { name: "已训练模型" }));
  await waitFor(() => expect(screen.getByText("smoke-train-run")).toBeInTheDocument());
  expect(screen.getAllByText("发起回测").length).toBeGreaterThan(0);
  expect(screen.getByRole("link", { name: "smoke_dataset" })).toHaveAttribute(
    "href",
    "/datasets/smoke_dataset",
  );
  expect(
    screen.queryByRole("link", { name: "smoke_dataset_market_anchor" }),
  ).not.toBeInTheDocument();
  expect(screen.getByRole("link", { name: "macro_liquidity_snapshot" })).toHaveAttribute(
    "href",
    "/datasets/macro_liquidity_snapshot",
  );
});

test("keeps the standard backtest action for composed runs", async () => {
  activeExperimentsFixture = {
    ...experimentsFixture,
    items: [
      {
        ...experimentsFixture.items[0],
        run_id: "legacy-composed-run",
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
      } as (typeof experimentsFixture.items)[number] & {
        official_template_eligible?: boolean;
        official_blocking_reasons?: string[];
      },
    ],
  };

  renderWithProviders(<ModelsPage />, "/models?tab=trained");

  await waitFor(() => expect(screen.getByText("legacy-composed-run")).toBeInTheDocument());

  const row = screen.getByText("legacy-composed-run").closest("tr") as HTMLElement;
  expect(within(row).getByRole("button", { name: I18N.action.launchBacktest })).toBeInTheDocument();
});

test("opens dataset-aware train drawer from query params", async () => {
  renderWithProviders(
    <ModelsPage />,
    "/models?launchTrain=1&datasetId=cross_asset_training_panel_v2",
  );

  await waitFor(() =>
    expect(
      screen.getByRole("heading", { name: "Launch training from this dataset" }),
    ).toBeInTheDocument(),
  );
  expect(
    screen.getByText(
      (content) =>
        content === "Cross Asset Training Panel" || content === "cross_asset_training_panel_v2",
    ),
  ).toBeInTheDocument();
  expect(
    screen.getByText("This launch was opened from a dataset page and will use that dataset directly."),
  ).toBeInTheDocument();

  const drawer = screen
    .getByRole("heading", { name: "Launch training from this dataset" })
    .closest(".drawer-panel");
  expect(drawer).not.toBeNull();

  const drawerQueries = within(drawer as HTMLElement);
  expect(drawerQueries.queryByText("Dataset Preset")).not.toBeInTheDocument();
  expect(drawerQueries.getByText("模型模板")).toBeInTheDocument();
});

test("launches multimodal composition from selected single-modality runs", async () => {
  renderWithProviders(<ModelsPage />, "/models?tab=trained");

  await waitFor(() => expect(screen.getByText("smoke-train-run")).toBeInTheDocument());

  fireEvent.click(screen.getByLabelText("Select smoke-train-run for multimodal composition"));
  fireEvent.click(screen.getByLabelText("Select macro-signal-run for multimodal composition"));
  fireEvent.change(screen.getAllByLabelText("Composition Name")[0], {
    target: { value: "Cross-modal blend A" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Launch composition" }));

  await waitFor(() =>
    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining("/api/launch/model-composition"),
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({
          source_run_ids: ["smoke-train-run", "macro-signal-run"],
          composition_name: "Cross-modal blend A",
          dataset_ids: ["smoke_dataset", "macro_liquidity_snapshot"],
        }),
      }),
    ),
  );
  await waitFor(() => expect(screen.getByText("job-compose-1")).toBeInTheDocument());
  expect(screen.getByRole("link", { name: "Open composed model" })).toHaveAttribute(
    "href",
    "/runs/multimodal-run-1",
  );
});
