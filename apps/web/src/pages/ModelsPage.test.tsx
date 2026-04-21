import { cleanup, fireEvent, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { ModelsPage } from "./ModelsPage";
import { experimentsFixture, modelTemplatesFixture, trainOptionsFixture } from "../test/fixtures";
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
    (url, init) => {
      if (url.includes("/api/models/trained/") && init?.method === "DELETE") {
        const runId = decodeURIComponent(url.split("/api/models/trained/")[1] ?? "");
        activeExperimentsFixture = {
          ...activeExperimentsFixture,
          items: activeExperimentsFixture.items.filter((item) => item.run_id !== runId),
          total: Math.max(0, activeExperimentsFixture.total - 1),
        };
        return jsonResponse({
          run_id: runId,
          model_name: "deleted",
          status: "success",
          metrics: {},
          is_deleted: true,
        });
      }
      return undefined;
    },
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
            readiness_status: "ready",
            blocking_issues: [],
            warnings: [],
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
            modality_quality_summary: {
              market: {
                modality: "market",
                status: "ready",
                blocking_reasons: [],
                usable_count: 4200,
                coverage_ratio: 0.99,
                duplicate_ratio: 0,
                max_gap_bars: 0,
              },
              macro: {
                modality: "macro",
                status: "ready",
                blocking_reasons: [],
                usable_count: 360,
                duplicate_ratio: 0,
                freshness_lag_days: 1,
              },
            },
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

test("renders run modality and source dataset quality in the trained models table", async () => {
  renderWithProviders(<ModelsPage />, "/models?tab=trained");

  await waitFor(() => expect(screen.getByText("smoke-train-run")).toBeInTheDocument());
  expect(screen.getAllByText("市场").length).toBeGreaterThan(0);
  expect(screen.getAllByText("宏观").length).toBeGreaterThan(0);
  expect(screen.getAllByText("可训练").length).toBeGreaterThan(0);
});

test("opens dataset-aware train drawer from query params and exposes modality selection", async () => {
  renderWithProviders(
    <ModelsPage />,
    "/models?launchTrain=1&datasetId=cross_asset_training_panel_v2",
  );

  await waitFor(() =>
    expect(
      screen.getByRole("heading", { name: "基于该数据集发起训练" }),
    ).toBeInTheDocument(),
  );
  expect(
    screen.getByText(/Cross Asset Training Panel|cross_asset_training_panel_v2/),
  ).toBeInTheDocument();
  expect(screen.getByLabelText("特征模态")).toBeInTheDocument();
});

test("launches composition from distinct quality-ready single-modality runs", async () => {
  renderWithProviders(<ModelsPage />, "/models?tab=trained");

  await waitFor(() => expect(screen.getByText("smoke-train-run")).toBeInTheDocument());

  fireEvent.click(screen.getByLabelText("选择 smoke-train-run 用于多模态组合"));
  fireEvent.click(screen.getByLabelText("选择 macro-signal-run 用于多模态组合"));
  fireEvent.change(screen.getAllByLabelText("组合名称")[0], {
    target: { value: "Cross-modal blend A" },
  });
  fireEvent.click(screen.getByRole("button", { name: "发起组合" }));

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
});

test("disables legacy or low-quality runs from composition selection", async () => {
  activeExperimentsFixture = {
    ...experimentsFixture,
    items: [
      ...experimentsFixture.items,
      {
        ...experimentsFixture.items[0],
        run_id: "legacy-run-no-modality",
        feature_scope_modality: null,
        source_dataset_quality_status: "ready",
      },
      {
        ...experimentsFixture.items[0],
        run_id: "nlp-run-failed",
        feature_scope_modality: "nlp",
        source_dataset_quality_status: "failed",
      },
    ],
  };

  renderWithProviders(<ModelsPage />, "/models?tab=trained");

  await waitFor(() => expect(screen.getByText("legacy-run-no-modality")).toBeInTheDocument());

  const legacyCheckbox = screen.getByLabelText("选择 legacy-run-no-modality 用于多模态组合");
  const failedCheckbox = screen.getByLabelText("选择 nlp-run-failed 用于多模态组合");
  expect(legacyCheckbox).toBeDisabled();
  expect(failedCheckbox).toBeDisabled();

  const legacyRow = screen.getByText("legacy-run-no-modality").closest("tr") as HTMLElement;
  expect(within(legacyRow).getByText("只有显式单模态训练实例才能参与组合。")).toBeInTheDocument();
});

test("bulk deletes selected trained models from the table", async () => {
  renderWithProviders(<ModelsPage />, "/models?tab=trained");

  await waitFor(() => expect(screen.getByText("smoke-train-run")).toBeInTheDocument());
  await waitFor(() => expect(screen.getByText("macro-signal-run")).toBeInTheDocument());

  fireEvent.click(screen.getByLabelText("选择训练实例删除 smoke-train-run"));
  fireEvent.click(screen.getByLabelText("选择训练实例删除 macro-signal-run"));
  fireEvent.click(screen.getByRole("button", { name: "批量删除 (2)" }));

  const dialog = await screen.findByRole("dialog", { name: "" });
  expect(within(dialog).getByText("批量删除训练实例")).toBeInTheDocument();

  fireEvent.click(within(dialog).getByRole("button", { name: "确认删除" }));

  await waitFor(() => expect(screen.queryByText("smoke-train-run")).not.toBeInTheDocument());
  await waitFor(() => expect(screen.queryByText("macro-signal-run")).not.toBeInTheDocument());
});
