import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { ModelsPage } from "./ModelsPage";
import { experimentsFixture } from "../test/fixtures";
import { createFetchMock, jsonResponse } from "../test/mockApi";
import { renderWithProviders } from "../test/renderWithProviders";

const fetchMock = vi.fn(
  createFetchMock([
    (url) => (url.includes("/api/runs?") ? jsonResponse(experimentsFixture) : undefined),
    (url) =>
      url.endsWith("/api/launch/train/options")
        ? jsonResponse({
            dataset_presets: [
              { value: "smoke", label: "Smoke", description: null, recommended: true },
            ],
            model_options: [
              { value: "elastic_net", label: "elastic_net", description: null, recommended: true },
            ],
            trainer_presets: [
              { value: "fast", label: "fast", description: null, recommended: true },
            ],
            default_seed: 7,
            constraints: {},
          })
        : undefined,
    (url) =>
      url.endsWith("/api/datasets/cross_asset_training_panel_v2")
        ? jsonResponse({
            dataset: {
              dataset_id: "cross_asset_training_panel_v2",
              display_name: "跨资产收益训练面板 / Cross Asset Training Panel",
              subtitle: "多资产训练样本",
              dataset_category: "训练面板",
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
              time_range_label: "2026-03-01 至 2026-04-07",
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
            display_name: "跨资产收益训练面板 / Cross Asset Training Panel",
            subtitle: "多资产训练样本",
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
              { value: "research_default", label: "default", description: null, recommended: true },
            ],
            cost_presets: [
              { value: "standard", label: "standard", description: null, recommended: true },
            ],
            default_benchmark_symbol: "BTCUSDT",
            constraints: {},
          })
        : undefined,
  ]),
);

beforeEach(() => {
  vi.stubGlobal("fetch", fetchMock);
  window.localStorage.clear();
});

afterEach(() => {
  vi.unstubAllGlobals();
  fetchMock.mockClear();
  window.localStorage.clear();
});

test("renders model templates and trained model tabs", async () => {
  renderWithProviders(<ModelsPage />, "/models");

  await waitFor(() =>
    expect(screen.getByRole("heading", { name: "模型模板" })).toBeInTheDocument(),
  );
  expect(screen.getByText("Elastic Net 模板")).toBeInTheDocument();

  fireEvent.click(screen.getByText("已训练模型"));
  await waitFor(() => expect(screen.getByText("smoke-train-run")).toBeInTheDocument());
  expect(screen.getAllByText("发起回测").length).toBeGreaterThan(0);
});

test("auto opens dataset-aware train drawer from query params", async () => {
  renderWithProviders(
    <ModelsPage />,
    "/models?launchTrain=1&datasetId=cross_asset_training_panel_v2",
  );

  await waitFor(() =>
    expect(screen.getByText("当前训练数据集")).toBeInTheDocument(),
  );
  expect(
    screen.getByText(
      (content) =>
        content === "跨资产收益训练面板 / Cross Asset Training Panel" ||
        content === "cross_asset_training_panel_v2",
    ),
  ).toBeInTheDocument();

  const drawer = screen
    .getByText("基于当前数据集发起训练")
    .closest(".drawer-panel");
  expect(drawer).not.toBeNull();

  const drawerQueries = within(drawer as HTMLElement);
  expect(drawerQueries.queryByText("数据集预置")).not.toBeInTheDocument();
  expect(
    drawerQueries.getByText(
      (content) =>
        content === "正在读取训练就绪度" || content === "这份数据集可以训练，但需要先留意",
    ),
  ).toBeInTheDocument();
});
