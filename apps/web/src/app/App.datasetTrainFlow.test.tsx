import { fireEvent, render, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { App } from "./App";
import { datasetsFixture, experimentsFixture } from "../test/fixtures";
import { createFetchMock, jsonResponse } from "../test/mockApi";

const fetchMock = vi.fn(
  createFetchMock([
    (url) =>
      url.includes("/api/datasets?page=1&per_page=100")
        ? jsonResponse(datasetsFixture)
        : undefined,
    (url) =>
      url.endsWith("/api/datasets/request-options")
        ? jsonResponse({
            domains: [
              { value: "market", label: "market", recommended: true },
              { value: "macro", label: "macro" },
              { value: "on_chain", label: "on_chain" },
            ],
            asset_modes: [
              { value: "single_asset", label: "single_asset", recommended: true },
              { value: "multi_asset", label: "multi_asset" },
            ],
            symbol_types: [{ value: "spot", label: "spot", recommended: true }],
            selection_modes: [
              { value: "manual_list", label: "manual_list" },
              { value: "top_n", label: "top_n", recommended: true },
            ],
            source_vendors: [
              { value: "internal_smoke", label: "internal_smoke", recommended: true },
            ],
            exchanges: [{ value: "binance", label: "Binance", recommended: true }],
            frequencies: [{ value: "1h", label: "1h", recommended: true }],
            feature_sets: [
              {
                value: "baseline_market_features",
                label: "Baseline Market Features",
                recommended: true,
              },
            ],
            label_horizons: [{ value: "1", label: "1 Bar", recommended: true }],
            split_strategies: [
              { value: "time_series", label: "time_series", recommended: true },
            ],
            sample_policies: [{ value: "balanced", label: "balanced", recommended: true }],
            alignment_policies: [
              { value: "entity_timestamp", label: "entity_timestamp", recommended: true },
            ],
            missing_feature_policies: [
              { value: "fail", label: "fail", recommended: true },
            ],
            constraints: {},
          })
        : undefined,
    (url, init) => {
      if (!(url.endsWith("/api/datasets/requests") && init?.method === "POST")) {
        return undefined;
      }
      return jsonResponse({
        job_id: "job-dataset-flow",
        status: "queued",
        job_api_path: "/api/jobs/job-dataset-flow",
        tracking_token: "job-dataset-flow",
        submitted_at: "2026-04-09T00:00:00Z",
      });
    },
    (url) =>
      url.endsWith("/api/jobs/job-dataset-flow")
        ? jsonResponse({
            job_id: "job-dataset-flow",
            job_type: "dataset_request",
            status: "success",
            created_at: "2026-04-09T00:00:00Z",
            updated_at: "2026-04-09T00:00:02Z",
            stages: [
              {
                name: "readiness",
                status: "success",
                summary: "Readiness=ready",
                started_at: null,
                finished_at: null,
              },
            ],
            result: {
              dataset_id: "frontend-contract-smoke",
              run_ids: [],
              backtest_ids: [],
              benchmark_names: [],
              fit_result_uris: [],
              summary_artifacts: [],
              deeplinks: {
                dataset_detail: "/datasets/frontend-contract-smoke",
                run_detail: null,
                backtest_detail: null,
                review_detail: null,
              },
            },
            error_message: null,
          })
        : undefined,
    (url) =>
      url.endsWith("/api/runs?page=1&per_page=100&sort_by=created_at&sort_order=desc")
        ? jsonResponse(experimentsFixture)
        : undefined,
    (url) =>
      url.endsWith("/api/datasets/frontend-contract-smoke")
        ? jsonResponse({
            dataset: {
              dataset_id: "frontend-contract-smoke",
              display_name: "Frontend Contract Smoke",
              subtitle: "Dataset for frontend contract smoke flow",
              dataset_category: "training_panel",
              asset_id: null,
              data_source: "binance",
              frequency: "1h",
              as_of_time: "2026-04-09T00:00:00Z",
              sample_count: 100,
              row_count: 100,
              feature_count: 8,
              label_count: 1,
              label_horizon: 1,
              split_strategy: "time_series",
              time_range_label: "2026-04-01 to 2026-04-09",
              source_vendor: "binance",
              exchange: "binance",
              entity_scope: "single_asset",
              entity_count: 1,
              symbols_preview: ["BTCUSDT"],
              snapshot_version: "v1",
              quality_status: "healthy",
              readiness_status: "ready",
              build_status: "success",
              request_origin: "dataset_request",
              is_smoke: false,
              freshness: {
                as_of_time: "2026-04-09T00:00:00Z",
                data_start_time: "2026-04-01T00:00:00Z",
                data_end_time: "2026-04-09T00:00:00Z",
                lag_seconds: 0,
                status: "fresh",
                summary: "fresh",
              },
              temporal_safety_summary: "ok",
              links: [],
            },
            display_name: "Frontend Contract Smoke",
            subtitle: "Dataset for frontend contract smoke flow",
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
      url.endsWith("/api/datasets/frontend-contract-smoke/readiness")
        ? jsonResponse({
            dataset_id: "frontend-contract-smoke",
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
            missing_feature_status: "clean",
            label_alignment_status: "aligned",
            split_integrity_status: "valid",
            temporal_safety_status: "passed",
            freshness_status: "fresh",
            recommended_next_actions: [],
          })
        : undefined,
    (url) =>
      url.endsWith("/api/launch/train/options")
        ? jsonResponse({
            dataset_presets: [
              { value: "smoke", label: "Smoke", description: null, recommended: true },
            ],
            model_options: [
              {
                value: "elastic_net",
                label: "elastic_net",
                description: null,
                recommended: true,
              },
            ],
            trainer_presets: [
              { value: "fast", label: "fast", description: null, recommended: true },
            ],
            default_seed: 7,
            constraints: {},
          })
        : undefined,
    (url) =>
      url.endsWith("/api/launch/backtest/options")
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
  ]),
);

beforeEach(() => {
  vi.stubGlobal("fetch", fetchMock);
  window.localStorage.clear();
  window.history.pushState({}, "", "/datasets/browser");
});

afterEach(() => {
  vi.unstubAllGlobals();
  fetchMock.mockClear();
  window.localStorage.clear();
});

test("flows from dataset request success CTA into models dataset-aware drawer", async () => {
  const { container } = render(<App />);

  await waitFor(() => {
    expect(container.querySelectorAll("button").length).toBeGreaterThan(0);
  });

  const openDrawerButton = Array.from(container.querySelectorAll("button")).find((button) =>
    button.textContent?.includes("数据集"),
  );
  expect(openDrawerButton).toBeTruthy();
  fireEvent.click(openDrawerButton as HTMLButtonElement);

  await waitFor(() => {
    expect(container.querySelector(".drawer-panel")).not.toBeNull();
  });

  const requestDrawers = Array.from(container.querySelectorAll(".drawer-panel"));
  const requestDrawer = requestDrawers.find((drawer) => drawer.querySelector('input[type="date"]'));
  expect(requestDrawer).toBeTruthy();

  const dateInputs = requestDrawer?.querySelectorAll('input[type="date"]');
  expect(dateInputs).toHaveLength(2);
  fireEvent.change(dateInputs?.[0] as HTMLInputElement, { target: { value: "2026-04-01" } });
  fireEvent.change(dateInputs?.[1] as HTMLInputElement, { target: { value: "2026-04-09" } });

  const requestDrawerQueries = within(requestDrawer as HTMLElement);
  const submitButton = Array.from(requestDrawerQueries.getAllByRole("button")).find(
    (button) => button.getAttribute("type") === "submit" || button.textContent?.includes("提交"),
  );
  expect(submitButton).toBeTruthy();
  fireEvent.click(submitButton as HTMLButtonElement);

  await waitFor(() => {
    const trainLink = container.querySelector(
      'a[href="/models?launchTrain=1&datasetId=frontend-contract-smoke"]',
    );
    expect(trainLink).not.toBeNull();
  });

  const trainLink = container.querySelector(
    'a[href="/models?launchTrain=1&datasetId=frontend-contract-smoke"]',
  );
  fireEvent.click(trainLink as HTMLAnchorElement);

  await waitFor(() =>
    expect(window.location.pathname + window.location.search).toBe(
      "/models?launchTrain=1&datasetId=frontend-contract-smoke",
    ),
  );

  await waitFor(() => {
    const urls = fetchMock.mock.calls.map(([input]) =>
      typeof input === "string"
        ? input
        : input instanceof URL
          ? input.toString()
          : input.url,
    );
    expect(urls.some((url) => url.includes("/api/datasets/frontend-contract-smoke"))).toBe(true);
    expect(
      urls.some((url) => url.includes("/api/datasets/frontend-contract-smoke/readiness")),
    ).toBe(true);
    expect(urls.some((url) => url.includes("/api/launch/train/options"))).toBe(true);
  });

  const trainDrawer = await waitFor(() => {
    const drawers = Array.from(container.querySelectorAll(".drawer-panel"));
    const match = drawers.find((drawer) => drawer.textContent?.includes("Frontend Contract Smoke"));
    expect(match).toBeTruthy();
    return match as HTMLElement;
  });
  expect(trainDrawer?.textContent).toContain("当前训练数据集");
  expect(trainDrawer?.textContent).toContain("Frontend Contract Smoke");
  expect(trainDrawer?.textContent).not.toContain("数据集预置");
});
