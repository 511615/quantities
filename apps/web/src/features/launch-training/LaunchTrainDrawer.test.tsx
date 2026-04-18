import { cleanup, fireEvent, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { LaunchTrainDrawer } from "./LaunchTrainDrawer";
import { createFetchMock, jsonResponse } from "../../test/mockApi";
import { renderWithProviders } from "../../test/renderWithProviders";

const fetchMock = vi.fn(
  createFetchMock([
    (url) =>
      url.includes("/api/launch/train/options")
        ? jsonResponse({
            dataset_presets: [
              { value: "smoke", label: "Smoke", description: null, recommended: true },
            ],
            model_options: [
              { value: "elastic_net", label: "elastic_net", description: null, recommended: true },
            ],
            template_options: [
              {
                value: "registry::elastic_net",
                label: "Elastic Net default",
                description: "Template sourced from model registry.",
                recommended: true,
              },
            ],
            trainer_presets: [
              { value: "fast", label: "fast", description: null, recommended: true },
            ],
            feature_scope_modalities: [
              { value: "market", label: "Market", description: null, recommended: true },
              { value: "macro", label: "Macro", description: null, recommended: false },
              { value: "nlp", label: "NLP", description: null, recommended: false },
            ],
            default_seed: 7,
            constraints: {},
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
                status: "failed",
                blocking_reasons: ["macro freshness lag is too high"],
                usable_count: 120,
                duplicate_ratio: 0.02,
                freshness_lag_days: 20,
              },
            },
          })
        : undefined,
    (url) =>
      url.endsWith("/api/datasets/blocked_train_request/readiness")
        ? jsonResponse({
            dataset_id: "blocked_train_request",
            build_status: "success",
            readiness_status: "not_ready",
            blocking_issues: ["label_alignment_failed"],
            warnings: [],
            raw_row_count: 100,
            usable_row_count: 0,
            dropped_row_count: 100,
            feature_count: 8,
            feature_schema_hash: "hash",
            feature_dimension_consistent: false,
            entity_scope: "single_asset",
            entity_count: 1,
            alignment_status: "failed",
            missing_feature_status: "failed",
            label_alignment_status: "failed",
            split_integrity_status: "failed",
            temporal_safety_status: "failed",
            freshness_status: "fresh",
            recommended_next_actions: [],
            modality_quality_summary: {
              market: {
                modality: "market",
                status: "failed",
                blocking_reasons: ["market coverage is below threshold"],
                usable_count: 200,
                coverage_ratio: 0.5,
                duplicate_ratio: 0.02,
                max_gap_bars: 48,
              },
            },
          })
        : undefined,
    (url, init) =>
      url.endsWith("/api/launch/train") && init?.method === "POST"
        ? jsonResponse({
            job_id: "job-train-1",
            status: "queued",
            tracking_token: "job:job-train-1",
            submitted_at: "2026-04-08T00:00:00Z",
          })
        : undefined,
    (url) =>
      url.endsWith("/api/jobs/job-train-1")
        ? jsonResponse({
            job_id: "job-train-1",
            job_type: "train",
            status: "success",
            created_at: "2026-04-08T00:00:00Z",
            updated_at: "2026-04-08T00:00:01Z",
            stages: [
              { name: "train", status: "success", summary: "ok", started_at: null, finished_at: null },
            ],
            result: {
              dataset_id: "smoke_dataset",
              run_ids: ["smoke-train-run"],
              backtest_ids: [],
              fit_result_uris: [],
              summary_artifacts: [],
              deeplinks: {
                run_detail: "/runs/smoke-train-run",
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
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  fetchMock.mockClear();
});

test("submits a modality-scoped train launch and shows the run deeplink button", async () => {
  renderWithProviders(<LaunchTrainDrawer />);

  fireEvent.click(screen.getByText("发起训练"));
  await waitFor(() => expect(screen.getByRole("option", { name: "Market" })).toBeInTheDocument());

  fireEvent.change(screen.getByLabelText("Feature Modality"), {
    target: { value: "market" },
  });
  fireEvent.click(screen.getByText("提交"));

  await waitFor(() =>
    expect(
      fetchMock.mock.calls.some(([input, init]) => {
        const url =
          typeof input === "string"
            ? input
            : input instanceof URL
              ? input.toString()
              : input.url;
        return url.endsWith("/api/launch/train") && init?.method === "POST";
      }),
    ).toBe(true),
  );

  const trainRequest = fetchMock.mock.calls.find(([input, init]) => {
    const url =
      typeof input === "string"
        ? input
        : input instanceof URL
          ? input.toString()
          : input.url;
    return url.endsWith("/api/launch/train") && init?.method === "POST";
  });
  expect(trainRequest).toBeTruthy();
  const [, requestInit] = trainRequest as [string | URL | Request, RequestInit];
  const body = JSON.parse(String(requestInit.body)) as {
    template_id?: string;
    feature_scope_modality?: string;
  };
  expect(body.template_id).toBe("registry::elastic_net");
  expect(body.feature_scope_modality).toBe("market");

  await waitFor(() => expect(screen.getByText("job-train-1")).toBeInTheDocument());
  expect(screen.getByRole("link", { name: "跳转运行详情" })).toHaveAttribute(
    "href",
    "/runs/smoke-train-run",
  );
});

test("renders dataset-aware launch with a modality selector and quality summary", async () => {
  renderWithProviders(
    <LaunchTrainDrawer
      datasetId="cross_asset_training_panel_v2"
      datasetLabel="Cross Asset Training Panel"
      triggerLabel="Train from dataset"
      title="Launch training from this dataset"
    />,
  );

  fireEvent.click(screen.getByText("Train from dataset"));

  const drawer = await screen.findByText("Launch training from this dataset");
  const panel = drawer.closest(".drawer-panel");
  expect(panel).not.toBeNull();
  const drawerQueries = within(panel as HTMLElement);

  await waitFor(() => expect(drawerQueries.getByRole("option", { name: "Market" })).toBeInTheDocument());
  expect(drawerQueries.queryByText("数据集预置")).not.toBeInTheDocument();
  expect(drawerQueries.getByLabelText("Feature Modality")).toBeInTheDocument();
  expect(drawerQueries.getByText("当前训练数据集")).toBeInTheDocument();
  expect(drawerQueries.getByText("Dataset modality quality")).toBeInTheDocument();
  expect(drawerQueries.getByText("Market")).toBeInTheDocument();
  expect(drawerQueries.getByText("Macro")).toBeInTheDocument();
});

test("blocks dataset-aware train launch when selected modality quality is not ready", async () => {
  renderWithProviders(
    <LaunchTrainDrawer
      datasetId="cross_asset_training_panel_v2"
      datasetLabel="Cross Asset Training Panel"
      triggerLabel="Train from dataset"
      title="Launch training from this dataset"
    />,
  );

  fireEvent.click(screen.getByText("Train from dataset"));
  await waitFor(() => expect(screen.getByRole("option", { name: "Macro" })).toBeInTheDocument());

  fireEvent.change(screen.getByLabelText("Feature Modality"), {
    target: { value: "macro" },
  });
  fireEvent.click(screen.getByText("提交"));

  await waitFor(() =>
    expect(screen.getAllByText("macro freshness lag is too high").length).toBeGreaterThan(0),
  );
  expect(
    fetchMock.mock.calls.some(([input, init]) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input.url;
      return url.endsWith("/api/launch/train") && init?.method === "POST";
    }),
  ).toBe(false);
});

test("blocks dataset-aware train launch when dataset readiness is not ready", async () => {
  renderWithProviders(
    <LaunchTrainDrawer
      datasetId="blocked_train_request"
      datasetLabel="Blocked Training Dataset"
      triggerLabel="Train from blocked dataset"
      title="Launch training from blocked dataset"
    />,
  );

  fireEvent.click(screen.getByText("Train from blocked dataset"));
  await waitFor(() => expect(screen.getByRole("option", { name: "Market" })).toBeInTheDocument());

  fireEvent.change(screen.getByLabelText("Feature Modality"), {
    target: { value: "market" },
  });
  fireEvent.click(screen.getByText("提交"));

  await waitFor(() =>
    expect(screen.getByText("This dataset is not trainable yet because readiness checks are still failing.")).toBeInTheDocument(),
  );
  expect(
    fetchMock.mock.calls.some(([input, init]) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input.url;
      return url.endsWith("/api/launch/train") && init?.method === "POST";
    }),
  ).toBe(false);
});
