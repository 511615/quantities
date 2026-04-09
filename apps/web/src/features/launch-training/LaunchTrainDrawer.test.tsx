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
            trainer_presets: [
              { value: "fast", label: "fast", description: null, recommended: true },
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
            recommended_next_actions: ["检查告警再训练"],
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
            recommended_next_actions: ["先回数据集详情页处理阻塞问题"],
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

test("submits train launch and shows run deeplink button", async () => {
  renderWithProviders(<LaunchTrainDrawer />);

  fireEvent.click(screen.getByText("发起训练"));
  await waitFor(() => expect(screen.getByText("elastic_net")).toBeInTheDocument());
  fireEvent.click(screen.getByText("提交"));

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
        .some((url) => url.endsWith("/api/launch/train")),
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
        .some((url) => url.endsWith("/api/jobs/job-train-1")),
    ).toBe(true),
  );
});

test("submits dataset-aware train launch with dataset_id and without preset selector", async () => {
  renderWithProviders(
    <LaunchTrainDrawer
      datasetId="cross_asset_training_panel_v2"
      datasetLabel="跨资产收益训练面板 / Cross Asset Training Panel"
      triggerLabel="基于此数据集训练"
      title="基于当前数据集发起训练"
    />,
  );

  fireEvent.click(screen.getByText("基于此数据集训练"));

  await waitFor(() =>
    expect(screen.getByText("跨资产收益训练面板 / Cross Asset Training Panel")).toBeInTheDocument(),
  );

  const drawer = screen.getByText("基于当前数据集发起训练").closest(".drawer-panel");
  expect(drawer).not.toBeNull();
  const drawerQueries = within(drawer as HTMLElement);
  expect(drawerQueries.getByText("当前训练数据集")).toBeInTheDocument();
  expect(drawerQueries.queryByText("数据集预置")).not.toBeInTheDocument();
  await waitFor(() =>
    expect(drawerQueries.getByText("这份数据集可以训练，但需要先留意")).toBeInTheDocument(),
  );

  fireEvent.click(drawerQueries.getByText("提交"));

  await waitFor(() => expect(screen.getByText("job-train-1")).toBeInTheDocument());

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
  const body = JSON.parse(String(requestInit.body)) as { dataset_id?: string };
  expect(body.dataset_id).toBe("cross_asset_training_panel_v2");
});

test("blocks dataset-aware train launch when readiness is not_ready", async () => {
  renderWithProviders(
    <LaunchTrainDrawer
      datasetId="blocked_train_request"
      datasetLabel="阻塞训练数据集"
      triggerLabel="基于此数据集训练"
      title="基于当前数据集发起训练"
    />,
  );

  fireEvent.click(screen.getByText("基于此数据集训练"));

  await waitFor(() => expect(screen.getByText("这份数据集暂不可训练")).toBeInTheDocument());

  fireEvent.click(screen.getByText("提交"));

  await waitFor(() =>
    expect(
      screen.getByText(
        "当前数据集还未通过训练就绪校验，请先检查数据集详情或回到训练数据集页重新选择。",
      ),
    ).toBeInTheDocument(),
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
