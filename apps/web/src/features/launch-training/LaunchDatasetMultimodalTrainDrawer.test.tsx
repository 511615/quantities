import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { LaunchDatasetMultimodalTrainDrawer } from "./LaunchDatasetMultimodalTrainDrawer";
import { datasetReadinessFixture } from "../../test/fixtures";
import { createFetchMock, jsonResponse } from "../../test/mockApi";

const fetchMock = vi.fn(
  createFetchMock([
    (url) =>
      url.endsWith("/api/launch/train/options")
        ? jsonResponse({
            dataset_presets: [],
            model_options: [],
            template_options: [
              {
                value: "registry::elastic_net",
                label: "Elastic Net default",
                description: "Template sourced from model registry.",
                recommended: true,
              },
            ],
            trainer_presets: [{ value: "fast", label: "Fast", description: null, recommended: true }],
            feature_scope_modalities: [
              { value: "market", label: "Market", description: null, recommended: true },
              { value: "macro", label: "Macro", description: null, recommended: false },
            ],
            default_seed: 7,
            constraints: {},
          })
        : undefined,
    (url) =>
      url.endsWith("/api/launch/backtest/options")
        ? jsonResponse({
            official_template_id: "system::official_backtest_protocol_v1",
            official_window_options: [
              { value: "30", label: "Recent 30d", description: null, recommended: true },
              { value: "180", label: "Recent 180d", description: null, recommended: false },
            ],
            dataset_presets: [],
            prediction_scopes: [{ value: "test", label: "test", description: null, recommended: true }],
            strategy_presets: [{ value: "sign", label: "sign", description: null, recommended: true }],
            portfolio_presets: [
              { value: "research_default", label: "research_default", description: null, recommended: true },
            ],
            cost_presets: [{ value: "standard", label: "standard", description: null, recommended: true }],
            research_backends: [{ value: "native", label: "native", description: null, recommended: true }],
            portfolio_methods: [{ value: "proportional", label: "proportional", description: null, recommended: true }],
            default_benchmark_symbol: "BTCUSDT",
            constraints: {},
          })
        : undefined,
    (url) =>
      url.endsWith("/api/datasets/smoke_dataset/readiness")
        ? jsonResponse({
            ...datasetReadinessFixture,
            readiness_status: "ready",
            modality_quality_summary: {
              ...datasetReadinessFixture.modality_quality_summary!,
              nlp: {
                ...datasetReadinessFixture.modality_quality_summary!.nlp,
                status: "failed",
                blocking_reasons: ["NLP event buckets 120 is below 500."],
              },
            },
          })
        : undefined,
    (url, init) =>
      url.endsWith("/api/launch/dataset-multimodal-train") && init?.method === "POST"
        ? jsonResponse({
            job_id: "job-dataset-multimodal-loop",
            status: "queued",
            job_api_path: "/api/jobs/job-dataset-multimodal-loop",
            tracking_token: "job-dataset-multimodal-loop",
            submitted_at: "2026-04-18T00:00:00Z",
          })
        : undefined,
    (url) =>
      url.endsWith("/api/jobs/job-dataset-multimodal-loop")
        ? jsonResponse({
            job_id: "job-dataset-multimodal-loop",
            job_type: "dataset_multimodal_train",
            status: "success",
            created_at: "2026-04-18T00:00:00Z",
            updated_at: "2026-04-18T00:00:04Z",
            stages: [
              {
                name: "compose",
                status: "success",
                summary: "Composed run is ready",
                started_at: "2026-04-18T00:00:01Z",
                finished_at: "2026-04-18T00:00:02Z",
              },
              {
                name: "backtest",
                status: "success",
                summary: "Completed 1 official backtest run",
                started_at: "2026-04-18T00:00:02Z",
                finished_at: "2026-04-18T00:00:04Z",
              },
            ],
            result: {
              dataset_id: "smoke_dataset",
              dataset_ids: ["smoke_dataset", "baseline_real_benchmark_dataset"],
              run_ids: ["multimodal-compose-123", "market-run-1", "macro-run-1"],
              backtest_ids: ["backtest-123"],
              benchmark_names: [],
              fit_result_uris: [],
              summary_artifacts: [],
              deeplinks: {
                dataset_detail: "/datasets/smoke_dataset",
                run_detail: "/runs/multimodal-compose-123",
                backtest_detail: "/backtests/backtest-123",
                review_detail: null,
              },
              result_links: [],
              summary: null,
              pipeline_summary: null,
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

function renderDrawer() {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
        refetchOnWindowFocus: false,
      },
    },
  });

  render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <LaunchDatasetMultimodalTrainDrawer
          datasetId="smoke_dataset"
          datasetLabel="Smoke Dataset"
          datasetModalities={["market", "macro", "nlp"]}
        />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

test("submits selected modalities with automatic official backtest and blocks failed modalities", async () => {
  renderDrawer();

  fireEvent.click(screen.getByTestId("launch-dataset-multimodal-train-trigger"));

  const marketCheckbox = await screen.findByTestId("multimodal-modality-market");
  const macroCheckbox = await screen.findByTestId("multimodal-modality-macro");
  const nlpCheckbox = await screen.findByTestId("multimodal-modality-nlp");

  await waitFor(() => {
    expect(marketCheckbox).not.toBeDisabled();
    expect(macroCheckbox).not.toBeDisabled();
    expect(nlpCheckbox).toBeDisabled();
  });

  fireEvent.click(marketCheckbox);
  fireEvent.click(macroCheckbox);
  fireEvent.click(screen.getByTestId("dataset-multimodal-submit"));

  await waitFor(() => {
    const launchCall = fetchMock.mock.calls.find(([input, init]) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input.url;
      return url.endsWith("/api/launch/dataset-multimodal-train") && init?.method === "POST";
    });
    expect(launchCall).toBeTruthy();
    const [, init] = launchCall as [RequestInfo | URL, RequestInit];
    const payload = JSON.parse(String(init.body));
    expect(payload.selected_modalities).toEqual(["market", "macro"]);
    expect(payload.fusion_strategy).toBe("attention_late_fusion");
    expect(payload.auto_launch_official_backtest).toBe(true);
    expect(payload.official_window_days).toBe(30);
  });

  await waitFor(() =>
    expect(screen.getByRole("link", { name: "鎵撳紑瀹樻柟鍥炴祴" })).toHaveAttribute(
      "href",
      "/backtests/backtest-123",
    ),
  );
});

test("allows switching back to late_score_blend explicitly", async () => {
  renderDrawer();

  fireEvent.click(screen.getByTestId("launch-dataset-multimodal-train-trigger"));

  const marketCheckbox = await screen.findByTestId("multimodal-modality-market");
  const macroCheckbox = await screen.findByTestId("multimodal-modality-macro");

  fireEvent.click(marketCheckbox);
  fireEvent.click(macroCheckbox);
  fireEvent.change(screen.getByTestId("dataset-multimodal-fusion-strategy"), {
    target: { value: "late_score_blend" },
  });
  fireEvent.click(screen.getByTestId("dataset-multimodal-submit"));

  await waitFor(() => {
    const launchCall = fetchMock.mock.calls.find(([input, init]) => {
      const url =
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.toString()
            : input.url;
      return url.endsWith("/api/launch/dataset-multimodal-train") && init?.method === "POST";
    });
    expect(launchCall).toBeTruthy();
    const [, init] = launchCall as [RequestInfo | URL, RequestInit];
    const payload = JSON.parse(String(init.body));
    expect(payload.fusion_strategy).toBe("late_score_blend");
  });
});

