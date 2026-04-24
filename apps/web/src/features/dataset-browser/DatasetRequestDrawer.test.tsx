import { cleanup, fireEvent, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { DatasetRequestDrawer } from "./DatasetRequestDrawer";
import { createFetchMock, jsonResponse } from "../../test/mockApi";
import { renderWithProviders } from "../../test/renderWithProviders";

const datasetRequestOptions = {
  domains: [
    { value: "market", label: "Market", recommended: true },
    { value: "macro", label: "Macro" },
    { value: "on_chain", label: "On-chain" },
    { value: "derivatives", label: "Derivatives" },
    { value: "sentiment_events", label: "Sentiment Events" },
  ],
  asset_modes: [
    { value: "single_asset", label: "single_asset", recommended: true },
    { value: "multi_asset", label: "multi_asset" },
  ],
  symbol_types: [{ value: "spot", label: "spot", recommended: true }],
  selection_modes: [
    { value: "manual_list", label: "manual_list", recommended: true },
    { value: "top_n", label: "top_n" },
  ],
  source_vendors: [
    { value: "binance", label: "Binance", recommended: true },
    { value: "fred", label: "FRED" },
    { value: "defillama", label: "DeFiLlama" },
    { value: "binance_futures", label: "Binance Futures" },
    { value: "news_archive", label: "News Archive" },
    { value: "reddit_archive", label: "Reddit Archive" },
  ],
  exchanges: [
    { value: "binance", label: "Binance", recommended: true },
    { value: "fred", label: "FRED" },
    { value: "defillama", label: "DeFiLlama" },
  ],
  frequencies: [{ value: "1h", label: "1h", recommended: true }],
  feature_sets: [
    {
      value: "baseline_market_features",
      label: "Baseline Market Features",
      recommended: true,
    },
  ],
  label_horizons: [{ value: "1", label: "1 Bar", recommended: true }],
  split_strategies: [{ value: "time_series", label: "time_series", recommended: true }],
  sample_policies: [{ value: "balanced", label: "balanced", recommended: true }],
  alignment_policies: [
    { value: "entity_timestamp", label: "entity_timestamp", recommended: true },
  ],
  missing_feature_policies: [{ value: "fail", label: "fail", recommended: true }],
  domain_capabilities: {
    market: {
      supported_vendors: ["binance"],
      supported_frequencies: ["1h", "1d"],
      supported_exchanges: ["binance"],
      supported_symbol_types: ["spot"],
      supported_selection_modes: ["manual_list"],
    },
    macro: {
      supported_vendors: ["fred"],
      supported_frequencies: ["1d"],
    },
    on_chain: {
      supported_vendors: ["defillama"],
      supported_frequencies: ["1d"],
    },
    derivatives: {
      supported_vendors: ["binance_futures"],
      supported_frequencies: ["1h"],
    },
    sentiment_events: {
      supported_vendors: ["news_archive", "reddit_archive"],
      supported_frequencies: ["1h"],
    },
  },
  constraints: {},
};

const jobStatusById: Record<string, object> = {
  "job-dataset-multi": {
    job_id: "job-dataset-multi",
    job_type: "dataset_request",
    status: "success",
    created_at: "2026-04-09T00:00:00Z",
    updated_at: "2026-04-09T00:00:10Z",
    stages: [
      {
        name: "readiness",
        status: "success",
        summary: "ready",
        started_at: null,
        finished_at: null,
      },
    ],
    result: {
      dataset_id: "frontend-multi-dataset",
      run_ids: ["frontend-multi-run"],
      backtest_ids: [],
      benchmark_names: [],
      fit_result_uris: [],
      summary_artifacts: [],
      requested_stages: ["acquire", "prepare", "readiness"],
      deeplinks: {
        dataset_detail: "/datasets/frontend-multi-dataset",
        run_detail: "/runs/frontend-multi-run",
        backtest_detail: null,
        review_detail: null,
      },
      summary: {
        headline: "数据集申请已完成",
        detail: "市场锚点和合并数据集已经准备完成",
        highlights: ["market anchor ready", "readiness passed"],
      },
      pipeline_summary: {
        requested_stages: ["acquire", "prepare", "readiness"],
      },
    },
    error_message: null,
  },
  "job-dataset-single": {
    job_id: "job-dataset-single",
    job_type: "dataset_request",
    status: "success",
    created_at: "2026-04-09T00:00:00Z",
    updated_at: "2026-04-09T00:00:10Z",
    stages: [
      {
        name: "readiness",
        status: "success",
        summary: "ready",
        started_at: null,
        finished_at: null,
      },
    ],
    result: {
      dataset_id: "frontend-single-dataset",
      run_ids: [],
      backtest_ids: [],
      benchmark_names: [],
      fit_result_uris: [],
      summary_artifacts: [],
      requested_stages: ["acquire", "prepare", "readiness"],
      deeplinks: {
        dataset_detail: "/datasets/frontend-single-dataset",
        run_detail: null,
        backtest_detail: null,
        review_detail: null,
      },
      pipeline_summary: {
        requested_stages: ["acquire", "prepare", "readiness"],
      },
    },
    error_message: null,
  },
};

const defaultFetchMock = createFetchMock([
  (url) =>
    url.endsWith("/api/datasets/request-options")
      ? jsonResponse(datasetRequestOptions)
      : undefined,
  (url, init) => {
    if (!(url.endsWith("/api/datasets/requests") && init?.method === "POST")) {
      return undefined;
    }
    const body = JSON.parse(String(init.body)) as {
      sources?: { data_domain: string; source_vendor: string; frequency: string }[];
    };
    const isMultiDomain = (body.sources?.length ?? 0) > 1;
    const jobId = isMultiDomain ? "job-dataset-multi" : "job-dataset-single";
    return jsonResponse({
      job_id: jobId,
      status: "queued",
      job_api_path: `/api/jobs/${jobId}`,
      tracking_token: jobId,
      submitted_at: "2026-04-09T00:00:00Z",
    });
  },
  (url) => {
    const match = url.match(/\/api\/jobs\/(job-dataset-(?:multi|single))/);
    if (!match) {
      return undefined;
    }
    return jsonResponse(jobStatusById[match[1]]);
  },
]);

const fetchMock = vi.fn(defaultFetchMock);

beforeEach(() => {
  vi.stubGlobal("fetch", fetchMock);
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.unstubAllGlobals();
  fetchMock.mockReset();
  fetchMock.mockImplementation(defaultFetchMock);
});

test("submits multi-domain request and surfaces dataset/run deeplinks", async () => {
  const { container } = renderWithProviders(<DatasetRequestDrawer />);

  fireEvent.click(screen.getByText("申请新数据集"));

  await waitFor(() => screen.getByRole("button", { name: /添加数据域/i }));
  fireEvent.click(screen.getByRole("button", { name: /添加数据域/i }));
  fireEvent.change(screen.getByTestId("dataset-request-name"), {
    target: { value: "btc_multi_domain_live" },
  });

  const dateInputs = container.querySelectorAll('input[type="date"]');
  fireEvent.change(dateInputs[0], { target: { value: "2026-03-28" } });
  fireEvent.change(dateInputs[1], { target: { value: "2026-04-07" } });

  fireEvent.click(screen.getByText("提交数据请求"));

  await waitFor(() =>
    expect(
      fetchMock.mock.calls.some(([input, init]) => {
        const url =
          typeof input === "string"
            ? input
            : input instanceof URL
              ? input.toString()
              : input.url;
        if (!(url.endsWith("/api/datasets/requests") && init?.method === "POST")) {
          return false;
        }
        const body = JSON.parse(String(init.body)) as {
          sources: { data_domain: string; source_vendor: string; frequency: string }[];
          merge_policy_name?: string;
          request_name?: string;
        };
        return (
          Array.isArray(body.sources) &&
          body.request_name === "btc_multi_domain_live" &&
          body.sources.some(
            (source) =>
              source.data_domain === "market" &&
              source.source_vendor === "binance" &&
              source.frequency === "1d",
          ) &&
          body.sources.some(
            (source) =>
              source.data_domain === "macro" &&
              source.source_vendor === "fred" &&
              source.frequency === "1d",
          ) &&
          body.merge_policy_name === "available_time_safe_asof"
        );
      }),
    ).toBe(true),
  );

  const runLink = await screen.findByRole("link", { name: "查看运行详情" });
  expect(runLink).toHaveAttribute("href", "/runs/frontend-multi-run");
  expect(screen.getByRole("progressbar")).toHaveAttribute("aria-valuenow", "100");
  expect(screen.getByText("数据集申请已完成")).toBeInTheDocument();

  const datasetLink = screen.getByRole("link", { name: "查看数据集详情" });
  expect(datasetLink).toHaveAttribute("href", "/datasets/frontend-multi-dataset");
});

test("shows train CTA after single-domain request succeeds", async () => {
  renderWithProviders(<DatasetRequestDrawer />);

  fireEvent.click(screen.getByText("申请新数据集"));

  await waitFor(() => screen.getByText("提交数据请求"));
  fireEvent.click(screen.getByText("提交数据请求"));

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
        .some((url) => url.endsWith("/api/datasets/requests")),
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
        .some((url) => url.endsWith("/api/jobs/job-dataset-single")),
    ).toBe(true),
  );

  const trainLink = await screen.findByRole("link", { name: "继续这份数据集训练" });
  expect(trainLink).toHaveAttribute(
    "href",
    "/models?launchTrain=1&datasetId=frontend-single-dataset",
  );

  const datasetLink = screen.getByRole("link", { name: "查看数据集详情" });
  expect(datasetLink).toHaveAttribute("href", "/datasets/frontend-single-dataset");
});

test("canonical five-modality preset submits mixed-frequency real-source request", async () => {
  renderWithProviders(<DatasetRequestDrawer />);

  fireEvent.click(screen.getByRole("button", { name: "申请新数据集" }));
  await waitFor(() => screen.getByRole("button", { name: "使用五模态真实预设" }));

  fireEvent.click(screen.getByRole("button", { name: "使用五模态真实预设" }));
  fireEvent.click(screen.getByRole("button", { name: "提交数据请求" }));

  await waitFor(() =>
    expect(
      fetchMock.mock.calls.some(([input, init]) => {
        const url =
          typeof input === "string"
            ? input
            : input instanceof URL
              ? input.toString()
              : input.url;
        if (!(url.endsWith("/api/datasets/requests") && init?.method === "POST")) {
          return false;
        }
        const body = JSON.parse(String(init.body)) as {
          sources?: Array<{
            data_domain: string;
            source_vendor: string;
            frequency: string;
            identifier?: string | null;
          }>;
        };
        const sources = body.sources ?? [];
        return (
          sources.length === 5 &&
          sources.some(
            (source) =>
              source.data_domain === "market" &&
              source.source_vendor === "binance" &&
              source.frequency === "1h",
          ) &&
          sources.some(
            (source) =>
              source.data_domain === "macro" &&
              source.source_vendor === "fred" &&
              source.frequency === "1d" &&
              source.identifier === "DFF",
          ) &&
          sources.some(
            (source) =>
              source.data_domain === "on_chain" &&
              source.source_vendor === "defillama" &&
              source.frequency === "1d" &&
              source.identifier === "ethereum",
          ) &&
          sources.some(
            (source) =>
              source.data_domain === "derivatives" &&
              source.source_vendor === "binance_futures" &&
              source.frequency === "1h" &&
              source.identifier === "BTCUSDT",
          ) &&
          sources.some(
            (source) =>
              source.data_domain === "sentiment_events" &&
              source.source_vendor === "reddit_archive" &&
              source.frequency === "1h" &&
              source.identifier === "btc_news",
          )
        );
      }),
    ).toBe(true),
  );
});

test("uses current timestamp instead of end-of-day when request end date is today", async () => {
  fetchMock.mockClear();
  fetchMock.mockImplementation(defaultFetchMock);
  const today = new Date().toISOString().slice(0, 10);

  renderWithProviders(<DatasetRequestDrawer />);

  fireEvent.click(screen.getByRole("button", { name: "申请新数据集" }));
  await waitFor(() => screen.getByRole("button", { name: "提交数据请求" }));
  fireEvent.click(screen.getByRole("button", { name: "提交数据请求" }));

  await waitFor(() =>
    expect(
      fetchMock.mock.calls.some(([input, init]) => {
        const url =
          typeof input === "string"
            ? input
            : input instanceof URL
              ? input.toString()
              : input.url;
        if (!(url.endsWith("/api/datasets/requests") && init?.method === "POST")) {
          return false;
        }
        const body = JSON.parse(String(init.body)) as {
          time_window?: { start_time?: string; end_time?: string };
        };
        return (
          body.time_window?.start_time?.endsWith("T00:00:00Z") === true &&
          body.time_window?.end_time?.startsWith(`${today}T`) === true &&
          body.time_window?.end_time !== `${today}T23:59:59Z`
        );
      }),
    ).toBe(true),
  );
});

test("submits sentiment-only request without forcing a market anchor", async () => {
  renderWithProviders(
    <DatasetRequestDrawer
      initialValues={{
        dataDomain: "sentiment_events",
        sourceVendor: "news_archive",
        frequency: "1h",
      }}
    />,
  );

  fireEvent.click(screen.getByRole("button", { name: "申请新数据集" }));

  const identifierInput = await screen.findByDisplayValue("btc_news");
  fireEvent.change(identifierInput, { target: { value: "btc_news" } });
  fireEvent.click(screen.getByRole("button", { name: "提交数据请求" }));

  await waitFor(() =>
    expect(
      fetchMock.mock.calls.some(([input, init]) => {
        const url =
          typeof input === "string"
            ? input
            : input instanceof URL
              ? input.toString()
              : input.url;
        return url.endsWith("/api/datasets/requests") && init?.method === "POST";
      }),
    ).toBe(true),
  );

  const requestCall = fetchMock.mock.calls.find(([input, init]) => {
    const url =
      typeof input === "string"
        ? input
        : input instanceof URL
          ? input.toString()
          : input.url;
    return url.endsWith("/api/datasets/requests") && init?.method === "POST";
  });

  expect(requestCall).toBeTruthy();
  const [, requestInit] = requestCall as [string | URL | Request, RequestInit];
  const body = JSON.parse(String(requestInit.body)) as {
    data_domain?: string;
    sources?: Array<{
      data_domain: string;
      identifier?: string | null;
    }>;
  };
  expect(body.data_domain).toBe("sentiment_events");
  expect(body.sources?.length).toBe(1);
  expect(body.sources?.[0]?.data_domain).toBe("sentiment_events");
  expect(body.sources?.[0]?.identifier).toBe("btc_news");
});

test("shows readable validation details when backend returns 422", async () => {
  fetchMock.mockImplementation(
    createFetchMock([
      (url) =>
        url.endsWith("/api/datasets/request-options")
          ? jsonResponse(datasetRequestOptions)
          : undefined,
      (url, init) =>
        url.endsWith("/api/datasets/requests") && init?.method === "POST"
          ? jsonResponse(
              {
                detail: [
                  {
                    loc: ["body", "sources", 1, "source_vendor"],
                    msg: "Field required",
                    type: "missing",
                  },
                ],
              },
              { status: 422 },
            )
          : undefined,
    ]),
  );

  renderWithProviders(<DatasetRequestDrawer />);

  fireEvent.click(screen.getByText("申请新数据集"));
  await waitFor(() => screen.getByRole("button", { name: /添加数据域/i }));

  fireEvent.click(screen.getByRole("button", { name: /添加数据域/i }));
  fireEvent.click(screen.getByText("提交数据请求"));

  expect(
    await screen.findByText("body.sources.1.source_vendor: Field required"),
  ).toBeInTheDocument();
});
