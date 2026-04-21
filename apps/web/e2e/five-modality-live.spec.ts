import { expect, request, test, type APIRequestContext, type Page } from "@playwright/test";

import { ensureQualityBlockedMarketRun } from "./helpers/artifacts";
import {
  fetchJson,
  LIVE_WINDOW_DAYS,
  openDatasetsPage,
  openModelsPage,
  openRunDetailPage,
  submitAndCaptureJson,
  switchToEnglish,
  waitForJob,
} from "./helpers/workbench";

const MODALITIES = ["market", "macro", "on_chain", "derivatives", "nlp"] as const;
const CANONICAL_DATASET_PATTERN = /^canonical_five_modality_live_debug_\d+_fusion$/;

type Modality = (typeof MODALITIES)[number];

type DatasetListResponse = {
  items: Array<{
    dataset_id: string;
  }>;
};

type RunListResponse = {
  items: Array<{
    run_id: string;
    dataset_id?: string | null;
    status?: string | null;
    feature_scope_modality?: string | null;
    source_dataset_quality_status?: string | null;
  }>;
};

type DatasetReadinessSummaryView = {
  feature_count: number;
  modality_quality_summary: Record<
    string,
    {
      status: string;
      blocking_reasons: string[];
    }
  >;
  aligned_multimodal_quality: {
    status: string;
  };
};

type LaunchJobResponse = {
  job_id: string;
};

type BacktestReportView = {
  backtest_id: string;
  dataset_ids: string[];
  protocol: {
    required_modalities: string[];
    official_dataset_ids: string[];
  };
  artifacts: Array<{
    kind: string;
    exists: boolean;
  }>;
};

type RunDetailView = {
  run_id: string;
  backend?: string | null;
  composition?: {
    fusion_strategy?: string | null;
    source_runs?: Array<{
      run_id: string;
      modality?: string | null;
    }>;
  } | null;
  tracking_params?: Record<string, string>;
  artifacts: Array<{
    kind: string;
    uri: string;
    exists: boolean;
  }>;
};

type ArtifactPreviewResponse = {
  uri: string;
  kind: string;
  is_json: boolean;
  content: Record<string, unknown>;
};

const suiteState: {
  datasetId: string | null;
  runIds: Partial<Record<Modality, string>>;
  compositionRunIds: Record<string, string>;
  backtestIds: Record<string, string>;
  qualityBlockedRunId: string | null;
} = {
  datasetId: null,
  runIds: {},
  compositionRunIds: {},
  backtestIds: {},
  qualityBlockedRunId: null,
};

let api: APIRequestContext;

test.describe.configure({ mode: "serial" });

test.beforeAll(async () => {
  api = await request.newContext({
    baseURL: process.env.PLAYWRIGHT_API_BASE || "http://127.0.0.1:8015",
  });
});

test.afterAll(async () => {
  await api.dispose();
});

async function findReusableCanonicalDataset() {
  const payload = await fetchJson<DatasetListResponse>(api, "/api/datasets?page=1&per_page=100");
  for (const item of payload.items) {
    if (!CANONICAL_DATASET_PATTERN.test(item.dataset_id)) {
      continue;
    }
    const readiness = await fetchJson<DatasetReadinessSummaryView>(
      api,
      `/api/datasets/${encodeURIComponent(item.dataset_id)}/readiness`,
    );
    const readyStatuses = MODALITIES.map((modality) => readiness.modality_quality_summary[modality]?.status);
    if (
      readiness.feature_count >= 28 &&
      readiness.aligned_multimodal_quality.status === "ready" &&
      readyStatuses.every((status) => status === "ready")
    ) {
      return item.dataset_id;
    }
  }
  return null;
}

async function ensureCanonicalDataset(page: Page) {
  if (suiteState.datasetId) {
    return suiteState.datasetId;
  }
  const reusableDatasetId = await findReusableCanonicalDataset();
  if (reusableDatasetId) {
    suiteState.datasetId = reusableDatasetId;
    return reusableDatasetId;
  }

  await openDatasetsPage(page);
  await page.getByTestId("dataset-request-trigger").first().click();
  await expect(page.getByTestId("canonical-five-modality-preset")).toBeVisible();
  await page.getByTestId("canonical-five-modality-preset").click();

  const launch = await submitAndCaptureJson<LaunchJobResponse>(
    page,
    (url, method) => method === "POST" && url.endsWith("/api/datasets/requests"),
    async () => {
      await page.getByTestId("submit-dataset-request").click();
    },
  );
  const job = await waitForJob(api, launch.job_id);
  const datasetId = String(job.result?.dataset_id || "");
  expect(datasetId).toBeTruthy();
  suiteState.datasetId = datasetId;
  return datasetId;
}

async function assertCanonicalDatasetQuality(page: Page, datasetId: string) {
  const readiness = await fetchJson<DatasetReadinessSummaryView>(
    api,
    `/api/datasets/${encodeURIComponent(datasetId)}/readiness`,
  );

  expect(readiness.feature_count).toBeGreaterThanOrEqual(28);
  expect(readiness.aligned_multimodal_quality.status).toBe("ready");
  for (const modality of MODALITIES) {
    expect(readiness.modality_quality_summary[modality]?.status).toBe("ready");
  }

  await page.goto(`/datasets/${encodeURIComponent(datasetId)}`);
  await switchToEnglish(page);
  await expect(page.getByTestId("launch-train-trigger")).toBeVisible();
  await expect(page.getByTestId("launch-dataset-multimodal-train-trigger")).toBeVisible();
  await expect(page.getByText("aligned_multimodal", { exact: false })).toBeVisible();
}

async function trainSingleModalityRun(page: Page, datasetId: string, modality: Modality) {
  await page.goto(`/datasets/${encodeURIComponent(datasetId)}`);
  await switchToEnglish(page);
  await page.getByTestId("launch-train-trigger").click();
  await expect(page.getByTestId("feature-modality-select")).toBeVisible();
  await page.getByTestId("feature-modality-select").selectOption(modality);
  await expect(page.getByTestId("submit-train-launch")).toBeEnabled();

  const launch = await submitAndCaptureJson<LaunchJobResponse>(
    page,
    (url, method) => method === "POST" && url.endsWith("/api/launch/train"),
    async () => {
      await page.getByTestId("submit-train-launch").click();
    },
  );
  const job = await waitForJob(api, launch.job_id, 10 * 60 * 1000);
  const runId = String(job.result?.run_ids?.[0] || "");
  expect(runId).toBeTruthy();
  suiteState.runIds[modality] = runId;
  return runId;
}

async function findReusableSingleModalityRun(datasetId: string, modality: Modality) {
  const payload = await fetchJson<RunListResponse>(api, "/api/runs?page=1&per_page=100");
  for (const item of payload.items) {
    if (item.dataset_id !== datasetId) {
      continue;
    }
    if (item.status !== "success") {
      continue;
    }
    if (item.feature_scope_modality !== modality) {
      continue;
    }
    if (item.source_dataset_quality_status !== "ready") {
      continue;
    }
    return item.run_id;
  }
  return null;
}

async function ensureSingleModalityRun(page: Page, datasetId: string, modality: Modality) {
  if (suiteState.runIds[modality]) {
    return suiteState.runIds[modality] as string;
  }
  const reusableRunId = await findReusableSingleModalityRun(datasetId, modality);
  if (reusableRunId) {
    suiteState.runIds[modality] = reusableRunId;
    return reusableRunId;
  }
  return trainSingleModalityRun(page, datasetId, modality);
}

async function createComposition(page: Page, runIds: string[], compositionName: string) {
  await openModelsPage(page);
  await page.getByTestId("trained-run-search").fill("");
  for (const runId of runIds) {
    await page.getByRole("checkbox", { name: `Select ${runId} for multimodal composition` }).check();
  }
  await page.getByTestId("composition-name-input").fill(compositionName);
  const launch = await submitAndCaptureJson<LaunchJobResponse>(
    page,
    (url, method) => method === "POST" && url.endsWith("/api/launch/model-composition"),
    async () => {
      await page.getByTestId("launch-composition-button").click();
    },
  );
  const job = await waitForJob(api, launch.job_id, 5 * 60 * 1000);
  const runId = String(job.result?.run_ids?.[0] || "");
  expect(runId).toBeTruthy();
  return runId;
}

async function launchOfficialBacktest(page: Page, runId: string) {
  await openRunDetailPage(page, runId);
  const officialModeToggle = page.getByTestId("backtest-mode-official");
  if (!(await officialModeToggle.isVisible().catch(() => false))) {
    await page.getByTestId("launch-backtest-trigger").click({ force: true });
  }
  await expect(officialModeToggle).toBeVisible();
  await officialModeToggle.click();
  await page.getByTestId("official-window-days-select").selectOption(LIVE_WINDOW_DAYS);
  await expect(page.getByTestId("submit-backtest-launch")).toBeEnabled({
    timeout: 2 * 60 * 1000,
  });

  const launch = await submitAndCaptureJson<LaunchJobResponse>(
    page,
    (url, method) => method === "POST" && url.endsWith("/api/launch/backtest"),
    async () => {
      await page.getByTestId("submit-backtest-launch").click();
    },
  );
  const job = await waitForJob(api, launch.job_id, 10 * 60 * 1000);
  const backtestId = String(job.result?.backtest_ids?.[0] || "");
  expect(backtestId).toBeTruthy();
  return backtestId;
}

async function launchDatasetDetailLoop(page: Page, datasetId: string, modalities: Modality[]) {
  await page.goto(`/datasets/${encodeURIComponent(datasetId)}`);
  await switchToEnglish(page);
  await page.getByTestId("launch-dataset-multimodal-train-trigger").click();
  await expect(page.getByTestId("dataset-multimodal-submit")).toBeVisible();

  for (const modality of modalities) {
    await page.getByTestId(`multimodal-modality-${modality}`).check();
    const templateSelect = page.getByTestId(`multimodal-template-${modality}`);
    if ((await templateSelect.inputValue()) === "") {
      await templateSelect.selectOption({ index: 1 });
    }
  }

  const autoBacktestToggle = page.getByTestId("dataset-multimodal-auto-backtest-toggle");
  if (!(await autoBacktestToggle.isChecked())) {
    await autoBacktestToggle.check();
  }
  await page.getByTestId("dataset-multimodal-official-window").selectOption(LIVE_WINDOW_DAYS);

  const launch = await submitAndCaptureJson<LaunchJobResponse>(
    page,
    (url, method) => method === "POST" && url.endsWith("/api/launch/dataset-multimodal-train"),
    async () => {
      await page.getByTestId("dataset-multimodal-submit").click();
    },
  );
  const job = await waitForJob(api, launch.job_id, 20 * 60 * 1000);
  const composedRunId = String(job.result?.run_ids?.[0] || "");
  const backtestId = String(job.result?.backtest_ids?.[0] || "");
  expect(composedRunId).toBeTruthy();
  expect(backtestId).toBeTruthy();
  return { composedRunId, backtestId };
}

async function fetchBacktest(backtestId: string) {
  return fetchJson<BacktestReportView>(api, `/api/backtests/${encodeURIComponent(backtestId)}`);
}

async function fetchRunDetail(runId: string) {
  return fetchJson<RunDetailView>(api, `/api/runs/${encodeURIComponent(runId)}`);
}

async function fetchArtifactPreview(uri: string) {
  return fetchJson<ArtifactPreviewResponse>(
    api,
    `/api/artifacts/preview?uri=${encodeURIComponent(uri)}`,
  );
}

test("dataset detail one-click flow reaches official backtest", async ({ page }) => {
  const datasetId = await ensureCanonicalDataset(page);
  await assertCanonicalDatasetQuality(page, datasetId);

  const { composedRunId, backtestId } = await launchDatasetDetailLoop(page, datasetId, [...MODALITIES]);
  suiteState.compositionRunIds.oneClickAll5 = composedRunId;
  suiteState.backtestIds.oneClickAll5 = backtestId;

  const runDetail = await fetchRunDetail(composedRunId);
  expect(runDetail.backend).toBe("attention_late_fusion");
  expect(runDetail.composition?.fusion_strategy).toBe("attention_late_fusion");
  const modelMetadataArtifact = runDetail.artifacts.find((artifact) => artifact.kind === "model_metadata");
  expect(modelMetadataArtifact?.exists).toBe(true);
  const metadataPreview = await fetchArtifactPreview(String(modelMetadataArtifact?.uri));
  const predictionMetadata = (metadataPreview.content?.prediction_metadata ?? {}) as Record<string, unknown>;
  expect(predictionMetadata.fusion_strategy).toBe("attention_late_fusion");
  expect(predictionMetadata.requested_fusion_strategy).toBe("attention_late_fusion");
  expect(predictionMetadata.effective_fusion_strategy).toBe("attention_late_fusion");
  expect(typeof predictionMetadata.explainability_uri).toBe("string");

  const backtest = await fetchBacktest(backtestId);
  expect(backtest.protocol.required_modalities).toEqual(MODALITIES);
  expect(backtest.protocol.official_dataset_ids).toEqual([
    "baseline_real_benchmark_dataset",
    "official_reddit_pullpush_multimodal_v2_fusion",
  ]);
  expect(backtest.artifacts.some((artifact) => artifact.kind === "research_result" && artifact.exists)).toBe(true);

  await page.goto(`/backtests/${encodeURIComponent(backtestId)}`);
  await switchToEnglish(page);
  await expect(page.getByRole("heading", { name: backtestId })).toBeVisible();
});

test("main five-modality user path works end-to-end", async ({ page }) => {
  const datasetId = await ensureCanonicalDataset(page);
  await assertCanonicalDatasetQuality(page, datasetId);

  for (const modality of MODALITIES) {
    await ensureSingleModalityRun(page, datasetId, modality);
  }

  const compositionRunId = await createComposition(
    page,
    MODALITIES.map((modality) => suiteState.runIds[modality] as string),
    `Playwright Five Modality ${Date.now()}`,
  );
  suiteState.compositionRunIds.all5 = compositionRunId;

  const runDetail = await fetchRunDetail(compositionRunId);
  expect(runDetail.backend).toBe("attention_late_fusion");
  expect(runDetail.composition?.fusion_strategy).toBe("attention_late_fusion");
  const modelMetadataArtifact = runDetail.artifacts.find((artifact) => artifact.kind === "model_metadata");
  expect(modelMetadataArtifact?.exists).toBe(true);
  const metadataPreview = await fetchArtifactPreview(String(modelMetadataArtifact?.uri));
  const predictionMetadata = (metadataPreview.content?.prediction_metadata ?? {}) as Record<string, unknown>;
  expect(predictionMetadata.fusion_strategy).toBe("attention_late_fusion");

  const backtestId = await launchOfficialBacktest(page, compositionRunId);
  suiteState.backtestIds.all5 = backtestId;

  const runDetailAfterBacktest = await fetchRunDetail(compositionRunId);
  expect(runDetailAfterBacktest.composition?.fusion_strategy).toBe("attention_late_fusion");
  const explainabilityArtifactUri = `C:\\Users\\1\\Desktop\\AI\\quantities_economy\\artifacts\\predictions\\${compositionRunId}\\test_explainability.json`;
  const explainabilityPreview = await fetchArtifactPreview(explainabilityArtifactUri);
  const explainabilityRows = (explainabilityPreview.content?.rows ?? []) as Array<Record<string, unknown>>;
  expect(explainabilityRows.length).toBeGreaterThan(0);
  const firstAttentionWeights = (explainabilityRows[0]?.attention_weights ?? {}) as Record<string, number>;
  expect(Object.keys(firstAttentionWeights).length).toBe(MODALITIES.length);
  const weightSum = Object.values(firstAttentionWeights).reduce((sum, value) => sum + Number(value), 0);
  expect(weightSum).toBeCloseTo(1.0, 5);

  const backtest = await fetchBacktest(backtestId);
  expect(backtest.protocol.required_modalities).toEqual(MODALITIES);
  expect(backtest.protocol.official_dataset_ids).toEqual([
    "baseline_real_benchmark_dataset",
    "official_reddit_pullpush_multimodal_v2_fusion",
  ]);
  expect(backtest.artifacts.some((artifact) => artifact.kind === "research_result" && artifact.exists)).toBe(true);

  await page.goto(`/backtests/${encodeURIComponent(backtestId)}`);
  await switchToEnglish(page);
  await expect(page.getByRole("heading", { name: backtestId })).toBeVisible();
  await expect(page.getByRole("link", { name: "baseline_real_benchmark_dataset" }).first()).toBeVisible();
  await expect(
    page.getByText("official_reddit_pullpush_multimodal_v2_fusion", { exact: false }).first(),
  ).toBeVisible();
});

test("market + nlp official combination backtests successfully", async ({ page }) => {
  const compositionRunId = await createComposition(
    page,
    [suiteState.runIds.market as string, suiteState.runIds.nlp as string],
    `Playwright Market NLP ${Date.now()}`,
  );
  suiteState.compositionRunIds.marketNlp = compositionRunId;
  const backtestId = await launchOfficialBacktest(page, compositionRunId);
  suiteState.backtestIds.marketNlp = backtestId;

  const backtest = await fetchBacktest(backtestId);
  expect(backtest.protocol.required_modalities).toEqual(["market", "nlp"]);
  expect(backtest.protocol.official_dataset_ids).toEqual([
    "baseline_real_benchmark_dataset",
    "official_reddit_pullpush_multimodal_v2_fusion",
  ]);
});

test("market + macro + on-chain official combination backtests successfully", async ({ page }) => {
  const compositionRunId = await createComposition(
    page,
    [
      suiteState.runIds.market as string,
      suiteState.runIds.macro as string,
      suiteState.runIds.on_chain as string,
    ],
    `Playwright Market Macro OnChain ${Date.now()}`,
  );
  suiteState.compositionRunIds.marketMacroOnChain = compositionRunId;
  const backtestId = await launchOfficialBacktest(page, compositionRunId);
  suiteState.backtestIds.marketMacroOnChain = backtestId;

  const backtest = await fetchBacktest(backtestId);
  expect(backtest.protocol.required_modalities).toEqual(["market", "macro", "on_chain"]);
  expect(backtest.protocol.official_dataset_ids).toEqual([
    "baseline_real_benchmark_dataset",
    "official_reddit_pullpush_multimodal_v2_fusion",
  ]);
});

test("auxiliary-only official combination auto-injects market anchor", async ({ page }) => {
  const compositionRunId = await createComposition(
    page,
    [
      suiteState.runIds.macro as string,
      suiteState.runIds.on_chain as string,
      suiteState.runIds.derivatives as string,
    ],
    `Playwright Auxiliary Only ${Date.now()}`,
  );
  suiteState.compositionRunIds.auxOnly = compositionRunId;
  const backtestId = await launchOfficialBacktest(page, compositionRunId);
  suiteState.backtestIds.auxOnly = backtestId;

  const backtest = await fetchBacktest(backtestId);
  expect(backtest.protocol.required_modalities).toEqual(["macro", "on_chain", "derivatives"]);
  expect(backtest.dataset_ids.some((datasetId) => datasetId.endsWith("__market_anchor_official_30d"))).toBe(true);
  expect(backtest.protocol.official_dataset_ids).toEqual([
    "baseline_real_benchmark_dataset",
    "official_reddit_pullpush_multimodal_v2_fusion",
  ]);
});

test("quality gates block training, composition, and official backtest", async ({ page }) => {
  const blockedRunId = `pw-quality-block-market-${Date.now()}`;
  const blockedReason = "Market usable 1h bars 6 is below 4000.";
  await ensureQualityBlockedMarketRun(blockedRunId);
  suiteState.qualityBlockedRunId = blockedRunId;

  await page.goto("/datasets/smoke_dataset");
  await switchToEnglish(page);
  await page.getByTestId("launch-train-trigger").click();
  await page.getByTestId("feature-modality-select").selectOption("market");
  await page.getByTestId("submit-train-launch").click();
  await expect(page.getByText(blockedReason).first()).toBeVisible();

  await openModelsPage(page);
  await page.getByTestId("trained-run-search").fill(blockedRunId);
  const blockedCheckbox = page.getByRole("checkbox", {
    name: `Select ${blockedRunId} for multimodal composition`,
  });
  await expect(blockedCheckbox).toBeDisabled();
  await expect(page.getByText("Source dataset quality is Failed.")).toBeVisible();

  const blockedRow = page.getByRole("row", { name: new RegExp(blockedRunId) });
  await blockedRow.getByRole("button", { name: "Launch Backtest" }).click();
  await page.getByTestId("backtest-mode-official").click();
  await expect(page.getByTestId("submit-backtest-launch")).toBeDisabled();
  await expect(page.getByText(blockedReason).first()).toBeVisible();
});
