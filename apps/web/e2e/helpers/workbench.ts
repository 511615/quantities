import { expect, type APIRequestContext, type Page } from "@playwright/test";

export const API_BASE = process.env.PLAYWRIGHT_API_BASE || "http://127.0.0.1:8015";
export const LIVE_WINDOW_DAYS = "30";

export type JobPayload = {
  job_id: string;
  status: string;
  result?: Record<string, unknown>;
  error_message?: string | null;
};

export async function fetchJson<T>(api: APIRequestContext, path: string): Promise<T> {
  let lastError: unknown = null;
  for (let attempt = 0; attempt < 3; attempt += 1) {
    try {
      const response = await api.get(path, { timeout: 30_000 });
      expect(response.ok(), `GET ${path} should succeed`).toBeTruthy();
      return (await response.json()) as T;
    } catch (error) {
      lastError = error;
      if (attempt === 2) {
        throw error;
      }
      await new Promise((resolve) => setTimeout(resolve, 2_000));
    }
  }
  throw lastError instanceof Error ? lastError : new Error(`GET ${path} failed.`);
}

export async function waitForJob(
  api: APIRequestContext,
  jobId: string,
  timeoutMs = 20 * 60 * 1000,
): Promise<JobPayload> {
  const startedAt = Date.now();
  for (;;) {
    try {
      const payload = await fetchJson<JobPayload>(api, `/api/jobs/${jobId}`);
      if (payload.status === "success") {
        return payload;
      }
      if (payload.status === "failed" || payload.status === "canceled") {
        throw new Error(payload.error_message || `Job ${jobId} ended with status ${payload.status}.`);
      }
    } catch (error) {
      if (Date.now() - startedAt > timeoutMs) {
        throw error;
      }
    }
    if (Date.now() - startedAt > timeoutMs) {
      throw new Error(`Timed out waiting for job ${jobId}.`);
    }
    await new Promise((resolve) => setTimeout(resolve, 2_000));
  }
}

export async function switchToEnglish(page: Page) {
  const englishButton = page.getByRole("button", { name: "EN" });
  if (await englishButton.isVisible()) {
    await englishButton.click();
  }
}

export async function openDatasetsPage(page: Page) {
  await page.goto("/datasets");
  await switchToEnglish(page);
  await expect(page.getByTestId("dataset-request-trigger").first()).toBeVisible();
}

export async function openModelsPage(page: Page) {
  await page.goto("/models?tab=trained");
  await switchToEnglish(page);
  await expect(page.getByTestId("trained-run-search")).toBeVisible();
}

export async function openRunDetailPage(page: Page, runId: string) {
  await page.goto(`/models/trained/${encodeURIComponent(runId)}`);
  await switchToEnglish(page);
  await expect(page.getByTestId("launch-backtest-trigger")).toBeVisible();
}

export async function submitAndCaptureJson<T>(
  page: Page,
  responseMatcher: (url: string, method: string) => boolean,
  action: () => Promise<void>,
): Promise<T> {
  const responsePromise = page.waitForResponse((response) =>
    responseMatcher(response.url(), response.request().method()),
  );
  await action();
  const response = await responsePromise;
  expect(response.ok(), `Request ${response.url()} should succeed`).toBeTruthy();
  return (await response.json()) as T;
}
