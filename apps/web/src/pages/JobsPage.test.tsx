import { fireEvent, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { JobsPage } from "./JobsPage";
import {
  backtestOptionsFixture,
  jobStatusFixture,
  jobsFixture,
  launchJobFixture,
  trainOptionsFixture,
} from "../test/fixtures";
import { createFetchMock, jsonResponse } from "../test/mockApi";
import { renderWithProviders } from "../test/renderWithProviders";

const fetchMock = vi.fn(
  createFetchMock([
    (url, init) => {
      if (url.endsWith("/api/jobs") && (!init?.method || init.method === "GET")) {
        return jsonResponse(jobsFixture);
      }
      return undefined;
    },
    (url) =>
      url.endsWith("/api/launch/train/options")
        ? jsonResponse(trainOptionsFixture)
        : undefined,
    (url) =>
      url.endsWith("/api/launch/backtest/options")
        ? jsonResponse(backtestOptionsFixture)
        : undefined,
    (url, init) =>
      url.endsWith("/api/launch/train") && init?.method === "POST"
        ? jsonResponse(launchJobFixture)
        : undefined,
    (url) =>
      url.endsWith("/api/jobs/job-new-1")
        ? jsonResponse(jobStatusFixture)
        : undefined,
  ]),
);

beforeEach(() => {
  vi.stubGlobal("fetch", fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
  fetchMock.mockClear();
});

test("renders jobs page with launch controls and recent jobs", async () => {
  renderWithProviders(<JobsPage />, "/jobs");

  await waitFor(() =>
    expect(screen.getByRole("heading", { name: "\u4efb\u52a1\u4e2d\u5fc3" })).toBeInTheDocument(),
  );
  expect(screen.getByRole("button", { name: "\u53d1\u8d77\u8bad\u7ec3" })).toBeInTheDocument();
  expect(screen.getAllByText("job-train-1").length).toBeGreaterThan(0);
});

test("submits train job and renders result deeplink area", async () => {
  renderWithProviders(<JobsPage />, "/jobs");

  await waitFor(() =>
    expect(screen.getByRole("button", { name: "\u53d1\u8d77\u8bad\u7ec3" })).toBeInTheDocument(),
  );
  fireEvent.click(screen.getByRole("button", { name: "\u53d1\u8d77\u8bad\u7ec3" }));

  await waitFor(() =>
    expect(screen.getByRole("link", { name: "\u8df3\u8f6c\u8fd0\u884c\u8be6\u60c5" })).toBeInTheDocument(),
  );
});

test("shows unsupported message for data task placeholders", async () => {
  renderWithProviders(<JobsPage />, "/jobs");

  await waitFor(() =>
    expect(screen.getAllByRole("button", { name: "\u63d0\u4ea4\u884c\u60c5\u540c\u6b65" }).length).toBeGreaterThan(0),
  );
  fireEvent.click(screen.getAllByRole("button", { name: "\u63d0\u4ea4\u884c\u60c5\u540c\u6b65" })[0]);
  expect(screen.getByText("\u8be5\u63a5\u53e3\u672a\u5c31\u7eea\u3002")).toBeInTheDocument();
});
