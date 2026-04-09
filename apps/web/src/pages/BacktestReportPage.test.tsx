import { render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { App } from "../app/App";
import { backtestDetailFixture } from "../test/fixtures";
import { createFetchMock, jsonResponse } from "../test/mockApi";

vi.mock("../shared/ui/WorkbenchChart", () => ({
  WorkbenchChart: ({ loadingLabel }: { loadingLabel?: string }) => (
    <div data-testid="workbench-chart">{loadingLabel ?? "chart"}</div>
  ),
}));

const fetchMock = vi.fn(
  createFetchMock([
    (url) =>
      url.endsWith("/api/backtests/smoke-backtest")
        ? jsonResponse(backtestDetailFixture)
        : undefined,
  ]),
);

beforeEach(() => {
  vi.stubGlobal("fetch", fetchMock);
  window.history.pushState({}, "", "/backtests/smoke-backtest");
});

afterEach(() => {
  vi.unstubAllGlobals();
  fetchMock.mockClear();
});

test("renders backtest report with warnings and chart sections", async () => {
  render(<App />);

  await waitFor(() =>
    expect(screen.getByRole("heading", { name: "\u5173\u952e\u6307\u6807\u5bf9\u6bd4" })).toBeInTheDocument(),
  );
  expect(screen.getByRole("heading", { name: "\u6301\u4ed3\u8f68\u8ff9" })).toBeInTheDocument();
  expect(screen.getByRole("heading", { name: "\u8bca\u65ad\u4e0e\u544a\u8b66" })).toBeInTheDocument();
  expect(screen.getByText(/research and simulation/)).toBeInTheDocument();
  expect(screen.getAllByTestId("workbench-chart").length).toBeGreaterThan(0);
});
