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
    expect(screen.getByRole("heading", { name: "Official Backtest Protocol v1" })).toBeInTheDocument(),
  );
  expect(screen.getByRole("heading", { name: "协议检查结果" })).toBeInTheDocument();
  expect(screen.getByRole("heading", { name: "仿真权益曲线" })).toBeInTheDocument();
  expect(screen.getByRole("heading", { name: "回测警告和泄漏检查" })).toBeInTheDocument();
  expect(screen.getByText("查看同模板对比")).toBeInTheDocument();
  expect(screen.getByText(/research and simulation/)).toBeInTheDocument();
  expect(screen.getAllByTestId("workbench-chart").length).toBeGreaterThan(0);
});
