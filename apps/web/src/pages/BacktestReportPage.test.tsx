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
      url.endsWith(`/api/backtests/${activeBacktestId}`)
        ? jsonResponse(activeFixture)
        : undefined,
  ]),
);

let activeFixture = backtestDetailFixture;
let activeBacktestId = "smoke-backtest";

beforeEach(() => {
  vi.stubGlobal("fetch", fetchMock);
  activeFixture = backtestDetailFixture;
  activeBacktestId = "smoke-backtest";
  window.history.pushState({}, "", `/backtests/${activeBacktestId}`);
});

afterEach(() => {
  vi.unstubAllGlobals();
  fetchMock.mockClear();
});

test("renders backtest report with protocol time window and template rules", async () => {
  render(<App />);

  await waitFor(() =>
    expect(screen.getByText("官方回测协议 v1")).toBeInTheDocument(),
  );

  expect(screen.getByText("官方滚动 benchmark")).toBeInTheDocument();
  expect(screen.getByText("官方 benchmark 版本")).toBeInTheDocument();
  expect(screen.getByText("官方窗口档位")).toBeInTheDocument();
  expect(screen.getByText("官方模板实际时间范围")).toBeInTheDocument();
  expect(screen.getByText("使用此模板必须满足的要求")).toBeInTheDocument();
  expect(screen.getAllByText("规则").length).toBeGreaterThan(0);
});

test("surfaces invalid official backtests at the top of the report", async () => {
  const invalidFixture = {
    ...backtestDetailFixture,
    protocol: {
      ...backtestDetailFixture.protocol!,
      gate_status: "failed",
      gate_results: [
        ...(backtestDetailFixture.protocol?.gate_results ?? []),
        {
          key: "risk_limits",
          label: "风险约束",
          passed: false,
          severity: "error",
          detail: "Requires max_drawdown <= 0.35.",
        },
      ],
    },
  };
  activeFixture = invalidFixture;
  activeBacktestId = invalidFixture.backtest_id;
  window.history.pushState({}, "", `/backtests/${activeBacktestId}`);

  render(<App />);

  await waitFor(() =>
    expect(screen.getByText("该结果不可用于官方比较")).toBeInTheDocument(),
  );

  expect(
    screen.getAllByText("协议门禁没有通过，这条历史回测记录只能作为排错样本，不能当作有效官方结果使用。").length,
  ).toBeGreaterThan(0);
  expect(screen.getAllByText("结果有效性").length).toBeGreaterThan(0);
  expect(screen.getAllByText("不可用于官方比较").length).toBeGreaterThan(0);
  expect(screen.getAllByText("复核项").length).toBeGreaterThan(0);
});

