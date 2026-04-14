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
    expect(screen.getByRole("heading", { name: "官方回测协议 v1" })).toBeInTheDocument(),
  );

  expect(screen.getByRole("heading", { name: "官方模板实际时间范围" })).toBeInTheDocument();
  expect(screen.getByRole("heading", { name: "官方滚动 benchmark" })).toBeInTheDocument();
  expect(screen.getByRole("heading", { name: "使用此模板必须满足的要求" })).toBeInTheDocument();
  expect(screen.getByRole("heading", { name: "仿真权益曲线" })).toBeInTheDocument();
  expect(screen.getByRole("heading", { name: "告警与审计信号" })).toBeInTheDocument();
  expect(screen.getByText("查看同模板对比")).toBeInTheDocument();
  expect(screen.getByRole("link", { name: "smoke-train-run" })).toHaveAttribute(
    "href",
    "/runs/smoke-train-run",
  );
  expect(screen.getByRole("link", { name: "smoke_dataset" })).toHaveAttribute(
    "href",
    "/datasets/smoke_dataset",
  );
  expect(screen.getByRole("link", { name: "macro_liquidity_snapshot" })).toHaveAttribute(
    "href",
    "/datasets/macro_liquidity_snapshot",
  );
  expect(
    screen.getByRole("link", { name: "baseline_real_benchmark_dataset" }),
  ).toHaveAttribute("href", "/datasets/baseline_real_benchmark_dataset");
  expect(
    screen.getByRole("link", { name: "official_reddit_pullpush_multimodal_v2_fusion" }),
  ).toHaveAttribute("href", "/datasets/official_reddit_pullpush_multimodal_v2_fusion");
  expect(screen.getAllByText(/实际市场窗口/i).length).toBeGreaterThan(0);
  expect(screen.getAllByText(/官方测试窗口/i).length).toBeGreaterThan(0);
  expect(screen.getAllByText(/实际文本信号窗口/i).length).toBeGreaterThan(0);
  expect(screen.getAllByText(/官方 benchmark 版本/i).length).toBeGreaterThan(0);
  expect(screen.getByText("180 天")).toBeInTheDocument();
  expect(screen.getAllByText(/仅允许归档型 NLP 数据源/i).length).toBeGreaterThan(0);
  expect(screen.getAllByTestId("workbench-chart").length).toBeGreaterThan(0);
});

test("surfaces invalid official backtests at the top of the report", async () => {
  const invalidFixture: typeof backtestDetailFixture = structuredClone(backtestDetailFixture);
  invalidFixture.backtest_id = "invalid-official-backtest";
  const invalidProtocol = invalidFixture.protocol!;
  const invalidSimulation = invalidFixture.simulation!;
  const invalidResearch = invalidFixture.research!;
  invalidFixture.protocol = {
    ...invalidProtocol,
    gate_status: "failed",
    gate_results: [
      ...invalidProtocol.gate_results,
      {
        key: "stress_bundle_complete",
        label: "Stress Bundle Complete",
        passed: false,
        severity: "error",
        detail: "Official comparison requires the fixed stress bundle to be present.",
      },
    ],
  };
  invalidFixture.comparison_warnings = [];
  invalidFixture.simulation = {
    ...invalidSimulation,
    warnings: [],
    diagnostics: {
      ...invalidSimulation.diagnostics,
      execution_metrics: {
        order_count: 0,
        fill_count: 0,
      },
      signal_metrics: {
        signal_count: 1,
      },
    },
  };
  invalidFixture.research = {
    ...invalidResearch,
    warnings: [],
    diagnostics: {
      ...invalidResearch.diagnostics,
      signal_metrics: {
        signal_count: 1,
      },
    },
  };
  activeFixture = invalidFixture;
  activeBacktestId = invalidFixture.backtest_id;
  const invalidFetchMock = vi.fn(
    createFetchMock([
      (url) =>
        url.endsWith(`/api/backtests/${activeBacktestId}`)
          ? jsonResponse(invalidFixture)
          : undefined,
    ]),
  );
  vi.stubGlobal("fetch", invalidFetchMock);
  window.history.pushState({}, "", `/backtests/${activeBacktestId}`);

  render(<App />);

  await waitFor(() =>
    expect(screen.getByText("该结果不可用于官方比较")).toBeInTheDocument(),
  );

  expect(
    screen.getByText("协议门禁没有通过，这条历史回测记录只能作为排错样本，不能当作有效官方结果使用。"),
  ).toBeInTheDocument();
  expect(screen.getAllByText("结果有效性").length).toBeGreaterThan(0);
  expect(screen.getAllByText("不可用于官方比较").length).toBeGreaterThan(0);
  expect(screen.getAllByText("复核项").length).toBeGreaterThan(0);
  expect(
    screen.getAllByText(
      "This backtest produced signals but no orders or fills, so the headline metrics are not actionable.",
    ).length,
  ).toBeGreaterThan(0);
});
