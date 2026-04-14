import { render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { App } from "../app/App";
import { comparisonFixture } from "../test/fixtures";
import { createFetchMock, jsonResponse } from "../test/mockApi";

vi.mock("../shared/ui/WorkbenchChart", () => ({
  WorkbenchChart: ({ loadingLabel }: { loadingLabel?: string }) => (
    <div data-testid="workbench-chart">{loadingLabel ?? "chart"}</div>
  ),
}));

const fetchMock = vi.fn(
  createFetchMock([
    (url, init) => {
      const isComparisonRequest =
        url.endsWith("/api/comparisons/models") && init?.method === "POST";
      return isComparisonRequest ? jsonResponse(comparisonFixture) : undefined;
    },
  ]),
);

beforeEach(() => {
  vi.stubGlobal("fetch", fetchMock);
  window.history.pushState({}, "", "/comparison?runs=smoke-train-run");
});

afterEach(() => {
  vi.unstubAllGlobals();
  fetchMock.mockClear();
});

test("renders model comparison rows and chart sections", async () => {
  render(<App />);

  await waitFor(() => expect(screen.getByText("\u6a21\u578b\u6027\u80fd\u5bf9\u6bd4")).toBeInTheDocument());
  expect(screen.getByText("\u6d4b\u8bd5\u96c6 MAE \u4e0e\u5e74\u5316\u6536\u76ca")).toBeInTheDocument();
  expect(screen.getByText("\u5bf9\u6bd4\u7ed3\u679c")).toBeInTheDocument();
  expect(screen.getByText("baseline_family_walk_forward / elastic_net")).toBeInTheDocument();
  await waitFor(() =>
    expect(screen.getAllByTestId("workbench-chart").length).toBeGreaterThan(0),
  );
});
