import { render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { App } from "../app/App";
import { benchmarkDetailFixture } from "../test/fixtures";
import { createFetchMock, jsonResponse } from "../test/mockApi";

const fetchMock = vi.fn(
  createFetchMock([
    (url) =>
      url.endsWith("/api/benchmarks/baseline_family_walk_forward")
        ? jsonResponse(benchmarkDetailFixture)
        : undefined,
  ]),
);

beforeEach(() => {
  vi.stubGlobal("fetch", fetchMock);
  window.history.pushState({}, "", "/benchmarks/baseline_family_walk_forward");
});

afterEach(() => {
  vi.unstubAllGlobals();
  fetchMock.mockClear();
});

test("renders benchmark detail with hover glossary triggers", async () => {
  render(<App />);
  await waitFor(() => expect(screen.getByText("\u6a21\u578b\u6392\u540d")).toBeInTheDocument());
  expect(screen.getByText("\u98ce\u9669\u63d0\u793a")).toBeInTheDocument();
  expect(screen.getAllByText("MAE").length).toBeGreaterThan(0);
});
