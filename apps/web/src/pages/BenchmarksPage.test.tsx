import { screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { BenchmarksPage } from "./BenchmarksPage";
import { benchmarkListFixture } from "../test/fixtures";
import { createFetchMock, jsonResponse } from "../test/mockApi";
import { renderWithProviders } from "../test/renderWithProviders";

const fetchMock = vi.fn(
  createFetchMock([
    (url) => (url.endsWith("/api/benchmarks") ? jsonResponse(benchmarkListFixture) : undefined),
  ]),
);

beforeEach(() => {
  vi.stubGlobal("fetch", fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
  fetchMock.mockClear();
});

test("renders benchmarks page list", async () => {
  renderWithProviders(<BenchmarksPage />);
  await waitFor(() =>
    expect(screen.getByRole("heading", { name: "\u57fa\u51c6\u5bf9\u6bd4" })).toBeInTheDocument(),
  );
  await waitFor(() =>
    expect(screen.getByRole("link", { name: "baseline_family_walk_forward" })).toBeInTheDocument(),
  );
});
