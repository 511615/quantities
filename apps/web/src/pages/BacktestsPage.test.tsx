import { screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { BacktestsPage } from "./BacktestsPage";
import { backtestsFixture } from "../test/fixtures";
import { createFetchMock, jsonResponse } from "../test/mockApi";
import { renderWithProviders } from "../test/renderWithProviders";

const fetchMock = vi.fn(
  createFetchMock([
    (url) => (url.includes("/api/backtests?") ? jsonResponse(backtestsFixture) : undefined),
  ]),
);

beforeEach(() => {
  vi.stubGlobal("fetch", fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
  fetchMock.mockClear();
});

test("renders backtests page list", async () => {
  renderWithProviders(<BacktestsPage />);
  await waitFor(() => expect(screen.getByText("\u56de\u6d4b\u5217\u8868")).toBeInTheDocument());
  await waitFor(() => expect(screen.getByText("smoke-backtest")).toBeInTheDocument());
});
