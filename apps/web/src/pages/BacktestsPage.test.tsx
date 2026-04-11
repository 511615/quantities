import { cleanup, fireEvent, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { BacktestsPage } from "./BacktestsPage";
import { backtestsFixture } from "../test/fixtures";
import { createFetchMock, jsonResponse } from "../test/mockApi";
import { renderWithProviders } from "../test/renderWithProviders";

const backtestsState = structuredClone(backtestsFixture);
const fetchMock = vi.fn(
  createFetchMock([
    (url, init) => {
      if (url.includes("/api/backtests?")) {
        return jsonResponse(backtestsState);
      }
      if (url.endsWith(`/api/backtests/${backtestsState.items[0]?.backtest_id}`) && init?.method === "DELETE") {
        const [removed] = backtestsState.items.splice(0, 1);
        backtestsState.total = backtestsState.items.length;
        return jsonResponse({
          backtest_id: removed.backtest_id,
          status: "deleted",
          message: "deleted",
          deleted_files: [],
        });
      }
      return undefined;
    },
  ]),
);

beforeEach(() => {
  vi.stubGlobal("fetch", fetchMock);
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  fetchMock.mockClear();
  backtestsState.items = structuredClone(backtestsFixture.items);
  backtestsState.total = backtestsFixture.total;
  backtestsState.page = backtestsFixture.page;
  backtestsState.per_page = backtestsFixture.per_page;
  backtestsState.available_statuses = structuredClone(backtestsFixture.available_statuses);
});

test("renders backtests page list", async () => {
  renderWithProviders(<BacktestsPage />);
  await waitFor(() => expect(screen.getByText("\u56de\u6d4b\u5217\u8868")).toBeInTheDocument());
  await waitFor(() => expect(screen.getByText("smoke-backtest")).toBeInTheDocument());
});

test("deletes a backtest from the list", async () => {
  renderWithProviders(<BacktestsPage />);

  await waitFor(() => expect(screen.getByText("smoke-backtest")).toBeInTheDocument());

  fireEvent.click(screen.getAllByRole("button", { name: "\u5220\u9664" })[0]);

  const dialog = await screen.findByRole("dialog", { name: "" });
  expect(within(dialog).getByText("\u5220\u9664\u56de\u6d4b")).toBeInTheDocument();

  fireEvent.click(within(dialog).getByRole("button", { name: "\u786e\u8ba4\u5220\u9664" }));

  await waitFor(() => expect(screen.queryByText("smoke-backtest")).not.toBeInTheDocument());
});
