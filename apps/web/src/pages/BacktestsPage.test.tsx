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
      if (url.includes("/api/backtests/") && init?.method === "DELETE") {
        const backtestId = url.split("/api/backtests/")[1];
        const index = backtestsState.items.findIndex((item) => item.backtest_id === backtestId);
        const [removed] = index >= 0 ? backtestsState.items.splice(index, 1) : [];
        backtestsState.total = backtestsState.items.length;
        return jsonResponse({
          backtest_id: removed?.backtest_id ?? backtestId,
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
  expect(screen.getByText("\u56de\u6d4b\u65f6\u95f4")).toBeInTheDocument();
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

test("bulk deletes selected backtests from the list", async () => {
  backtestsState.items = [
    ...structuredClone(backtestsFixture.items),
    {
      ...structuredClone(backtestsFixture.items[0]),
      backtest_id: "smoke-backtest-2",
      run_id: "macro-run-2",
    },
  ];
  backtestsState.total = backtestsState.items.length;

  renderWithProviders(<BacktestsPage />);

  await waitFor(() => expect(screen.getByText("smoke-backtest")).toBeInTheDocument());
  await waitFor(() => expect(screen.getByText("smoke-backtest-2")).toBeInTheDocument());

  fireEvent.click(screen.getByLabelText("\u9009\u62e9\u56de\u6d4b smoke-backtest"));
  fireEvent.click(screen.getByLabelText("\u9009\u62e9\u56de\u6d4b smoke-backtest-2"));
  fireEvent.click(screen.getByRole("button", { name: "\u6279\u91cf\u5220\u9664 (2)" }));

  const dialog = await screen.findByRole("dialog", { name: "" });
  expect(within(dialog).getByText("\u6279\u91cf\u5220\u9664\u56de\u6d4b")).toBeInTheDocument();

  fireEvent.click(within(dialog).getByRole("button", { name: "\u786e\u8ba4\u5220\u9664" }));

  await waitFor(() => expect(screen.queryByText("smoke-backtest")).not.toBeInTheDocument());
  await waitFor(() => expect(screen.queryByText("smoke-backtest-2")).not.toBeInTheDocument());
});
