import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

vi.mock("echarts-for-react/lib/core", () => ({
  default: {
    default: ({
      className,
    }: {
      className?: string;
    }) => <div className={className} data-testid="echarts-core" />,
  },
}));

import { WorkbenchChart } from "./WorkbenchChart";

describe("WorkbenchChart", () => {
  it("renders when echarts-for-react resolves through a nested default export", async () => {
    render(<WorkbenchChart className="chart" option={{}} />);

    const chart = await screen.findByTestId("echarts-core");
    expect(chart).toHaveClass("chart");
  });
});
