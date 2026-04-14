import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { Suspense, lazy, useState, type ReactElement } from "react";
import { createBrowserRouter, Navigate, RouterProvider } from "react-router-dom";

import { AppShell } from "./AppShell";
import { I18N } from "../shared/lib/i18n";
import { LoadingState } from "../shared/ui/StateViews";

const HomePage = lazy(async () => ({ default: (await import("../pages/HomePage")).HomePage }));
const ModelsPage = lazy(async () => ({ default: (await import("../pages/ModelsPage")).ModelsPage }));
const DatasetsPage = lazy(async () => ({ default: (await import("../pages/DatasetsPage")).DatasetsPage }));
const DatasetsBrowserPage = lazy(
  async () => ({ default: (await import("../pages/DatasetsBrowserPage")).DatasetsBrowserPage }),
);
const DatasetDetailPage = lazy(
  async () => ({ default: (await import("../pages/DatasetDetailPage")).DatasetDetailPage }),
);
const TrainingDatasetsPage = lazy(
  async () => ({ default: (await import("../pages/TrainingDatasetsPage")).TrainingDatasetsPage }),
);
const RunDetailPage = lazy(
  async () => ({ default: (await import("../pages/RunDetailPage")).RunDetailPage }),
);
const BacktestsPage = lazy(
  async () => ({ default: (await import("../pages/BacktestsPage")).BacktestsPage }),
);
const BacktestReportPage = lazy(
  async () => ({ default: (await import("../pages/BacktestReportPage")).BacktestReportPage }),
);
const BenchmarksPage = lazy(
  async () => ({ default: (await import("../pages/BenchmarksPage")).BenchmarksPage }),
);
const BenchmarkDetailPage = lazy(
  async () =>
    ({ default: (await import("../pages/BenchmarkDetailPage")).BenchmarkDetailPage }),
);
const JobsPage = lazy(async () => ({ default: (await import("../pages/JobsPage")).JobsPage }));
const ComparisonPage = lazy(
  async () => ({ default: (await import("../pages/ComparisonPage")).ComparisonPage }),
);

function withSuspense(node: ReactElement) {
  return <Suspense fallback={<LoadingState label={I18N.state.loading} />}>{node}</Suspense>;
}

function createWorkbenchRouter() {
  return createBrowserRouter(
    [
      {
        path: "/",
        element: <AppShell />,
        children: [
          { index: true, element: withSuspense(<HomePage />) },
          { path: "models", element: withSuspense(<ModelsPage />) },
          { path: "models/trained/:runId", element: withSuspense(<RunDetailPage />) },
          { path: "datasets", element: withSuspense(<DatasetsPage />) },
          { path: "datasets/browser", element: withSuspense(<DatasetsBrowserPage />) },
          { path: "datasets/training", element: withSuspense(<TrainingDatasetsPage />) },
          { path: "datasets/:datasetId", element: withSuspense(<DatasetDetailPage />) },
          { path: "runs", element: <Navigate to="/models?tab=trained" replace /> },
          { path: "runs/:runId", element: withSuspense(<RunDetailPage />) },
          { path: "backtests", element: withSuspense(<BacktestsPage />) },
          { path: "backtests/:backtestId", element: withSuspense(<BacktestReportPage />) },
          { path: "benchmarks", element: withSuspense(<BenchmarksPage />) },
          { path: "benchmarks/:benchmarkName", element: withSuspense(<BenchmarkDetailPage />) },
          { path: "jobs", element: withSuspense(<JobsPage />) },
          { path: "comparison", element: withSuspense(<ComparisonPage />) },
          { path: "*", element: <Navigate to="/" replace /> },
        ],
      },
    ],
  );
}

export function App() {
  const [queryClient] = useState(
    () =>
      new QueryClient({
        defaultOptions: {
          queries: {
            retry: false,
            refetchOnWindowFocus: false,
            staleTime: 15_000,
          },
        },
      }),
  );
  const [router] = useState(createWorkbenchRouter);

  return (
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  );
}
