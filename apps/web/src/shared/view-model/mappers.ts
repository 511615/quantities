import type {
  BacktestListItemView,
  BacktestReportView,
  BenchmarkDetailView,
  BenchmarkListItemView,
  ExperimentListItem,
  JobStatusView,
  RunDetailView,
  WorkbenchOverviewView,
} from "../api/types";

export function mapOverviewView(view: WorkbenchOverviewView) {
  return {
    recentRuns: view.recent_runs,
    recentBacktests: view.recent_backtests,
    recentBenchmarks: view.recent_benchmarks,
    recentJobs: view.recent_jobs,
    freshness: view.data_freshness,
    actions: view.recommended_actions,
  };
}

export function mapRunList(items: ExperimentListItem[]): ExperimentListItem[] {
  return items;
}

export function mapBacktestList(items: BacktestListItemView[]): BacktestListItemView[] {
  return items;
}

export function mapBenchmarkList(items: BenchmarkListItemView[]): BenchmarkListItemView[] {
  return items;
}

export function mapRunDetail(view: RunDetailView): RunDetailView {
  return view;
}

export function mapBacktestDetail(view: BacktestReportView): BacktestReportView {
  return view;
}

export function mapBenchmarkDetail(view: BenchmarkDetailView): BenchmarkDetailView {
  return view;
}

export function mapJobStatus(view: JobStatusView): JobStatusView {
  return view;
}
