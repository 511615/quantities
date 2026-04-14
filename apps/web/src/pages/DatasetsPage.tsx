import { Navigate, useSearchParams } from "react-router-dom";

import { DatasetsOverviewPage } from "./DatasetsOverviewPage";

export function DatasetsPage() {
  const [searchParams] = useSearchParams();
  const legacyTab = searchParams.get("tab");
  const datasetId = searchParams.get("dataset_id");
  const rangePreset = searchParams.get("range_preset") ?? "30d";

  if (legacyTab === "market" && datasetId) {
    return (
      <Navigate
        replace
        to={`/datasets/${encodeURIComponent(datasetId)}?range_preset=${encodeURIComponent(rangePreset)}`}
      />
    );
  }

  if (legacyTab === "macro") {
    return <Navigate replace to="/datasets/browser?data_domain=macro" />;
  }

  if (legacyTab === "catalog") {
    return <Navigate replace to="/datasets/browser?dataset_type=feature_snapshot" />;
  }

  return <DatasetsOverviewPage />;
}
