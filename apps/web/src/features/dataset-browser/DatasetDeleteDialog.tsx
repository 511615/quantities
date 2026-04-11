import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";

import { useDatasetDependencies, useDeleteDatasetMutation } from "../../shared/api/hooks";
import type { DatasetDeleteResponse, DatasetDependencyView } from "../../shared/api/types";
import { ConfirmDialog } from "../../shared/ui/ConfirmDialog";

type DatasetDeleteDialogProps = {
  open: boolean;
  datasetId: string | null;
  datasetLabel: string;
  onClose: () => void;
  onDeleted?: (response: DatasetDeleteResponse) => void;
};

function dependencyKindLabel(kind: string) {
  const normalized = kind.trim().toLowerCase();
  if (normalized === "run") {
    return "Training run";
  }
  if (normalized === "backtest") {
    return "Backtest";
  }
  if (normalized === "dataset") {
    return "Derived dataset";
  }
  if (normalized === "data_asset") {
    return "Upstream asset";
  }
  return kind || "Dependency";
}

function dependencyText(item: DatasetDependencyView) {
  return item.dependency_label || item.dependency_id;
}

export function DatasetDeleteDialog({
  open,
  datasetId,
  datasetLabel,
  onClose,
  onDeleted,
}: DatasetDeleteDialogProps) {
  const dependenciesQuery = useDatasetDependencies(datasetId, open);
  const deleteMutation = useDeleteDatasetMutation();
  const [serverResult, setServerResult] = useState<DatasetDeleteResponse | null>(null);

  useEffect(() => {
    if (!open) {
      setServerResult(null);
    }
  }, [open]);

  const dependencyItems = dependenciesQuery.data?.items ?? [];
  const blockingItems = useMemo(() => {
    if (serverResult?.blocking_items?.length) {
      return serverResult.blocking_items;
    }
    return dependenciesQuery.data?.blocking_items ?? [];
  }, [dependenciesQuery.data?.blocking_items, serverResult?.blocking_items]);

  const handleConfirm = async () => {
    if (!datasetId) {
      return;
    }
    const result = await deleteMutation.mutateAsync(datasetId);
    setServerResult(result);
    if (result.status === "deleted") {
      onDeleted?.(result);
      onClose();
    }
  };

  const message =
    serverResult?.message ??
    `This permanently deletes ${datasetLabel} from the registry and local artifacts. Existing runs, backtests, and downstream datasets will keep their ids and surface missing dataset references instead of blocking deletion.`;

  return (
    <ConfirmDialog
      cancelLabel="Cancel"
      confirmDisabled={!datasetId || deleteMutation.isPending}
      confirmLabel={deleteMutation.isPending ? "Deleting..." : "Hard delete dataset"}
      message={message}
      onCancel={onClose}
      onConfirm={handleConfirm}
      open={open}
      title="Delete dataset"
      tone="danger"
    >
      <div className="dialog-section-list">
        <div className="dialog-section">
          <strong>Deletion scope</strong>
          <p>
            Registry entry, dataset artifacts, manifest, samples, and feature-view side files are removed.
            Dependency records are kept only as informational context in the response shown before deletion.
          </p>
        </div>

        {dependenciesQuery.isLoading ? (
          <div className="dialog-section">
            <strong>Scanning dependency graph</strong>
            <p>Checking runs, backtests, and downstream datasets that still reference this dataset id.</p>
          </div>
        ) : null}

        {dependenciesQuery.isError ? (
          <div className="dialog-section">
            <strong>Dependency scan failed</strong>
            <p>{(dependenciesQuery.error as Error).message}</p>
          </div>
        ) : null}

        <div className="dialog-section">
          <strong>Hard delete is enabled</strong>
          <p>
            {blockingItems.length > 0
              ? `${blockingItems.length} downstream references still exist, but they no longer block deletion.`
              : "No downstream references are currently blocking or warning this action."}
          </p>
        </div>

        {blockingItems.length > 0 ? (
          <div className="dialog-section">
            <strong>Existing downstream references</strong>
            <div className="dialog-dependency-list">
              {blockingItems.map((item) =>
                item.href ? (
                  <Link className="artifact-row" key={`${item.dependency_kind}-${item.dependency_id}`} to={item.href}>
                    {dependencyKindLabel(item.dependency_kind)}
                    <span>{dependencyText(item)}</span>
                  </Link>
                ) : (
                  <div className="stack-item align-start" key={`${item.dependency_kind}-${item.dependency_id}`}>
                    <strong>{dependencyKindLabel(item.dependency_kind)}</strong>
                    <span>{dependencyText(item)}</span>
                  </div>
                ),
              )}
            </div>
          </div>
        ) : null}

        {dependencyItems.length > 0 ? (
          <div className="dialog-section">
            <strong>All known dependencies</strong>
            <div className="dialog-dependency-list">
              {dependencyItems.map((item) =>
                item.href ? (
                  <Link className="artifact-row" key={`${item.dependency_kind}-${item.dependency_id}-all`} to={item.href}>
                    {dependencyKindLabel(item.dependency_kind)}
                    <span>{dependencyText(item)}</span>
                  </Link>
                ) : (
                  <div className="stack-item align-start" key={`${item.dependency_kind}-${item.dependency_id}-all`}>
                    <strong>{dependencyKindLabel(item.dependency_kind)}</strong>
                    <span>{dependencyText(item)}</span>
                  </div>
                ),
              )}
            </div>
          </div>
        ) : null}
      </div>
    </ConfirmDialog>
  );
}
