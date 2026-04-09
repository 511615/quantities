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
    return "训练运行";
  }
  if (normalized === "backtest") {
    return "回测结果";
  }
  if (normalized === "dataset") {
    return "派生数据集";
  }
  if (normalized === "data_asset") {
    return "上游数据资产";
  }
  return kind || "依赖项";
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

  const canDelete =
    !dependenciesQuery.isLoading &&
    (dependenciesQuery.data?.can_delete ?? blockingItems.length === 0);

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
    `你将从数据目录、本地产物和缓存中永久删除“${datasetLabel}”。删除后不能恢复。`;

  return (
    <ConfirmDialog
      cancelLabel="取消"
      confirmDisabled={!datasetId || !canDelete || deleteMutation.isPending}
      confirmLabel={deleteMutation.isPending ? "删除中..." : "确认永久删除"}
      message={message}
      onCancel={onClose}
      onConfirm={handleConfirm}
      open={open}
      title="删除数据集"
      tone="danger"
    >
      <div className="dialog-section-list">
        <div className="dialog-section">
          <strong>删除范围</strong>
          <p>会移除数据集注册信息、该数据集的本地产物，以及由它直接拥有的缓存文件。</p>
        </div>

        {dependenciesQuery.isLoading ? (
          <div className="dialog-section">
            <strong>正在检查依赖关系</strong>
            <p>系统正在确认是否仍有训练运行、回测结果或派生数据集引用这份数据。</p>
          </div>
        ) : null}

        {dependenciesQuery.isError ? (
          <div className="dialog-section">
            <strong>依赖检查失败</strong>
            <p>{(dependenciesQuery.error as Error).message}</p>
          </div>
        ) : null}

        <div className="dialog-section">
          <strong>{blockingItems.length > 0 ? "当前不允许删除" : "当前允许删除"}</strong>
          <p>
            {blockingItems.length > 0
              ? "下游仍有对象引用这份数据集，必须先清理这些引用，才能执行物理删除。"
              : "当前没有发现阻塞删除的下游引用，可以执行永久删除。"}
          </p>
        </div>

        {blockingItems.length > 0 ? (
          <div className="dialog-section">
            <strong>阻塞删除的依赖</strong>
            <div className="dialog-dependency-list">
              {blockingItems.map((item) =>
                item.href ? (
                  <Link
                    className="artifact-row"
                    key={`${item.dependency_kind}-${item.dependency_id}`}
                    to={item.href}
                  >
                    {dependencyKindLabel(item.dependency_kind)}
                    <span>{dependencyText(item)}</span>
                  </Link>
                ) : (
                  <div
                    className="stack-item align-start"
                    key={`${item.dependency_kind}-${item.dependency_id}`}
                  >
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
            <strong>依赖概览</strong>
            <div className="dialog-dependency-list">
              {dependencyItems.map((item) =>
                item.href ? (
                  <Link
                    className="artifact-row"
                    key={`${item.dependency_kind}-${item.dependency_id}-all`}
                    to={item.href}
                  >
                    {dependencyKindLabel(item.dependency_kind)}
                    <span>{dependencyText(item)}</span>
                  </Link>
                ) : (
                  <div
                    className="stack-item align-start"
                    key={`${item.dependency_kind}-${item.dependency_id}-all`}
                  >
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
