import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";

import { useDatasetDependencies, useDeleteDatasetMutation } from "../../shared/api/hooks";
import type { DatasetDeleteResponse, DatasetDependencyView } from "../../shared/api/types";
import { translateText } from "../../shared/lib/i18n";
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
    return translateText("训练实例");
  }
  if (normalized === "backtest") {
    return translateText("回测");
  }
  if (normalized === "dataset") {
    return translateText("派生数据集");
  }
  if (normalized === "data_asset") {
    return translateText("上游资产");
  }
  return kind || translateText("依赖项");
}

function dependencyText(item: DatasetDependencyView) {
  return item.dependency_label || item.dependency_id;
}

function deriveDeleteReasons(data?: {
  can_delete?: boolean;
  blocking_items?: DatasetDependencyView[];
  delete_block_reasons?: string[];
  protection_reason?: string | null;
  protection_kind?: string | null;
} | null) {
  if (!data || data.can_delete !== false) {
    return [];
  }
  const explicitReasons = [
    ...(data.delete_block_reasons ?? []),
    ...(data.protection_reason ? [data.protection_reason] : []),
  ].filter(Boolean);
  if (explicitReasons.length > 0) {
    return explicitReasons;
  }
  if (data.protection_kind === "recommended") {
    return [translateText("推荐数据集由系统维护，不能直接删除。")];
  }
  if (data.protection_kind === "system") {
    return [translateText("系统数据集承载基线流程，不允许在前端发起删除。")];
  }
  if ((data.blocking_items ?? []).length > 0) {
    return [translateText("当前数据集仍被工作流引用，后端未允许删除。")];
  }
  return [translateText("当前数据集被后端标记为不可删除，请保留该数据集。")];
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
  const deleteReasons = useMemo(
    () => deriveDeleteReasons(serverResult ?? dependenciesQuery.data ?? null),
    [dependenciesQuery.data, serverResult],
  );
  const canDelete = (serverResult ?? dependenciesQuery.data)?.can_delete !== false;

  const handleConfirm = async () => {
    if (!datasetId || !canDelete) {
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
    translateText("这会把 {datasetLabel} 从注册表和本地工件中永久删除。已有训练实例、回测和下游数据集会保留自己的 id，但之后会显示缺失的数据集引用，而不是阻止删除。").replace("{datasetLabel}", datasetLabel);

  return (
    <ConfirmDialog
      cancelLabel={translateText("取消")}
      confirmDisabled={!datasetId || deleteMutation.isPending || !canDelete}
      confirmLabel={
        !canDelete ? translateText("当前不可删除") : deleteMutation.isPending ? translateText("删除中...") : translateText("硬删除数据集")
      }
      message={message}
      onCancel={onClose}
      onConfirm={handleConfirm}
      open={open}
      title={translateText("删除数据集")}
      tone="danger"
    >
      <div className="dialog-section-list">
        <div className="dialog-section">
          <strong>{translateText("删除范围")}</strong>
          <p>
            {translateText("注册表记录、数据集工件、manifest、样本文件和特征视图侧文件都会被移除。")}
            {translateText("依赖记录只会作为删除前展示的说明性上下文保留在响应中。")}
          </p>
        </div>

        {dependenciesQuery.isLoading ? (
          <div className="dialog-section">
            <strong>{translateText("正在扫描依赖关系图")}</strong>
            <p>{translateText("正在检查仍然引用此数据集 id 的训练实例、回测和下游数据集。")}</p>
          </div>
        ) : null}

        {dependenciesQuery.isError ? (
          <div className="dialog-section">
            <strong>{translateText("依赖扫描失败")}</strong>
            <p>{(dependenciesQuery.error as Error).message}</p>
          </div>
        ) : null}

        <div className="dialog-section">
          <strong>{translateText("当前已启用硬删除")}</strong>
          <p>
            {!canDelete
              ? translateText("当前数据集已被后端标记为不可删除，需要先处理保护原因。")
              : blockingItems.length > 0
              ? translateText("当前仍有 {count} 个下游引用，但它们已经不会阻止删除。").replace("{count}", String(blockingItems.length))
              : translateText("当前没有会阻止或告警本次操作的下游引用。")}
          </p>
        </div>

        {!canDelete && deleteReasons.length > 0 ? (
          <div className="dialog-section">
            <strong>{translateText("当前不能删除的原因")}</strong>
            <div className="stack-list">
              {deleteReasons.map((reason) => (
                <div className="stack-item align-start" key={reason}>
                  <span>{reason}</span>
                </div>
              ))}
            </div>
          </div>
        ) : null}

        {blockingItems.length > 0 ? (
          <div className="dialog-section">
            <strong>{translateText("现存下游引用")}</strong>
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
            <strong>{translateText("全部已知依赖")}</strong>
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
