import { useEffect, useState } from "react";

import { useRenameDatasetMutation } from "../../shared/api/hooks";
import type { DatasetRenameResponse } from "../../shared/api/types";
import { translateText } from "../../shared/lib/i18n";
import { ConfirmDialog } from "../../shared/ui/ConfirmDialog";

type DatasetRenameDialogProps = {
  open: boolean;
  datasetId: string | null;
  currentLabel: string;
  onClose: () => void;
  onRenamed?: (response: DatasetRenameResponse) => void;
};

export function DatasetRenameDialog({
  open,
  datasetId,
  currentLabel,
  onClose,
  onRenamed,
}: DatasetRenameDialogProps) {
  const renameMutation = useRenameDatasetMutation();
  const [displayName, setDisplayName] = useState(currentLabel);

  useEffect(() => {
    if (open) {
      setDisplayName(currentLabel);
    }
  }, [currentLabel, open]);

  const normalizedDisplayName = displayName.trim();

  const handleConfirm = async () => {
    if (!datasetId || !normalizedDisplayName) {
      return;
    }
    const result = await renameMutation.mutateAsync({
      datasetId,
      body: { display_name: normalizedDisplayName },
    });
    onRenamed?.(result);
    onClose();
  };

  return (
    <ConfirmDialog
      bodyClassName="dataset-rename-dialog-body"
      cancelLabel={translateText("取消")}
      className="dataset-rename-dialog"
      confirmDisabled={!datasetId || !normalizedDisplayName || renameMutation.isPending}
      confirmLabel={renameMutation.isPending ? translateText("保存中...") : translateText("保存名称")}
      message={translateText("只会更新数据集的显示名称，不会修改技术 ID。已有引用会继续使用原来的 dataset_id，但会展示新名称。")}
      onCancel={onClose}
      onConfirm={handleConfirm}
      open={open}
      title={translateText("重命名数据集")}
    >
      <label className="field-group">
        <span>{translateText("显示名称")}</span>
        <input
          autoFocus
          className="field"
          onChange={(event) => setDisplayName(event.target.value)}
          placeholder={translateText("输入新的数据集名称")}
          type="text"
          value={displayName}
        />
      </label>
      {renameMutation.isError ? (
        <p className="form-help error-text">{(renameMutation.error as Error).message}</p>
      ) : null}
      <p className="form-help">
        {translateText("技术 ID 会保持不变，因此训练实例、回测和数据链接不会失效。")}
      </p>
    </ConfirmDialog>
  );
}
