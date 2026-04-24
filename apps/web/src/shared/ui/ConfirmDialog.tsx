import type { ReactNode } from "react";

type ConfirmDialogProps = {
  open: boolean;
  title: string;
  message: string;
  confirmLabel: string;
  cancelLabel: string;
  tone?: "default" | "danger";
  confirmDisabled?: boolean;
  className?: string;
  bodyClassName?: string;
  children?: ReactNode;
  onConfirm: () => void;
  onCancel: () => void;
};

export function ConfirmDialog({
  open,
  title,
  message,
  confirmLabel,
  cancelLabel,
  tone = "default",
  confirmDisabled = false,
  className,
  bodyClassName,
  children,
  onConfirm,
  onCancel,
}: ConfirmDialogProps) {
  if (!open) {
    return null;
  }

  return (
    <div className="dialog-backdrop" role="presentation">
      <div
        aria-modal="true"
        className={`dialog-shell${className ? ` ${className}` : ""}`}
        role="dialog"
      >
        <div className={`dialog-copy${bodyClassName ? ` ${bodyClassName}` : ""}`}>
          <strong>{title}</strong>
          <p>{message}</p>
          {children}
        </div>
        <div className="dialog-actions">
          <button className="link-button" onClick={onCancel} type="button">
            {cancelLabel}
          </button>
          <button
            className={`action-button${tone === "danger" ? " danger" : ""}`}
            disabled={confirmDisabled}
            onClick={onConfirm}
            type="button"
          >
            {confirmLabel}
          </button>
        </div>
      </div>
    </div>
  );
}
