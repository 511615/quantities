import type { ReactNode } from "react";

import { I18N } from "../lib/i18n";

export function LoadingState({ label = I18N.state.loading }: { label?: string }) {
  return (
    <div className="panel panel-empty shimmer">
      <div className="empty-state-copy">
        <div className="empty-state-mark" aria-hidden="true" />
        <p>{label}</p>
      </div>
    </div>
  );
}

export function ErrorState({
  title = I18N.state.requestFailed,
  message,
  action,
}: {
  title?: string;
  message: string;
  action?: ReactNode;
}) {
  return (
    <div className="panel panel-empty panel-error">
      <div className="empty-state-copy">
        <div className="empty-state-mark" aria-hidden="true" />
        <h3>{title}</h3>
        <p>{message}</p>
        {action}
      </div>
    </div>
  );
}

export function EmptyState({
  title,
  body,
  action,
}: {
  title: string;
  body: string;
  action?: ReactNode;
}) {
  return (
    <div className="panel panel-empty">
      <div className="empty-state-copy">
        <div className="empty-state-mark" aria-hidden="true" />
        <h3>{title}</h3>
        <p>{body}</p>
        {action}
      </div>
    </div>
  );
}
