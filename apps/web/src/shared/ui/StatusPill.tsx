import { formatStatusLabel } from "../lib/labels";

export function StatusPill({ status }: { status: string | null | undefined }) {
  const normalized = (status ?? "unknown").toLowerCase();
  return <span className={`status-pill status-${normalized}`}>{formatStatusLabel(status)}</span>;
}
