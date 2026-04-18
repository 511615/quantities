import type { ModalityQualityView } from "../api/types";
import { formatPercent } from "../lib/format";
import { formatModalityLabel, formatStatusLabel } from "../lib/labels";

type ModalityQualitySummaryProps = {
  summary?: Record<string, ModalityQualityView> | null;
  modalities?: string[];
  emptyText?: string;
  title?: string;
};

function orderedEntries(
  summary: Record<string, ModalityQualityView>,
  modalities?: string[],
) {
  const preferred = modalities?.length
    ? modalities
    : ["market", "macro", "on_chain", "derivatives", "nlp"];
  const seen = new Set<string>();
  const entries: Array<[string, ModalityQualityView]> = [];
  preferred.forEach((key) => {
    const item = summary[key];
    if (!item) {
      return;
    }
    seen.add(key);
    entries.push([key, item]);
  });
  Object.entries(summary).forEach(([key, item]) => {
    if (seen.has(key)) {
      return;
    }
    entries.push([key, item]);
  });
  return entries;
}

function metricText(item: ModalityQualityView) {
  const parts: string[] = [];
  if (item.usable_count !== null && item.usable_count !== undefined) {
    const usableLabel =
      item.modality === "macro" || item.modality === "on_chain"
        ? "usable observations"
        : item.modality === "market"
          ? "usable bars"
          : "usable";
    parts.push(`${usableLabel} ${item.usable_count}`);
  }
  if (item.coverage_ratio !== null && item.coverage_ratio !== undefined) {
    parts.push(`coverage ${formatPercent(item.coverage_ratio, 1)}`);
  }
  if (item.non_null_coverage_ratio !== null && item.non_null_coverage_ratio !== undefined) {
    parts.push(`non-null ${formatPercent(item.non_null_coverage_ratio, 1)}`);
  }
  if (item.duplicate_ratio !== null && item.duplicate_ratio !== undefined) {
    parts.push(`duplicates ${formatPercent(item.duplicate_ratio, 1)}`);
  }
  if (item.max_gap_bars !== null && item.max_gap_bars !== undefined) {
    parts.push(`max gap ${item.max_gap_bars}`);
  }
  if (item.freshness_lag_days !== null && item.freshness_lag_days !== undefined) {
    parts.push(`freshness lag ${item.freshness_lag_days}d`);
  }
  return parts.join(" / ");
}

export function ModalityQualitySummary({
  summary,
  modalities,
  emptyText = "No modality quality summary available.",
  title = "Modality quality summary",
}: ModalityQualitySummaryProps) {
  const entries = summary ? orderedEntries(summary, modalities) : [];
  if (!entries.length) {
    return <p className="drawer-copy">{emptyText}</p>;
  }

  return (
    <div className="stack-list" aria-label={title}>
      {entries.map(([key, item]) => (
        <div className="stack-item align-start" key={`${key}-${item.status}`}>
          <strong>{formatModalityLabel(item.modality || key)}</strong>
          <span>{formatStatusLabel(item.status)}</span>
          {metricText(item) ? <span>{metricText(item)}</span> : null}
          {item.blocking_reasons.length > 0 ? (
            <span>{item.blocking_reasons.join(" / ")}</span>
          ) : null}
        </div>
      ))}
    </div>
  );
}
