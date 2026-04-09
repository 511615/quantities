import { useId } from "react";
import type { ReactNode } from "react";

type TooltipSection = {
  title: string;
  body: string;
};

type InfoTooltipProps = {
  label?: string;
  buttonLabel?: string;
  sections: TooltipSection[];
  compact?: boolean;
  className?: string;
  children?: ReactNode;
};

export function InfoTooltip({
  label = "?",
  buttonLabel,
  sections,
  compact = false,
  className,
  children,
}: InfoTooltipProps) {
  const panelId = useId();

  return (
    <span className={`info-tooltip ${className ?? ""}`.trim()}>
      {children}
      <button
        aria-describedby={panelId}
        aria-label={buttonLabel ?? label}
        className={`info-tooltip-trigger${compact ? " compact" : ""}`}
        type="button"
      >
        <span aria-hidden="true">{label}</span>
      </button>
      <span className="info-tooltip-panel" id={panelId} role="tooltip">
        {sections.map((section) => (
          <span className="info-tooltip-section" key={section.title}>
            <strong>{section.title}</strong>
            <span>{section.body}</span>
          </span>
        ))}
      </span>
    </span>
  );
}
