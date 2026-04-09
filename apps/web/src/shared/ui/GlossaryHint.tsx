import { I18N } from "../lib/i18n";
import type { GlossaryKey } from "../lib/i18n";
import { InfoTooltip } from "./InfoTooltip";

type GlossaryHintProps = {
  hintKey: GlossaryKey;
  termOverride?: string | null;
  iconOnly?: boolean;
  override?: {
    definition?: string | null;
    howToRead?: string | null;
    watchout?: string | null;
  } | null;
  className?: string;
};

export function GlossaryHint({
  hintKey,
  termOverride,
  iconOnly = false,
  override,
  className,
}: GlossaryHintProps) {
  const record = I18N.glossary[hintKey];
  const term = termOverride ?? record.term;

  return (
    <span className={`term-hint ${className ?? ""}`.trim()}>
      {!iconOnly ? <span>{term}</span> : null}
      <InfoTooltip
        buttonLabel={`关于 ${term}`}
        compact
        sections={[
          {
            title: "\u672f\u8bed\u5b9a\u4e49",
            body: override?.definition ?? record.definition,
          },
          {
            title: "\u600e\u4e48\u770b",
            body: override?.howToRead ?? record.howToRead,
          },
          {
            title: "\u4ec0\u4e48\u65f6\u5019\u8981\u8b66\u60d5",
            body: override?.watchout ?? record.watchout,
          },
        ]}
      />
    </span>
  );
}
