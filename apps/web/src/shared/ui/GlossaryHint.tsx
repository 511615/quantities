import { I18N, type GlossaryKey, translateText } from "../lib/i18n";
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
        buttonLabel={`${translateText("关于")} ${term}`}
        compact
        sections={[
          {
            title: translateText("术语定义"),
            body: override?.definition ?? record.definition,
          },
          {
            title: translateText("怎么看"),
            body: override?.howToRead ?? record.howToRead,
          },
          {
            title: translateText("什么时候要警惕"),
            body: override?.watchout ?? record.watchout,
          },
        ]}
      />
    </span>
  );
}
