import { NavLink, Outlet } from "react-router-dom";

import { I18N, translateText } from "../shared/lib/i18n";
import { useUiPreferences } from "../shared/preferences/UiPreferencesContext";

export function AppShell() {
  const { locale, setLocale, theme, setTheme } = useUiPreferences();
  const primaryLinks = [
    { to: "/", label: I18N.nav.workbench, end: true },
    { to: "/models", label: I18N.nav.models },
    { to: "/datasets", label: I18N.nav.datasets },
    { to: "/backtests", label: I18N.nav.backtests },
    { to: "/benchmarks", label: I18N.nav.benchmarks },
    { to: "/jobs", label: I18N.nav.jobs },
  ];

  return (
    <div className="workbench-shell">
      <aside className="workspace-nav">
        <div className="nav-top">
          <div className="brand-mark">QP</div>
          <div className="brand-copy">
            <strong>{I18N.app.brand}</strong>
            <span>{I18N.app.subtitle}</span>
          </div>
        </div>

        <section className="nav-section">
          <div className="nav-caption">{I18N.app.navTitle}</div>
          <nav className="workspace-links">
            {primaryLinks.map((link) => (
              <NavLink end={link.end} key={link.to} to={link.to}>
                <span>{link.label}</span>
              </NavLink>
            ))}
          </nav>
        </section>

        <section className="workspace-note">
          <div className="nav-caption">{translateText("API / BFF")}</div>
          <p>{I18N.app.note}</p>
          <NavLink className="comparison-link" to="/comparison">
            {I18N.nav.comparison}
          </NavLink>
        </section>

        <section className="nav-section nav-settings">
          <div className="nav-caption">{I18N.app.settingsTitle}</div>
          <div className="settings-group">
            <span className="settings-label">{I18N.app.languageLabel}</span>
            <div className="segmented-tabs compact" role="tablist" aria-label={I18N.app.languageLabel}>
              <button
                className={locale === "zh-CN" ? "active" : ""}
                onClick={() => setLocale("zh-CN")}
                type="button"
              >
                {translateText("中")}
              </button>
              <button
                className={locale === "en-US" ? "active" : ""}
                onClick={() => setLocale("en-US")}
                type="button"
              >
                EN
              </button>
            </div>
          </div>
          <div className="settings-group">
            <span className="settings-label">{I18N.app.themeLabel}</span>
            <div className="segmented-tabs compact" role="tablist" aria-label={I18N.app.themeLabel}>
              <button
                className={theme === "light" ? "active" : ""}
                onClick={() => setTheme("light")}
                type="button"
              >
                {I18N.app.themeLight}
              </button>
              <button
                className={theme === "dark" ? "active" : ""}
                onClick={() => setTheme("dark")}
                type="button"
              >
                {I18N.app.themeDark}
              </button>
            </div>
          </div>
        </section>
      </aside>

      <main className="workspace-main">
        <Outlet />
      </main>
    </div>
  );
}
