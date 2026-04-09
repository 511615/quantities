import { NavLink, Outlet } from "react-router-dom";

import { I18N } from "../shared/lib/i18n";

const PRIMARY_LINKS = [
  { to: "/", label: I18N.nav.workbench, end: true },
  { to: "/models", label: I18N.nav.models },
  { to: "/datasets", label: I18N.nav.datasets },
  { to: "/backtests", label: I18N.nav.backtests },
  { to: "/benchmarks", label: I18N.nav.benchmarks },
  { to: "/jobs", label: I18N.nav.jobs },
];

export function AppShell() {
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
            {PRIMARY_LINKS.map((link) => (
              <NavLink end={link.end} key={link.to} to={link.to}>
                <span>{link.label}</span>
              </NavLink>
            ))}
          </nav>
        </section>

        <section className="workspace-note">
          <div className="nav-caption">API / BFF</div>
          <p>{I18N.app.note}</p>
          <NavLink className="comparison-link" to="/comparison">
            {I18N.nav.comparison}
          </NavLink>
        </section>
      </aside>
      <main className="workspace-main">
        <Outlet />
      </main>
    </div>
  );
}
