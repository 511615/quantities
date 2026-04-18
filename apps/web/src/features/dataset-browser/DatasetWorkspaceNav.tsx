import { NavLink } from "react-router-dom";

import { translateText } from "../../shared/lib/i18n";

type DatasetWorkspaceNavProps = {
  detailLabel?: string | null;
};

export function DatasetWorkspaceNav({ detailLabel }: DatasetWorkspaceNavProps) {
  return (
    <div
      className="segmented-tabs compact dataset-workspace-nav"
      role="tablist"
      aria-label={translateText("数据集工作区导航")}
    >
      <NavLink className={({ isActive }) => (isActive ? "active" : "")} end role="tab" to="/datasets">
        {translateText("总览")}
      </NavLink>
      <NavLink className={({ isActive }) => (isActive ? "active" : "")} role="tab" to="/datasets/browser">
        {translateText("浏览器")}
      </NavLink>
      <NavLink className={({ isActive }) => (isActive ? "active" : "")} role="tab" to="/datasets/training">
        {translateText("训练面板")}
      </NavLink>
      {detailLabel ? <span className="dataset-workspace-current">{translateText(detailLabel)}</span> : null}
    </div>
  );
}
