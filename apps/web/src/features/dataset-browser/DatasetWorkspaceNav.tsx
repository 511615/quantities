import { NavLink } from "react-router-dom";

type DatasetWorkspaceNavProps = {
  detailLabel?: string | null;
};

export function DatasetWorkspaceNav({ detailLabel }: DatasetWorkspaceNavProps) {
  return (
    <div className="segmented-tabs compact dataset-workspace-nav" role="tablist" aria-label="数据集工作区导航">
      <NavLink className={({ isActive }) => (isActive ? "active" : "")} end role="tab" to="/datasets">
        总览
      </NavLink>
      <NavLink
        className={({ isActive }) => (isActive ? "active" : "")}
        role="tab"
        to="/datasets/browser"
      >
        浏览器
      </NavLink>
      <NavLink
        className={({ isActive }) => (isActive ? "active" : "")}
        role="tab"
        to="/datasets/training"
      >
        训练面板
      </NavLink>
      {detailLabel ? <span className="dataset-workspace-current">{detailLabel}</span> : null}
    </div>
  );
}
