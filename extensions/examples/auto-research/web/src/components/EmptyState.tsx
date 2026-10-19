import { Link } from "react-router-dom";
import type { ReactNode } from "react";
import { Icon } from "./Icon";
import type { IconName } from "./Icon";

export function EmptyState({
  title,
  description,
  icon = "folder",
  action,
  home = false,
  loading = false,
}: {
  title: string;
  description: string;
  icon?: IconName;
  action?: ReactNode;
  home?: boolean;
  loading?: boolean;
}) {
  return (
    <section
      className={`empty-state ${loading ? "is-loading" : ""}`}
      aria-busy={loading}
    >
      <div className="empty-symbol">
        <Icon name={icon} size={32} />
      </div>
      <h1>{title}</h1>
      <p>{description}</p>
      {action}
      {home && (
        <Link className="button secondary" to="/">
          返回项目列表
          <Icon name="arrow" size={16} />
        </Link>
      )}
    </section>
  );
}
