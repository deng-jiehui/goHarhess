import { useState } from "react";
import { Link, NavLink } from "react-router-dom";
import type { Project, Session } from "../types";
import { Icon } from "./Icon";
import { ModelSettingsDialog } from "./ModelSettingsDialog";

export function ProjectNav({
  project,
  sessions = [],
  onNewSession,
  onDeleteSession,
  creating = false,
  archivedSessions = false,
  onToggleArchived,
  mobileOpen = false,
  onClose,
}: {
  project?: Project;
  sessions?: Session[];
  onNewSession?: () => void;
  onDeleteSession?: (session: Session) => void;
  creating?: boolean;
  archivedSessions?: boolean;
  onToggleArchived?: () => void;
  mobileOpen?: boolean;
  onClose?: () => void;
}) {
  const [modelDialog, setModelDialog] = useState(false);
  return (
    <>
      {mobileOpen && (
        <button
          className="nav-backdrop"
          aria-label="收起导航"
          onClick={onClose}
        />
      )}
      <aside className={`sidebar ${mobileOpen ? "mobile-open" : ""}`}>
        <Link to="/" className="brand" onClick={onClose}>
          <span className="brand-mark">
            <Icon name="branch" size={22} />
          </span>
          <span>
            AutoResearch<small>把想法，变成研究</small>
          </span>
        </Link>
        <Link
          to="/"
          className={`nav-link all-projects ${!project ? "active" : ""}`}
          onClick={onClose}
        >
          <Icon name={project ? "back" : "grid"} size={18} />
          {project ? "所有项目" : "我的项目"}
        </Link>
        {project ? (
          <>
            <div className="nav-section-label">当前研究项目</div>
            <div className="current-project">
              <span className="project-initial">
                {Array.from(project.title)[0]}
              </span>
              <span>{project.title}</span>
            </div>
            <NavLink
              to={`/p/${project.id}`}
              end
              className={({ isActive }) =>
                `nav-link ${isActive ? "active" : ""}`
              }
              onClick={onClose}
            >
              <Icon name="grid" size={17} />
              项目概览
            </NavLink>
            <div className="future-nav">
              <span>
                <Icon name="paper" size={17} />
                论文库
              </span>
              <small>即将开放</small>
            </div>
            <div className="future-nav">
              <span>
                <Icon name="branch" size={17} />
                研究方向
              </span>
              <small>即将开放</small>
            </div>
            <div className="sidebar-divider" />
            <div className="session-label">
              <span className="nav-section-label">
                {archivedSessions ? "归档会话" : "会话"}
              </span>
              <button
                className="icon-button"
                aria-label="新建会话"
                title="新建会话"
                disabled={creating}
                onClick={onNewSession}
              >
                <Icon name="plus" size={17} />
              </button>
            </div>
            <nav aria-label="会话列表" className="sessions-nav">
              {sessions.map((session) => (
                <div className="session-nav-item" key={session.id}>
                  <NavLink
                    to={`/p/${project.id}/s/${session.id}`}
                    className={({ isActive }) =>
                      `nav-link session-link ${isActive ? "active" : ""}`
                    }
                    onClick={onClose}
                  >
                    <Icon name="chat" size={16} />
                    <span>{session.title}</span>
                  </NavLink>
                  {onDeleteSession && (
                    <button
                      type="button"
                      className="icon-button danger-icon session-delete"
                      aria-label="删除会话"
                      title={`删除会话：${session.title}`}
                      onClick={(event) => {
                        event.preventDefault();
                        event.stopPropagation();
                        onDeleteSession(session);
                      }}
                    >
                      <Icon name="trash" size={14} />
                    </button>
                  )}
                </div>
              ))}
              {sessions.length === 0 && (
                <p className="nav-empty">
                  {archivedSessions ? "暂无归档会话" : "为一个问题开启新会话"}
                </p>
              )}
            </nav>
            <button className="nav-archive" onClick={onToggleArchived}>
              <Icon name="archive" size={14} />
              {archivedSessions ? "查看活动会话" : "查看归档会话"}
            </button>
          </>
        ) : (
          <div className="sidebar-intro">
            <span className="eyebrow">ONE PROJECT, ONE FOCUS</span>
            <p>
              论文、问题与探索过程，
              <br />
              都从一个项目开始。
            </p>
            <div className="sidebar-line-art">
              <span />
              <span />
              <span />
            </div>
          </div>
        )}
        <button className="model-settings-trigger" aria-label="模型设置" onClick={() => setModelDialog(true)}>
          <Icon name="settings" size={15} />
          <span>模型设置<small>Fixture / Remote</small></span>
        </button>
        <div className="local-workspace">
          <span className="local-dot" />
          <div>
            本地工作区<small>研究资料保存在此设备</small>
          </div>
          <Icon name="laptop" size={17} />
        </div>
      </aside>
      {modelDialog && <ModelSettingsDialog onClose={() => setModelDialog(false)} />}
    </>
  );
}
