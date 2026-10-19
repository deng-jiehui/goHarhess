import { useEffect, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { api, APIError, errorText } from "../api/client";
import type { Project, Session } from "../types";
import { ProjectNav } from "../components/ProjectNav";
import { ContextPanel } from "../components/ContextPanel";
import { EmptyState } from "../components/EmptyState";
import { ProjectSettingsDialog } from "../components/ProjectDialogs";
import { DeleteSessionDialog } from "../components/DeleteSessionDialog";
import { Icon } from "../components/Icon";
import { useNotice } from "../components/Notifications";
import { SessionPage } from "./SessionPage";

export function WorkspacePage() {
  const { projectId = "", sessionId } = useParams();
  const [project, setProject] = useState<Project | null>(null);
  const [sessions, setSessions] = useState<Session[]>([]);
  const [session, setSession] = useState<Session | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [loading, setLoading] = useState(true);
  const [settings, setSettings] = useState(false);
  const [refresh, setRefresh] = useState(0);
  const [creating, setCreating] = useState(false);
  const [actionError, setActionError] = useState("");
  const [archived, setArchived] = useState(false);
  const [mobileNav, setMobileNav] = useState(false);
  const [deletingSession, setDeletingSession] = useState<Session | null>(null);
  const navigate = useNavigate();
  const notice = useNotice();
  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setError(null);
    setActionError("");
    Promise.all([
      api.project(projectId, controller.signal),
      api.sessions(projectId, archived, controller.signal),
      sessionId
        ? api.session(projectId, sessionId, controller.signal)
        : Promise.resolve(null),
    ])
      .then(([p, list, s]) => {
        if (!controller.signal.aborted) {
          setProject(p);
          setSessions(list.items);
          setSession(s);
        }
      })
      .catch((error) => {
        if (!controller.signal.aborted) setError(error);
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => controller.abort();
  }, [projectId, sessionId, archived, refresh]);
  async function createSession() {
    if (creating) return;
    setCreating(true);
    setActionError("");
    try {
      const created = await api.createSession(projectId);
      setArchived(false);
      setMobileNav(false);
      navigate(`/p/${projectId}/s/${created.id}`);
      notice("会话已创建");
    } catch (error) {
      setActionError(errorText(error));
    } finally {
      setCreating(false);
    }
  }
  const notFound = error instanceof APIError && error.status === 404;
  if (loading || error || !project)
    return (
      <div className="app-shell">
        <ProjectNav />
        <main className="loading-main">
          <EmptyState
            loading={loading}
            title={
              loading
                ? "正在打开项目"
                : notFound
                  ? "内容不存在"
                  : "暂时无法打开项目"
            }
            description={
              loading
                ? "正在读取研究设置与会话…"
                : notFound
                  ? "项目或会话不存在，也可能不属于当前项目。"
                  : errorText(error)
            }
            home={!loading}
            action={
              !loading && !notFound ? (
                <button
                  className="button secondary"
                  onClick={() => setRefresh((value) => value + 1)}
                >
                  重新加载
                </button>
              ) : undefined
            }
          />
        </main>
      </div>
    );
  return (
    <div className="app-shell">
      <ProjectNav
        project={project}
        sessions={sessions}
        onNewSession={createSession}
        onDeleteSession={setDeletingSession}
        creating={creating}
        archivedSessions={archived}
        onToggleArchived={() => setArchived((value) => !value)}
        mobileOpen={mobileNav}
        onClose={() => setMobileNav(false)}
      />
      <div className={`main-shell workspace-main${session ? " has-session" : ""}`}>
        <header className="topbar">
          <div className="breadcrumb">
            <button
              className="icon-button mobile-menu"
              aria-label="展开导航"
              onClick={() => setMobileNav(true)}
            >
              <Icon name="menu" />
            </button>
            <Link to="/">我的项目</Link>
            <span className="crumb-divider">/</span>
            <strong>{project.title}</strong>
          </div>
          <span className="quiet-status">
            <span className="status-dot" />
            本地存储
          </span>
        </header>
        <header className="workspace-header">
          <div>
            <div className="eyebrow">RESEARCH PROJECT</div>
            <div className="workspace-title">
              <h1>{project.title}</h1>
              <span className={`tag ${project.archived ? "neutral" : ""}`}>
                {project.archived ? "已归档" : "进行中"}
              </span>
            </div>
          </div>
          <div className="header-actions">
            <button
              className="icon-button"
              aria-label="刷新项目"
              title="刷新项目"
              onClick={() => setRefresh((value) => value + 1)}
            >
              <Icon name="refresh" size={17} />
            </button>
            <button
              className="button secondary"
              onClick={() => setSettings(true)}
            >
              <Icon name="settings" size={17} />
              项目设置
            </button>
            <Link className="button secondary" to={`/p/${projectId}/papers`}>论文库</Link>
          </div>
        </header>
        {actionError && (
          <p className="action-error" role="alert">
            {actionError}
          </p>
        )}
        <div className="workspace-columns">
          <main className="workspace-content">
            {session ? (
              <SessionPage
                key={session.id}
                session={session}
                onSaved={(updated) => {
                  setSession(updated);
                  setSessions((old) =>
                    old.map((item) =>
                      item.id === updated.id ? updated : item,
                    ),
                  );
                  notice("会话名称已更新");
                }}
                onArchived={() => {
                  notice(session.archived ? "会话已恢复" : "会话已归档");
                  navigate(`/p/${projectId}`);
                  setRefresh((value) => value + 1);
                }}
                onDeleted={() => {
                  setSession(null);
                  setSessions((old) => old.filter((item) => item.id !== session.id));
                  notice("会话已删除");
                  navigate(`/p/${projectId}`);
                }}
              />
            ) : (
              <div className="project-overview">
                <div className="research-brief">
                  <div className="brief-symbol">
                    <Icon name="branch" size={25} />
                  </div>
                  <span className="eyebrow">RESEARCH BRIEF</span>
                  <h2>{project.question || "为这个项目，确定一个研究问题"}</h2>
                  <p>
                    {project.question
                      ? "从这个问题出发，积累资料、记录思考，逐步缩小研究范围。"
                      : "好的问题不必一步到位。先记录你想探索的方向，再在研究中逐步完善。"}
                  </p>
                  <button
                    className="text-button"
                    onClick={() => setSettings(true)}
                  >
                    {project.question ? "调整研究问题" : "完善研究问题"}
                    <Icon name="arrow" size={16} />
                  </button>
                </div>
                <section className="overview-sessions">
                  <div className="section-heading">
                    <h2>
                      {archived ? "归档会话" : "项目会话"}
                      <span>{sessions.length}</span>
                    </h2>
                    <button
                      className="text-button"
                      disabled={creating}
                      onClick={createSession}
                    >
                      <Icon name="plus" size={16} />
                      开始一个会话
                    </button>
                  </div>
                  {sessions.length ? (
                    <div className="session-cards">
                      {sessions.map((item) => (
                        <Link
                          to={`/p/${projectId}/s/${item.id}`}
                          className="session-card"
                          key={item.id}
                        >
                          <span>
                            <Icon name="chat" size={20} />
                          </span>
                          <div>
                            <strong>{item.title}</strong>
                            <small>
                              {item.archived
                                ? "已归档，可随时恢复"
                                : "会话空间已创建"}
                            </small>
                          </div>
                          <Icon name="arrow" size={16} />
                        </Link>
                      ))}
                    </div>
                  ) : (
                    <div className="session-empty">
                      <Icon name="chat" size={26} />
                      <strong>
                        {archived ? "暂无归档会话" : "还没有会话"}
                      </strong>
                      <p>每个问题，都可以拥有一段独立的探索过程。</p>
                    </div>
                  )}
                </section>
              </div>
            )}
          </main>
          <ContextPanel project={project} onEdit={() => setSettings(true)} />
        </div>
      </div>
      {settings && (
        <ProjectSettingsDialog
          project={project}
          onClose={() => setSettings(false)}
          onSaved={(updated) => {
            setProject(updated);
            setSettings(false);
            notice("研究设置已保存");
          }}
          onArchived={(updated) => {
            setSettings(false);
            setProject(updated);
            notice(updated.archived ? "项目已归档" : "项目已恢复");
            if (updated.archived) navigate("/");
          }}
          onDeleted={() => {
            setSettings(false);
            notice("项目已删除");
            navigate("/");
          }}
        />
      )}
      {deletingSession && (
        <DeleteSessionDialog
          session={deletingSession}
          onClose={() => setDeletingSession(null)}
          onDeleted={() => {
            const deletedID = deletingSession.id;
            setDeletingSession(null);
            setSessions((old) => old.filter((item) => item.id !== deletedID));
            if (session?.id === deletedID) {
              setSession(null);
              navigate(`/p/${projectId}`);
            }
            notice("会话已删除");
          }}
        />
      )}
    </div>
  );
}
