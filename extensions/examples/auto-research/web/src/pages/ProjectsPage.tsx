import { useEffect, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { api, errorText } from "../api/client";
import type { Project } from "../types";
import { ProjectNav } from "../components/ProjectNav";
import { CreateProjectDialog } from "../components/ProjectDialogs";
import { EmptyState } from "../components/EmptyState";
import { Icon } from "../components/Icon";
import { useNotice } from "../components/Notifications";

export function ProjectsPage() {
  const [items, setItems] = useState<Project[]>([]);
  const [archived, setArchived] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [query, setQuery] = useState("");
  const [creating, setCreating] = useState(false);
  const [refresh, setRefresh] = useState(0);
  const [mobileNav, setMobileNav] = useState(false);
  const navigate = useNavigate();
  const notice = useNotice();
  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setError("");
    api
      .projects(archived, controller.signal)
      .then((result) => {
        if (!controller.signal.aborted) setItems(result.items);
      })
      .catch((error) => {
        if (!controller.signal.aborted) setError(errorText(error));
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => controller.abort();
  }, [archived, refresh]);
  const filtered = items.filter((project) =>
    `${project.title} ${project.question} ${project.field}`
      .toLowerCase()
      .includes(query.toLowerCase()),
  );
  return (
    <div className="app-shell">
      <ProjectNav mobileOpen={mobileNav} onClose={() => setMobileNav(false)} />
      <div className="main-shell">
        <header className="topbar">
          <div className="breadcrumb">
            <button
              className="icon-button mobile-menu"
              aria-label="展开导航"
              onClick={() => setMobileNav(true)}
            >
              <Icon name="menu" />
            </button>
            <Icon name="folder" size={16} />
            <span>研究空间</span>
            <span className="crumb-divider">/</span>
            <strong>我的项目</strong>
          </div>
          <span className="quiet-status">
            <span className="status-dot" />
            本地存储
          </span>
        </header>
        <main className="projects-main">
          <div className="page-heading">
            <div>
              <div className="eyebrow">YOUR RESEARCH SPACE</div>
              <h1>
                我的研究项目<span className="title-dot">.</span>
              </h1>
              <p>给每个方向一个空间，让研究持续积累。</p>
            </div>
            <button
              className="button primary"
              onClick={() => setCreating(true)}
            >
              <Icon name="plus" size={18} />
              新建项目
            </button>
          </div>
          <div className="project-toolbar">
            <div className="tabs">
              <button
                className={archived ? "" : "selected"}
                onClick={() => setArchived(false)}
              >
                进行中{!archived && !loading && <span>{items.length}</span>}
              </button>
              <button
                className={archived ? "selected" : ""}
                onClick={() => setArchived(true)}
              >
                已归档
              </button>
            </div>
            <label className="search-field">
              <Icon name="search" size={16} />
              <input
                aria-label="搜索项目"
                placeholder="搜索项目…"
                value={query}
                onChange={(event) => setQuery(event.target.value)}
              />
            </label>
          </div>
          {loading ? (
            <EmptyState
              loading
              title="正在打开研究空间"
              description="正在读取本地项目…"
            />
          ) : error ? (
            <EmptyState
              title="暂时无法读取项目"
              description={error}
              action={
                <button
                  className="button secondary"
                  onClick={() => setRefresh((value) => value + 1)}
                >
                  重新加载
                </button>
              }
            />
          ) : filtered.length ? (
            <div className="project-grid">
              {filtered.map((project) => (
                <article className="project-card" key={project.id}>
                  <div className="card-top">
                    <span className="project-card-icon">
                      <Icon name="folder" size={22} />
                    </span>
                    <span
                      className={`tag ${project.archived ? "neutral" : ""}`}
                    >
                      {project.archived ? "已归档" : "进行中"}
                    </span>
                  </div>
                  <Link className="project-card-title" to={`/p/${project.id}`}>
                    {project.title}
                  </Link>
                  <p>
                    {project.question ||
                      "还没有记录研究问题，进入项目完善你的研究方向。"}
                  </p>
                  <div className="project-card-meta">
                    <span>{project.field || "领域待设置"}</span>
                    <span>arXiv</span>
                  </div>
                  <Link className="card-open" to={`/p/${project.id}`}>
                    打开工作区
                    <Icon name="arrow" size={16} />
                  </Link>
                </article>
              ))}
            </div>
          ) : (
            <div className="project-empty">
              <EmptyState
                title={
                  query
                    ? "没有找到匹配的项目"
                    : archived
                      ? "暂无归档项目"
                      : "从一个研究问题开始"
                }
                description={
                  query
                    ? "试试其他关键词，或清空搜索。"
                    : archived
                      ? "暂时停止研究的项目，可以归档后保留在这里。"
                      : "为感兴趣的主题创建项目，保存研究范围，建立专属会话。"
                }
                icon={archived ? "archive" : "branch"}
                action={
                  !archived && !query ? (
                    <button
                      className="button secondary"
                      onClick={() => setCreating(true)}
                    >
                      创建第一个项目
                      <Icon name="arrow" size={16} />
                    </button>
                  ) : undefined
                }
              />
              {!archived && !query && (
                <div className="getting-started">
                  <div>
                    <span>01</span>
                    <strong>确定一个主题</strong>
                    <p>从你感兴趣的问题出发</p>
                  </div>
                  <div>
                    <span>02</span>
                    <strong>划定研究范围</strong>
                    <p>记录领域、年份和资料来源</p>
                  </div>
                  <div>
                    <span>03</span>
                    <strong>留下探索过程</strong>
                    <p>按问题建立可继续的会话</p>
                  </div>
                </div>
              )}
            </div>
          )}
          <footer className="page-footer">
            <span>一点想法，一步研究。</span>
            <span>AutoResearch 工作区</span>
          </footer>
        </main>
      </div>
      {creating && (
        <CreateProjectDialog
          onClose={() => setCreating(false)}
          onCreated={(project) => {
            setCreating(false);
            notice("项目已创建");
            navigate(`/p/${project.id}`);
          }}
        />
      )}
    </div>
  );
}
