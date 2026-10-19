import { useState } from "react";
import type { FormEvent } from "react";
import { api, errorText } from "../api/client";
import type { Project, ProjectUpdate } from "../types";
import { Modal } from "./Modal";
import { Icon } from "./Icon";

export function CreateProjectDialog({
  onClose,
  onCreated,
}: {
  onClose: () => void;
  onCreated: (project: Project) => void;
}) {
  const [title, setTitle] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  async function submit(event: FormEvent) {
    event.preventDefault();
    if (busy) return;
    setBusy(true);
    setError("");
    try {
      onCreated(await api.createProject(title));
    } catch (error) {
      setError(errorText(error));
    } finally {
      setBusy(false);
    }
  }
  return (
    <Modal
      title="新建研究项目"
      description="用一个项目，持续积累同一主题的研究。"
      onClose={onClose}
      busy={busy}
    >
      <form onSubmit={submit}>
        <div className="modal-body">
          <label className="form-field">
            项目名称
            <input
              autoFocus
              required
              value={title}
              onChange={(event) => setTitle(event.target.value)}
              placeholder="例如：LLM Agent 的代码缺陷修复"
            />
          </label>
          <p className="field-help">
            先给方向起个名字，研究问题和范围可以稍后完善。
          </p>
          {error && (
            <p className="form-error" role="alert">
              {error}
            </p>
          )}
        </div>
        <footer className="modal-footer">
          <button
            type="button"
            className="button secondary"
            disabled={busy}
            onClick={onClose}
          >
            取消
          </button>
          <button className="button primary" disabled={busy || !title.trim()}>
            {busy ? "正在创建…" : "创建项目"}
            <Icon name="arrow" size={16} />
          </button>
        </footer>
      </form>
    </Modal>
  );
}

export function ProjectSettingsDialog({
  project,
  onClose,
  onSaved,
  onArchived,
  onDeleted,
}: {
  project: Project;
  onClose: () => void;
  onSaved: (project: Project) => void;
  onArchived: (project: Project) => void;
  onDeleted: () => void;
}) {
  const [draft, setDraft] = useState<ProjectUpdate>(() => ({
    title: project.title,
    question: project.question,
    field: project.field,
    settings: {
      ...project.settings,
      providers: [...project.settings.providers],
    },
    revision: project.revision,
  }));
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [deleting, setDeleting] = useState(false);
  const set = (key: "title" | "question" | "field", value: string) =>
    setDraft((old) => ({ ...old, [key]: value }));
  const number = (
    key: "year_from" | "year_to" | "candidate_limit" | "analyze_limit",
    value: string,
  ) =>
    setDraft((old) => ({
      ...old,
      settings: { ...old.settings, [key]: Number(value) },
    }));
  async function save(event: FormEvent) {
    event.preventDefault();
    if (busy) return;
    setBusy(true);
    setError("");
    try {
      onSaved(await api.updateProject(project.id, draft));
    } catch (error) {
      setError(errorText(error));
    } finally {
      setBusy(false);
    }
  }
  async function archive() {
    setBusy(true);
    setError("");
    try {
      onArchived(await api.archiveProject(project, !project.archived));
    } catch (error) {
      setError(errorText(error));
    } finally {
      setBusy(false);
    }
  }
  if (deleting)
    return (
      <DeleteProjectDialog
        project={project}
        onClose={() => setDeleting(false)}
        onDeleted={onDeleted}
      />
    );
  return (
    <Modal
      title="项目设置"
      description="为当前研究保存一个清晰的范围。"
      wide
      onClose={onClose}
      busy={busy}
    >
      <form onSubmit={save}>
        <div className="modal-body settings-body">
          <fieldset disabled={busy}>
            <label className="form-field">
              项目名称
              <input
                required
                value={draft.title}
                onChange={(event) => set("title", event.target.value)}
              />
            </label>
            <label className="form-field">
              研究问题
              <textarea
                rows={3}
                value={draft.question}
                placeholder="你希望通过这个项目回答什么问题？"
                onChange={(event) => set("question", event.target.value)}
              />
            </label>
            <label className="form-field">
              研究领域
              <input
                value={draft.field}
                placeholder="例如：软件工程、信息检索"
                onChange={(event) => set("field", event.target.value)}
              />
            </label>
            <div className="form-section-title">研究范围</div>
            <div className="form-grid">
              <label className="form-field">
                起始年份
                <input
                  type="number"
                  min="1"
                  max="9999"
                  placeholder="不限"
                  value={draft.settings.year_from || ""}
                  onChange={(event) => number("year_from", event.target.value)}
                />
              </label>
              <label className="form-field">
                结束年份
                <input
                  type="number"
                  min="1"
                  max="9999"
                  placeholder="不限"
                  value={draft.settings.year_to || ""}
                  onChange={(event) => number("year_to", event.target.value)}
                />
              </label>
            </div>
            <div className="form-field">
              论文来源
              <div className="source-choice">
                <Icon name="check" size={15} />
                <span>arXiv</span>
                <small>公开论文与预印本</small>
              </div>
            </div>
            <div className="form-grid">
              <label className="form-field">
                候选论文上限
                <input
                  required
                  type="number"
                  min="1"
                  max="100"
                  value={draft.settings.candidate_limit}
                  onChange={(event) =>
                    number("candidate_limit", event.target.value)
                  }
                />
              </label>
              <label className="form-field">
                分析论文上限
                <input
                  required
                  type="number"
                  min="1"
                  max="30"
                  value={draft.settings.analyze_limit}
                  onChange={(event) =>
                    number("analyze_limit", event.target.value)
                  }
                />
              </label>
            </div>
          </fieldset>
          {error && (
            <p className="form-error" role="alert">
              {error}
            </p>
          )}
          <div className="project-lifecycle">
            <button
              className="text-button"
              type="button"
              disabled={busy}
              onClick={archive}
            >
              <Icon name="archive" size={16} />
              {project.archived ? "恢复项目" : "归档项目"}
            </button>
            <button
              className="text-button danger-text"
              type="button"
              disabled={busy}
              onClick={() => setDeleting(true)}
            >
              永久删除项目
            </button>
          </div>
        </div>
        <footer className="modal-footer">
          <button
            type="button"
            className="button secondary"
            disabled={busy}
            onClick={onClose}
          >
            取消
          </button>
          <button className="button primary" disabled={busy}>
            {busy ? "正在保存…" : "保存设置"}
          </button>
        </footer>
      </form>
    </Modal>
  );
}

function DeleteProjectDialog({
  project,
  onClose,
  onDeleted,
}: {
  project: Project;
  onClose: () => void;
  onDeleted: () => void;
}) {
  const [confirm, setConfirm] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  async function remove(event: FormEvent) {
    event.preventDefault();
    if (busy || confirm !== project.title) return;
    setBusy(true);
    setError("");
    try {
      await api.deleteProject(project, confirm);
      onDeleted();
    } catch (error) {
      setError(errorText(error));
    } finally {
      setBusy(false);
    }
  }
  return (
    <Modal
      title="删除项目"
      description="项目及其会话记录将被永久删除，无法撤销。"
      onClose={onClose}
      busy={busy}
    >
      <form onSubmit={remove}>
        <div className="modal-body">
          <p className="delete-warning">
            你即将删除 <strong>{project.title}</strong>
            。如果只是暂时停止研究，可以取消并选择归档。
          </p>
          <label className="form-field">
            输入项目名称确认
            <input
              autoFocus
              value={confirm}
              onChange={(event) => setConfirm(event.target.value)}
              autoComplete="off"
            />
          </label>
          {error && (
            <p className="form-error" role="alert">
              {error}
            </p>
          )}
        </div>
        <footer className="modal-footer">
          <button
            type="button"
            className="button secondary"
            disabled={busy}
            onClick={onClose}
          >
            取消
          </button>
          <button
            className="button danger"
            disabled={busy || confirm !== project.title}
          >
            {busy ? "正在删除…" : "确认永久删除"}
          </button>
        </footer>
      </form>
    </Modal>
  );
}
