import { useState } from "react";
import type { ProjectNote } from "../types";
import { api, errorText } from "../api/client";
import { noteSummary } from "../notes";
import { Icon } from "./Icon";
import { Modal } from "./Modal";
import { MarkdownRenderer } from "./MarkdownRenderer";

const purpose = "保存已确认的研究目标、关键结论和实验计划。同一项目的所有会话共享，助手会在后续对话中参考；修改项目主题不会自动清空历史笔记。";

export function ProjectNotes({ projectID, sessionID, turnID, notes, onDeleted }: {
  projectID: string;
  sessionID: string;
  turnID?: string;
  notes: ProjectNote[];
  onDeleted: (noteID: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const [expanded, setExpanded] = useState(false);
  const [deleting, setDeleting] = useState<ProjectNote | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  function origin(note: ProjectNote) {
    if (turnID && note.source_turn_id === turnID) return "本轮保存";
    if (note.source_session_id === sessionID) return "本会话历史笔记";
    return note.source_session_id ? "历史笔记 · 来自其他会话" : "项目历史笔记";
  }

  async function remove() {
    if (!deleting || busy) return;
    setBusy(true);
    setError("");
    try {
      await api.deleteNote(projectID, deleting.id);
      onDeleted(deleting.id);
      setDeleting(null);
    } catch (reason) {
      setError(errorText(reason));
    } finally {
      setBusy(false);
    }
  }

  const latest = notes[0];
  return (
    <>
      <section className={`note-reference${expanded ? " expanded" : ""}`} aria-label="项目笔记">
        <div className="note-reference-heading">
          <span><Icon name="paper" size={15} />项目笔记</span>
          <small>{notes.length} 条 · 项目共享</small>
        </div>
        <p className="note-purpose">{purpose}</p>
        {latest ? <>
          <div className="note-origin"><span>{origin(latest)}</span><span>{turnID && latest.source_turn_id === turnID ? "已保存，后续会话可复用" : "历史内容，并非本轮新生成"}</span></div>
          {expanded ? <MarkdownRenderer source={latest.text} className="note-markdown note-reference-markdown" /> : <p>{noteSummary(latest.text)}</p>}
          <div className="note-reference-actions">
            <button type="button" className="text-button" onClick={() => setExpanded((value) => !value)}>{expanded ? "收起项目笔记" : "展开项目笔记"}</button>
            <button type="button" className="text-button" onClick={() => setOpen(true)}>打开项目笔记<Icon name="arrow" size={15} /></button>
            <button type="button" className="text-button danger-text" onClick={() => { setError(""); setDeleting(latest); }}>删除这条笔记</button>
          </div>
        </> : <p>暂无项目笔记。需要保留结论时，可以告诉助手“保存为项目笔记”。</p>}
      </section>
      {open && <Modal title="项目笔记" description={purpose} onClose={() => setOpen(false)} wide>
        <div className="note-dialog-body">
          {notes.length === 0 && <p className="muted">暂无项目笔记</p>}
          {notes.map((note) => <article className="note-entry" key={note.id}>
            <div><span>{origin(note)}</span><time>{note.created_at ? new Date(note.created_at).toLocaleString("zh-CN") : ""}</time></div>
            <h3>{noteSummary(note.text)}</h3>
            <MarkdownRenderer source={note.text} className="note-markdown" />
            <div className="note-entry-actions"><button type="button" className="text-button danger-text" aria-label="删除笔记" onClick={() => { setError(""); setDeleting(note); }}><Icon name="trash" size={14} />删除笔记</button></div>
          </article>)}
        </div>
      </Modal>}
      {deleting && <Modal title="删除项目笔记" description="此操作会从本项目所有会话的笔记列表移除这条内容。" busy={busy} onClose={() => setDeleting(null)}>
        <div className="modal-body">
          <p className="delete-warning">确认删除“{noteSummary(deleting.text, 70)}”？删除后，后续新消息不再引用这条项目笔记，已有会话消息不会删除。正在执行的任务仍使用提交时的资料。</p>
          {error && <p className="form-error" role="alert">{error}</p>}
        </div>
        <footer className="modal-footer">
          <button type="button" className="button secondary" disabled={busy} onClick={() => setDeleting(null)}>取消</button>
          <button type="button" className="button danger" disabled={busy} onClick={() => void remove()}>{busy ? "正在删除…" : "确认删除"}</button>
        </footer>
      </Modal>}
    </>
  );
}
