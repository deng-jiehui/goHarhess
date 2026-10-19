import { useState } from "react";
import type { FormEvent } from "react";
import type { Session } from "../types";
import { api, errorText } from "../api/client";
import { Modal } from "./Modal";

export function RenameSessionDialog({
  session,
  onClose,
  onSaved,
}: {
  session: Session;
  onClose: () => void;
  onSaved: (session: Session) => void;
}) {
  const [title, setTitle] = useState(session.title);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  async function submit(event: FormEvent) {
    event.preventDefault();
    if (busy) return;
    setBusy(true);
    setError("");
    try {
      onSaved(
        await api.updateSession(session.project_id, session.id, { title }),
      );
    } catch (error) {
      setError(errorText(error));
    } finally {
      setBusy(false);
    }
  }
  return (
    <Modal title="重命名会话" onClose={onClose} busy={busy}>
      <form onSubmit={submit}>
        <div className="modal-body">
          <label className="form-field">
            会话名称
            <input
              autoFocus
              required
              value={title}
              onChange={(event) => setTitle(event.target.value)}
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
          <button className="button primary" disabled={busy || !title.trim()}>
            {busy ? "正在保存…" : "保存会话名称"}
          </button>
        </footer>
      </form>
    </Modal>
  );
}
