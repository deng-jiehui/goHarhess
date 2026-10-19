import { useState } from "react";
import type { FormEvent } from "react";
import type { Session } from "../types";
import { api, errorText } from "../api/client";
import { Modal } from "./Modal";

export function DeleteSessionDialog({
  session,
  onClose,
  onDeleted,
}: {
  session: Session;
  onClose: () => void;
  onDeleted: () => void;
}) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  async function remove(event: FormEvent) {
    event.preventDefault();
    if (busy) return;
    setBusy(true);
    setError("");
    try {
      await api.deleteSession(session.project_id, session.id);
      onDeleted();
    } catch (reason) {
      setError(errorText(reason));
    } finally {
      setBusy(false);
    }
  }
  return (
    <Modal
      title="删除会话"
      description="会话消息、运行记录和检查点将被永久删除，无法撤销。"
      onClose={onClose}
      busy={busy}
    >
      <form onSubmit={remove}>
        <div className="modal-body">
          <p className="delete-warning">
            你即将删除 <strong>{session.title}</strong>。如果只是暂时停止使用，可以取消并选择归档。
          </p>
          {error && <p className="form-error" role="alert">{error}</p>}
        </div>
        <footer className="modal-footer">
          <button type="button" className="button secondary" disabled={busy} onClick={onClose}>
            取消
          </button>
          <button className="button danger" disabled={busy}>
            {busy ? "正在删除…" : "确认永久删除"}
          </button>
        </footer>
      </form>
    </Modal>
  );
}
