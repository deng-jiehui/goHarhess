import { useEffect, useState } from "react";
import type { ModelSettings, ModelSettingsUpdate } from "../types";
import { api, errorText } from "../api/client";
import { Icon } from "./Icon";
import { Modal } from "./Modal";

const STORAGE_KEY = "autoresearch.model.api_key";
const ENDPOINT_KEY = "autoresearch.model.endpoint";
const MODEL_KEY = "autoresearch.model.name";
const MODE_KEY = "autoresearch.model.mode";

function readStorage(key: string, fallback: string) {
  try {
    return window.localStorage.getItem(key) || fallback;
  } catch {
    return fallback;
  }
}

function writeStorage(key: string, value: string) {
  try {
    window.localStorage.setItem(key, value);
  } catch {
    // 浏览器禁用本地存储时，配置仍会发送给当前运行中的服务。
  }
}

export function ModelSettingsDialog({
  onClose,
  onSaved,
}: {
  onClose: () => void;
  onSaved?: (settings: ModelSettings) => void;
}) {
  const [draft, setDraft] = useState<ModelSettingsUpdate>({
    mode: readStorage(MODE_KEY, "fixture") as ModelSettingsUpdate["mode"],
    endpoint: readStorage(ENDPOINT_KEY, "https://api.openai.com/v1"),
    model: readStorage(MODEL_KEY, "gpt-4o-mini"),
    api_key: readStorage(STORAGE_KEY, ""),
  });
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    let active = true;
    void api
      .modelSettings()
      .then((settings) => {
        if (!active) return;
        setDraft((old) => ({
          ...old,
          mode: settings.mode,
          endpoint: settings.endpoint || old.endpoint,
          model: settings.model || old.model,
        }));
      })
      .catch((reason) => {
        if (active) setError(errorText(reason));
      })
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => {
      active = false;
    };
  }, []);

  function update<K extends keyof ModelSettingsUpdate>(key: K, value: ModelSettingsUpdate[K]) {
    setDraft((old) => ({ ...old, [key]: value }));
  }

  async function save() {
    setBusy(true);
    setError("");
    try {
      const settings = await api.updateModelSettings({
        ...draft,
        endpoint: draft.mode === "remote" ? draft.endpoint.trim() : "",
        model: draft.mode === "remote" ? draft.model.trim() : "",
        api_key: draft.mode === "remote" ? draft.api_key.trim() : "",
      });
      writeStorage(MODE_KEY, draft.mode);
      writeStorage(ENDPOINT_KEY, draft.endpoint.trim());
      writeStorage(MODEL_KEY, draft.model.trim());
      writeStorage(STORAGE_KEY, draft.api_key.trim());
      onSaved?.(settings);
      onClose();
    } catch (reason) {
      setError(errorText(reason));
    } finally {
      setBusy(false);
    }
  }

  return (
    <Modal
      title="模型设置"
      description="切换离线演示或真实的 OpenAI-compatible 模型。"
      onClose={onClose}
      busy={busy}
    >
      <div className="modal-body model-settings-body">
        {error && <p className="form-error" role="alert">{error}</p>}
        {loading ? (
          <p className="field-help">正在读取当前模型状态…</p>
        ) : (
          <>
            <label className="form-field">
              模式
              <select aria-label="模型模式" value={draft.mode} onChange={(event) => update("mode", event.target.value as ModelSettingsUpdate["mode"])}>
                <option value="fixture">Fixture（离线演示）</option>
                <option value="remote">Remote（真实模型）</option>
              </select>
            </label>
            {draft.mode === "remote" && (
              <>
                <label className="form-field">
                  API Key
                  <input aria-label="API Key" type="password" autoComplete="off" value={draft.api_key} onChange={(event) => update("api_key", event.target.value)} placeholder="sk-…" />
                  <span className="field-help">只保存在当前浏览器和运行中服务的内存里，不写入项目数据库。</span>
                </label>
                <label className="form-field">
                  模型端点
                  <input aria-label="模型端点" value={draft.endpoint} onChange={(event) => update("endpoint", event.target.value)} placeholder="https://api.openai.com/v1" />
                </label>
                <label className="form-field">
                  模型名称
                  <input aria-label="模型名称" value={draft.model} onChange={(event) => update("model", event.target.value)} placeholder="gpt-4o-mini" />
                </label>
              </>
            )}
            <div className="model-settings-note">
              <Icon name="spark" size={15} />
              <span>{draft.mode === "remote" ? "保存后，新提交的研究轮次会使用该模型。" : "Fixture 模式不调用外部服务，适合先验证流程。"}</span>
            </div>
          </>
        )}
      </div>
      <footer className="modal-footer">
        <button type="button" className="button secondary" onClick={onClose} disabled={busy}>取消</button>
        <button type="button" className="button primary" onClick={() => void save()} disabled={busy || loading}>{busy ? "保存中…" : "保存模型设置"}</button>
      </footer>
    </Modal>
  );
}
