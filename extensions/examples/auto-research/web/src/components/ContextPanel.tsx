import type { Project } from "../types";
import { Icon } from "./Icon";

export function ContextPanel({
  project,
  onEdit,
}: {
  project: Project;
  onEdit: () => void;
}) {
  const settings = project.settings;
  return (
    <aside className="context-panel" aria-label="研究上下文">
      <div className="context-heading">
        <span>
          <Icon name="target" size={18} />
          研究上下文
        </span>
        <button
          className="icon-button"
          aria-label="编辑研究上下文"
          onClick={onEdit}
        >
          <Icon name="edit" size={16} />
        </button>
      </div>
      <section className="context-section">
        <h2>研究问题</h2>
        <p className={project.question ? "question-text" : "muted"}>
          {project.question ||
            "还没有确定研究问题。先记录一个想法，之后可以逐步完善。"}
        </p>
        {project.field && (
          <span className="tag field-tag">{project.field}</span>
        )}
      </section>
      <section className="context-section">
        <h2>研究范围</h2>
        <dl className="scope-list">
          <div>
            <dt>论文来源</dt>
            <dd>
              <Icon name="globe" size={14} />
              arXiv
            </dd>
          </div>
          <div>
            <dt>发表年份</dt>
            <dd>
              {settings.year_from
                ? `${settings.year_from}–${settings.year_to}`
                : "不限年份"}
            </dd>
          </div>
          <div>
            <dt>候选论文</dt>
            <dd>最多 {settings.candidate_limit} 篇</dd>
          </div>
          <div>
            <dt>深入分析</dt>
            <dd>最多 {settings.analyze_limit} 篇</dd>
          </div>
        </dl>
      </section>
      <section className="context-section">
        <h2>
          参考资料<span className="soft-label">待接入</span>
        </h2>
        <div className="context-placeholder">
          <Icon name="paper" size={22} />
          <p>研究资料将在这里汇集</p>
          <span>论文库开放后，可为会话选择论文与证据。</span>
        </div>
      </section>
      <div className="context-note">
        <Icon name="spark" size={16} />
        <p>
          好的研究，从一个清晰的问题开始。
          <br />
          项目设置可以随时调整。
        </p>
      </div>
    </aside>
  );
}
