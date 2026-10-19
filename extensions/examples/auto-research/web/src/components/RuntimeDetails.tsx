import { useMemo } from "react";
import type { SessionEvent, Turn } from "../types";
import { Modal } from "./Modal";

export interface RuntimeEvent {
  id: number;
  kind: string;
  phase: string;
  name: string;
  time?: string;
  durationMs: number;
  traceId?: string;
  spanId?: string;
  parentSpanId?: string;
  attrs?: Record<string, unknown>;
  usage?: {
    input_tokens?: number;
    output_tokens?: number;
    total_tokens?: number;
    cache_read_tokens?: number;
    cache_write_tokens?: number;
  };
  error?: string;
}

export interface RuntimeMetrics {
  modelCalls: number;
  nodes: number;
  totalTokens: number;
  cacheReadTokens: number;
  cacheWriteTokens: number;
  cacheHitRate: number | null;
  errors: number;
  durationMs: number;
}

function parseJSONText(value: string): unknown {
  try {
    return JSON.parse(value);
  } catch {
    try {
      const binary = globalThis.atob(value);
      const bytes = Uint8Array.from(binary, (character) => character.charCodeAt(0));
      return JSON.parse(new TextDecoder().decode(bytes));
    } catch {
      return null;
    }
  }
}

export function parseRuntimeEvent(event: Pick<SessionEvent, "id" | "kind" | "payload_json">): RuntimeEvent | null {
  if (!event.kind.startsWith("runtime.")) return null;
  const payload = parseJSONText(event.payload_json) as Record<string, unknown> | null;
  if (!payload || typeof payload !== "object") return null;
  const kind = typeof payload.kind === "string" ? payload.kind : event.kind.slice("runtime.".length);
  const phase = typeof payload.phase === "string" ? payload.phase : "event";
  return {
    id: event.id,
    kind,
    phase,
    name: typeof payload.name === "string" ? payload.name : "未命名操作",
    time: typeof payload.time === "string" ? payload.time : undefined,
    durationMs: typeof payload.duration_ms === "number" ? payload.duration_ms : 0,
    traceId: typeof payload.trace_id === "string" ? payload.trace_id : undefined,
    spanId: typeof payload.span_id === "string" ? payload.span_id : undefined,
    parentSpanId: typeof payload.parent_span_id === "string" ? payload.parent_span_id : undefined,
    attrs: payload.attrs && typeof payload.attrs === "object" ? payload.attrs as Record<string, unknown> : undefined,
    usage: payload.usage && typeof payload.usage === "object" ? payload.usage as RuntimeEvent["usage"] : undefined,
    error: typeof payload.error === "string" ? payload.error : undefined,
  };
}

export function runtimeMetrics(events: RuntimeEvent[]): RuntimeMetrics {
  const modelSpans = new Set<string>();
  const nodeSpans = new Set<string>();
  const usageSpans = new Set<string>();
  let totalTokens = 0;
  let inputTokens = 0;
  let cacheReadTokens = 0;
  let cacheWriteTokens = 0;
  let cacheObserved = false;
  let errors = 0;
  let durationMs = 0;
  for (const event of events) {
    const span = event.spanId || `${event.kind}:${event.name}`;
    if (event.kind === "llm") modelSpans.add(span);
    if (event.kind === "node") nodeSpans.add(span);
    if (event.phase === "error" || event.error) errors += 1;
    durationMs = Math.max(durationMs, event.durationMs);
    if (event.kind === "llm" && event.usage && !usageSpans.has(span)) {
      usageSpans.add(span);
      const usage = event.usage;
      totalTokens += usage.total_tokens || 0;
      inputTokens += usage.input_tokens || 0;
      cacheReadTokens += usage.cache_read_tokens || 0;
      cacheWriteTokens += usage.cache_write_tokens || 0;
      cacheObserved = cacheObserved || usage.cache_read_tokens !== undefined || usage.cache_write_tokens !== undefined;
    }
  }
  return {
    modelCalls: modelSpans.size,
    nodes: nodeSpans.size,
    totalTokens,
    cacheReadTokens,
    cacheWriteTokens,
    cacheHitRate: cacheObserved && inputTokens > 0 ? (cacheReadTokens / inputTokens) * 100 : null,
    errors,
    durationMs,
  };
}

function phaseLabel(phase: string) {
  switch (phase) {
    case "start": return "开始";
    case "end": return "完成";
    case "error": return "失败";
    case "output": return "输出";
    default: return phase;
  }
}

function kindLabel(kind: string) {
  switch (kind) {
    case "run": return "Graph";
    case "node": return "节点";
    case "llm": return "模型";
    case "checkpoint": return "检查点";
    case "interrupt": return "交互";
    default: return kind;
  }
}

function cacheHitRateLabel(value: number | null) {
  return value === null ? "—" : `${value.toFixed(1)}%`;
}

export function RuntimeDetails({ events, turn, onClose }: { events: SessionEvent[]; turn?: Turn; onClose: () => void }) {
  const parsed = useMemo(() => events.map(parseRuntimeEvent).filter((event): event is RuntimeEvent => event !== null), [events]);
  const metrics = useMemo(() => runtimeMetrics(parsed), [parsed]);
  const activeNode = [...parsed].reverse().find((event) => event.kind === "node" && event.phase === "start" && !parsed.some((candidate) => candidate.spanId === event.spanId && ["end", "error"].includes(candidate.phase)));
  return (
    <Modal title="开发运行详情" description="只显示脱敏的 Graph、模型和检查点生命周期信息" onClose={onClose} wide>
      <div className="runtime-details">
        <div className="runtime-metrics" aria-label="运行指标">
          <div><strong>{metrics.nodes}</strong><span>节点</span></div>
          <div><strong>{metrics.modelCalls}</strong><span>模型调用</span></div>
          <div><strong>{metrics.totalTokens || "—"}</strong><span>Token</span></div>
          <div><strong>{metrics.durationMs ? `${(metrics.durationMs / 1000).toFixed(1)}s` : "—"}</strong><span>最长耗时</span></div>
          <div title="缓存读取 Token ÷ 输入 Token"><strong>{cacheHitRateLabel(metrics.cacheHitRate)}</strong><span>缓存命中率</span></div>
        </div>
        {activeNode && <p className="runtime-active">当前节点：<b>{activeNode.name}</b></p>}
        {metrics.errors > 0 && <p className="runtime-error-count">已记录 {metrics.errors} 个错误事件</p>}
        {turn?.state && <p className="runtime-state">Turn 状态：{turn.state}</p>}
        <ol className="runtime-timeline" aria-label="运行事件时间线">
          {parsed.slice(-80).map((event) => (
            <li key={`${event.id}-${event.phase}`} className={event.phase === "error" ? "error" : ""}>
              <span className="runtime-timeline-kind">{kindLabel(event.kind)}</span>
              <span className="runtime-timeline-name">{event.name}</span>
              <span className="runtime-timeline-phase">{phaseLabel(event.phase)}</span>
              {event.durationMs > 0 && <small>{event.durationMs}ms</small>}
              {event.usage?.total_tokens && <small>{event.usage.total_tokens} tokens</small>}
              {event.error && <details><summary>错误</summary><p>{event.error}</p></details>}
            </li>
          ))}
        </ol>
        {parsed.length === 0 && <p className="runtime-empty">当前还没有可展示的运行事件。</p>}
      </div>
    </Modal>
  );
}
