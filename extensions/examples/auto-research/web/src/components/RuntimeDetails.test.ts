import { describe, expect, it } from "vitest";
import { parseRuntimeEvent, runtimeMetrics } from "./RuntimeDetails";

describe("开发运行指标", () => {
  it("可以解析 Base64 事件并统计模型、节点和 Token", () => {
    const payload = JSON.stringify({
      kind: "llm",
      phase: "end",
      name: "draft_notes",
      duration_ms: 1500,
      usage: { input_tokens: 10, output_tokens: 6, total_tokens: 16, cache_read_tokens: 4, cache_write_tokens: 1 },
    });
    const event = parseRuntimeEvent({
      id: 1,
      kind: "runtime.llm",
      payload_json: btoa(payload),
    });
    if (!event) throw new Error("运行事件解析失败");
    expect(event).toMatchObject({ kind: "llm", phase: "end", name: "draft_notes", durationMs: 1500 });
    expect(runtimeMetrics([
      event,
      { ...event, kind: "node", phase: "end", name: "draft_notes", usage: undefined },
      { ...event, id: 3, spanId: "span-2", kind: "llm", phase: "error", usage: undefined, error: "timeout" },
    ])).toMatchObject({ modelCalls: 2, nodes: 1, totalTokens: 16, cacheReadTokens: 4, cacheWriteTokens: 1, cacheHitRate: 40, errors: 1 });
  });

  it("未知或损坏事件会被忽略", () => {
    expect(parseRuntimeEvent({ id: 2, kind: "turn.accepted", payload_json: "not-json" })).toBeNull();
    expect(runtimeMetrics([])).toEqual({ modelCalls: 0, nodes: 0, totalTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0, cacheHitRate: null, errors: 0, durationMs: 0 });
  });
});
