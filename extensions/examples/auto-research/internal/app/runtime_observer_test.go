package app

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/schema"
)

var errTestRuntime = errors.New("runtime test error")

func TestRuntimeEventPayloadOmitsPayloadAndSerializesMetrics(t *testing.T) {
	data, err := marshalRuntimeEvent(observe.Event{
		Kind:     observe.KindLLM,
		Phase:    observe.PhaseEnd,
		Name:     "draft_notes",
		Time:     time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC),
		Duration: 1500 * time.Millisecond,
		Trace:    observe.Trace{TraceID: "trace-1", SpanID: "span-1", ParentSpanID: "node-1"},
		Usage:    &schema.TokenUsage{InputTokens: 10, OutputTokens: 6, TotalTokens: 16},
		Attrs:    map[string]any{"llm.model_type": "openai-compatible"},
		Payload:  "secret prompt and response",
		Err:      errTestRuntime,
	})
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err = json.Unmarshal(data, &payload); err != nil {
		t.Fatal(err)
	}
	if _, ok := payload["payload"]; ok {
		t.Fatal("运行事件不应保存 Payload")
	}
	if payload["kind"] != "llm" || payload["phase"] != "end" || payload["name"] != "draft_notes" {
		t.Fatalf("生命周期字段错误: %s", data)
	}
	if payload["duration_ms"] != float64(1500) {
		t.Fatalf("耗时字段错误: %s", data)
	}
	if payload["error"] != "runtime test error" {
		t.Fatalf("错误字段错误: %s", data)
	}
	if !strings.Contains(string(data), `"total_tokens":16`) {
		t.Fatalf("Token 用量未序列化: %s", data)
	}
}
