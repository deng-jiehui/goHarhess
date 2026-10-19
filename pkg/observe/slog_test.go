package observe_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/goharness/goharness/pkg/observe"
)

// 日志必须可以按调用身份查询，普通元数据不能覆盖真实链路身份。
func TestSlogIncludesCallbackIdentity(t *testing.T) {
	var output bytes.Buffer
	observer := observe.Slog(slog.New(slog.NewJSONHandler(&output, nil)))
	observer.OnEvent(context.Background(), observe.Event{
		Kind: observe.KindLLM, Phase: observe.PhaseStart, Name: "model",
		Trace: observe.Trace{
			TraceID: "trace-1", SpanID: "span-1", ParentSpanID: "parent-1", RunID: "run-1",
		},
		Attrs: map[string]any{"trace_id": "unrelated", "llm.provider": "provider"},
	})
	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{
		"trace_id": "trace-1", "span_id": "span-1", "parent_span_id": "parent-1",
		"run_id": "run-1", "kind": "llm", "phase": "start", "llm.provider": "provider",
	} {
		if record[key] != want {
			t.Errorf("%s = %v，期望 %s", key, record[key], want)
		}
	}
}
