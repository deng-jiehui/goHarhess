package retrieval_test

import (
	"context"
	"testing"

	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/extensions/retrieval"
)

func TestRetrieverCallbackInputRequiresOneTypedValue(t *testing.T) {
	valid := retrieval.CallbackInput{Operation: retrieval.OperationHybridSearch, Hybrid: &retrieval.HybridQuery{Text: "query"}}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	invalid := valid
	invalid.Vector = &retrieval.VectorRequest{}
	if err := invalid.Validate(); err == nil {
		t.Fatal("multiple active callback values were accepted")
	}
}

func TestRetrieverCallbackCloneIsolatesMetadata(t *testing.T) {
	input := retrieval.CallbackInput{Operation: retrieval.OperationHybridSearch, Hybrid: &retrieval.HybridQuery{Text: "query", Filter: retrieval.Metadata{"nested": map[string]any{"owner": "original"}}}}
	cloned := input.Clone()
	cloned.Hybrid.Filter["nested"].(map[string]any)["owner"] = "changed"
	if input.Hybrid.Filter["nested"].(map[string]any)["owner"] != "original" {
		t.Fatal("metadata mutation leaked")
	}
}

// filterMeta 按 Clone() any 约定的元数据类型：CallbackInput.Clone() 内部
// 是宽松复制会共享指针，脱敏递归处理时应由 Clone() any 完成隔离。
type filterMeta struct{ values map[string]string }

func (m *filterMeta) Clone() any {
	out := make(map[string]string, len(m.values))
	for k, v := range m.values {
		out[k] = v
	}
	return &filterMeta{values: out}
}

// TestRetrieverRedactCannotMutateQueryMetadata 复核点：检索载荷经宽松
// Clone 共享的元数据，脱敏改写不能穿透回业务请求。
func TestRetrieverRedactCannotMutateQueryMetadata(t *testing.T) {
	var events []observe.Event
	ctx := observe.With(context.Background(),
		observe.CapturePayload(func(_ observe.Event, raw any) any {
			input := raw.(retrieval.CallbackInput)
			meta := input.Hybrid.Filter["k"].(*filterMeta)
			meta.values["token"] = "***"
			return input
		}),
		observe.ObserverFunc(func(_ context.Context, event observe.Event) { events = append(events, event) }),
	)
	meta := &filterMeta{values: map[string]string{"token": "s3cr3t"}}
	input := retrieval.CallbackInput{
		Operation: retrieval.OperationHybridSearch,
		Hybrid:    &retrieval.HybridQuery{Text: "q", Filter: retrieval.Metadata{"k": meta}},
	}
	opCtx, invocation := retrieval.BeginCallback(ctx, input)
	retrieval.EndCallback(opCtx, invocation, retrieval.CallbackOutput{Operation: retrieval.OperationHybridSearch}, nil)

	if meta.values["token"] != "s3cr3t" {
		t.Fatalf("redact mutated business metadata: %q", meta.values["token"])
	}
	payload, ok := events[0].Payload.(retrieval.CallbackInput)
	if !ok {
		t.Fatalf("payload = %#v", events[0].Payload)
	}
	got := payload.Hybrid.Filter["k"].(*filterMeta)
	if got == meta {
		t.Fatal("payload shares metadata pointer with business input")
	}
	if got.values["token"] != "***" {
		t.Fatalf("payload metadata = %q", got.values["token"])
	}
}
