package otel_test

import (
	"context"
	"errors"
	"testing"

	otel "github.com/goharness/goharness/extensions/otel"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/state"
)

type askState struct {
	Text string
}

// TestGraphSuspendAndCheckpointSpanTree 用真实图 + Interrupt + MemoryStore
// 验证审查复核点：挂起时所有已开始的 span 都收尾；checkpoint save/load 是
// 挂在调用树下的子 span，不是 OTel 兜底创建的独立 trace。
func TestGraphSuspendAndCheckpointSpanTree(t *testing.T) {
	recorder, tracer := newRecorder(t)
	ctx := observe.With(context.Background(), otel.Observer(tracer))

	g := graph.NewGraph[string, string]("ask")
	if err := g.SetStateFactory(func(_ context.Context, in string) (*askState, error) {
		return &askState{Text: in}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("ask", runnable.Func[string, string](func(ctx context.Context, in string, _ ...runnable.Option) (string, error) {
		name, err := graph.Interrupt(ctx, map[string]any{"field": "name"})
		if err != nil {
			return "", err
		}
		return in + "/" + name.(string), nil
	}), graph.WithPreHandler(func(_ context.Context, s *askState) (string, error) { return s.Text, nil })); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{{graph.Start, "ask"}, {"ask", graph.End}} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	compiled, err := g.Compile()
	if err != nil {
		t.Fatal(err)
	}

	option := graph.WithCheckpoint(state.NewMemoryStore(), "ask-run")
	_, err = compiled.Invoke(ctx, "hello", option)
	var suspended *graph.InterruptedError
	if !errors.As(err, &suspended) {
		t.Fatalf("Invoke err=%v, want InterruptedError", err)
	}

	// 挂起必须收尾全部已开始的 span（run + 活动节点 + checkpoint save）。
	started, ended := recorder.Started(), recorder.Ended()
	if len(started) != len(ended) {
		var open []string
		for _, s := range started {
			if s.EndTime().IsZero() {
				open = append(open, s.Name())
			}
		}
		t.Fatalf("suspend left %d open spans: %v", len(started)-len(ended), open)
	}
	var sawSave, sawSuspendedRun bool
	for _, s := range ended {
		switch s.Name() {
		case "checkpoint:save":
			sawSave = true
			// checkpoint span 必须有父 span 且与调用树同一 TraceID。
			if !s.Parent().SpanID().IsValid() {
				t.Fatal("checkpoint save span has no parent")
			}
		case "run:ask":
			for _, attr := range s.Attributes() {
				if string(attr.Key) == "graph.outcome" && attr.Value.AsString() == "suspended" {
					sawSuspendedRun = true
				}
			}
		}
	}
	if !sawSave {
		t.Fatal("no checkpoint save span")
	}
	if !sawSuspendedRun {
		t.Fatal("run span not closed with suspended outcome")
	}

	// 恢复建立新执行记录；checkpoint load 也是子 span；全部 span 收尾。
	before := len(recorder.Started())
	pending := suspended.PendingInterrupts()
	out, err := compiled.Resume(ctx, option, graph.WithResume(pending[0].ID, "Ada"))
	if err != nil || out != "hello/Ada" {
		t.Fatalf("resume out=%q err=%v", out, err)
	}
	started, ended = recorder.Started(), recorder.Ended()
	if len(started) != len(ended) {
		t.Fatalf("resume left open spans: started=%d ended=%d", len(started), len(ended))
	}
	var sawLoad bool
	for _, s := range ended[before:] {
		if s.Name() == "checkpoint:load" {
			sawLoad = true
			if !s.Parent().SpanID().IsValid() {
				t.Fatal("checkpoint load span has no parent")
			}
		}
	}
	if !sawLoad {
		t.Fatal("no checkpoint load span on resume")
	}
}
