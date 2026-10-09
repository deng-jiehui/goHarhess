package graph_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/state"
)

type eventLog struct {
	mu     sync.Mutex
	events []observe.Event
}

func (l *eventLog) OnEvent(_ context.Context, event observe.Event) {
	// 异步 checkpoint 在 drain 协程发射事件，收集器必须并发安全。
	l.mu.Lock()
	l.events = append(l.events, event)
	l.mu.Unlock()
}

func sequence(events []observe.Event) []string {
	out := make([]string, len(events))
	for i, event := range events {
		out[i] = string(event.Kind) + "/" + string(event.Phase) + ":" + event.Name
	}
	return out
}

func TestObserveGoldenEventSequence(t *testing.T) {
	log := &eventLog{}
	g := graph.NewGraph[int, int]("pipeline")
	addOne := runnable.Func[int, int](func(_ context.Context, v int, _ ...runnable.Option) (int, error) { return v + 1, nil })
	doubleIt := runnable.Func[int, int](func(_ context.Context, v int, _ ...runnable.Option) (int, error) { return v * 2, nil })
	if err := g.AddNode("inc", addOne); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("dbl", doubleIt); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "inc"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("inc", "dbl"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("dbl", graph.End); err != nil {
		t.Fatal(err)
	}
	ctx := observe.With(context.Background(), log)
	out, err := mustCompile(t, g).Invoke(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	if out != 8 {
		t.Fatalf("out = %d", out)
	}
	want := []string{
		"run/start:pipeline",
		"node/start:inc",
		"node/output:inc",
		"node/end:inc",
		"node/start:dbl",
		"node/output:dbl",
		"node/end:dbl",
		"run/end:pipeline",
	}
	if got := sequence(log.events); !equalSeq(got, want) {
		t.Fatalf("sequence = %v, want %v", got, want)
	}
	run := log.events[0]
	for i, event := range log.events {
		if event.Trace.TraceID != run.Trace.TraceID || event.Trace.RunID != run.Trace.RunID {
			t.Fatalf("event %d trace = %+v, want run trace %+v", i, event.Trace, run.Trace)
		}
	}
	// 节点 span 必须挂在 run span 下，形成调用树。
	for i := 1; i <= 6; i++ {
		if log.events[i].Trace.ParentSpanID != run.Trace.SpanID {
			t.Fatalf("event %d parent = %q, want %q", i, log.events[i].Trace.ParentSpanID, run.Trace.SpanID)
		}
	}
	if log.events[3].Duration < 0 || log.events[7].Duration < 0 || log.events[3].Time.IsZero() {
		t.Fatal("end events lack duration or timestamp")
	}
}

func TestObserveGraphErrorSequence(t *testing.T) {
	log := &eventLog{}
	broken := errors.New("broken")
	g := graph.NewGraph[int, int]("fails")
	if err := g.AddNode("bad", runnable.Func[int, int](func(context.Context, int, ...runnable.Option) (int, error) { return 0, broken })); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "bad"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("bad", graph.End); err != nil {
		t.Fatal(err)
	}
	ctx := observe.With(context.Background(), log)
	if _, err := mustCompile(t, g).Invoke(ctx, 1); err == nil {
		t.Fatal("Invoke succeeded")
	}
	var nodeErr, runErr *observe.Event
	for i := range log.events {
		switch {
		case log.events[i].Kind == observe.KindNode && log.events[i].Phase == observe.PhaseError:
			nodeErr = &log.events[i]
		case log.events[i].Kind == observe.KindRun && log.events[i].Phase == observe.PhaseError:
			runErr = &log.events[i]
		}
	}
	if nodeErr == nil || !errors.Is(nodeErr.Err, broken) {
		t.Fatalf("node error event missing: %v", sequence(log.events))
	}
	if runErr == nil || runErr.Err == nil {
		t.Fatalf("run error event missing: %v", sequence(log.events))
	}
	// 节点 error 必须先于 run error。
	var nodeIdx, runIdx int
	for i, event := range log.events {
		if event.Kind == observe.KindNode && event.Phase == observe.PhaseError {
			nodeIdx = i
		}
		if event.Kind == observe.KindRun && event.Phase == observe.PhaseError {
			runIdx = i
		}
	}
	if nodeIdx >= runIdx {
		t.Fatalf("node error at %d after run error at %d: %v", nodeIdx, runIdx, sequence(log.events))
	}
}

// TestObserveWithoutManagerStillEmits 验证 observe.With 注册观测者后
// 图发射完整生命周期事件。
func TestObserveWithoutManagerStillEmits(t *testing.T) {
	log := &eventLog{}
	g := graph.NewGraph[int, int]("solo")
	if err := g.AddNode("n", runnable.Func[int, int](func(_ context.Context, v int, _ ...runnable.Option) (int, error) { return v, nil })); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "n"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("n", graph.End); err != nil {
		t.Fatal(err)
	}
	ctx := observe.With(context.Background(), log)
	if _, err := mustCompile(t, g).Invoke(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if len(log.events) == 0 {
		t.Fatal("no events emitted with only observe.With")
	}
}

// TestObserveNestedGraphTree 子图在节点内 Invoke：子 run 的 span 挂到父节点
// span 下，子图节点再挂到子 run 下，形成三层调用树。
func TestObserveNestedGraphTree(t *testing.T) {
	log := &eventLog{}
	child := graph.NewGraph[string, string]("child")
	if err := child.AddNode("inner", runnable.Func[string, string](func(_ context.Context, in string, _ ...runnable.Option) (string, error) { return in + "!", nil })); err != nil {
		t.Fatal(err)
	}
	if err := child.AddEdge(graph.Start, "inner"); err != nil {
		t.Fatal(err)
	}
	if err := child.AddEdge("inner", graph.End); err != nil {
		t.Fatal(err)
	}
	compiledChild := mustCompile(t, child)

	parent := graph.NewGraph[string, string]("parent")
	if err := parent.AddNode("outer", runnable.Func[string, string](func(ctx context.Context, in string, _ ...runnable.Option) (string, error) {
		return compiledChild.Invoke(ctx, in)
	})); err != nil {
		t.Fatal(err)
	}
	if err := parent.AddEdge(graph.Start, "outer"); err != nil {
		t.Fatal(err)
	}
	if err := parent.AddEdge("outer", graph.End); err != nil {
		t.Fatal(err)
	}
	ctx := observe.With(context.Background(), log)
	out, err := mustCompile(t, parent).Invoke(ctx, "x")
	if err != nil || out != "x!" {
		t.Fatalf("out=%q err=%v", out, err)
	}
	// 找子 run 与外层节点 span，验证三级父子关系。
	var parentRun, outerNode, childRun, innerNode *observe.Event
	for i := range log.events {
		event := &log.events[i]
		switch {
		case event.Kind == observe.KindRun && event.Name == "parent" && event.Phase == observe.PhaseStart:
			parentRun = event
		case event.Kind == observe.KindNode && event.Name == "outer" && event.Phase == observe.PhaseStart:
			outerNode = event
		case event.Kind == observe.KindRun && event.Name == "child" && event.Phase == observe.PhaseStart:
			childRun = event
		case event.Kind == observe.KindNode && event.Name == "inner" && event.Phase == observe.PhaseStart:
			innerNode = event
		}
	}
	if parentRun == nil || outerNode == nil || childRun == nil || innerNode == nil {
		t.Fatalf("missing spans: %v", sequence(log.events))
	}
	if childRun.Trace.ParentSpanID != outerNode.Trace.SpanID {
		t.Fatalf("child run parent = %q, want outer node %q", childRun.Trace.ParentSpanID, outerNode.Trace.SpanID)
	}
	if innerNode.Trace.ParentSpanID != childRun.Trace.SpanID {
		t.Fatalf("inner node parent = %q, want child run %q", innerNode.Trace.ParentSpanID, childRun.Trace.SpanID)
	}
	if childRun.Trace.TraceID != parentRun.Trace.TraceID {
		t.Fatal("child run left the parent trace")
	}
}

// TestObserveInterruptAndCheckpoint 挂起时发射 interrupt 瞬时事件（带待答
// ID 列表）与 checkpoint save；恢复时发射 checkpoint load 后完成运行。
func TestObserveInterruptAndCheckpoint(t *testing.T) {
	log := &eventLog{}
	g := graph.NewGraph[string, string]("ask")
	if err := g.SetStateFactory(func(_ context.Context, in string) (*inputState, error) { return &inputState{Text: in}, nil }); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("ask", runnable.Func[string, string](func(ctx context.Context, in string, _ ...runnable.Option) (string, error) {
		name, err := graph.Interrupt(ctx, map[string]any{"field": "name"})
		if err != nil {
			return "", err
		}
		return in + "/" + name.(string), nil
	}), graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil })); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "ask"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("ask", graph.End); err != nil {
		t.Fatal(err)
	}
	store := state.NewMemoryStore()
	option := graph.WithCheckpoint(store, "ask-run")
	ctx := observe.With(context.Background(), log)
	compiled := mustCompile(t, g)
	_, err := compiled.Invoke(ctx, "hello", option)
	var suspended *graph.InterruptedError
	if !errors.As(err, &suspended) {
		t.Fatalf("Invoke err=%v, want InterruptedError", err)
	}
	pending := suspended.PendingInterrupts()
	if len(pending) != 1 {
		t.Fatalf("pending=%v", pending)
	}
	var interruptEvent *observe.Event
	saveSpans := map[string][]observe.Phase{}
	for i := range log.events {
		event := &log.events[i]
		if event.Kind == observe.KindInterrupt {
			interruptEvent = event
		}
		if event.Kind == observe.KindCheckpoint && event.Name == "save" {
			saveSpans[event.Trace.SpanID] = append(saveSpans[event.Trace.SpanID], event.Phase)
		}
	}
	if interruptEvent == nil {
		t.Fatalf("no interrupt event: %v", sequence(log.events))
	}
	if ids, ok := interruptEvent.Attrs["interrupt.ids"].([]string); !ok || len(ids) != 1 || ids[0] != pending[0].ID {
		t.Fatalf("interrupt ids = %v", interruptEvent.Attrs)
	}
	if interruptEvent.Attrs["interrupt.pending"] != 1 {
		t.Fatalf("pending attr = %v", interruptEvent.Attrs["interrupt.pending"])
	}
	// 每次保存都必须 start/end 配对且共享 SpanID：OTel 桥接靠这对
	// 事件把 checkpoint span 挂到运行调用树下，而不是临时孤儿 span。
	if len(saveSpans) == 0 {
		t.Fatalf("no checkpoint save events: %v", sequence(log.events))
	}
	for spanID, phases := range saveSpans {
		if !equalSeq(phasesToStrings(phases), []string{"start", "end"}) {
			t.Fatalf("save span %s phases = %v", spanID, phases)
		}
	}

	// 挂起收尾：活动节点与 run 都以 graph.outcome=suspended 的 end 事件
	// 闭合，恢复前的最后一个事件必须是 run 收尾，不留下未关 span。
	last := log.events[len(log.events)-1]
	if last.Kind != observe.KindRun || last.Phase != observe.PhaseEnd || last.Attrs["graph.outcome"] != "suspended" {
		t.Fatalf("run not closed on suspend: %v", sequence(log.events))
	}
	nodeSuspended := false
	for _, event := range log.events {
		if event.Kind == observe.KindNode && event.Phase == observe.PhaseEnd && event.Attrs["graph.outcome"] == "suspended" {
			nodeSuspended = true
		}
	}
	if !nodeSuspended {
		t.Fatalf("no suspended node end: %v", sequence(log.events))
	}

	before := len(log.events)
	out, err := compiled.Resume(ctx, option, graph.WithResume(pending[0].ID, "Ada"))
	if err != nil || out != "hello/Ada" {
		t.Fatalf("resume out=%q err=%v", out, err)
	}
	loadSpans := map[string][]observe.Phase{}
	var loadEnd *observe.Event
	var runStart *observe.Event
	for i := before; i < len(log.events); i++ {
		event := &log.events[i]
		switch {
		case event.Kind == observe.KindCheckpoint && event.Name == "load":
			loadSpans[event.Trace.SpanID] = append(loadSpans[event.Trace.SpanID], event.Phase)
			if event.Phase == observe.PhaseEnd {
				loadEnd = event
			}
		case event.Kind == observe.KindRun && event.Phase == observe.PhaseStart:
			runStart = event
		}
	}
	if len(loadSpans) != 1 {
		t.Fatalf("checkpoint load spans = %v", loadSpans)
	}
	for spanID, phases := range loadSpans {
		if !equalSeq(phasesToStrings(phases), []string{"start", "end"}) {
			t.Fatalf("load span %s phases = %v", spanID, phases)
		}
	}
	if loadEnd == nil || loadEnd.Attrs["checkpoint.found"] != true {
		t.Fatalf("load end = %#v", loadEnd)
	}
	// 恢复建立新的 run 记录，通过 checkpoint.key 关联到原检查点。
	if runStart == nil || runStart.Attrs["checkpoint.key"] != "ask-run" {
		t.Fatalf("resume run start = %v", sequence(log.events[before:]))
	}
	if runStart.Trace.SpanID == last.Trace.SpanID {
		t.Fatal("resumed run reused suspended span id")
	}
}

func phasesToStrings(phases []observe.Phase) []string {
	out := make([]string, len(phases))
	for i, phase := range phases {
		out[i] = string(phase)
	}
	return out
}

func equalSeq(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
