package callback_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"goharness/pkg/callback"
	"goharness/pkg/graph"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

type graphRuntimeContextKey struct{}

type callbackGraphState struct {
	Messages []schema.Message
}

type callbackSpanState struct{}

func TestGraphCallbackReleasesStateLockBeforeHandler(t *testing.T) {
	var sink graph.EventSink
	returned := make(chan error, 1)
	dispatcher, err := callback.New([]callback.Handler{callback.CallbackFunc(func(ctx context.Context, event callback.Event) error {
		if event.Type == callback.EventGraphStart {
			sink.Publish(ctx, graph.Event{Type: graph.EventNodeStart, NodeID: "reentrant"})
		}
		return nil
	})}, callback.Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := callback.WithDispatcher(context.Background(), dispatcher)
	ctx = callback.BindGraph(ctx, "reentrant.graph")
	var ok bool
	sink, ok = graph.EventSinkFromContext(ctx)
	if !ok {
		t.Fatal("missing graph callback sink")
	}
	go func() {
		sink.Publish(ctx, graph.Event{Type: graph.EventGraphStart})
		returned <- nil
	}()
	select {
	case err := <-returned:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Handler re-entry deadlocked graph callback state")
	}
}

func TestGraphCallbackUsesPublishedRuntimeContext(t *testing.T) {
	seen := make(chan any, 1)
	dispatcher, err := callback.New([]callback.Handler{callback.CallbackFunc(func(ctx context.Context, event callback.Event) error {
		if event.Type == callback.EventGraphNode || event.Type == callback.EventGraphNodeStart || event.Type == callback.EventGraphNodeOutput || event.Type == callback.EventGraphNodeEnd {
			seen <- ctx.Value(graphRuntimeContextKey{})
		}
		return nil
	})}, callback.Config{})
	if err != nil {
		t.Fatal(err)
	}
	base := callback.WithDispatcher(context.Background(), dispatcher)
	bound := callback.BindGraph(base, "runtime-context.graph")
	sink, ok := graph.EventSinkFromContext(bound)
	if !ok {
		t.Fatal("missing graph callback sink")
	}
	runtimeCtx := context.WithValue(bound, graphRuntimeContextKey{}, "node-value")
	sink.Publish(runtimeCtx, graph.Event{Type: graph.EventNodeStart, NodeID: "node"})
	select {
	case value := <-seen:
		if value != "node-value" {
			t.Fatalf("Handler context value = %#v", value)
		}
	default:
		t.Fatal("graph Handler did not run")
	}
}

func TestGraphNodeEventsUseTypedPhasesAndIndependentSpans(t *testing.T) {
	var mu sync.Mutex
	var events []callback.Event
	dispatcher, err := callback.New([]callback.Handler{callback.CallbackFunc(func(_ context.Context, event callback.Event) error {
		mu.Lock()
		events = append(events, event.Clone())
		mu.Unlock()
		return nil
	})}, callback.Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := callback.BindGraph(callback.WithDispatcher(context.Background(), dispatcher), "parent.graph")
	sink, ok := graph.EventSinkFromContext(ctx)
	if !ok {
		t.Fatal("missing graph callback sink")
	}
	for _, event := range []graph.Event{
		{Type: graph.EventGraphStart},
		{Type: graph.EventNodeStart, NodeID: "retriever", Step: 3},
		{Type: graph.EventNodeStart, NodeID: "retriever", Step: 3},
		{Type: graph.EventNodeOutput, NodeID: "retriever", Step: 3},
		{Type: graph.EventNodeEnd, NodeID: "retriever", Step: 3},
		{Type: graph.EventGraphEnd},
	} {
		sink.Publish(ctx, event)
	}

	mu.Lock()
	defer mu.Unlock()
	var graphSpan string
	var nodeSpan string
	phases := []callback.GraphNodePhase{}
	for _, event := range events {
		if event.Type == callback.EventGraphStart {
			graphSpan = event.Trace.SpanID
		}
		if event.Component != callback.ComponentGraph || event.Metadata[callback.MetadataGraphNodeID] != "retriever" {
			continue
		}
		if event.Metrics.Custom != nil {
			t.Fatalf("node event retained identity in Metrics.Custom: %#v", event.Metrics.Custom)
		}
		if event.Metadata[callback.MetadataGraphStep] != 3 {
			t.Fatalf("node step metadata = %#v", event.Metadata[callback.MetadataGraphStep])
		}
		phase, ok := event.Metadata[callback.MetadataGraphNodePhase].(callback.GraphNodePhase)
		if !ok {
			t.Fatalf("node phase metadata type = %T", event.Metadata[callback.MetadataGraphNodePhase])
		}
		phases = append(phases, phase)
		if nodeSpan == "" {
			nodeSpan = event.Trace.SpanID
		}
		if event.Trace.SpanID != nodeSpan {
			t.Fatalf("node event span changed: %q -> %q", nodeSpan, event.Trace.SpanID)
		}
		if event.Trace.ParentSpanID != graphSpan {
			t.Fatalf("node parent span = %q, want graph %q", event.Trace.ParentSpanID, graphSpan)
		}
		if event.Type == callback.EventGraphNodeEnd && event.Duration < 0 {
			t.Fatalf("node duration = %s", event.Duration)
		}
	}
	want := []callback.GraphNodePhase{callback.GraphNodePhaseStart, callback.GraphNodePhaseOutput, callback.GraphNodePhaseEnd}
	if fmt.Sprint(phases) != fmt.Sprint(want) {
		t.Fatalf("node phases = %v, want %v", phases, want)
	}
}

func TestGraphNodeExecutionContextUsesNodeSpan(t *testing.T) {
	var nodeTrace callback.Trace
	var childOperation callback.Operation
	value := graph.NewGraph[callbackSpanState, callbackSpanState]("node-context")
	node := runnable.Func[callbackSpanState, callbackSpanState](func(ctx context.Context, state callbackSpanState, _ ...runnable.Option) (callbackSpanState, error) {
		nodeTrace, _ = callback.TraceFromContext(ctx)
		_, childOperation = callback.Begin(ctx, callback.ComponentLLM, "inside-node", nil)
		return state, nil
	})
	if err := value.AddNode("work", node); err != nil {
		t.Fatal(err)
	}
	if err := value.AddEdge(graph.Start, "work"); err != nil {
		t.Fatal(err)
	}
	if err := value.AddEdge("work", graph.End); err != nil {
		t.Fatal(err)
	}

	var nodeEvent callback.Event
	dispatcher, err := callback.New([]callback.Handler{callback.CallbackFunc(func(_ context.Context, event callback.Event) error {
		if event.Type == callback.EventGraphNodeStart && event.Metadata[callback.MetadataGraphNodeID] == "work" {
			nodeEvent = event.Clone()
		}
		return nil
	})}, callback.Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := callback.BindGraph(callback.WithDispatcher(context.Background(), dispatcher), "node-context.graph")
	if _, err := value.Invoke(ctx, callbackSpanState{}); err != nil {
		t.Fatal(err)
	}
	if nodeEvent.Trace.SpanID == "" || nodeTrace != nodeEvent.Trace {
		t.Fatalf("node execution trace = %#v, node event trace = %#v", nodeTrace, nodeEvent.Trace)
	}
	if childOperation.Trace.ParentSpanID != nodeEvent.Trace.SpanID {
		t.Fatalf("child operation parent = %q, want node span %q", childOperation.Trace.ParentSpanID, nodeEvent.Trace.SpanID)
	}
}

func TestGraphErrorClosesActiveNodeSpan(t *testing.T) {
	var mu sync.Mutex
	var events []callback.Event
	dispatcher, err := callback.New([]callback.Handler{callback.CallbackFunc(func(ctx context.Context, event callback.Event) error {
		trace, ok := callback.TraceFromContext(ctx)
		if !ok || trace != event.Trace {
			t.Errorf("event %s context trace = %#v, event trace = %#v", event.Type, trace, event.Trace)
		}
		mu.Lock()
		events = append(events, event.Clone())
		mu.Unlock()
		return nil
	})}, callback.Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := callback.BindGraph(callback.WithDispatcher(context.Background(), dispatcher), "error.graph")
	sink, ok := graph.EventSinkFromContext(ctx)
	if !ok {
		t.Fatal("missing graph callback sink")
	}
	sink.Publish(ctx, graph.Event{Type: graph.EventGraphStart})
	nodeStartEvent := graph.Event{Type: graph.EventNodeStart, NodeID: "broken", Step: 1}
	binder, ok := sink.(interface {
		BindEventContext(context.Context, graph.Event) context.Context
	})
	if !ok {
		t.Fatal("graph callback sink does not bind node context")
	}
	nodeCtx := binder.BindEventContext(ctx, nodeStartEvent)
	sink.Publish(nodeCtx, nodeStartEvent)
	sink.Publish(nodeCtx, graph.Event{Type: graph.EventGraphError, Err: fmt.Errorf("node failed")})

	mu.Lock()
	defer mu.Unlock()
	var nodeStart *callback.Event
	var nodeEnd *callback.Event
	nodeEndIndex, graphErrorIndex := -1, -1
	for index := range events {
		event := events[index]
		if event.Type == callback.EventGraphNodeStart && event.Metadata[callback.MetadataGraphNodeID] == "broken" {
			nodeStart = &event
		}
		if event.Type == callback.EventGraphNodeEnd && event.Metadata[callback.MetadataGraphNodeID] == "broken" {
			nodeEnd = &event
			nodeEndIndex = index
		}
		if event.Type == callback.EventGraphError {
			graphErrorIndex = index
		}
	}
	if nodeStart == nil || nodeEnd == nil {
		t.Fatal("graph error left active node span open")
	}
	if nodeEnd.Status != callback.StatusError || nodeEnd.ErrorText != "node failed" || nodeEnd.Duration < 0 {
		t.Fatalf("node error event = %#v", *nodeEnd)
	}
	if nodeEnd.Trace.SpanID != nodeStart.Trace.SpanID || nodeEndIndex < 0 || graphErrorIndex <= nodeEndIndex {
		t.Fatalf("node error ordering/span start=%#v end=%#v nodeEndIndex=%d graphErrorIndex=%d", *nodeStart, *nodeEnd, nodeEndIndex, graphErrorIndex)
	}
}

func TestGraphCallbackSerializesConcurrentLifecycleDispatch(t *testing.T) {
	startEntered := make(chan struct{})
	releaseStart := make(chan struct{})
	var mu sync.Mutex
	var phases []callback.GraphNodePhase
	dispatcher, err := callback.New([]callback.Handler{callback.CallbackFunc(func(_ context.Context, event callback.Event) error {
		phase, _ := event.Metadata[callback.MetadataGraphNodePhase].(callback.GraphNodePhase)
		if phase == "" {
			return nil
		}
		if phase == callback.GraphNodePhaseStart {
			close(startEntered)
			<-releaseStart
		}
		mu.Lock()
		phases = append(phases, phase)
		mu.Unlock()
		return nil
	})}, callback.Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := callback.BindGraph(callback.WithDispatcher(context.Background(), dispatcher), "concurrent.graph")
	sink, ok := graph.EventSinkFromContext(ctx)
	if !ok {
		t.Fatal("missing graph callback sink")
	}
	startDone := make(chan struct{})
	go func() {
		defer close(startDone)
		sink.Publish(ctx, graph.Event{Type: graph.EventNodeStart, NodeID: "node", Step: 1})
	}()
	<-startEntered
	outputDone := make(chan struct{})
	go func() {
		defer close(outputDone)
		sink.Publish(ctx, graph.Event{Type: graph.EventNodeOutput, NodeID: "node", Step: 1})
	}()
	select {
	case <-outputDone:
		t.Fatal("node output returned before the queued start callback completed")
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseStart)
	<-startDone
	<-outputDone
	mu.Lock()
	defer mu.Unlock()
	want := []callback.GraphNodePhase{callback.GraphNodePhaseStart, callback.GraphNodePhaseOutput}
	if fmt.Sprint(phases) != fmt.Sprint(want) {
		t.Fatalf("concurrent phases = %v, want %v", phases, want)
	}
}

func TestNestedGraphUsesIndependentCallbackLifecycleScope(t *testing.T) {
	var childManager *callback.SyncDispatcher
	var childTrace callback.Trace
	child := graph.NewGraph[callbackGraphState, callbackGraphState]("child-graph")
	childNode := runnable.Func[callbackGraphState, callbackGraphState](func(ctx context.Context, current callbackGraphState, _ ...runnable.Option) (callbackGraphState, error) {
		childManager, _ = callback.DispatcherFromContext(ctx)
		childTrace, _ = callback.TraceFromContext(ctx)
		current.Messages = append(current.Messages, schema.AssistantMessage("child"))
		return current, nil
	})
	if err := child.AddNode("child-node", childNode); err != nil {
		t.Fatal(err)
	}
	if err := child.AddEdge(graph.Start, "child-node"); err != nil {
		t.Fatal(err)
	}
	if err := child.AddEdge("child-node", graph.End); err != nil {
		t.Fatal(err)
	}

	parent := graph.NewGraph[[]schema.Message, []schema.Message]("parent-graph")
	if err := parent.SetStateFactory(func(_ context.Context, input []schema.Message) (*callbackGraphState, error) {
		return &callbackGraphState{Messages: schema.CloneMessages(input)}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := parent.AddNode("child-graph", child,
		graph.WithPreHandler(func(_ context.Context, current *callbackGraphState) (callbackGraphState, error) { return *current, nil }),
		graph.WithPostHandler(func(_ context.Context, current *callbackGraphState, output callbackGraphState) error {
			*current = output
			return nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	tail := runnable.Func[callbackGraphState, callbackGraphState](func(_ context.Context, current callbackGraphState, _ ...runnable.Option) (callbackGraphState, error) {
		return current, nil
	})
	if err := parent.AddNode("tail", tail, graph.WithPreHandler(func(_ context.Context, current *callbackGraphState) (callbackGraphState, error) { return *current, nil })); err != nil {
		t.Fatal(err)
	}
	finish := runnable.Func[[]schema.Message, []schema.Message](func(_ context.Context, messages []schema.Message, _ ...runnable.Option) ([]schema.Message, error) {
		return schema.CloneMessages(messages), nil
	})
	if err := parent.AddNode("finish", finish, graph.WithPreHandler(func(_ context.Context, current *callbackGraphState) ([]schema.Message, error) {
		return schema.CloneMessages(current.Messages), nil
	})); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{{graph.Start, "child-graph"}, {"child-graph", "tail"}, {"tail", "finish"}, {"finish", graph.End}} {
		if err := parent.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}

	var mu sync.Mutex
	var events []callback.Event
	dispatcher, err := callback.New([]callback.Callback{callback.Funcs{OnEvent: func(_ context.Context, event callback.Event) error {
		mu.Lock()
		events = append(events, event.Clone())
		mu.Unlock()
		return nil
	}}}, callback.Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := callback.WithDispatcher(context.Background(), dispatcher)
	ctx = callback.BindGraph(ctx, "parent.graph")
	if _, err := parent.Invoke(ctx, nil); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	parentEnds, childEnds := 0, 0
	parentEndIndex, childEndIndex := -1, -1
	parentSpanID, parentNodeSpanID, childSpanID, childNodeSpanID, childParentSpanID := "", "", "", "", ""
	for index, event := range events {
		if event.Type == callback.EventGraphStart {
			switch event.Name {
			case "parent.graph":
				parentSpanID = event.Trace.SpanID
			case "child-graph":
				childSpanID = event.Trace.SpanID
				childParentSpanID = event.Trace.ParentSpanID
			}
		}
		if event.Type == callback.EventGraphNodeStart {
			switch event.Name {
			case "child-graph":
				parentNodeSpanID = event.Trace.SpanID
			case "child-node":
				childNodeSpanID = event.Trace.SpanID
			}
		}
		if event.Type != callback.EventGraphEnd {
			continue
		}
		switch event.Name {
		case "parent.graph":
			parentEnds++
			parentEndIndex = index
		case "child-graph":
			childEnds++
			childEndIndex = index
		}
	}
	if parentEnds != 1 || childEnds != 1 || childEndIndex < 0 || parentEndIndex <= childEndIndex {
		t.Fatalf("graph end scopes parent=%d child=%d parentIndex=%d childIndex=%d events=%#v", parentEnds, childEnds, parentEndIndex, childEndIndex, events)
	}
	if parentSpanID == "" || parentNodeSpanID == "" || childParentSpanID != parentNodeSpanID {
		t.Fatalf("child graph parent span = %q, want parent node %q (graph %q)", childParentSpanID, parentNodeSpanID, parentSpanID)
	}
	if childManager == nil || childManager == dispatcher || childTrace.SpanID != childNodeSpanID || childTrace.ParentSpanID != childSpanID {
		t.Fatalf("child callback context manager=%p root=%p trace=%#v childGraphSpan=%q childNodeSpan=%q", childManager, dispatcher, childTrace, childSpanID, childNodeSpanID)
	}
	sequences := map[uint64]bool{}
	for _, event := range events {
		if event.Sequence == 0 || sequences[event.Sequence] {
			t.Fatalf("duplicate graph callback sequence %d", event.Sequence)
		}
		sequences[event.Sequence] = true
	}
}
