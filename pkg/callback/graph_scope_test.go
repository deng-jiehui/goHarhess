package callback_test

import (
	"context"
	"sync"
	"testing"

	"goharness/pkg/callback"
	"goharness/pkg/graph"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

type callbackGraphState struct {
	Messages []schema.Message
}

func (s callbackGraphState) Clone() callbackGraphState {
	s.Messages = schema.CloneMessages(s.Messages)
	return s
}

func TestNestedGraphUsesIndependentCallbackLifecycleScope(t *testing.T) {
	var childManager *callback.SyncDispatcher
	var childTrace callback.Trace
	child := graph.NewStateGraph[callbackGraphState, callbackGraphState, callbackGraphState]("child-graph", func(_ context.Context, input callbackGraphState) callbackGraphState {
		return input.Clone()
	})
	childNode := runnable.Func[callbackGraphState, callbackGraphState](func(ctx context.Context, state callbackGraphState, _ ...runnable.Option) (callbackGraphState, error) {
		childManager, _ = callback.DispatcherFromContext(ctx)
		childTrace, _ = callback.TraceFromContext(ctx)
		state.Messages = append(state.Messages, schema.AssistantMessage("child"))
		return state, nil
	})
	if err := graph.AddNode(child, "child-node", childNode); err != nil {
		t.Fatal(err)
	}
	if err := child.AddEdge(graph.Start, "child-node"); err != nil {
		t.Fatal(err)
	}
	if err := child.AddEdge("child-node", graph.End); err != nil {
		t.Fatal(err)
	}

	parent := graph.NewStateGraph[[]schema.Message, []schema.Message, callbackGraphState]("parent-graph", func(_ context.Context, input []schema.Message) callbackGraphState {
		return callbackGraphState{Messages: schema.CloneMessages(input)}
	})
	if err := graph.AddNode(parent, "child-graph", child); err != nil {
		t.Fatal(err)
	}
	tail := runnable.Func[callbackGraphState, callbackGraphState](func(_ context.Context, state callbackGraphState, _ ...runnable.Option) (callbackGraphState, error) {
		return state, nil
	})
	if err := graph.AddNode(parent, "tail", tail); err != nil {
		t.Fatal(err)
	}
	finish := runnable.Func[callbackGraphState, []schema.Message](func(_ context.Context, state callbackGraphState, _ ...runnable.Option) ([]schema.Message, error) {
		return schema.CloneMessages(state.Messages), nil
	})
	if err := graph.AddNode(parent, "finish", finish); err != nil {
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
	parentSpanID, childSpanID, childParentSpanID := "", "", ""
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
	if parentSpanID == "" || childParentSpanID != parentSpanID {
		t.Fatalf("child graph parent span = %q, want %q", childParentSpanID, parentSpanID)
	}
	if childManager == nil || childManager == dispatcher || childTrace.SpanID != childSpanID {
		t.Fatalf("child callback context manager=%p root=%p trace=%#v childSpan=%q", childManager, dispatcher, childTrace, childSpanID)
	}
}
