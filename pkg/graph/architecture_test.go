package graph_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"goharness/pkg/graph"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
	"goharness/pkg/state"
)

type streamState struct {
	Messages []schema.Message
	Output   []schema.Message
}

func (s streamState) Clone() streamState {
	s.Messages = schema.CloneMessages(s.Messages)
	s.Output = schema.CloneMessages(s.Output)
	return s
}

func newStreamGraph(name string) *graph.StateGraph[[]schema.Message, []schema.Message, streamState] {
	return graph.NewStateGraph[[]schema.Message, []schema.Message, streamState](name, func(_ context.Context, input []schema.Message) streamState {
		return streamState{Messages: schema.CloneMessages(input)}
	})
}

func addStreamFinish(t *testing.T, g *graph.StateGraph[[]schema.Message, []schema.Message, streamState], from string) {
	t.Helper()
	finish := runnable.Func[streamState, []schema.Message](func(_ context.Context, state streamState, _ ...runnable.Option) ([]schema.Message, error) {
		return schema.CloneMessages(state.Output), nil
	})
	if err := graph.AddNode(g, "finish", finish); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(from, "finish"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("finish", graph.End); err != nil {
		t.Fatal(err)
	}
}

func TestMessageStreamSeparatesDataAndControlPlanes(t *testing.T) {
	g := newStreamGraph("planes")
	node := runnable.Func[streamState, streamState](func(ctx context.Context, current streamState, _ ...runnable.Option) (streamState, error) {
		graph.EmitData(ctx, schema.MessageEvent{Kind: schema.MessageEventDelta, Messages: []schema.Message{schema.AssistantMessage("hel")}})
		message := schema.AssistantMessage("hello")
		current.Messages = append(current.Messages, message)
		current.Output = append(current.Output, message)
		return current, nil
	})
	if err := graph.AddNode(g, "model", node); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "model"); err != nil {
		t.Fatal(err)
	}
	addStreamFinish(t, g, "model")
	var mu sync.Mutex
	var control []graph.EventType
	ctx := graph.WithEventSink(context.Background(), graph.EventSinkFunc(func(_ context.Context, event graph.Event) {
		mu.Lock()
		control = append(control, event.Type)
		mu.Unlock()
	}))
	stream, err := graph.StreamMessages(ctx, g, []schema.Message{schema.UserMessage("hi")})
	if err != nil {
		t.Fatal(err)
	}
	var data []schema.MessageEventKind
	for item := range stream {
		if item.Err != nil {
			t.Fatal(item.Err)
		}
		data = append(data, item.Value.Kind)
	}
	if len(data) != 3 || data[0] != schema.MessageEventDelta || data[1] != schema.MessageEventSnapshot || data[2] != schema.MessageEventDone {
		t.Fatalf("data = %#v", data)
	}
	mu.Lock()
	defer mu.Unlock()
	if control[0] != graph.EventGraphStart || control[len(control)-1] != graph.EventGraphEnd {
		t.Fatalf("control = %#v", control)
	}
}

func TestStreamDoesNotDuplicateAuthoritativeSnapshot(t *testing.T) {
	g := newStreamGraph("snapshot")
	node := runnable.Func[streamState, streamState](func(ctx context.Context, current streamState, _ ...runnable.Option) (streamState, error) {
		message := schema.AssistantMessage("answer")
		current.Output = []schema.Message{message}
		graph.EmitData(ctx, schema.MessageEvent{Kind: schema.MessageEventSnapshot, Messages: current.Output})
		return current, nil
	})
	if err := graph.AddNode(g, "model", node); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "model"); err != nil {
		t.Fatal(err)
	}
	addStreamFinish(t, g, "model")
	stream, err := graph.StreamMessages(context.Background(), g, nil)
	if err != nil {
		t.Fatal(err)
	}
	snapshots := 0
	for item := range stream {
		if item.Err != nil {
			t.Fatal(item.Err)
		}
		if item.Value.Kind == schema.MessageEventSnapshot {
			snapshots++
		}
	}
	if snapshots != 1 {
		t.Fatalf("snapshots = %d", snapshots)
	}
}

func TestCursorResumeUsesTypedNodeWithoutStateEmbedding(t *testing.T) {
	g := newStreamGraph("resume")
	depth := 0
	node := runnable.Func[streamState, streamState](func(ctx context.Context, current streamState, _ ...runnable.Option) (streamState, error) {
		depth = len(state.CursorStackFromContext(ctx))
		current.Output = []schema.Message{schema.AssistantMessage("resumed")}
		return current, nil
	})
	if err := graph.AddNode(g, "work", node); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "work"); err != nil {
		t.Fatal(err)
	}
	addStreamFinish(t, g, "work")
	ctx := state.WithCursorStack(context.Background(), state.CursorStack{{Graph: "resume", Node: "work", Step: 0, Iteration: 2}})
	output, err := g.Invoke(ctx, nil)
	if err != nil || depth != 1 || len(output) != 1 {
		t.Fatalf("depth=%d output=%#v err=%v", depth, output, err)
	}
}

type pointerState struct {
	Value *string
}

func (s pointerState) Clone() pointerState {
	if s.Value != nil {
		value := *s.Value
		s.Value = &value
	}
	return s
}

func TestNativeClonerOwnsPointerSemanticsWithoutReflection(t *testing.T) {
	text := "original"
	input := pointerState{Value: &text}
	g := graph.NewStateGraph[pointerState, pointerState, pointerState]("pointer", func(_ context.Context, state pointerState) pointerState { return state })
	node := runnable.Func[pointerState, pointerState](func(_ context.Context, state pointerState, _ ...runnable.Option) (pointerState, error) {
		*state.Value = "changed"
		return state, nil
	})
	if err := graph.AddNode(g, "node", node); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "node"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("node", graph.End); err != nil {
		t.Fatal(err)
	}
	output, err := g.Invoke(context.Background(), input)
	if err != nil || *input.Value != "original" || *output.Value != "changed" {
		t.Fatalf("input=%q output=%q err=%v", *input.Value, *output.Value, err)
	}
	if _, exists := reflect.TypeOf(graph.Event{}).FieldByName("State"); exists {
		t.Fatal("control Event exposes erased state")
	}
}

type panicCloneState struct{}

func (panicCloneState) Clone() panicCloneState { panic("clone failed") }

func TestClonePanicBecomesExecutionError(t *testing.T) {
	g := graph.NewStateGraph[int, int, panicCloneState]("panic", func(context.Context, int) panicCloneState { return panicCloneState{} })
	node := runnable.Func[panicCloneState, int](func(_ context.Context, _ panicCloneState, _ ...runnable.Option) (int, error) { return 1, nil })
	if err := graph.AddNode(g, "node", node); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "node"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("node", graph.End); err != nil {
		t.Fatal(err)
	}
	_, err := g.Invoke(context.Background(), 0)
	if err == nil || !strings.Contains(err.Error(), "clone failed") {
		t.Fatalf("Invoke error = %v", err)
	}
}

type checkpointTestState struct {
	Value  int
	secret string
}

func (s checkpointTestState) Clone() checkpointTestState { return s }

type checkpointTestStateCodec struct{}

func (checkpointTestStateCodec) ID() string   { return "architecture-test-state" }
func (checkpointTestStateCodec) Version() int { return 7 }
func (checkpointTestStateCodec) Marshal(_ context.Context, value checkpointTestState) ([]byte, error) {
	return json.Marshal(struct {
		Value  int    `json:"value"`
		Secret string `json:"secret"`
	}{Value: value.Value, Secret: value.secret})
}
func (checkpointTestStateCodec) Unmarshal(_ context.Context, data []byte) (checkpointTestState, error) {
	var decoded struct {
		Value  int    `json:"value"`
		Secret string `json:"secret"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return checkpointTestState{}, err
	}
	return checkpointTestState{Value: decoded.Value, secret: decoded.Secret}, nil
}

func newCheckpointGraph(t *testing.T) *graph.StateGraph[string, checkpointTestState, checkpointTestState] {
	t.Helper()
	g := graph.NewStateGraph[string, checkpointTestState, checkpointTestState]("checkpoint", func(_ context.Context, input string) checkpointTestState {
		return checkpointTestState{Value: len(input), secret: "generated"}
	})
	if err := graph.AddNode(g, "echo", runnable.Func[checkpointTestState, checkpointTestState](func(_ context.Context, value checkpointTestState, _ ...runnable.Option) (checkpointTestState, error) {
		return value, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "echo"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("echo", graph.End); err != nil {
		t.Fatal(err)
	}
	return g
}

func TestStateGraphUsesConfiguredStateCodecForCheckpoint(t *testing.T) {
	g := newCheckpointGraph(t)
	if err := g.SetStateCodec(checkpointTestStateCodec{}); err != nil {
		t.Fatal(err)
	}
	data, err := (checkpointTestStateCodec{}).Marshal(context.Background(), checkpointTestState{Value: 42, secret: "restored"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := state.WithGraphStateCheckpoint(context.Background(), state.GraphStateCheckpoint{
		CodecID: "architecture-test-state", Version: 7, Data: data,
	})
	output, err := g.Invoke(ctx, "ignored")
	if err != nil {
		t.Fatal(err)
	}
	if output.Value != 42 || output.secret != "restored" {
		t.Fatalf("decoded graph state = %#v", output)
	}
	data[0] ^= 0xff
}

func TestStateGraphRejectsInvalidOrMissingStateCodec(t *testing.T) {
	data := []byte(`{"value":1,"secret":"checkpoint"}`)
	withoutCodec := newCheckpointGraph(t)
	ctx := state.WithGraphStateCheckpoint(context.Background(), state.GraphStateCheckpoint{CodecID: "architecture-test-state", Version: 7, Data: data})
	if _, err := withoutCodec.Invoke(ctx, "ignored"); err == nil || !strings.Contains(err.Error(), "codec") {
		t.Fatalf("missing codec error = %v", err)
	}
	wrongID := newCheckpointGraph(t)
	if err := wrongID.SetStateCodec(checkpointTestStateCodec{}); err != nil {
		t.Fatal(err)
	}
	ctx = state.WithGraphStateCheckpoint(context.Background(), state.GraphStateCheckpoint{CodecID: "other", Version: 7, Data: data})
	if _, err := wrongID.Invoke(ctx, "ignored"); err == nil || !strings.Contains(err.Error(), "codec ID") {
		t.Fatalf("wrong codec ID error = %v", err)
	}
	wrongVersion := newCheckpointGraph(t)
	if err := wrongVersion.SetStateCodec(checkpointTestStateCodec{}); err != nil {
		t.Fatal(err)
	}
	ctx = state.WithGraphStateCheckpoint(context.Background(), state.GraphStateCheckpoint{CodecID: "architecture-test-state", Version: 8, Data: data})
	if _, err := wrongVersion.Invoke(ctx, "ignored"); err == nil || !strings.Contains(err.Error(), "version") {
		t.Fatalf("wrong codec version error = %v", err)
	}
}

type childGraphState struct {
	Text   string
	Visits int
}

func (s childGraphState) Clone() childGraphState { return s }

type childRequest struct{ Text string }
type childResponse struct{ Text string }

func TestAddNodeMountsChildGraphWithStateMappings(t *testing.T) {
	child := graph.NewStateGraph[childRequest, childResponse, childGraphState]("child", func(_ context.Context, input childRequest) childGraphState {
		return childGraphState{Text: input.Text, Visits: 100}
	})
	childNode := runnable.Func[childGraphState, childResponse](func(_ context.Context, value childGraphState, _ ...runnable.Option) (childResponse, error) {
		value.Visits++
		return childResponse{Text: value.Text + ":child"}, nil
	})
	if err := graph.AddNode(child, "work", childNode); err != nil {
		t.Fatal(err)
	}
	if err := child.AddEdge(graph.Start, "work"); err != nil {
		t.Fatal(err)
	}
	if err := child.AddEdge("work", graph.End); err != nil {
		t.Fatal(err)
	}

	parent := graph.NewStateGraph[string, string, typedState]("parent", func(_ context.Context, input string) typedState {
		return typedState{Text: input}
	})
	if err := graph.AddNode(parent, "child", child,
		graph.WithStatePreHandler[typedState, childRequest](func(_ context.Context, value typedState) (childRequest, error) {
			return childRequest{Text: value.Text}, nil
		}),
		graph.WithStatePostHandler[typedState, childResponse](func(_ context.Context, value typedState, output childResponse) (typedState, error) {
			value.History = append(value.History, output.Text)
			value.Count++
			return value, nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	finish := runnable.Func[typedState, string](func(_ context.Context, value typedState, _ ...runnable.Option) (string, error) {
		return fmt.Sprintf("%d:%s", value.Count, value.History[0]), nil
	})
	if err := graph.AddNode(parent, "finish", finish); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{{graph.Start, "child"}, {"child", "finish"}, {"finish", graph.End}} {
		if err := parent.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	output, err := parent.Invoke(context.Background(), "parent")
	if err != nil || output != "1:parent:child" {
		t.Fatalf("nested graph output=%q err=%v", output, err)
	}
}
