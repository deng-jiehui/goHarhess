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

func newStreamGraph(t *testing.T, name string) *graph.Graph[[]schema.Message, []schema.Message] {
	t.Helper()
	g := graph.NewGraph[[]schema.Message, []schema.Message](name)
	if err := g.SetStateFactory(func(_ context.Context, input []schema.Message) (*streamState, error) {
		return &streamState{Messages: schema.CloneMessages(input)}, nil
	}); err != nil {
		t.Fatal(err)
	}
	return g
}

func addStreamNode(t *testing.T, g *graph.Graph[[]schema.Message, []schema.Message], name string, node runnable.Runnable[*streamState, *streamState]) {
	t.Helper()
	if err := g.AddNode(name, node,
		graph.WithPreHandler(func(_ context.Context, current *streamState) (*streamState, error) { return current, nil }),
		graph.WithPostHandler(func(_ context.Context, current *streamState, output *streamState) error {
			if current != output {
				*current = *output
			}
			return nil
		}),
	); err != nil {
		t.Fatal(err)
	}
}

func addStreamFinish(t *testing.T, g *graph.Graph[[]schema.Message, []schema.Message], from string) {
	t.Helper()
	finish := runnable.Func[[]schema.Message, []schema.Message](func(_ context.Context, output []schema.Message, _ ...runnable.Option) ([]schema.Message, error) {
		return schema.CloneMessages(output), nil
	})
	if err := g.AddNode("finish", finish, graph.WithPreHandler(func(_ context.Context, current *streamState) ([]schema.Message, error) {
		return schema.CloneMessages(current.Output), nil
	})); err != nil {
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
	g := newStreamGraph(t, "planes")
	node := runnable.Func[*streamState, *streamState](func(ctx context.Context, current *streamState, _ ...runnable.Option) (*streamState, error) {
		graph.EmitData(ctx, schema.MessageEvent{Kind: schema.MessageEventDelta, Messages: []schema.Message{schema.AssistantMessage("hel")}})
		message := schema.AssistantMessage("hello")
		current.Messages = append(current.Messages, message)
		current.Output = append(current.Output, message)
		return current, nil
	})
	addStreamNode(t, g, "model", node)
	_ = g.AddEdge(graph.Start, "model")
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
	if fmt.Sprint(data) != fmt.Sprint([]schema.MessageEventKind{schema.MessageEventDelta, schema.MessageEventSnapshot, schema.MessageEventDone}) {
		t.Fatalf("data=%#v", data)
	}
	mu.Lock()
	defer mu.Unlock()
	if control[0] != graph.EventGraphStart || control[len(control)-1] != graph.EventGraphEnd {
		t.Fatalf("control=%#v", control)
	}
}

func TestStreamDoesNotDuplicateAuthoritativeSnapshot(t *testing.T) {
	g := newStreamGraph(t, "snapshot")
	node := runnable.Func[*streamState, *streamState](func(ctx context.Context, current *streamState, _ ...runnable.Option) (*streamState, error) {
		current.Output = []schema.Message{schema.AssistantMessage("answer")}
		graph.EmitData(ctx, schema.MessageEvent{Kind: schema.MessageEventSnapshot, Messages: current.Output})
		return current, nil
	})
	addStreamNode(t, g, "model", node)
	_ = g.AddEdge(graph.Start, "model")
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
		t.Fatalf("snapshots=%d", snapshots)
	}
}

func TestStreamEmitsEmptyAuthoritativeFinalSnapshot(t *testing.T) {
	g := newStreamGraph(t, "empty-final")
	node := runnable.Func[*streamState, *streamState](func(ctx context.Context, current *streamState, _ ...runnable.Option) (*streamState, error) {
		graph.EmitData(ctx, schema.MessageEvent{Kind: schema.MessageEventSnapshot, Messages: []schema.Message{schema.AssistantMessage("stale")}})
		return current, nil
	})
	addStreamNode(t, g, "model", node)
	_ = g.AddEdge(graph.Start, "model")
	addStreamFinish(t, g, "model")
	stream, err := graph.StreamMessages(context.Background(), g, nil)
	if err != nil {
		t.Fatal(err)
	}
	var snapshots [][]schema.Message
	for item := range stream {
		if item.Err != nil {
			t.Fatal(item.Err)
		}
		if item.Value.Kind == schema.MessageEventSnapshot {
			snapshots = append(snapshots, item.Value.Messages)
		}
	}
	if len(snapshots) != 2 || len(snapshots[0]) != 1 || len(snapshots[1]) != 0 {
		t.Fatalf("snapshots=%#v", snapshots)
	}
}

func TestCursorResumeUsesRuntimeStatePreHandler(t *testing.T) {
	g := newStreamGraph(t, "resume")
	depth := 0
	node := runnable.Func[*streamState, *streamState](func(ctx context.Context, current *streamState, _ ...runnable.Option) (*streamState, error) {
		depth = len(state.CursorStackFromContext(ctx))
		current.Output = []schema.Message{schema.AssistantMessage("resumed")}
		return current, nil
	})
	addStreamNode(t, g, "work", node)
	_ = g.AddEdge(graph.Start, "work")
	addStreamFinish(t, g, "work")
	ctx := state.WithCursorStack(context.Background(), state.CursorStack{{Graph: "resume", Node: "work", Step: 0, Iteration: 2}})
	output, err := g.Invoke(ctx, nil)
	if err != nil || depth != 1 || len(output) != 1 {
		t.Fatalf("depth=%d output=%#v err=%v", depth, output, err)
	}
}

func TestControlEventDoesNotExposeState(t *testing.T) {
	if _, exists := reflect.TypeOf(graph.Event{}).FieldByName("State"); exists {
		t.Fatal("control Event exposes runtime State")
	}
}

type checkpointTestState struct {
	Value  int
	Secret string
}

func newCheckpointGraph(t *testing.T) (*graph.Graph[string, int], graph.StateCodec) {
	t.Helper()
	codec, err := graph.NewStateCodec[*checkpointTestState]("architecture-test-state", 7,
		func(_ context.Context, current *checkpointTestState) ([]byte, error) { return json.Marshal(current) },
		func(_ context.Context, data []byte) (*checkpointTestState, error) {
			var current checkpointTestState
			return &current, json.Unmarshal(data, &current)
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	g := newTypedGraph[string, int](t, "checkpoint", func(_ context.Context, input string) (*checkpointTestState, error) {
		return &checkpointTestState{Value: len(input), Secret: "generated"}, nil
	})
	if err := g.SetStateCodec(codec); err != nil {
		t.Fatal(err)
	}
	node := runnable.Func[int, int](func(_ context.Context, value int, _ ...runnable.Option) (int, error) { return value, nil })
	if err := g.AddNode("echo", node, graph.WithPreHandler(func(_ context.Context, current *checkpointTestState) (int, error) { return current.Value, nil })); err != nil {
		t.Fatal(err)
	}
	_ = g.AddEdge(graph.Start, "echo")
	_ = g.AddEdge("echo", graph.End)
	return g, codec
}

func TestGraphUsesConfiguredStateCodecForResume(t *testing.T) {
	g, codec := newCheckpointGraph(t)
	data, err := codec.Encode(context.Background(), &checkpointTestState{Value: 42, Secret: "restored"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := state.WithGraphStateCheckpoint(context.Background(), state.GraphStateCheckpoint{CodecID: codec.ID(), Version: codec.Version(), Data: data})
	output, err := g.Invoke(ctx, "ignored")
	if err != nil || output != 42 {
		t.Fatalf("output=%d err=%v", output, err)
	}
	data[0] ^= 0xff
}

func TestGraphRejectsCheckpointCodecMismatch(t *testing.T) {
	g, codec := newCheckpointGraph(t)
	data := []byte(`{"Value":1,"Secret":"checkpoint"}`)
	for _, checkpoint := range []state.GraphStateCheckpoint{{CodecID: "other", Version: codec.Version(), Data: data}, {CodecID: codec.ID(), Version: codec.Version() + 1, Data: data}} {
		_, err := g.Invoke(state.WithGraphStateCheckpoint(context.Background(), checkpoint), "ignored")
		if err == nil || !strings.Contains(err.Error(), "does not match") {
			t.Fatalf("mismatch error=%v", err)
		}
	}
}

type childRequest struct{ Text string }
type childResponse struct{ Text string }
type childGraphState struct {
	Text   string
	Visits int
}

func TestAddNodeMountsChildGraphWithStateMappings(t *testing.T) {
	child := graph.NewGraph[childRequest, childResponse]("child")
	if err := child.SetStateFactory(func(_ context.Context, input childRequest) (*childGraphState, error) {
		return &childGraphState{Text: input.Text, Visits: 100}, nil
	}); err != nil {
		t.Fatal(err)
	}
	childNode := runnable.Func[string, childResponse](func(_ context.Context, value string, _ ...runnable.Option) (childResponse, error) {
		return childResponse{Text: value + ":child"}, nil
	})
	if err := child.AddNode("work", childNode, graph.WithPreHandler(func(_ context.Context, current *childGraphState) (string, error) {
		current.Visits++
		return current.Text, nil
	})); err != nil {
		t.Fatal(err)
	}
	_ = child.AddEdge(graph.Start, "work")
	_ = child.AddEdge("work", graph.End)

	parent := newTypedGraph[string, string](t, "parent", func(_ context.Context, input string) (*typedState, error) { return &typedState{Text: input}, nil })
	if err := parent.AddNode("child", child,
		graph.WithPreHandler(func(_ context.Context, current *typedState) (childRequest, error) {
			return childRequest{Text: current.Text}, nil
		}),
		graph.WithPostHandler(func(_ context.Context, current *typedState, output childResponse) error {
			current.History = append(current.History, output.Text)
			current.Count++
			return nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	finish := runnable.Func[string, string](func(_ context.Context, value string, _ ...runnable.Option) (string, error) { return value, nil })
	if err := parent.AddNode("finish", finish, graph.WithPreHandler(func(_ context.Context, current *typedState) (string, error) {
		return fmt.Sprintf("%d:%s", current.Count, current.History[0]), nil
	})); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{{graph.Start, "child"}, {"child", "finish"}, {"finish", graph.End}} {
		_ = parent.AddEdge(edge[0], edge[1])
	}
	output, err := parent.Invoke(context.Background(), "parent")
	if err != nil || output != "1:parent:child" {
		t.Fatalf("output=%q err=%v", output, err)
	}
}
