package graph_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"goharness/pkg/graph"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
	"goharness/pkg/state"
)

type typedState struct {
	Text     string
	History  []string
	Count    int
	Approved bool
}

type mappedRequest struct{ Topic string }
type mappedResponse struct{ Summary string }

func newTypedGraph[I, O any](t *testing.T, name string, factory any) *graph.Graph[I, O] {
	t.Helper()
	g := graph.NewGraph[I, O](name)
	if err := g.SetStateFactory(factory); err != nil {
		t.Fatal(err)
	}
	return g
}

func mustCompile[I, O any](t *testing.T, builder *graph.Graph[I, O]) *graph.CompiledGraph[I, O] {
	t.Helper()
	compiled, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func TestGraphRunsHeterogeneousPureNodes(t *testing.T) {
	g := graph.NewGraph[string, string]("heterogeneous")
	length := runnable.Func[string, int](func(_ context.Context, input string, _ ...runnable.Option) (int, error) { return len(input), nil })
	render := runnable.Func[int, string](func(_ context.Context, value int, _ ...runnable.Option) (string, error) {
		return fmt.Sprintf("length=%d", value), nil
	})
	if err := g.AddNode("length", length); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("render", render); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{{graph.Start, "length"}, {"length", "render"}, {"render", graph.End}} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	output, err := mustCompile(t, g).Invoke(context.Background(), "hello")
	if err != nil || output != "length=5" {
		t.Fatalf("output=%q err=%v", output, err)
	}
	snapshot := g.Snapshot()
	if snapshot.StateType != "<nil>" || snapshot.Nodes[0].InputType != "string" || snapshot.Nodes[0].OutputType != "int" {
		t.Fatalf("snapshot=%#v", snapshot)
	}
}

func TestGraphStateHandlersMapPureNode(t *testing.T) {
	g := newTypedGraph[string, string](t, "mapping", func(_ context.Context, input string) (*typedState, error) { return &typedState{Text: input}, nil })
	worker := runnable.Func[mappedRequest, mappedResponse](func(_ context.Context, request mappedRequest, options ...runnable.Option) (mappedResponse, error) {
		if len(options) != 0 {
			t.Fatalf("State options leaked: %d", len(options))
		}
		return mappedResponse{Summary: request.Topic + " done"}, nil
	})
	if err := g.AddNode("worker", worker,
		graph.WithPreHandler(func(_ context.Context, current *typedState) (mappedRequest, error) {
			return mappedRequest{Topic: current.Text}, nil
		}),
		graph.WithPostHandler(func(_ context.Context, current *typedState, output mappedResponse) error {
			current.History = append(current.History, output.Summary)
			current.Count++
			return nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	finish := runnable.Func[*typedState, string](func(_ context.Context, current *typedState, _ ...runnable.Option) (string, error) {
		return fmt.Sprintf("%d:%s", current.Count, current.History[0]), nil
	})
	if err := g.AddNode("finish", finish, graph.WithPreHandler(func(_ context.Context, current *typedState) (*typedState, error) { return current, nil })); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{{graph.Start, "worker"}, {"worker", "finish"}, {"finish", graph.End}} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	output, err := mustCompile(t, g).Invoke(context.Background(), "topic")
	if err != nil || output != "1:topic done" {
		t.Fatalf("output=%q err=%v", output, err)
	}
}

func TestCompileRejectsMismatchedNodeEdgeTypes(t *testing.T) {
	g := graph.NewGraph[string, string]("mismatch")
	_ = g.AddNode("number", runnable.Func[string, int](func(context.Context, string, ...runnable.Option) (int, error) { return 1, nil }))
	_ = g.AddNode("text", runnable.Func[string, string](func(_ context.Context, value string, _ ...runnable.Option) (string, error) { return value, nil }))
	for _, edge := range [][2]string{{graph.Start, "number"}, {"number", "text"}, {"text", graph.End}} {
		_ = g.AddEdge(edge[0], edge[1])
	}
	_, err := g.Compile()
	if err == nil || !strings.Contains(err.Error(), "edge.type_mismatch") {
		t.Fatalf("Compile error=%v", err)
	}
}

func TestResumeRequiresStatePreHandler(t *testing.T) {
	g := newTypedGraph[string, string](t, "resume-mismatch", func(_ context.Context, input string) (*typedState, error) { return &typedState{Text: input}, nil })
	first := runnable.Func[string, int](func(_ context.Context, value string, _ ...runnable.Option) (int, error) { return len(value), nil })
	second := runnable.Func[int, string](func(_ context.Context, value int, _ ...runnable.Option) (string, error) {
		return fmt.Sprint(value), nil
	})
	_ = g.AddNode("first", first)
	_ = g.AddNode("second", second)
	for _, edge := range [][2]string{{graph.Start, "first"}, {"first", "second"}, {"second", graph.End}} {
		_ = g.AddEdge(edge[0], edge[1])
	}
	ctx := state.WithCursorStack(context.Background(), state.CursorStack{{Graph: "resume-mismatch", Node: "second", Step: 1}})
	_, err := mustCompile(t, g).Invoke(ctx, "hello")
	if err == nil || !strings.Contains(err.Error(), "graph.resume.type_mismatch") {
		t.Fatalf("Invoke error=%v", err)
	}
}

func TestStateBranchLoopUsesPointerState(t *testing.T) {
	g := newTypedGraph[int, int](t, "loop", func(_ context.Context, input int) (*typedState, error) { return &typedState{Count: input}, nil })
	step := runnable.Func[int, int](func(_ context.Context, value int, _ ...runnable.Option) (int, error) { return value + 1, nil })
	if err := g.AddNode("step", step,
		graph.WithPreHandler(func(_ context.Context, current *typedState) (int, error) { return current.Count, nil }),
		graph.WithPostHandler(func(_ context.Context, current *typedState, output int) error { current.Count = output; return nil }),
	); err != nil {
		t.Fatal(err)
	}
	finish := runnable.Func[int, int](func(_ context.Context, value int, _ ...runnable.Option) (int, error) { return value, nil })
	_ = g.AddNode("finish", finish, graph.WithPreHandler(func(_ context.Context, current *typedState) (int, error) { return current.Count, nil }))
	_ = g.AddEdge(graph.Start, "step")
	_ = g.AddBranch("step", "step", "again", func(_ context.Context, current *typedState) (bool, error) { return current.Count < 3, nil })
	_ = g.AddEdge("step", "finish")
	_ = g.AddEdge("finish", graph.End)
	output, err := mustCompile(t, g).Invoke(context.Background(), 0)
	if err != nil || output != 3 {
		t.Fatalf("output=%d err=%v", output, err)
	}
}

func TestGraphForwardsOnlyNodeOptionsAndKeepsCompiledSnapshot(t *testing.T) {
	applications := 0
	global := runnable.GlobalOption(runnable.WithMetadata("tenant", "global"))
	nodeOption := runnable.NodeOption(runnable.OptionFunc(func(set *runnable.OptionSet) { applications++; set.Metadata["mode"] = "parent" }))
	var observed runnable.OptionSet
	g := graph.NewGraph[string, string]("options")
	node := runnable.Func[string, string](func(_ context.Context, value string, opts ...runnable.Option) (string, error) {
		var err error
		observed, err = runnable.ResolveOptions(opts...)
		return value, err
	})
	if err := g.AddNode("node", node, graph.WithNodeOptions(runnable.WithMetadata("mode", "local"))); err != nil {
		t.Fatal(err)
	}
	_ = g.AddEdge(graph.Start, "node")
	_ = g.AddEdge("node", graph.End)
	compiled := mustCompile(t, g)
	if _, err := compiled.Invoke(context.Background(), "ok", global, nodeOption); err != nil {
		t.Fatal(err)
	}
	if applications != 1 || observed.Metadata["tenant"] != nil || observed.Metadata["mode"] != "local" {
		t.Fatalf("applications=%d options=%#v", applications, observed)
	}
	if err := g.AddNode("late", node); err != nil {
		t.Fatalf("late AddNode error=%v", err)
	}
	if len(compiled.Snapshot().Nodes) != 1 {
		t.Fatalf("compiled snapshot changed: %#v", compiled.Snapshot())
	}
}

func TestCompileRejectsInvalidFallbackAndMutableNestedGraph(t *testing.T) {
	g := graph.NewGraph[string, string]("fallback")
	node := runnable.Func[string, string](func(_ context.Context, value string, _ ...runnable.Option) (string, error) { return value, nil })
	_ = g.AddNode("node", node)
	_ = g.AddEdge(graph.Start, "node")
	_ = g.AddBranch("node", graph.End, "maybe", func(_ context.Context, _ string) (bool, error) { return true, nil })
	if _, err := g.Compile(); err == nil || !strings.Contains(err.Error(), "edge.fallback.required") {
		t.Fatalf("Compile error=%v", err)
	}

	parent := graph.NewGraph[string, string]("parent")
	child := graph.NewGraph[string, string]("child")
	if err := parent.AddNode("child", child); err == nil || !strings.Contains(err.Error(), "Invoke") {
		t.Fatalf("mutable nested graph error=%v", err)
	}
}

type recordingAgent struct{ name, text string }

func (a recordingAgent) Name() string        { return a.name }
func (a recordingAgent) Description() string { return a.name }
func (a recordingAgent) Invoke(context.Context, []schema.Message, ...runnable.Option) ([]schema.Message, error) {
	return []schema.Message{schema.AssistantMessage(a.text)}, nil
}

func TestComposeMessageGraphAdaptsAgents(t *testing.T) {
	type teamState struct {
		Messages []schema.Message
		Output   []schema.Message
	}
	g := graph.NewGraph[[]schema.Message, []schema.Message]("team")
	err := g.SetStateFactory(func(_ context.Context, input []schema.Message) (*teamState, error) {
		return &teamState{Messages: schema.CloneMessages(input)}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	addAgent := func(name string, value recordingAgent) error {
		return g.AddNode(name, value,
			graph.WithPreHandler(func(_ context.Context, state *teamState) ([]schema.Message, error) {
				return schema.CloneMessages(state.Messages), nil
			}),
			graph.WithPostHandler(func(_ context.Context, state *teamState, output []schema.Message) error {
				state.Messages = append(state.Messages, schema.CloneMessages(output)...)
				state.Output = append(state.Output, schema.CloneMessages(output)...)
				return nil
			}),
		)
	}
	if err = addAgent("first", recordingAgent{"first", "one"}); err != nil {
		t.Fatal(err)
	}
	if err = addAgent("second", recordingAgent{"second", "two"}); err != nil {
		t.Fatal(err)
	}
	finish := runnable.Func[[]schema.Message, []schema.Message](func(_ context.Context, input []schema.Message, _ ...runnable.Option) ([]schema.Message, error) {
		return schema.CloneMessages(input), nil
	})
	if err = g.AddNode("finish", finish, graph.WithPreHandler(func(_ context.Context, state *teamState) ([]schema.Message, error) {
		return schema.CloneMessages(state.Output), nil
	})); err != nil {
		t.Fatal(err)
	}
	_ = g.AddEdge(graph.Start, "first")
	_ = g.AddEdge("first", "second")
	_ = g.AddEdge("second", "finish")
	_ = g.AddEdge("finish", graph.End)
	compiled, err := g.Compile()
	if err != nil {
		t.Fatal(err)
	}
	output, err := compiled.Invoke(context.Background(), []schema.Message{schema.UserMessage("start")})
	if err != nil || len(output) != 2 || output[0].Text() != "one" || output[1].Text() != "two" {
		t.Fatalf("output=%#v err=%v", output, err)
	}
}
