package graph_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"goharness/pkg/compose"
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

func (s typedState) Clone() typedState {
	s.History = append([]string(nil), s.History...)
	return s
}

func TestStateGraphRunsHeterogeneousTypedNodes(t *testing.T) {
	g := graph.NewStateGraph[string, string, typedState]("heterogeneous", func(_ context.Context, input string) typedState {
		return typedState{Text: input}
	})
	length := runnable.Func[typedState, int](func(_ context.Context, state typedState, _ ...runnable.Option) (int, error) {
		return len(state.Text), nil
	})
	render := runnable.Func[int, string](func(_ context.Context, value int, _ ...runnable.Option) (string, error) {
		return fmt.Sprintf("length=%d", value), nil
	})
	if err := graph.AddNode(g, "length", length); err != nil {
		t.Fatal(err)
	}
	if err := graph.AddNode(g, "render", render); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{{graph.Start, "length"}, {"length", "render"}, {"render", graph.End}} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	output, err := g.Invoke(context.Background(), "hello")
	if err != nil || output != "length=5" {
		t.Fatalf("Invoke output=%q err=%v", output, err)
	}
	snapshot := g.Snapshot()
	if snapshot.StateType != "graph_test.typedState" || snapshot.Nodes[0].InputType != "graph_test.typedState" || snapshot.Nodes[0].OutputType != "int" {
		t.Fatalf("typed snapshot = %#v", snapshot)
	}
}

type mappedRequest struct {
	Topic string
}

type mappedResponse struct {
	Summary string
}

type parallelTestState struct {
	Values []string
}

func (s parallelTestState) Clone() parallelTestState {
	s.Values = append([]string(nil), s.Values...)
	return s
}

func TestAddNodeStatePreAndPostHandlersMapGlobalState(t *testing.T) {
	g := graph.NewStateGraph[string, string, typedState]("state-mapping", func(_ context.Context, input string) typedState {
		return typedState{Text: input}
	})
	var forwardedOptions int
	worker := runnable.Func[mappedRequest, mappedResponse](func(_ context.Context, request mappedRequest, options ...runnable.Option) (mappedResponse, error) {
		forwardedOptions = len(options)
		return mappedResponse{Summary: request.Topic + " done"}, nil
	})
	workerOptions := []runnable.Option{
		graph.WithStatePreHandler[typedState, mappedRequest](func(_ context.Context, state typedState) (mappedRequest, error) {
			return mappedRequest{Topic: state.Text}, nil
		}),
		graph.WithStatePostHandler[typedState, mappedResponse](func(_ context.Context, state typedState, response mappedResponse) (typedState, error) {
			state.History = append(state.History, response.Summary)
			state.Count++
			return state, nil
		}),
	}
	if err := graph.AddNode(g, "worker", worker, workerOptions...); err != nil {
		t.Fatal(err)
	}
	finish := runnable.Func[typedState, string](func(_ context.Context, state typedState, _ ...runnable.Option) (string, error) {
		return fmt.Sprintf("%d:%s", state.Count, state.History[0]), nil
	})
	if err := graph.AddNode(g, "finish", finish, graph.WithStatePreHandler[typedState, typedState](func(_ context.Context, state typedState) (typedState, error) {
		return state, nil
	})); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{{graph.Start, "worker"}, {"worker", "finish"}, {"finish", graph.End}} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	output, err := g.Invoke(context.Background(), "topic")
	if err != nil || output != "1:topic done" {
		t.Fatalf("mapped output=%q err=%v", output, err)
	}
	if forwardedOptions != 0 {
		t.Fatalf("state handler options leaked to node: %d", forwardedOptions)
	}
}

func TestAddParallelNodeClonesBranchesAndUsesDeterministicMerger(t *testing.T) {
	g := graph.NewStateGraph[string, parallelTestState, parallelTestState]("parallel", func(_ context.Context, input string) parallelTestState {
		return parallelTestState{Values: []string{input}}
	})
	first := runnable.Func[parallelTestState, parallelTestState](func(_ context.Context, state parallelTestState, _ ...runnable.Option) (parallelTestState, error) {
		state.Values = append(state.Values, "first")
		return state, nil
	})
	second := runnable.Func[parallelTestState, parallelTestState](func(_ context.Context, state parallelTestState, _ ...runnable.Option) (parallelTestState, error) {
		state.Values = append(state.Values, "second")
		return state, nil
	})
	merger := graph.MergerFunc[parallelTestState](func(_ context.Context, base parallelTestState, branches []parallelTestState) (parallelTestState, error) {
		for _, branch := range branches {
			base.Values = append(base.Values, branch.Values[len(branch.Values)-1])
		}
		return base, nil
	})
	if err := graph.AddParallelNode(g, "parallel", []graph.ParallelBranch[parallelTestState]{
		{Name: "first", Node: first},
		{Name: "second", Node: second},
	}, merger); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "parallel"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("parallel", graph.End); err != nil {
		t.Fatal(err)
	}
	output, err := g.Invoke(context.Background(), "base")
	if err != nil {
		t.Fatal(err)
	}
	if len(output.Values) != 3 || output.Values[0] != "base" || output.Values[1] != "first" || output.Values[2] != "second" {
		t.Fatalf("parallel output=%#v", output)
	}
}

func TestCompileRejectsMismatchedNodeEdgeTypes(t *testing.T) {
	g := graph.NewStateGraph[string, string, typedState]("mismatch", func(_ context.Context, input string) typedState {
		return typedState{Text: input}
	})
	if err := graph.AddNode(g, "number", runnable.Func[typedState, int](func(_ context.Context, _ typedState, _ ...runnable.Option) (int, error) {
		return 1, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := graph.AddNode(g, "text", runnable.Func[string, string](func(_ context.Context, value string, _ ...runnable.Option) (string, error) {
		return value, nil
	})); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{{graph.Start, "number"}, {"number", "text"}, {"text", graph.End}} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	err := g.Compile()
	if err == nil || !strings.Contains(err.Error(), "edge.type_mismatch") || !strings.Contains(err.Error(), "int") || !strings.Contains(err.Error(), "string") {
		t.Fatalf("Compile error = %v", err)
	}
}

func TestStateGraphUsesNativeCloneBeforeNodeInvocation(t *testing.T) {
	input := typedState{History: []string{"original"}}
	g := graph.NewStateGraph[typedState, typedState, typedState]("clone", func(_ context.Context, value typedState) typedState {
		return value
	})
	mutate := runnable.Func[typedState, typedState](func(_ context.Context, state typedState, _ ...runnable.Option) (typedState, error) {
		state.History[0] = "node"
		return state, nil
	})
	if err := graph.AddNode(g, "mutate", mutate); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "mutate"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("mutate", graph.End); err != nil {
		t.Fatal(err)
	}
	output, err := g.Invoke(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if input.History[0] != "original" || output.History[0] != "node" {
		t.Fatalf("input=%#v output=%#v", input, output)
	}
}

func TestResumeRejectsHeterogeneousIntermediateWithoutTypedState(t *testing.T) {
	g := graph.NewStateGraph[string, string, typedState]("resume-mismatch", func(_ context.Context, input string) typedState {
		return typedState{Text: input}
	})
	first := runnable.Func[typedState, int](func(_ context.Context, state typedState, _ ...runnable.Option) (int, error) {
		return len(state.Text), nil
	})
	second := runnable.Func[int, string](func(_ context.Context, value int, _ ...runnable.Option) (string, error) {
		return fmt.Sprint(value), nil
	})
	if err := graph.AddNode(g, "first", first); err != nil {
		t.Fatal(err)
	}
	if err := graph.AddNode(g, "second", second); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{{graph.Start, "first"}, {"first", "second"}, {"second", graph.End}} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	ctx := state.WithCursorStack(context.Background(), state.CursorStack{{Graph: "resume-mismatch", Node: "second", Step: 1}})
	_, err := g.Invoke(ctx, "hello")
	if err == nil || !strings.Contains(err.Error(), "graph.resume.type_mismatch") {
		t.Fatalf("Invoke error = %v", err)
	}
}

func TestTypedConditionalLoopUsesSourceValue(t *testing.T) {
	g := graph.NewStateGraph[int, int, typedState]("loop", func(_ context.Context, input int) typedState {
		return typedState{Count: input}
	})
	step := runnable.Func[typedState, typedState](func(_ context.Context, state typedState, _ ...runnable.Option) (typedState, error) {
		state.Count++
		return state, nil
	})
	finish := runnable.Func[typedState, int](func(_ context.Context, state typedState, _ ...runnable.Option) (int, error) {
		return state.Count, nil
	})
	if err := graph.AddNode(g, "step", step); err != nil {
		t.Fatal(err)
	}
	if err := graph.AddNode(g, "finish", finish); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "step"); err != nil {
		t.Fatal(err)
	}
	if err := graph.AddConditionalEdge(g, "step", "step", "again", func(_ context.Context, state typedState) (bool, error) {
		return state.Count < 3, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("step", "finish"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("finish", graph.End); err != nil {
		t.Fatal(err)
	}
	output, err := g.Invoke(context.Background(), 0)
	if err != nil || output != 3 {
		t.Fatalf("loop output=%d err=%v", output, err)
	}
}

func TestGraphForwardsOnlyNodeOptionsAndAppliesOnce(t *testing.T) {
	applications := 0
	global := runnable.GlobalOption(runnable.WithMetadata("tenant", "global"))
	nodeOption := runnable.NodeOption(runnable.OptionFunc(func(set *runnable.OptionSet) {
		applications++
		set.Metadata["mode"] = "parent"
	}))
	var observed runnable.OptionSet
	g := graph.NewStateGraph[string, string, typedState]("options", func(_ context.Context, input string) typedState {
		return typedState{Text: input}
	})
	node := runnable.Func[typedState, string](func(_ context.Context, state typedState, opts ...runnable.Option) (string, error) {
		var err error
		observed, err = runnable.ResolveOptions(opts...)
		return state.Text, err
	})
	if err := graph.AddNode(g, "node", node, runnable.WithMetadata("mode", "local")); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "node"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("node", graph.End); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Invoke(context.Background(), "ok", global, nodeOption); err != nil {
		t.Fatal(err)
	}
	if applications != 1 || observed.Metadata["tenant"] != nil || observed.Metadata["mode"] != "local" {
		t.Fatalf("applications=%d options=%#v", applications, observed)
	}
}

func TestCompileFreezesGraphAndRejectsInvalidFallbacks(t *testing.T) {
	g := graph.NewStateGraph[string, string, typedState]("freeze", func(_ context.Context, input string) typedState { return typedState{Text: input} })
	node := runnable.Func[typedState, string](func(_ context.Context, state typedState, _ ...runnable.Option) (string, error) {
		return state.Text, nil
	})
	if err := graph.AddNode(g, "node", node); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "node"); err != nil {
		t.Fatal(err)
	}
	if err := graph.AddConditionalEdge(g, "node", graph.End, "maybe", func(_ context.Context, _ string) (bool, error) { return true, nil }); err != nil {
		t.Fatal(err)
	}
	if err := g.Compile(); err == nil || !strings.Contains(err.Error(), "edge.fallback.required") {
		t.Fatalf("Compile error = %v", err)
	}
	if err := g.AddEdge("node", graph.End); err != nil {
		t.Fatal(err)
	}
	if err := g.Compile(); err != nil {
		t.Fatal(err)
	}
	if err := graph.AddNode(g, "late", node); err == nil || !strings.Contains(err.Error(), "graph.built") {
		t.Fatalf("late AddNode error = %v", err)
	}
}

func TestCompileRejectsIndirectNestedGraphCycle(t *testing.T) {
	gen := func(_ context.Context, state typedState) typedState { return state }
	first := graph.NewStateGraph[typedState, typedState, typedState]("first", gen)
	second := graph.NewStateGraph[typedState, typedState, typedState]("second", gen)
	if err := graph.AddNode(first, "second", second); err != nil {
		t.Fatal(err)
	}
	if err := graph.AddNode(second, "first", first); err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		graph *graph.StateGraph[typedState, typedState, typedState]
		node  string
	}{{first, "second"}, {second, "first"}} {
		if err := item.graph.AddEdge(graph.Start, item.node); err != nil {
			t.Fatal(err)
		}
		if err := item.graph.AddEdge(item.node, graph.End); err != nil {
			t.Fatal(err)
		}
	}
	if err := first.Compile(); err == nil || !strings.Contains(err.Error(), "graph.cycle") {
		t.Fatalf("Compile error = %v", err)
	}
}

type recordingAgent struct {
	name string
	text string
}

func (a recordingAgent) Name() string        { return a.name }
func (a recordingAgent) Description() string { return a.name }
func (a recordingAgent) Invoke(_ context.Context, _ []schema.Message, _ ...runnable.Option) ([]schema.Message, error) {
	return []schema.Message{schema.AssistantMessage(a.text)}, nil
}

func TestComposeMessageGraphAdaptsAgentsToTypedState(t *testing.T) {
	g, err := compose.NewGraph("team").
		Agent("first", recordingAgent{name: "first", text: "one"}).
		Agent("second", recordingAgent{name: "second", text: "two"}).
		Edge(graph.Start, "first").
		Edge("first", "second").
		Edge("second", graph.End).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	output, err := g.Invoke(context.Background(), []schema.Message{schema.UserMessage("start")})
	if err != nil || len(output) != 2 || output[0].Text() != "one" || output[1].Text() != "two" {
		t.Fatalf("output=%#v err=%v", output, err)
	}
}
