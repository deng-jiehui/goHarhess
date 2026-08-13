package graph_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"

	"goharness/pkg/graph"
	"goharness/pkg/runnable"
	"goharness/pkg/state"
)

type runtimeAPIState struct {
	Text   string
	Length int
}

func TestRuntimeStateFactoryRunsOnceAndSequentialHandlersSharePointer(t *testing.T) {
	var factoryCalls atomic.Int32
	g := graph.NewGraph[string, string]("runtime-state-identity")
	if err := g.SetStateFactory(func(_ context.Context, input string) (*runtimeAPIState, error) {
		factoryCalls.Add(1)
		return &runtimeAPIState{Text: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	var firstState *runtimeAPIState
	first := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
		return input + ":first", nil
	})
	if err := g.AddNode("first", first,
		graph.WithPreHandler(func(_ context.Context, current *runtimeAPIState) (string, error) {
			firstState = current
			return current.Text, nil
		}),
		graph.WithPostHandler(func(_ context.Context, current *runtimeAPIState, output string) error {
			if current != firstState {
				t.Fatal("post handler received a different State pointer")
			}
			current.Text = output
			return nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	second := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
		return input + ":second", nil
	})
	if err := g.AddNode("second", second, graph.WithPreHandler(func(_ context.Context, current *runtimeAPIState) (string, error) {
		if current != firstState {
			t.Fatal("sequential node received a different State pointer")
		}
		return current.Text, nil
	})); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{{graph.Start, "first"}, {"first", "second"}, {"second", graph.End}} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	output, err := g.Invoke(context.Background(), "input")
	if err != nil {
		t.Fatal(err)
	}
	if output != "input:first:second" || factoryCalls.Load() != 1 {
		t.Fatalf("output=%q factory calls=%d", output, factoryCalls.Load())
	}
}

func TestRuntimeStateResumeDecodesOnceAndRequiresPreHandler(t *testing.T) {
	var factoryCalls atomic.Int32
	var decodes atomic.Int32
	codec, err := graph.NewStateCodec[*runtimeAPIState](
		"runtime-api-state",
		1,
		func(_ context.Context, current *runtimeAPIState) ([]byte, error) { return json.Marshal(current) },
		func(_ context.Context, data []byte) (*runtimeAPIState, error) {
			decodes.Add(1)
			var current runtimeAPIState
			return &current, json.Unmarshal(data, &current)
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	checkpointData, err := json.Marshal(&runtimeAPIState{Text: "restored"})
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := state.GraphStateCheckpoint{CodecID: "runtime-api-state", Version: 1, Data: checkpointData}

	build := func(withPre bool) *graph.Graph[string, string] {
		g := graph.NewGraph[string, string]("runtime-resume")
		if err := g.SetStateFactory(func(_ context.Context, input string) (*runtimeAPIState, error) {
			factoryCalls.Add(1)
			return &runtimeAPIState{Text: input}, nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := g.SetStateCodec(codec); err != nil {
			t.Fatal(err)
		}
		node := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) { return input, nil })
		var options []graph.NodeOption
		if withPre {
			options = append(options, graph.WithPreHandler(func(_ context.Context, current *runtimeAPIState) (string, error) {
				return current.Text, nil
			}))
		}
		if err := g.AddNode("work", node, options...); err != nil {
			t.Fatal(err)
		}
		if err := g.AddEdge(graph.Start, "work"); err != nil {
			t.Fatal(err)
		}
		if err := g.AddEdge("work", graph.End); err != nil {
			t.Fatal(err)
		}
		return g
	}

	ctx := state.WithGraphStateCheckpoint(context.Background(), checkpoint)
	ctx = state.WithCursorStack(ctx, state.CursorStack{{Graph: "runtime-resume", Node: "work", Step: 0}})
	output, err := build(true).Invoke(ctx, "ignored")
	if err != nil || output != "restored" || factoryCalls.Load() != 0 || decodes.Load() != 1 {
		t.Fatalf("output=%q factory=%d decodes=%d err=%v", output, factoryCalls.Load(), decodes.Load(), err)
	}

	_, err = build(false).Invoke(ctx, "ignored")
	if err == nil || !strings.Contains(err.Error(), "graph.resume.type_mismatch") {
		t.Fatalf("resume without PreHandler error = %v", err)
	}
}

func TestWithNodeOptionsForwardsEachOptionOnce(t *testing.T) {
	g := graph.NewGraph[string, string]("node-options")
	var applications atomic.Int32
	option := func(key string) runnable.Option {
		return runnable.OptionFunc(func(set *runnable.OptionSet) {
			applications.Add(1)
			set.Metadata[key] = true
		})
	}
	node := runnable.Func[string, string](func(_ context.Context, input string, options ...runnable.Option) (string, error) {
		resolved, err := runnable.ResolveOptions(options...)
		if err != nil {
			return "", err
		}
		if resolved.Metadata["first"] != true || resolved.Metadata["second"] != true {
			t.Fatalf("node metadata = %#v", resolved.Metadata)
		}
		return input, nil
	})
	if err := g.AddNode("node", node, graph.WithNodeOptions(option("first"), option("second"))); err != nil {
		t.Fatal(err)
	}
	_ = g.AddEdge(graph.Start, "node")
	_ = g.AddEdge("node", graph.End)
	if _, err := g.Invoke(context.Background(), "ok"); err != nil {
		t.Fatal(err)
	}
	if applications.Load() != 2 {
		t.Fatalf("option applications = %d", applications.Load())
	}
}

func TestGraphSupportsNilInterfaceNodeValues(t *testing.T) {
	g := graph.NewGraph[string, any]("nil-interface")
	produce := runnable.Func[string, any](func(_ context.Context, _ string, _ ...runnable.Option) (any, error) { return nil, nil })
	consume := runnable.Func[any, any](func(_ context.Context, input any, _ ...runnable.Option) (any, error) {
		if input != nil {
			t.Fatalf("input = %#v", input)
		}
		return nil, nil
	})
	if err := g.AddNode("produce", produce); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("consume", consume); err != nil {
		t.Fatal(err)
	}
	_ = g.AddEdge(graph.Start, "produce")
	_ = g.AddEdge("produce", "consume")
	_ = g.AddEdge("consume", graph.End)
	output, err := g.Invoke(context.Background(), "input")
	if err != nil || output != nil {
		t.Fatalf("output=%#v err=%v", output, err)
	}
}

func TestGraphRuntimeStateAPIUsesPureNodes(t *testing.T) {
	g := graph.NewGraph[string, string]("runtime-state")
	if err := g.SetStateFactory(func(_ context.Context, input string) (*runtimeAPIState, error) {
		return &runtimeAPIState{Text: input}, nil
	}); err != nil {
		t.Fatal(err)
	}

	length := runnable.Func[string, int](func(_ context.Context, input string, _ ...runnable.Option) (int, error) {
		return len(input), nil
	})
	if err := g.AddNode(
		"length",
		length,
		graph.WithPreHandler(func(_ context.Context, state *runtimeAPIState) (string, error) {
			return state.Text, nil
		}),
		graph.WithPostHandler(func(_ context.Context, state *runtimeAPIState, output int) error {
			state.Length = output
			return nil
		}),
	); err != nil {
		t.Fatal(err)
	}

	render := runnable.Func[int, string](func(_ context.Context, input int, _ ...runnable.Option) (string, error) {
		return strings.Repeat("x", input), nil
	})
	if err := g.AddNode("render", render, graph.WithPreHandler(func(_ context.Context, state *runtimeAPIState) (int, error) {
		return state.Length, nil
	})); err != nil {
		t.Fatal(err)
	}

	if err := g.AddEdge(graph.Start, "length"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("length", "render"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("render", graph.End); err != nil {
		t.Fatal(err)
	}
	if err := g.Compile(); err != nil {
		t.Fatal(err)
	}

	output, err := g.Invoke(context.Background(), "hello")
	if err != nil {
		t.Fatal(err)
	}
	if output != "xxxxx" {
		t.Fatalf("output = %q", output)
	}
}

func TestGraphBranchCanReadRuntimeState(t *testing.T) {
	g := graph.NewGraph[string, string]("runtime-branch")
	if err := g.SetStateFactory(func(_ context.Context, input string) (*runtimeAPIState, error) {
		return &runtimeAPIState{Text: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	setLength := runnable.Func[string, int](func(_ context.Context, input string, _ ...runnable.Option) (int, error) {
		return len(input), nil
	})
	if err := g.AddNode("measure", setLength,
		graph.WithPreHandler(func(_ context.Context, state *runtimeAPIState) (string, error) { return state.Text, nil }),
		graph.WithPostHandler(func(_ context.Context, state *runtimeAPIState, output int) error {
			state.Length = output
			return nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	long := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
		return "long:" + input, nil
	})
	short := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
		return "short:" + input, nil
	})
	if err := g.AddNode("long", long, graph.WithPreHandler(func(_ context.Context, state *runtimeAPIState) (string, error) { return state.Text, nil })); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("short", short, graph.WithPreHandler(func(_ context.Context, state *runtimeAPIState) (string, error) { return state.Text, nil })); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "measure"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddBranch("measure", "long", "length_gt_3", func(_ context.Context, state *runtimeAPIState) (bool, error) {
		return state.Length > 3, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("measure", "short"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("long", graph.End); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("short", graph.End); err != nil {
		t.Fatal(err)
	}

	output, err := g.Invoke(context.Background(), "hello")
	if err != nil {
		t.Fatal(err)
	}
	if output != "long:hello" {
		t.Fatalf("output = %q", output)
	}
}
