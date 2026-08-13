package graph_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"goharness/pkg/graph"
	"goharness/pkg/runnable"
)

type runtimeParallelState struct {
	Values []string
}

type sharedPointerCodec struct{ current *runtimeParallelState }

func (sharedPointerCodec) ID() string                                    { return "shared-pointer" }
func (sharedPointerCodec) Version() int                                  { return 1 }
func (sharedPointerCodec) StateType() reflect.Type                       { return reflect.TypeFor[*runtimeParallelState]() }
func (sharedPointerCodec) Encode(context.Context, any) ([]byte, error)   { return []byte("state"), nil }
func (c sharedPointerCodec) Decode(context.Context, []byte) (any, error) { return c.current, nil }

func TestRuntimeParallelSnapshotsBranchesAndMergesInDeclarationOrder(t *testing.T) {
	var encodes atomic.Int32
	var decodes atomic.Int32
	codec, err := graph.NewStateCodec[*runtimeParallelState](
		"parallel-state", 1,
		func(_ context.Context, current *runtimeParallelState) ([]byte, error) {
			encodes.Add(1)
			return json.Marshal(current)
		},
		func(_ context.Context, data []byte) (*runtimeParallelState, error) {
			decodes.Add(1)
			var current runtimeParallelState
			return &current, json.Unmarshal(data, &current)
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	g := graph.NewGraph[string, []string]("parallel")
	if err := g.SetStateFactory(func(_ context.Context, input string) (*runtimeParallelState, error) {
		return &runtimeParallelState{Values: []string{input}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.SetStateCodec(codec); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	seen := map[*runtimeParallelState]bool{}
	branch := func(name string) graph.ParallelBranch {
		return graph.ParallelBranch{
			Name: name,
			Node: runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
				return input + ":" + name, nil
			}),
			Pre: func(_ context.Context, current *runtimeParallelState) (string, error) {
				mu.Lock()
				defer mu.Unlock()
				if seen[current] {
					t.Fatalf("parallel branch reused State pointer %p", current)
				}
				seen[current] = true
				return current.Values[0], nil
			},
			Post: func(_ context.Context, current *runtimeParallelState, output string) error {
				current.Values = append(current.Values, output)
				return nil
			},
		}
	}
	if err := g.AddParallelNode("fanout", []graph.ParallelBranch{branch("first"), branch("second")}, graph.WithStateMerger(
		func(_ context.Context, base *runtimeParallelState, branches []*runtimeParallelState) error {
			for _, current := range branches {
				base.Values = append(base.Values, current.Values[len(current.Values)-1])
			}
			return nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	finish := runnable.Func[[]string, []string](func(_ context.Context, input []string, _ ...runnable.Option) ([]string, error) { return input, nil })
	if err := g.AddNode("finish", finish, graph.WithPreHandler(func(_ context.Context, current *runtimeParallelState) ([]string, error) {
		return append([]string(nil), current.Values...), nil
	})); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{{graph.Start, "fanout"}, {"fanout", "finish"}, {"finish", graph.End}} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	output, err := g.Invoke(context.Background(), "base")
	if err != nil {
		t.Fatal(err)
	}
	if len(output) != 3 || output[0] != "base" || output[1] != "base:first" || output[2] != "base:second" {
		t.Fatalf("parallel output = %#v", output)
	}
	if encodes.Load() != 1 || decodes.Load() != 2 {
		t.Fatalf("codec calls = encode:%d decode:%d", encodes.Load(), decodes.Load())
	}
	if len(seen) != 2 {
		t.Fatalf("branch State pointers = %d", len(seen))
	}
}

func TestRuntimeParallelFailureSkipsMerger(t *testing.T) {
	g := graph.NewGraph[string, string]("parallel-error")
	if err := g.SetStateFactory(func(_ context.Context, input string) (*runtimeParallelState, error) {
		return &runtimeParallelState{Values: []string{input}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("branch failed")
	failed := runnable.Func[string, string](func(_ context.Context, _ string, _ ...runnable.Option) (string, error) { return "", boom })
	waiting := runnable.Func[string, string](func(ctx context.Context, _ string, _ ...runnable.Option) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})
	branch := func(name string, node runnable.Runnable[string, string]) graph.ParallelBranch {
		return graph.ParallelBranch{Name: name, Node: node, Pre: func(_ context.Context, current *runtimeParallelState) (string, error) {
			return current.Values[0], nil
		}, Post: func(_ context.Context, _ *runtimeParallelState, _ string) error { return nil }}
	}
	var merged atomic.Bool
	if err := g.AddParallelNode("fanout", []graph.ParallelBranch{branch("failed", failed), branch("waiting", waiting)}, graph.WithStateMerger(
		func(_ context.Context, _ *runtimeParallelState, _ []*runtimeParallelState) error {
			merged.Store(true)
			return nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	finish := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) { return input, nil })
	if err := g.AddNode("finish", finish, graph.WithPreHandler(func(_ context.Context, current *runtimeParallelState) (string, error) {
		return current.Values[0], nil
	})); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{{graph.Start, "fanout"}, {"fanout", "finish"}, {"finish", graph.End}} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	_, err := g.Invoke(context.Background(), "base")
	if !errors.Is(err, boom) || merged.Load() {
		t.Fatalf("Invoke error=%v merged=%t", err, merged.Load())
	}
}

func TestRuntimeParallelRejectsMissingPostHandler(t *testing.T) {
	g := graph.NewGraph[string, string]("parallel-post")
	if err := g.SetStateFactory(func(_ context.Context, input string) (*runtimeParallelState, error) {
		return &runtimeParallelState{Values: []string{input}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	node := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) { return input, nil })
	if err := g.AddParallelNode("fanout", []graph.ParallelBranch{{
		Name: "missing-post", Node: node,
		Pre: func(_ context.Context, current *runtimeParallelState) (string, error) { return current.Values[0], nil },
	}}, graph.WithStateMerger(func(_ context.Context, _ *runtimeParallelState, _ []*runtimeParallelState) error { return nil })); err != nil {
		t.Fatal(err)
	}
	finish := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) { return input, nil })
	if err := g.AddNode("finish", finish, graph.WithPreHandler(func(_ context.Context, current *runtimeParallelState) (string, error) { return current.Values[0], nil })); err != nil {
		t.Fatal(err)
	}
	_ = g.AddEdge(graph.Start, "fanout")
	_ = g.AddEdge("fanout", "finish")
	_ = g.AddEdge("finish", graph.End)
	if err := g.Compile(); err == nil || !strings.Contains(err.Error(), "PostHandler") {
		t.Fatalf("Compile error = %v", err)
	}
}

func TestRuntimeParallelRejectsDefaultCodecDynamicTypeLoss(t *testing.T) {
	g := graph.NewGraph[string, string]("parallel-dynamic")
	if err := g.SetStateFactory(func(_ context.Context, input string) (*strictState, error) {
		return &strictState{Text: input, Payload: int(1)}, nil
	}); err != nil {
		t.Fatal(err)
	}
	node := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) { return input, nil })
	if err := g.AddParallelNode("fanout", []graph.ParallelBranch{{
		Name: "branch", Node: node,
		Pre:  func(_ context.Context, current *strictState) (string, error) { return current.Text, nil },
		Post: func(_ context.Context, _ *strictState, _ string) error { return nil },
	}}, graph.WithStateMerger(func(_ context.Context, _ *strictState, _ []*strictState) error { return nil })); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("finish", node, graph.WithPreHandler(func(_ context.Context, current *strictState) (string, error) { return current.Text, nil })); err != nil {
		t.Fatal(err)
	}
	_ = g.AddEdge(graph.Start, "fanout")
	_ = g.AddEdge("fanout", "finish")
	_ = g.AddEdge("finish", graph.End)
	_, err := g.Invoke(context.Background(), "value")
	if err == nil || !strings.Contains(err.Error(), "dynamic type") {
		t.Fatalf("Invoke error = %v", err)
	}
}

func TestRuntimeParallelRejectsCodecThatSharesStatePointers(t *testing.T) {
	shared := &runtimeParallelState{Values: []string{"shared"}}
	g := graph.NewGraph[string, string]("parallel-shared")
	if err := g.SetStateFactory(func(_ context.Context, input string) (*runtimeParallelState, error) {
		return &runtimeParallelState{Values: []string{input}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.SetStateCodec(sharedPointerCodec{current: shared}); err != nil {
		t.Fatal(err)
	}
	node := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) { return input, nil })
	branch := func(name string) graph.ParallelBranch {
		return graph.ParallelBranch{
			Name: name, Node: node,
			Pre:  func(_ context.Context, current *runtimeParallelState) (string, error) { return current.Values[0], nil },
			Post: func(_ context.Context, _ *runtimeParallelState, _ string) error { return nil },
		}
	}
	if err := g.AddParallelNode("fanout", []graph.ParallelBranch{branch("one"), branch("two")}, graph.WithStateMerger(func(_ context.Context, _ *runtimeParallelState, _ []*runtimeParallelState) error { return nil })); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("finish", node, graph.WithPreHandler(func(_ context.Context, current *runtimeParallelState) (string, error) { return current.Values[0], nil })); err != nil {
		t.Fatal(err)
	}
	_ = g.AddEdge(graph.Start, "fanout")
	_ = g.AddEdge("fanout", "finish")
	_ = g.AddEdge("finish", graph.End)
	_, err := g.Invoke(context.Background(), "value")
	if err == nil || !strings.Contains(err.Error(), "independent State") {
		t.Fatalf("Invoke error = %v", err)
	}
}

func TestRuntimeParallelNestedGraphParticipatesInCycleDetection(t *testing.T) {
	build := func(name string) *graph.Graph[string, string] {
		g := graph.NewGraph[string, string](name)
		if err := g.SetStateFactory(func(_ context.Context, input string) (*runtimeParallelState, error) {
			return &runtimeParallelState{Values: []string{input}}, nil
		}); err != nil {
			t.Fatal(err)
		}
		return g
	}
	first := build("first-parallel")
	second := build("second-parallel")
	addChild := func(parent, child *graph.Graph[string, string]) {
		branch := graph.ParallelBranch{
			Name: "child", Node: child,
			Pre:  func(_ context.Context, current *runtimeParallelState) (string, error) { return current.Values[0], nil },
			Post: func(_ context.Context, _ *runtimeParallelState, _ string) error { return nil },
		}
		if err := parent.AddParallelNode("fanout", []graph.ParallelBranch{branch}, graph.WithStateMerger(func(_ context.Context, _ *runtimeParallelState, _ []*runtimeParallelState) error { return nil })); err != nil {
			t.Fatal(err)
		}
		finish := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) { return input, nil })
		if err := parent.AddNode("finish", finish, graph.WithPreHandler(func(_ context.Context, current *runtimeParallelState) (string, error) { return current.Values[0], nil })); err != nil {
			t.Fatal(err)
		}
		_ = parent.AddEdge(graph.Start, "fanout")
		_ = parent.AddEdge("fanout", "finish")
		_ = parent.AddEdge("finish", graph.End)
	}
	addChild(first, second)
	addChild(second, first)
	if err := first.Compile(); err == nil || !strings.Contains(err.Error(), "graph.cycle") {
		t.Fatalf("Compile error = %v", err)
	}
}
