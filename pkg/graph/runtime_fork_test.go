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

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
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

func TestRuntimeForkSnapshotsBranchesAndMergesInDeclarationOrder(t *testing.T) {
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
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	var mu sync.Mutex
	seen := map[*runtimeParallelState]bool{}
	member := func(name string) {
		node := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
			return input + ":" + name, nil
		})
		must(g.AddNode(name, node,
			graph.WithPreHandler(func(_ context.Context, current *runtimeParallelState) (string, error) {
				mu.Lock()
				defer mu.Unlock()
				if seen[current] {
					t.Fatalf("fork member reused State pointer %p", current)
				}
				seen[current] = true
				return current.Values[0], nil
			}),
			graph.WithPostHandler(func(_ context.Context, current *runtimeParallelState, output string) error {
				current.Values = append(current.Values, output)
				return nil
			})))
	}
	member("first")
	member("second")
	// fork 源是普通节点：扇出按分支声明产生成员任务。
	must(g.AddNode("q", runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
		return input, nil
	}), graph.WithPreHandler(func(_ context.Context, current *runtimeParallelState) (string, error) {
		return current.Values[0], nil
	})))
	finish := runnable.Func[[]string, []string](func(_ context.Context, input []string, _ ...runnable.Option) ([]string, error) { return input, nil })
	must(g.AddNode("finish", finish, graph.WithPreHandler(func(_ context.Context, current *runtimeParallelState) ([]string, error) {
		return append([]string(nil), current.Values...), nil
	})))
	must(g.AddEdge(graph.Start, "q"))
	must(g.AddFork("q", "first", "second"))
	must(g.AddJoin("q", "finish"))
	must(g.AddEdge("first", "finish"))
	must(g.AddEdge("second", "finish"))
	must(g.AddEdge("finish", graph.End))
	must(graph.SetForkMerger(g, "q",
		func(_ context.Context, base *runtimeParallelState, rs []graph.StepResult[*runtimeParallelState]) error {
			for _, r := range rs {
				base.Values = append(base.Values, r.State.Values[len(r.State.Values)-1])
			}
			return nil
		},
	))
	output, err := mustCompile(t, g).Invoke(context.Background(), "base")
	if err != nil {
		t.Fatal(err)
	}
	if len(output) != 3 || output[0] != "base" || output[1] != "base:first" || output[2] != "base:second" {
		t.Fatalf("fork output = %#v", output)
	}
	if len(seen) != 2 {
		t.Fatalf("member State pointers = %d", len(seen))
	}
}

func TestRuntimeForkFailureSkipsMerger(t *testing.T) {
	g := graph.NewGraph[string, string]("parallel-error")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(g.SetStateFactory(func(_ context.Context, input string) (*runtimeParallelState, error) {
		return &runtimeParallelState{Values: []string{input}}, nil
	}))
	boom := errors.New("branch failed")
	failed := runnable.Func[string, string](func(_ context.Context, _ string, _ ...runnable.Option) (string, error) { return "", boom })
	waiting := runnable.Func[string, string](func(ctx context.Context, _ string, _ ...runnable.Option) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})
	member := func(name string, node runnable.Runnable[string, string]) {
		must(g.AddNode(name, node,
			graph.WithPreHandler(func(_ context.Context, current *runtimeParallelState) (string, error) {
				return current.Values[0], nil
			}),
			graph.WithPostHandler(func(_ context.Context, _ *runtimeParallelState, _ string) error { return nil })))
	}
	member("failed", failed)
	member("waiting", waiting)
	must(g.AddNode("q", runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
		return input, nil
	}), graph.WithPreHandler(func(_ context.Context, current *runtimeParallelState) (string, error) {
		return current.Values[0], nil
	})))
	finish := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) { return input, nil })
	must(g.AddNode("finish", finish, graph.WithPreHandler(func(_ context.Context, current *runtimeParallelState) (string, error) {
		return current.Values[0], nil
	})))
	must(g.AddEdge(graph.Start, "q"))
	must(g.AddFork("q", "failed", "waiting"))
	must(g.AddJoin("q", "finish"))
	must(g.AddEdge("failed", "finish"))
	must(g.AddEdge("waiting", "finish"))
	must(g.AddEdge("finish", graph.End))
	var merged atomic.Bool
	must(graph.SetForkMerger(g, "q",
		func(_ context.Context, _ *runtimeParallelState, _ []graph.StepResult[*runtimeParallelState]) error {
			merged.Store(true)
			return nil
		},
	))
	_, err := mustCompile(t, g).Invoke(context.Background(), "base")
	if !errors.Is(err, boom) || merged.Load() {
		t.Fatalf("Invoke error=%v merged=%t", err, merged.Load())
	}
}

func TestRuntimeForkRejectsDefaultCodecDynamicTypeLoss(t *testing.T) {
	g := graph.NewGraph[string, string]("parallel-dynamic")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(g.SetStateFactory(func(_ context.Context, input string) (*strictState, error) {
		return &strictState{Text: input, Payload: int(1)}, nil
	}))
	node := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) { return input, nil })
	pre := func(_ context.Context, current *strictState) (string, error) { return current.Text, nil }
	post := func(_ context.Context, _ *strictState, _ string) error { return nil }
	must(g.AddNode("q", node, graph.WithPreHandler(pre), graph.WithPostHandler(post)))
	must(g.AddNode("first", node, graph.WithPreHandler(pre), graph.WithPostHandler(post)))
	must(g.AddNode("second", node, graph.WithPreHandler(pre), graph.WithPostHandler(post)))
	must(g.AddNode("finish", node, graph.WithPreHandler(pre), graph.WithPostHandler(post)))
	must(g.AddEdge(graph.Start, "q"))
	must(g.AddFork("q", "first", "second"))
	must(g.AddJoin("q", "finish"))
	must(g.AddEdge("first", "finish"))
	must(g.AddEdge("second", "finish"))
	must(g.AddEdge("finish", graph.End))
	must(graph.SetForkMerger(g, "q", func(_ context.Context, _ *strictState, _ []graph.StepResult[*strictState]) error { return nil }))
	_, err := mustCompile(t, g).Invoke(context.Background(), "value")
	if err == nil || !strings.Contains(err.Error(), "dynamic type") {
		t.Fatalf("Invoke error = %v", err)
	}
}

func TestRuntimeForkRejectsCodecThatSharesStatePointers(t *testing.T) {
	shared := &runtimeParallelState{Values: []string{"shared"}}
	g := graph.NewGraph[string, string]("parallel-shared")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(g.SetStateFactory(func(_ context.Context, input string) (*runtimeParallelState, error) {
		return &runtimeParallelState{Values: []string{input}}, nil
	}))
	must(g.SetStateCodec(sharedPointerCodec{current: shared}))
	node := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) { return input, nil })
	pre := func(_ context.Context, current *runtimeParallelState) (string, error) { return current.Values[0], nil }
	post := func(_ context.Context, _ *runtimeParallelState, _ string) error { return nil }
	must(g.AddNode("q", node, graph.WithPreHandler(pre), graph.WithPostHandler(post)))
	must(g.AddNode("first", node, graph.WithPreHandler(pre), graph.WithPostHandler(post)))
	must(g.AddNode("second", node, graph.WithPreHandler(pre), graph.WithPostHandler(post)))
	must(g.AddNode("finish", node, graph.WithPreHandler(pre), graph.WithPostHandler(post)))
	must(g.AddEdge(graph.Start, "q"))
	must(g.AddFork("q", "first", "second"))
	must(g.AddJoin("q", "finish"))
	must(g.AddEdge("first", "finish"))
	must(g.AddEdge("second", "finish"))
	must(g.AddEdge("finish", graph.End))
	must(graph.SetForkMerger(g, "q", func(_ context.Context, _ *runtimeParallelState, _ []graph.StepResult[*runtimeParallelState]) error {
		return nil
	}))
	_, err := mustCompile(t, g).Invoke(context.Background(), "value")
	if err == nil || !strings.Contains(err.Error(), "independent State") {
		t.Fatalf("Invoke error = %v", err)
	}
}
