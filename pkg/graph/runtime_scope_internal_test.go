package graph

import (
	"context"
	"testing"
	"time"

	"goharness/pkg/runnable"
)

type runtimeScopeState struct{ Value string }

func TestRuntimeStateTravelsThroughPrivateContextScope(t *testing.T) {
	g := NewGraph[string, string]("context-scope")
	var created *runtimeScopeState
	if err := g.SetStateFactory(func(_ context.Context, input string) (*runtimeScopeState, error) {
		created = &runtimeScopeState{Value: input}
		return created, nil
	}); err != nil {
		t.Fatal(err)
	}
	node := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		current, ok := runtimeStateFromContext(ctx)
		if !ok || current != created {
			t.Fatalf("Context State = %#v, ok=%t, want %p", current, ok, created)
		}
		return input, nil
	})
	if err := g.AddNode("node", node, WithPreHandler(func(_ context.Context, current *runtimeScopeState) (string, error) {
		return current.Value, nil
	})); err != nil {
		t.Fatal(err)
	}
	_ = g.AddEdge(Start, "node")
	_ = g.AddEdge("node", End)
	if _, err := g.Invoke(context.Background(), "value"); err != nil {
		t.Fatal(err)
	}
}

type reentrantScopeOption struct{ onScope func() }

func (reentrantScopeOption) Apply(*runnable.OptionSet) error { return nil }
func (o reentrantScopeOption) OptionScope() runnable.OptionScope {
	o.onScope()
	return runnable.OptionScopeNode
}

func TestSetNodeOptionsResolvesUserScopeOutsideGraphLock(t *testing.T) {
	g := NewGraph[string, string]("reentrant-options")
	node := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) { return input, nil })
	if err := g.AddNode("node", node); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- g.SetNodeOptions("node", reentrantScopeOption{onScope: func() { _ = g.Snapshot() }})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("SetNodeOptions called OptionScope while holding the Graph lock")
	}
}

func TestParallelBranchesCarryTheirOwnedStateInContext(t *testing.T) {
	g := NewGraph[string, string]("parallel-context-scope")
	var base *runtimeScopeState
	if err := g.SetStateFactory(func(_ context.Context, input string) (*runtimeScopeState, error) {
		base = &runtimeScopeState{Value: input}
		return base, nil
	}); err != nil {
		t.Fatal(err)
	}
	node := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) { return input, nil })
	branch := func(name string) ParallelBranch {
		return ParallelBranch{
			Name: name,
			Node: node,
			Pre: func(ctx context.Context, current *runtimeScopeState) (string, error) {
				scoped, ok := runtimeStateFromContext(ctx)
				if !ok || scoped != current || current == base {
					t.Fatalf("branch %s Context State=%#v current=%p base=%p", name, scoped, current, base)
				}
				return current.Value, nil
			},
			Post: func(_ context.Context, _ *runtimeScopeState, _ string) error { return nil },
		}
	}
	if err := g.AddParallelNode("fanout", []ParallelBranch{branch("one"), branch("two")}, WithStateMerger(func(_ context.Context, _ *runtimeScopeState, _ []*runtimeScopeState) error { return nil })); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("finish", node, WithPreHandler(func(_ context.Context, current *runtimeScopeState) (string, error) { return current.Value, nil })); err != nil {
		t.Fatal(err)
	}
	_ = g.AddEdge(Start, "fanout")
	_ = g.AddEdge("fanout", "finish")
	_ = g.AddEdge("finish", End)
	if _, err := g.Invoke(context.Background(), "value"); err != nil {
		t.Fatal(err)
	}
}
