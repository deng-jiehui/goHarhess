package graph

import (
	"context"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/runnable"
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
	compiled, err := g.Compile()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := compiled.Invoke(context.Background(), "value"); err != nil {
		t.Fatal(err)
	}
}

type boundConfig struct{ Value string }

// SetNodeOptions 只登记不透明的选项值，不在建图时执行任何设置函数；
// 绑定选项在运行入口分发给节点，由节点解析。
func TestSetNodeOptionsDoesNotExecuteOptionCode(t *testing.T) {
	g := NewGraph[string, string]("lazy-node-options")
	applied := 0
	option := runnable.WrapOption(func(c *boundConfig) { applied++; c.Value = "bound" })
	node := runnable.Func[string, string](func(_ context.Context, input string, options ...runnable.Option) (string, error) {
		resolved, remaining, err := runnable.ResolveComponentOptions(boundConfig{}, options...)
		if err != nil {
			return "", err
		}
		if err := runnable.CheckNoRemainingOptions(remaining...); err != nil {
			return "", err
		}
		return input + resolved.Value, nil
	})
	if err := g.AddNode("node", node); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- g.SetNodeOptions("node", option)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("SetNodeOptions blocked on the Graph lock")
	}
	if applied != 0 {
		t.Fatalf("option applied at registration: %d", applied)
	}
	_ = g.AddEdge(Start, "node")
	_ = g.AddEdge("node", End)
	compiled, err := g.Compile()
	if err != nil {
		t.Fatal(err)
	}
	out, err := compiled.Invoke(context.Background(), "in-")
	if err != nil || out != "in-bound" {
		t.Fatalf("Invoke() = %q, %v", out, err)
	}
	if applied != 1 {
		t.Fatalf("option applied %d times, want 1", applied)
	}
}

// 扇出成员在各自上下文中看到的是成员局部 State，不是主干入口对象。
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
	member := func(name string) {
		if err := g.AddNode(name, node, WithPreHandler(func(ctx context.Context, current *runtimeScopeState) (string, error) {
			scoped, ok := runtimeStateFromContext(ctx)
			if !ok || scoped != current || current == base {
				t.Fatalf("member %s Context State=%#v current=%p base=%p", name, scoped, current, base)
			}
			return current.Value, nil
		}), WithPostHandler(func(_ context.Context, _ *runtimeScopeState, _ string) error { return nil })); err != nil {
			t.Fatal(err)
		}
	}
	member("one")
	member("two")
	if err := g.AddNode("q", node, WithPreHandler(func(_ context.Context, current *runtimeScopeState) (string, error) { return current.Value, nil })); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("finish", node, WithPreHandler(func(_ context.Context, current *runtimeScopeState) (string, error) { return current.Value, nil })); err != nil {
		t.Fatal(err)
	}
	_ = g.AddEdge(Start, "q")
	_ = g.AddFork("q", "one", "two")
	_ = g.AddJoin("q", "finish")
	_ = g.AddEdge("one", "finish")
	_ = g.AddEdge("two", "finish")
	_ = g.AddEdge("finish", End)
	_ = SetForkMerger(g, "q", func(_ context.Context, _ *runtimeScopeState, _ []StepResult[*runtimeScopeState]) error { return nil })
	compiled, err := g.Compile()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := compiled.Invoke(context.Background(), "value"); err != nil {
		t.Fatal(err)
	}
}
