package graph

import (
	"context"
	"strings"
	"testing"

	"github.com/goharness/goharness/pkg/runnable"
)

// 路由漏检目标类型：源输出与目标输入不匹配时 Compile 必须报
// route.target.type_mismatch，而不是到运行期才暴露。
func TestRouteTargetTypeMismatch(t *testing.T) {
	g := NewGraph[string, string]("route-types")
	if err := g.AddNode("src", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
		return s, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("dst", runnable.Func[int, string](func(_ context.Context, _ int, _ ...runnable.Option) (string, error) {
		return "", nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(Start, "src"); err != nil {
		t.Fatal(err)
	}
	route := newRuntimeRoute(func(_ context.Context, s string) (string, error) { return "dst", nil })
	route.targets["dst"] = true
	if err := g.addRuntimeRoute("src", route); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("dst", End); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Compile(); err == nil || !strings.Contains(err.Error(), "route.target.type_mismatch") {
		t.Fatalf("expected route.target.type_mismatch, got %v", err)
	}
}

// 路由到 End 时按图输出类型校验。
func TestRouteEndTypeMismatch(t *testing.T) {
	g := NewGraph[string, int]("route-end")
	if err := g.AddNode("src", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
		return s, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(Start, "src"); err != nil {
		t.Fatal(err)
	}
	route := newRuntimeRoute(func(_ context.Context, s string) (string, error) { return End, nil })
	route.targets[End] = true
	if err := g.addRuntimeRoute("src", route); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Compile(); err == nil || !strings.Contains(err.Error(), "route.end.type_mismatch") {
		t.Fatalf("expected route.end.type_mismatch, got %v", err)
	}
}

// 并行组成员内部的路由：成员以组名为路由目标等价于到达本组汇合；
// 路由成员不再被补默认汇合边，也不会被判成"routed node 混用出边"。
func TestParallelMemberRouteResolvesToJoin(t *testing.T) {
	type stateT struct {
		Log []string `json:"log"`
	}
	g := NewGraph[string, string]("member-route")
	if err := g.SetStateFactory(func(_ context.Context, in string) (*stateT, error) {
		return &stateT{Log: []string{in}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	member := func(tag string) ParallelBranch {
		return NewParallelBranch[stateT, *stateT, *stateT](tag, StateNode(func(_ context.Context, s *stateT) error {
			s.Log = append(s.Log, tag)
			return nil
		}), nil, nil)
	}
	if err := g.AddParallelNode("fan", []ParallelBranch{member("a"), member("b")},
		WithStateMerger(func(_ context.Context, base *stateT, branches []*stateT) error {
			for _, branch := range branches {
				base.Log = append(base.Log, branch.Log[len(branch.Log)-1])
			}
			return nil
		})); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(Start, "fan"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("sink", runnable.Func[*stateT, string](func(_ context.Context, s *stateT, _ ...runnable.Option) (string, error) {
		return strings.Join(s.Log, "|"), nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("fan", "sink"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("sink", End); err != nil {
		t.Fatal(err)
	}
	// 成员 a 路由到接续节点 a2；a2 的路由以组名为目标——编译期重写为
	// 本组汇合，运行时由包装后的 invoke 翻译返回值。
	if err := g.AddNode("a2", StateNode(func(_ context.Context, s *stateT) error {
		s.Log = append(s.Log, "a2")
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	routeA := newRuntimeRoute(func(_ context.Context, s *stateT) (string, error) { return "a2", nil })
	routeA.targets["a2"] = true
	if err := g.addRuntimeRoute("fan/a", routeA); err != nil {
		t.Fatal(err)
	}
	routeA2 := newRuntimeRoute(func(_ context.Context, s *stateT) (string, error) { return "fan", nil })
	routeA2.targets["fan"] = true
	if err := g.addRuntimeRoute("a2", routeA2); err != nil {
		t.Fatal(err)
	}
	app, err := g.Compile()
	if err != nil {
		t.Fatal(err)
	}
	out, err := app.Invoke(context.Background(), "base")
	if err != nil {
		t.Fatal(err)
	}
	// a 的成员局部态走 a→a2 路由链，b 直连汇合；按声明序合并。
	if out != "base|a2|b" {
		t.Fatalf("out=%q", out)
	}
}
