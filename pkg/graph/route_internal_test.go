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

// 组外路由以并行组名为目标表示进入组入口（fan-out），不是汇合——编译期
// 不得把它误判为「组外路由进入汇合」。运行时入口节点仍按声明扇出全部
// 成员，组内合并器在汇合处执行一次。
func TestOutsideRouteEntersParallelGroup(t *testing.T) {
	type stateT struct {
		Log []string `json:"log"`
	}
	g := NewGraph[string, string]("route-into-fan")
	if err := g.SetStateFactory(func(_ context.Context, in string) (*stateT, error) {
		return &stateT{Log: []string{in}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	prep := StateNode(func(_ context.Context, s *stateT) error {
		s.Log = append(s.Log, "prep")
		return nil
	})
	if err := g.AddNode("prep", prep); err != nil {
		t.Fatal(err)
	}
	var merges int
	member := func(tag string) ParallelBranch {
		return NewParallelBranch[stateT, *stateT, *stateT](tag, StateNode(func(_ context.Context, s *stateT) error {
			s.Log = append(s.Log, tag)
			return nil
		}), nil, nil)
	}
	if err := g.AddParallelNode("fan", []ParallelBranch{member("a"), member("b")},
		WithStateMerger(func(_ context.Context, base *stateT, branches []*stateT) error {
			merges++
			for _, branch := range branches {
				base.Log = append(base.Log, branch.Log[len(branch.Log)-1])
			}
			return nil
		})); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("alt", StateNode(func(_ context.Context, s *stateT) error {
		s.Log = append(s.Log, "alt")
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("sink", runnable.Func[*stateT, string](func(_ context.Context, s *stateT, _ ...runnable.Option) (string, error) {
		return strings.Join(s.Log, "|"), nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(Start, "prep"); err != nil {
		t.Fatal(err)
	}
	// prep 路由到组名 fan（进入扇出）或 alt；路由目标是组名而非汇合。
	route := newRuntimeRoute(func(_ context.Context, s *stateT) (string, error) {
		if s.Log[0] == "fanout" {
			return "fan", nil
		}
		return "alt", nil
	})
	route.targets["fan"] = true
	route.targets["alt"] = true
	if err := g.addRuntimeRoute("prep", route); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("fan", "sink"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("alt", "sink"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("sink", End); err != nil {
		t.Fatal(err)
	}
	app, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := app.Invoke(context.Background(), "fanout")
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	// 路由进入组入口：两个成员都执行，合并一次，组出口到 sink。
	if out != "fanout|prep|a|b" {
		t.Fatalf("out=%q", out)
	}
	if merges != 1 {
		t.Fatalf("merger invoked %d times, want 1", merges)
	}
	out, err = app.Invoke(context.Background(), "other")
	if err != nil {
		t.Fatalf("invoke alt: %v", err)
	}
	if out != "other|prep|alt" {
		t.Fatalf("alt out=%q", out)
	}
}

// 动态分派成员内部的单次求值路由与普通边一样参与「能否到达汇合」校验：
// worker 依 State 在补充查询与直接汇总之间选择，两条路径最终都到 join，
// Compile 必须放行且运行期按成员局部态选路。
func TestDispatchMemberRouteReachesJoin(t *testing.T) {
	type stateT struct {
		Last    string   `json:"last"`
		Results []string `json:"results"`
	}
	g := NewGraph[string, string]("dispatch-member-route")
	if err := g.SetStateFactory(func(_ context.Context, in string) (*stateT, error) {
		return &stateT{Last: in}, nil
	}); err != nil {
		t.Fatal(err)
	}
	planner := runnable.Func[string, string](func(_ context.Context, in string, _ ...runnable.Option) (string, error) {
		return in, nil
	})
	if err := g.AddNode("planner", planner,
		WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Last, nil }),
	); err != nil {
		t.Fatal(err)
	}
	if err := AddDispatch(g, "planner", []string{"worker"}, "join",
		func(_ context.Context, _ *stateT) ([]DispatchTask, error) {
			return []DispatchTask{Send("worker", "need-more"), Send("worker", "enough")}, nil
		}); err != nil {
		t.Fatal(err)
	}
	worker := runnable.Func[string, string](func(_ context.Context, in string, _ ...runnable.Option) (string, error) {
		return "w:" + in, nil
	})
	if err := g.AddNode("worker", worker,
		WithPostHandler(func(_ context.Context, s *stateT, out string) error { s.Last = out; return nil }),
	); err != nil {
		t.Fatal(err)
	}
	simple := func(tag string) runnable.Func[string, string] {
		return runnable.Func[string, string](func(_ context.Context, in string, _ ...runnable.Option) (string, error) {
			return tag + "(" + in + ")", nil
		})
	}
	for _, id := range []string{"enrich", "summarize"} {
		if err := g.AddNode(id, simple(id),
			WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Last, nil }),
			WithPostHandler(func(_ context.Context, s *stateT, out string) error {
				s.Results = append(s.Results, out)
				return nil
			}),
		); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.AddNode("join", runnable.Func[string, string](func(_ context.Context, in string, _ ...runnable.Option) (string, error) {
		return in, nil
	}),
		WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return strings.Join(s.Results, ","), nil }),
	); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(Start, "planner"); err != nil {
		t.Fatal(err)
	}
	// worker 的路由候选：补充查询走 enrich，直接汇总走 summarize；
	// 两条路径都能到达 join。
	route := newRuntimeRoute(func(_ context.Context, s *stateT) (string, error) {
		if s.Last == "w:need-more" {
			return "enrich", nil
		}
		return "summarize", nil
	})
	route.targets["enrich"] = true
	route.targets["summarize"] = true
	if err := g.addRuntimeRoute("worker", route); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("enrich", "join"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("summarize", "join"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("join", End); err != nil {
		t.Fatal(err)
	}
	if err := SetStepMerger(g, func(_ context.Context, base *stateT, results []StepResult[*stateT]) error {
		base.Results = nil
		for _, r := range results {
			base.Results = append(base.Results, r.State.Results...)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	app, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	out, err := app.Invoke(context.Background(), "x")
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if out != "enrich(w:need-more),summarize(w:enough)" {
		t.Fatalf("out=%q", out)
	}
}
