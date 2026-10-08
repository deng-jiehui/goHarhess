package graph_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
)

type typedAPIState struct {
	Text    string
	Values  []string
	Count   int
	History []string
}

func mustCompileGraph[I, O any](t *testing.T, g *graph.Graph[I, O]) *graph.CompiledGraph[I, O] {
	t.Helper()
	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return compiled
}

// 泛型入口与 any 入口在同一图上等价。
func TestTypedHandlersEquivalentChain(t *testing.T) {
	build := func(typed bool) *graph.Graph[string, string] {
		g := graph.NewGraph[string, string]("typed-chain")
		factory := func(_ context.Context, input string) (*typedAPIState, error) {
			return &typedAPIState{Text: input}, nil
		}
		if typed {
			if err := graph.SetStateFactory(g, factory); err != nil {
				t.Fatal(err)
			}
		} else if err := g.SetStateFactory(factory); err != nil {
			t.Fatal(err)
		}
		upper := runnable.InvokableLambda(func(_ context.Context, input string) (string, error) {
			return strings.ToUpper(input), nil
		})
		render := runnable.InvokableLambda(func(_ context.Context, input int) (string, error) {
			return fmt.Sprintf("%d", input), nil
		})
		pre := func(_ context.Context, s *typedAPIState) (string, error) { return s.Text, nil }
		post := func(_ context.Context, s *typedAPIState, output string) error {
			s.Values = append(s.Values, output)
			return nil
		}
		preRender := func(_ context.Context, s *typedAPIState) (int, error) { return len(s.Values[0]), nil }
		postRender := func(_ context.Context, s *typedAPIState, output string) error {
			s.History = append(s.History, output)
			return nil
		}
		if err := g.AddNode("upper", upper, graph.WithPreHandler(pre), graph.WithPostHandler(post)); err != nil {
			t.Fatal(err)
		}
		if err := g.AddNode("render", render, graph.WithPreHandler(preRender), graph.WithPostHandler(postRender)); err != nil {
			t.Fatal(err)
		}
		if err := g.AddEdge(graph.Start, "upper"); err != nil {
			t.Fatal(err)
		}
		if err := g.AddEdge("upper", "render"); err != nil {
			t.Fatal(err)
		}
		if err := g.AddEdge("render", graph.End); err != nil {
			t.Fatal(err)
		}
		return g
	}
	want, err := mustCompileGraph(t, build(false)).Invoke(context.Background(), "abc")
	if err != nil {
		t.Fatal(err)
	}
	got, err := mustCompileGraph(t, build(true)).Invoke(context.Background(), "abc")
	if err != nil {
		t.Fatal(err)
	}
	if want != "3" || got != want {
		t.Fatalf("want=%q got=%q", want, got)
	}
}

// 泛型条件分支：谓词既可声明节点输出类型也可声明 State 类型，行为与 any 入口一致。
func TestTypedBranchPredicates(t *testing.T) {
	newGraph := func() *graph.Graph[int, string] {
		g := graph.NewGraph[int, string]("typed-branch")
		if err := graph.SetStateFactory(g, func(_ context.Context, input int) (*typedAPIState, error) {
			return &typedAPIState{Count: input}, nil
		}); err != nil {
			t.Fatal(err)
		}
		measure := runnable.InvokableLambda(func(_ context.Context, input int) (int, error) { return input, nil })
		long := runnable.InvokableLambda(func(_ context.Context, input int) (string, error) { return "long", nil })
		short := runnable.InvokableLambda(func(_ context.Context, input int) (string, error) { return "short", nil })
		finish := runnable.InvokableLambda(func(_ context.Context, input string) (string, error) { return input, nil })
		pre := func(_ context.Context, s *typedAPIState) (int, error) { return s.Count, nil }
		preFinish := func(_ context.Context, s *typedAPIState) (string, error) { return s.Values[0], nil }
		post := func(_ context.Context, s *typedAPIState, output string) error {
			s.Values = append(s.Values, output)
			return nil
		}
		for _, setup := range []func() error{
			func() error { return g.AddNode("measure", measure, graph.WithPreHandler(pre)) },
			func() error { return g.AddNode("long", long, graph.WithPreHandler(pre), graph.WithPostHandler(post)) },
			func() error { return g.AddNode("short", short, graph.WithPreHandler(pre), graph.WithPostHandler(post)) },
			func() error { return g.AddNode("finish", finish, graph.WithPreHandler(preFinish)) },
			func() error { return g.AddEdge(graph.Start, "measure") },
			func() error { return g.AddEdge("long", "finish") },
			func() error { return g.AddEdge("short", "finish") },
			func() error { return g.AddEdge("finish", graph.End) },
		} {
			if err := setup(); err != nil {
				t.Fatal(err)
			}
		}
		return g
	}
	// 谓词按上游节点输出类型声明。
	g := newGraph()
	if err := g.AddEdge("measure", "short"); err != nil {
		t.Fatal(err)
	}
	if err := graph.AddBranch(g, "measure", "long", "gt_2", func(_ context.Context, output int) (bool, error) {
		return output > 2, nil
	}); err != nil {
		t.Fatal(err)
	}
	out, err := mustCompileGraph(t, g).Invoke(context.Background(), 5)
	if err != nil || out != "long" {
		t.Fatalf("output=%q err=%v", out, err)
	}
	// 谓词按 State 类型声明。
	g2 := newGraph()
	if err := g2.AddEdge("measure", "short"); err != nil {
		t.Fatal(err)
	}
	if err := graph.AddBranch(g2, "measure", "long", "state_gt_2", func(_ context.Context, s *typedAPIState) (bool, error) {
		return s.Count > 2, nil
	}); err != nil {
		t.Fatal(err)
	}
	out, err = mustCompileGraph(t, g2).Invoke(context.Background(), 1)
	if err != nil || out != "short" {
		t.Fatalf("output=%q err=%v", out, err)
	}
}

// 泛型并行分支与 State merger 等价于 ParallelBranch 字面量写法。
func TestTypedParallelBranchAndMerger(t *testing.T) {
	build := func(typed bool) *graph.Graph[string, []string] {
		g := graph.NewGraph[string, []string]("typed-parallel")
		if err := graph.SetStateFactory(g, func(_ context.Context, input string) (*typedAPIState, error) {
			return &typedAPIState{Values: []string{input}}, nil
		}); err != nil {
			t.Fatal(err)
		}
		pre := func(_ context.Context, s *typedAPIState) (string, error) { return s.Values[0], nil }
		post := func(_ context.Context, s *typedAPIState, output string) error {
			s.Values = append(s.Values, output)
			return nil
		}
		var branches []graph.ParallelBranch
		for _, name := range []string{"first", "second"} {
			node := runnable.InvokableLambda(func(_ context.Context, input string) (string, error) {
				return input + ":" + name, nil
			})
			if typed {
				branches = append(branches, graph.NewParallelBranch(name, node, pre, post))
			} else {
				branches = append(branches, graph.ParallelBranch{Name: name, Node: node, Pre: pre, Post: post})
			}
		}
		merger := func(_ context.Context, base *typedAPIState, states []*typedAPIState) error {
			for _, s := range states {
				base.Values = append(base.Values, s.Values[len(s.Values)-1])
			}
			return nil
		}
		var err error
		if typed {
			err = g.AddParallelNode("fanout", branches, graph.WithStateMerger(merger))
		} else {
			err = g.AddParallelNode("fanout", branches, graph.WithStateMerger(merger))
		}
		if err != nil {
			t.Fatal(err)
		}
		finish := runnable.InvokableLambda(func(_ context.Context, input []string) ([]string, error) { return input, nil })
		if err := g.AddNode("finish", finish, graph.WithPreHandler(func(_ context.Context, s *typedAPIState) ([]string, error) {
			return append([]string(nil), s.Values...), nil
		})); err != nil {
			t.Fatal(err)
		}
		for _, edge := range [][2]string{{graph.Start, "fanout"}, {"fanout", "finish"}, {"finish", graph.End}} {
			if err := g.AddEdge(edge[0], edge[1]); err != nil {
				t.Fatal(err)
			}
		}
		return g
	}
	want, err := mustCompileGraph(t, build(false)).Invoke(context.Background(), "base")
	if err != nil {
		t.Fatal(err)
	}
	got, err := mustCompileGraph(t, build(true)).Invoke(context.Background(), "base")
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(want) != fmt.Sprint(got) || len(got) != 3 {
		t.Fatalf("want=%v got=%v", want, got)
	}
}

// 泛型 step merger 与 fork/join 配合，行为与方法版一致。
func TestTypedStepMerger(t *testing.T) {
	build := func(typed bool) *graph.Graph[string, string] {
		g := graph.NewGraph[string, string]("typed-steps")
		if err := graph.SetStateFactory(g, func(context.Context, string) (*typedAPIState, error) {
			return &typedAPIState{}, nil
		}); err != nil {
			t.Fatal(err)
		}
		merge := func(_ context.Context, base *typedAPIState, results []graph.StepResult[*typedAPIState]) error {
			n := len(base.Values)
			for _, r := range results {
				base.Values = append(base.Values, r.State.Values[n:]...)
			}
			return nil
		}
		var err error
		if typed {
			err = graph.SetStepMerger(g, merge)
		} else {
			err = g.SetStepMerger(merge)
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"fork", "a", "b", "join"} {
			n := name
			node := runnable.InvokableLambda(func(_ context.Context, input string) (string, error) { return input, nil })
			if err := g.AddNode(n, node,
				graph.WithPreHandler(func(_ context.Context, s *typedAPIState) (string, error) { return n, nil }),
				graph.WithPostHandler(func(_ context.Context, s *typedAPIState, out string) error {
					s.Values = append(s.Values, out)
					return nil
				}),
			); err != nil {
				t.Fatal(err)
			}
		}
		// finish 读取合并后的 State，使分支写回的结果可从图输出观察。
		finish := runnable.InvokableLambda(func(_ context.Context, input string) (string, error) { return input, nil })
		if err := g.AddNode("finish", finish, graph.WithPreHandler(func(_ context.Context, s *typedAPIState) (string, error) {
			return strings.Join(s.Values, ","), nil
		})); err != nil {
			t.Fatal(err)
		}
		for _, setup := range []func() error{
			func() error { return g.AddEdge(graph.Start, "fork") },
			func() error { return g.AddFork("fork", "a", "b") },
			func() error { return g.AddJoin("fork", "join") },
			func() error { return g.AddEdge("a", "join") },
			func() error { return g.AddEdge("b", "join") },
			func() error { return g.AddEdge("join", "finish") },
			func() error { return g.AddEdge("finish", graph.End) },
		} {
			if err := setup(); err != nil {
				t.Fatal(err)
			}
		}
		return g
	}
	for _, typed := range []bool{false, true} {
		out, err := mustCompileGraph(t, build(typed)).Invoke(context.Background(), "go")
		if err != nil {
			t.Fatalf("typed=%v err=%v", typed, err)
		}
		parts := strings.Split(out, ",")
		seen := map[string]bool{}
		for _, part := range parts {
			seen[part] = true
		}
		if !seen["a"] || !seen["b"] || !seen["join"] {
			t.Fatalf("typed=%v merged values missing branches: %q", typed, out)
		}
	}
}

// 泛型入口与图的 State 不一致时，仍由 Compile 的既有校验报错。
func TestTypedHandlerStateMismatchFailsAtCompile(t *testing.T) {
	type otherState struct{ X int }
	g := graph.NewGraph[string, string]("mismatch")
	if err := graph.SetStateFactory(g, func(_ context.Context, input string) (*typedAPIState, error) {
		return &typedAPIState{Text: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	node := runnable.InvokableLambda(func(_ context.Context, input string) (string, error) { return input, nil })
	if err := g.AddNode("work", node, graph.WithPreHandler(func(_ context.Context, s *otherState) (string, error) {
		return "", nil
	})); err != nil {
		t.Fatal(err)
	}
	_ = g.AddEdge(graph.Start, "work")
	_ = g.AddEdge("work", graph.End)
	_, err := g.Compile()
	if err == nil || !strings.Contains(err.Error(), "node.pre.type_mismatch") {
		t.Fatalf("expected node.pre.type_mismatch, got %v", err)
	}
}

// 泛型入口的 panic 依旧被转换为带名字的节点错误。
func TestTypedHandlerPanicStillWrapped(t *testing.T) {
	g := graph.NewGraph[string, string]("panic")
	if err := graph.SetStateFactory(g, func(_ context.Context, input string) (*typedAPIState, error) {
		return &typedAPIState{Text: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	node := runnable.InvokableLambda(func(_ context.Context, input string) (string, error) { return input, nil })
	if err := g.AddNode("work", node, graph.WithPreHandler(func(_ context.Context, s *typedAPIState) (string, error) {
		panic("boom")
	})); err != nil {
		t.Fatal(err)
	}
	_ = g.AddEdge(graph.Start, "work")
	_ = g.AddEdge("work", graph.End)
	_, err := mustCompileGraph(t, g).Invoke(context.Background(), "x")
	if err == nil || !strings.Contains(err.Error(), "pre handler panicked") {
		t.Fatalf("expected wrapped panic, got %v", err)
	}
}

// 泛型 State factory 拒绝返回非结构体指针的工厂函数。
func TestTypedStateFactoryRejectsNonStruct(t *testing.T) {
	g := graph.NewGraph[string, string]("factory")
	err := graph.SetStateFactory(g, func(_ context.Context, input string) (*int, error) {
		return nil, nil
	})
	if err == nil || !strings.Contains(err.Error(), "graph.state_factory.pointer") {
		t.Fatalf("expected pointer error, got %v", err)
	}
	// nil 工厂（经由变量传递，绕过编译期推断限制）依旧返回错误。
	var nilFactory func(context.Context, string) (*typedAPIState, error)
	if err = graph.SetStateFactory(g, nilFactory); err == nil {
		t.Fatal("expected error for nil factory")
	}
}

// 泛型入口在中断、落盘与恢复链路上的行为与 any 入口一致。
func TestTypedHandlersSurviveInterruptAndResume(t *testing.T) {
	build := func() *graph.Graph[string, string] {
		g := graph.NewGraph[string, string]("typed-resume")
		if err := graph.SetStateFactory(g, func(_ context.Context, input string) (*typedAPIState, error) {
			return &typedAPIState{Text: input}, nil
		}); err != nil {
			t.Fatal(err)
		}
		form := runnable.Func[string, string](func(ctx context.Context, text string, _ ...runnable.Option) (string, error) {
			name, err := graph.Interrupt(ctx, map[string]any{"field": "name"})
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%s/%v", text, name), nil
		})
		if err := g.AddNode("form", form,
			graph.WithPreHandler(func(_ context.Context, s *typedAPIState) (string, error) { return s.Text, nil }),
			graph.WithPostHandler(func(_ context.Context, s *typedAPIState, out string) error { s.Text = out; return nil }),
		); err != nil {
			t.Fatal(err)
		}
		_ = g.AddEdge(graph.Start, "form")
		_ = g.AddEdge("form", graph.End)
		return g
	}
	store := &byteCheckpointStore{}
	checkpoint := graph.WithCheckpoint(store, "typed")
	compiled := mustCompileGraph(t, build())
	_, err := compiled.Invoke(context.Background(), "hello", checkpoint)
	id := inputID(t, err)
	// 跨编译实例恢复，验证类型化 Pre/Post 在恢复路径上照常工作。
	out, err := mustCompileGraph(t, build()).Resume(context.Background(), checkpoint, graph.WithResume(id, "Ada"))
	if err != nil || out != "hello/Ada" {
		t.Fatalf("output=%q err=%v", out, err)
	}
}

// nil 接口值在类型擦除边界上与反射路径行为一致：按目标类型零值传递。
func TestTypedHandlersAcceptNilValues(t *testing.T) {
	// 节点输出为 nil 的 any 时，PostHandler 收到 nil 而非 panic。
	var seen any = "unset"
	g := graph.NewGraph[string, string]("nil-post")
	if err := graph.SetStateFactory(g, func(_ context.Context, input string) (*typedAPIState, error) {
		return &typedAPIState{Text: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	nilNode := runnable.InvokableLambda(func(_ context.Context, input string) (any, error) { return nil, nil })
	if err := g.AddNode("work", nilNode,
		graph.WithPreHandler(func(_ context.Context, s *typedAPIState) (string, error) { return s.Text, nil }),
		graph.WithPostHandler(func(_ context.Context, s *typedAPIState, out any) error {
			seen = out
			return nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	finish := runnable.InvokableLambda(func(_ context.Context, input string) (string, error) { return input, nil })
	if err := g.AddNode("finish", finish, graph.WithPreHandler(func(_ context.Context, s *typedAPIState) (string, error) {
		return s.Text, nil
	})); err != nil {
		t.Fatal(err)
	}
	_ = g.AddEdge(graph.Start, "work")
	_ = g.AddEdge("work", "finish")
	_ = g.AddEdge("finish", graph.End)
	out, err := mustCompileGraph(t, g).Invoke(context.Background(), "x")
	if err != nil || out != "x" || seen != nil {
		t.Fatalf("output=%q seen=%v err=%v", out, seen, err)
	}
}

// 图输入为 nil 接口值时，泛型谓词收到零值而非 panic。
func TestTypedBranchPredicateAcceptsNilInput(t *testing.T) {
	g := graph.NewGraph[any, string]("nil-branch")
	left := runnable.InvokableLambda(func(_ context.Context, input any) (string, error) { return "left", nil })
	right := runnable.InvokableLambda(func(_ context.Context, input any) (string, error) { return "right", nil })
	if err := g.AddNode("left", left); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("right", right); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "left"); err != nil {
		t.Fatal(err)
	}
	var sawNil bool
	if err := graph.AddBranch(g, graph.Start, "right", "non_nil", func(_ context.Context, input any) (bool, error) {
		sawNil = input == nil
		return input != nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	_ = g.AddEdge("left", graph.End)
	_ = g.AddEdge("right", graph.End)
	out, err := mustCompileGraph(t, g).Invoke(context.Background(), nil)
	if err != nil || out != "left" || !sawNil {
		t.Fatalf("output=%q sawNil=%v err=%v", out, sawNil, err)
	}
}
