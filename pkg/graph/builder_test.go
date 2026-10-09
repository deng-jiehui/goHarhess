package graph_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/goharness/goharness/pkg/graph"
)

type builderState struct {
	Query string
	Docs  []string
	Done  bool
}

// 三节点链式图：Node/Start/Edge/Compile 直通运行，输出最终 *S。
func TestBuilderChainInvokes(t *testing.T) {
	app, err := graph.NewState[builderState]("chain").
		Node("search", func(_ context.Context, s *builderState) error {
			s.Docs = []string{s.Query + ":a", s.Query + ":b"}
			return nil
		}).
		Node("summarize", func(_ context.Context, s *builderState) error {
			s.Docs = append(s.Docs, "sum")
			return nil
		}).
		Node("finish", func(_ context.Context, s *builderState) error {
			s.Done = true
			return nil
		}).
		Start("search").
		Edge("search", "summarize").
		Edge("summarize", "finish").
		Edge("finish", graph.End).
		Compile()
	if err != nil {
		t.Fatal(err)
	}
	out, err := app.Invoke(context.Background(), builderState{Query: "q"})
	if err != nil {
		t.Fatal(err)
	}
	if !out.Done || fmt.Sprint(out.Docs) != "[q:a q:b sum]" {
		t.Fatalf("output=%+v", out)
	}
}

// 路由每次决策只求值一次：计数器必须为 1；返回预声明目标。
func TestBuilderBranchEvaluatesOnce(t *testing.T) {
	calls := 0
	app, err := graph.NewState[builderState]("route").
		Node("plan", func(_ context.Context, s *builderState) error {
			s.Done = true
			return nil
		}).
		Node("search", func(_ context.Context, s *builderState) error {
			s.Docs = append(s.Docs, "hit")
			return nil
		}).
		Start("plan").
		Branch("plan", func(_ context.Context, s *builderState) (string, error) {
			calls++
			if s.Done {
				return graph.End, nil
			}
			return "search", nil
		}, "search", graph.End).
		Edge("search", "plan").
		Compile()
	if err != nil {
		t.Fatal(err)
	}
	out, err := app.Invoke(context.Background(), builderState{})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("route evaluated %d times, want 1", calls)
	}
	if len(out.Docs) != 0 || !out.Done {
		t.Fatalf("output=%+v", out)
	}
}

// 路由返回未声明目标：运行期按路由错误处理。
func TestBuilderBranchUndeclaredTargetFails(t *testing.T) {
	app, err := graph.NewState[builderState]("route-bad").
		Node("plan", func(_ context.Context, _ *builderState) error { return nil }).
		Node("search", func(_ context.Context, _ *builderState) error { return nil }).
		Start("plan").
		Branch("plan", func(_ context.Context, _ *builderState) (string, error) {
			return "missing", nil
		}, "search", graph.End).
		Edge("search", graph.End).
		Compile()
	if err != nil {
		t.Fatal(err)
	}
	_, err = app.Invoke(context.Background(), builderState{})
	if err == nil || !strings.Contains(err.Error(), "undeclared target") {
		t.Fatalf("expected undeclared target error, got %v", err)
	}
}

// 路由目标未注册：Compile 报错。
func TestBuilderBranchTargetMissingFailsCompile(t *testing.T) {
	_, err := graph.NewState[builderState]("route-missing").
		Node("plan", func(_ context.Context, _ *builderState) error { return nil }).
		Start("plan").
		Branch("plan", func(_ context.Context, _ *builderState) (string, error) {
			return "ghost", nil
		}, "ghost").
		Compile()
	if err == nil || !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("expected missing target error, got %v", err)
	}
}

// 路由回环：plan→search→plan，第二次路由出 END。
func TestBuilderBranchLoop(t *testing.T) {
	rounds := 0
	app, err := graph.NewState[builderState]("route-loop").
		Node("plan", func(_ context.Context, s *builderState) error {
			rounds++
			if len(s.Docs) > 0 {
				s.Done = true
			}
			return nil
		}).
		Node("search", func(_ context.Context, s *builderState) error {
			s.Docs = append(s.Docs, "hit")
			return nil
		}).
		Start("plan").
		Branch("plan", func(_ context.Context, s *builderState) (string, error) {
			if s.Done {
				return graph.End, nil
			}
			return "search", nil
		}, "search", graph.End).
		Edge("search", "plan").
		Compile()
	if err != nil {
		t.Fatal(err)
	}
	out, err := app.Invoke(context.Background(), builderState{})
	if err != nil {
		t.Fatal(err)
	}
	if rounds != 2 || fmt.Sprint(out.Docs) != "[hit]" || !out.Done {
		t.Fatalf("rounds=%d output=%+v", rounds, out)
	}
}

// 建图错误累积：多处非法声明在 Compile 一并报告。
func TestBuilderErrorAccumulation(t *testing.T) {
	_, err := graph.NewState[builderState]("bad").
		Node("a", nil).
		Edge("ghost", "a").
		Compile()
	if err == nil {
		t.Fatal("expected accumulated errors")
	}
	msg := err.Error()
	if !strings.Contains(msg, "non-nil") || !strings.Contains(msg, "ghost") {
		t.Fatalf("errors not accumulated: %v", err)
	}
}

// 路由与普通出边互斥：两个方向都拒绝。
func TestBuilderRouteRejectsMixedEdges(t *testing.T) {
	_, err := graph.NewState[builderState]("mix").
		Node("plan", func(_ context.Context, _ *builderState) error { return nil }).
		Node("search", func(_ context.Context, _ *builderState) error { return nil }).
		Start("plan").
		Branch("plan", func(_ context.Context, _ *builderState) (string, error) {
			return "search", nil
		}, "search").
		Edge("plan", "search").
		Compile()
	if err == nil || !strings.Contains(err.Error(), "mix") && !strings.Contains(err.Error(), "route") {
		t.Fatalf("expected route/edge mix error, got %v", err)
	}
}

// 并行扇出：门面 Parallel 对接 Fork/Join/StepMerger。
func TestBuilderParallel(t *testing.T) {
	app, err := graph.NewState[builderState]("fan").
		Node("prep", func(_ context.Context, s *builderState) error {
			s.Docs = []string{s.Query}
			return nil
		}).
		Node("a", func(_ context.Context, s *builderState) error {
			s.Docs = append(s.Docs, "a")
			return nil
		}).
		Node("b", func(_ context.Context, s *builderState) error {
			s.Docs = append(s.Docs, "b")
			return nil
		}).
		Node("join", func(_ context.Context, s *builderState) error {
			s.Done = true
			return nil
		}).
		Start("prep").
		Parallel("prep", []string{"a", "b"}, "join",
			func(_ context.Context, base *builderState, results []graph.StepResult[*builderState]) error {
				// 成员各自在 prep 之后的 State 副本上执行：合并保留 base 的
				// 前缀，只把每个成员追加的最后一项并回主干。
				for _, r := range results {
					base.Docs = append(base.Docs, r.State.Docs[len(r.State.Docs)-1])
				}
				return nil
			}).
		Edge("join", graph.End).
		Compile()
	if err != nil {
		t.Fatal(err)
	}
	out, err := app.Invoke(context.Background(), builderState{Query: "q"})
	if err != nil {
		t.Fatal(err)
	}
	if !out.Done || fmt.Sprint(out.Docs) != "[q a b]" {
		t.Fatalf("output=%+v", out)
	}
}

// 动态分派：门面 Send 对接 AddDispatch，成员完成后汇合。
func TestBuilderSend(t *testing.T) {
	app, err := graph.NewState[builderState]("dispatch").
		Node("plan", func(_ context.Context, _ *builderState) error { return nil }).
		Node("worker", func(_ context.Context, s *builderState) error {
			s.Docs = append(s.Docs, s.Query)
			return nil
		}).
		Node("join", func(_ context.Context, s *builderState) error {
			s.Done = true
			return nil
		}).
		Start("plan").
		Send("plan", []string{"worker"}, "join",
			func(_ context.Context, s *builderState) ([]graph.DispatchTask, error) {
				return []graph.DispatchTask{
					graph.Send("worker", &builderState{Query: "x"}),
					graph.Send("worker", &builderState{Query: "y"}),
				}, nil
			}).
		Merge(func(_ context.Context, base *builderState, results []graph.StepResult[*builderState]) error {
			for _, r := range results {
				base.Docs = append(base.Docs, r.State.Docs...)
			}
			return nil
		}).
		Edge("worker", "join").
		Edge("join", graph.End).
		Compile()
	if err != nil {
		t.Fatal(err)
	}
	out, err := app.Invoke(context.Background(), builderState{})
	if err != nil {
		t.Fatal(err)
	}
	if !out.Done || fmt.Sprint(out.Docs) != "[x y]" {
		t.Fatalf("output=%+v", out)
	}
}

// 路由在 Stream 模式下按同一决策路径求值。
func TestBuilderBranchUnderStreamMode(t *testing.T) {
	calls := 0
	app, err := graph.NewState[builderState]("route-stream").
		Node("plan", func(_ context.Context, s *builderState) error {
			s.Done = true
			return nil
		}).
		Node("search", func(_ context.Context, s *builderState) error {
			s.Docs = append(s.Docs, "hit")
			return nil
		}).
		Start("plan").
		Branch("plan", func(_ context.Context, s *builderState) (string, error) {
			calls++
			if s.Done {
				return graph.End, nil
			}
			return "search", nil
		}, "search", graph.End).
		Edge("search", "plan").
		Compile()
	if err != nil {
		t.Fatal(err)
	}
	stream, err := app.Stream(context.Background(), builderState{})
	if err != nil {
		t.Fatal(err)
	}
	out, err := stream.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || !out.Done || len(out.Docs) != 0 {
		t.Fatalf("calls=%d output=%+v", calls, out)
	}
}

// 路由源可以是 START：按初始 State 决定首节点。
func TestBuilderBranchFromStart(t *testing.T) {
	app, err := graph.NewState[builderState]("route-start").
		Node("search", func(_ context.Context, s *builderState) error {
			s.Docs = append(s.Docs, "hit")
			return nil
		}).
		Node("finish", func(_ context.Context, s *builderState) error {
			s.Done = true
			return nil
		}).
		Branch(graph.Start, func(_ context.Context, s *builderState) (string, error) {
			if s.Query == "" {
				return "finish", nil
			}
			return "search", nil
		}, "search", "finish").
		Edge("search", "finish").
		Edge("finish", graph.End).
		Compile()
	if err != nil {
		t.Fatal(err)
	}
	out, err := app.Invoke(context.Background(), builderState{Query: "q"})
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(out.Docs) != "[hit]" || !out.Done {
		t.Fatalf("output=%+v", out)
	}
}

// 节点错误照常向上传递。
func TestBuilderPropagatesNodeError(t *testing.T) {
	sentinel := errors.New("boom")
	app, err := graph.NewState[builderState]("err").
		Node("work", func(_ context.Context, _ *builderState) error { return sentinel }).
		Start("work").
		Edge("work", graph.End).
		Compile()
	if err != nil {
		t.Fatal(err)
	}
	_, err = app.Invoke(context.Background(), builderState{})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel, got %v", err)
	}
}
