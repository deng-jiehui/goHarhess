package graph_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/state"
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

// Parallel 的 merge 只作用于本次扇出的批内汇合：图内其他顺序步骤不再
// 经过合并函数——此前 merge 经全局 stepMerger 安装，每个节点都会调用一次。
func TestBuilderParallelMergeScoped(t *testing.T) {
	var calls atomic.Int32
	merge := func(_ context.Context, base *builderState, results []graph.StepResult[*builderState]) error {
		calls.Add(1)
		for _, r := range results {
			base.Docs = append(base.Docs, r.State.Docs[len(r.State.Docs)-1])
		}
		return nil
	}
	app, err := graph.NewState[builderState]("scoped-merge").
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
			s.Docs = append(s.Docs, "joined")
			return nil
		}).
		Node("tail", func(_ context.Context, s *builderState) error {
			s.Docs = append(s.Docs, "tail")
			return nil
		}).
		Start("prep").
		Parallel("prep", []string{"a", "b"}, "join", merge).
		Edge("join", "tail").
		Edge("tail", graph.End).
		Compile()
	if err != nil {
		t.Fatal(err)
	}
	out, err := app.Invoke(context.Background(), builderState{Query: "q"})
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("merge invoked %d times, want exactly 1 (join batch)", calls.Load())
	}
	if fmt.Sprint(out.Docs) != "[q a b joined tail]" {
		t.Fatalf("output=%+v", out)
	}
}

// 并行成员自行声明 Branch 路由时不再补默认汇合边：成员路由按声明到达
// 本分支内的接续节点再回到汇合。fork 组成员持有局部 State，接续节点从
// 成员自己的提交态继续；合并函数只在全部成员到达汇合时执行一次。
func TestBuilderParallelMemberRoute(t *testing.T) {
	var merges atomic.Int32
	var received [][]string
	app, err := graph.NewState[builderState]("member-route").
		Node("prep", func(_ context.Context, s *builderState) error {
			s.Docs = []string{s.Query}
			return nil
		}).
		Node("a", func(_ context.Context, s *builderState) error {
			s.Docs = append(s.Docs, "a")
			return nil
		}).
		Node("a2", func(_ context.Context, s *builderState) error {
			s.Docs = append(s.Docs, "a2")
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
				merges.Add(1)
				for _, r := range results {
					received = append(received, append([]string(nil), r.State.Docs...))
					base.Docs = append(base.Docs, r.State.Docs[len(r.State.Docs)-1])
				}
				return nil
			}).
		Branch("a", func(_ context.Context, _ *builderState) (string, error) {
			return "a2", nil
		}, "a2", "join").
		Edge("a2", "join").
		Edge("join", graph.End).
		Compile()
	if err != nil {
		t.Fatal(err)
	}
	out, err := app.Invoke(context.Background(), builderState{Query: "q"})
	if err != nil {
		t.Fatal(err)
	}
	// 多步分支的中间态不触发合并：a 经路由走到 a2 后到达汇合，b 先到汇合
	// 等待；合并只在全部成员到达后按声明序消费各自的终态 State——
	// a 分支的终态包含 a2，不是首轮的中间结果 [q a]。
	if merges.Load() != 1 {
		t.Fatalf("merge invoked %d times, want 1", merges.Load())
	}
	want := [][]string{{"q", "a", "a2"}, {"q", "b"}}
	if !reflect.DeepEqual(received, want) {
		t.Fatalf("merger received %v, want final member states %v", received, want)
	}
	if !out.Done || fmt.Sprint(out.Docs) != "[q a2 b]" {
		t.Fatalf("output=%+v", out)
	}
}

// 路由目标做类型校验：State 节点路由到原始组件时按普通边同一套规则
// 检查源输出与目标输入的可赋值性。
func TestBuilderRouteTargetTypeChecked(t *testing.T) {
	b := graph.NewState[builderState]("route-type")
	b.Node("plan", func(_ context.Context, _ *builderState) error { return nil })
	if err := b.Graph().AddNode("raw", runnable.Func[int, *builderState](func(_ context.Context, _ int, _ ...runnable.Option) (*builderState, error) {
		return &builderState{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := b.Graph().AddEdge("raw", graph.End); err != nil {
		t.Fatal(err)
	}
	_, err := b.Start("plan").
		Branch("plan", func(_ context.Context, _ *builderState) (string, error) {
			return "raw", nil
		}, "raw").
		Compile()
	if err == nil || !strings.Contains(err.Error(), "route.target.type_mismatch") {
		t.Fatalf("expected route.target.type_mismatch, got %v", err)
	}
}

// Checkpointer 在编译期绑定存储，Thread 在运行期选择线程；两者配对生效，
// 缺绑店时 Thread 报错。
func TestBuilderCheckpointerThread(t *testing.T) {
	build := func(name string, options ...graph.CompileOption) *graph.CompiledGraph[builderState, *builderState] {
		app, err := graph.NewState[builderState](name).
			Node("work", func(_ context.Context, s *builderState) error {
				s.Done = true
				return nil
			}).
			Start("work").
			Edge("work", graph.End).
			Compile(options...)
		if err != nil {
			t.Fatal(err)
		}
		return app
	}
	store := state.NewMemoryStore()
	app := build("persisted", graph.Checkpointer(store))
	out, err := app.Invoke(context.Background(), builderState{}, graph.Thread("t1"))
	if err != nil {
		t.Fatal(err)
	}
	if !out.Done {
		t.Fatalf("output=%+v", out)
	}
	record, found, err := store.Load(context.Background(), "t1")
	if err != nil || !found {
		t.Fatalf("checkpoint not persisted: found=%v err=%v", found, err)
	}
	if record.Status == "" {
		t.Fatalf("checkpoint record missing status: %+v", record)
	}
	// 只有 Thread 没有绑店：Store 为空，finalize 拒绝。
	plain := build("plain")
	if _, err := plain.Invoke(context.Background(), builderState{}, graph.Thread("t9")); err == nil ||
		!strings.Contains(err.Error(), "checkpoint store") {
		t.Fatalf("expected checkpoint store error, got %v", err)
	}
	// 绑店但不传 Thread：不启用持久化，存储保持空。
	if _, err := build("another", graph.Checkpointer(store)).Invoke(context.Background(), builderState{}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Load(context.Background(), "another-run"); err != nil || found {
		t.Fatalf("unexpected checkpoint write: found=%v err=%v", found, err)
	}
}
