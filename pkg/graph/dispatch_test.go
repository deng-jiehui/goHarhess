package graph_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/state"
)

// dispatchState 记录成员写入的业务结果与清单生成次数。
type dispatchState struct {
	In      string
	Results []string
	Calls   int
}

// newDispatchGraph 构造 planner → 动态分派 → join 的最小图：
// planner 是分派来源（无静态出边），worker 是唯一允许目标；workerFn
// 为空时使用默认的 "w:"+input 行为。
func newDispatchGraph(t *testing.T, members func(*dispatchState) []graph.DispatchTask, calls *atomic.Int32, workerFn func(context.Context, string) (string, error)) *graph.Graph[string, string] {
	t.Helper()
	g := graph.NewGraph[string, string]("dispatch-test")
	if err := g.SetStateFactory(func(_ context.Context, input string) (*dispatchState, error) {
		return &dispatchState{In: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	planner := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
		return input, nil
	})
	if err := g.AddNode("planner", planner,
		graph.WithPreHandler(func(_ context.Context, s *dispatchState) (string, error) { return s.In, nil }),
	); err != nil {
		t.Fatal(err)
	}
	if err := graph.AddDispatch(g, "planner", []string{"worker"}, "join",
		func(_ context.Context, s *dispatchState) ([]graph.DispatchTask, error) {
			if calls != nil {
				calls.Add(1)
			}
			return members(s), nil
		}); err != nil {
		t.Fatal(err)
	}
	if workerFn == nil {
		workerFn = func(_ context.Context, input string) (string, error) {
			return "w:" + input, nil
		}
	}
	worker := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		return workerFn(ctx, input)
	})
	if err := g.AddNode("worker", worker,
		graph.WithPostHandler(func(_ context.Context, s *dispatchState, output string) error {
			s.Results = append(s.Results, output)
			return nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	join := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
		return input, nil
	})
	if err := g.AddNode("join", join,
		graph.WithPreHandler(func(_ context.Context, s *dispatchState) (string, error) {
			return strings.Join(s.Results, ","), nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{
		{graph.Start, "planner"}, {"worker", "join"}, {"join", graph.End},
	} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	if err := graph.SetStepMerger(g, func(_ context.Context, base *dispatchState, results []graph.StepResult[*dispatchState]) error {
		base.Results = nil
		for _, r := range results {
			base.Results = append(base.Results, r.State.Results...)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return g
}

// 零成员直接进入汇合节点，成员任务不创建。
func TestDispatchZeroMembersGoesStraightToJoin(t *testing.T) {
	g := newDispatchGraph(t, func(*dispatchState) []graph.DispatchTask { return nil }, nil, nil)
	out, err := mustCompile(t, g).Invoke(context.Background(), "go")
	if err != nil {
		t.Fatal(err)
	}
	if out != "" {
		t.Fatalf("output=%q, want empty join result", out)
	}
}

// 一成员与多成员：每个成员收到独立冻结输入，结果按清单序号汇合。
func TestDispatchMembersOrderedByManifest(t *testing.T) {
	var calls atomic.Int32
	g := newDispatchGraph(t, func(s *dispatchState) []graph.DispatchTask {
		return []graph.DispatchTask{
			graph.Send("worker", "c-"+s.In),
			graph.Send("worker", "a-"+s.In),
			graph.Send("worker", "b-"+s.In),
		}
	}, &calls, nil)
	out, err := mustCompile(t, g).Invoke(context.Background(), "x")
	if err != nil {
		t.Fatal(err)
	}
	if out != "w:c-x,w:a-x,w:b-x" {
		t.Fatalf("output=%q, want manifest order", out)
	}
	if calls.Load() != 1 {
		t.Fatalf("manifest calls=%d, want 1", calls.Load())
	}
}

// 成员可以使用成员链上的后续节点：首个任务用分派输入，后续任务
// 经 PreHandler 从成员 State 重建输入。
type dispatchChainState struct {
	Results []string
	Last    string
}

// memberChainState 与 dispatchChainState 同构，专供成员局部 State
// 回归测试使用。
type memberChainState = dispatchChainState

// 多步成员链：两个成员分派到同一入口，后续节点的 Pre 与条件路由
// 都必须看到成员自己上一节点提交的 State，而非合并后的公共态。
func TestDispatchMemberLocalStateAcrossNodes(t *testing.T) {
	for _, mode := range []string{"memory", "checkpoint"} {
		t.Run(mode, func(t *testing.T) {
			g := graph.NewGraph[string, string]("member-chain")
			if err := g.SetStateFactory(func(_ context.Context, _ string) (*memberChainState, error) {
				return &memberChainState{}, nil
			}); err != nil {
				t.Fatal(err)
			}
			planner := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
				return input, nil
			})
			if err := g.AddNode("planner", planner,
				graph.WithPreHandler(func(_ context.Context, _ *memberChainState) (string, error) { return "go", nil }),
			); err != nil {
				t.Fatal(err)
			}
			if err := graph.AddDispatch(g, "planner", []string{"fetch", "parse"}, "join",
				func(_ context.Context, _ *memberChainState) ([]graph.DispatchTask, error) {
					return []graph.DispatchTask{graph.Send("fetch", "a"), graph.Send("fetch", "b")}, nil
				}); err != nil {
				t.Fatal(err)
			}
			fetch := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
				return "fetched:" + input, nil
			})
			if err := g.AddNode("fetch", fetch,
				graph.WithPostHandler(func(_ context.Context, s *memberChainState, output string) error {
					s.Last = output
					return nil
				}),
			); err != nil {
				t.Fatal(err)
			}
			parse := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
				return "parsed:" + input, nil
			})
			if err := g.AddNode("parse", parse,
				graph.WithPreHandler(func(_ context.Context, s *memberChainState) (string, error) { return s.Last, nil }),
				graph.WithPostHandler(func(_ context.Context, s *memberChainState, output string) error {
					s.Results = append(s.Results, output)
					return nil
				}),
			); err != nil {
				t.Fatal(err)
			}
			join := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
				return input, nil
			})
			if err := g.AddNode("join", join,
				graph.WithPreHandler(func(_ context.Context, s *memberChainState) (string, error) {
					return strings.Join(s.Results, ","), nil
				}),
			); err != nil {
				t.Fatal(err)
			}
			for _, edge := range [][2]string{
				{graph.Start, "planner"}, {"fetch", "parse"}, {"parse", "join"}, {"join", graph.End},
			} {
				if err := g.AddEdge(edge[0], edge[1]); err != nil {
					t.Fatal(err)
				}
			}
			if err := graph.SetStepMerger(g, func(_ context.Context, base *memberChainState, results []graph.StepResult[*memberChainState]) error {
				base.Results = nil
				for _, r := range results {
					base.Results = append(base.Results, r.State.Results...)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			compiled := mustCompile(t, g)
			var opts []runnable.Option
			if mode == "checkpoint" {
				opts = append(opts, graph.WithCheckpoint(state.NewMemoryStore(), "chain"))
			}
			out, err := compiled.Invoke(context.Background(), "x", opts...)
			if err != nil {
				t.Fatal(err)
			}
			if out != "parsed:fetched:a,parsed:fetched:b" {
				t.Fatalf("output=%q, want member-local chain results", out)
			}
		})
	}
}

// 成员在第二节点审批挂起：恢复时已完成成员复用结果、挂起成员
// 从自己的局部 State 继续，汇合不重复也不串数据。
func TestDispatchMemberSuspendAtSecondNodeResumes(t *testing.T) {
	var fetchRuns, parseRuns atomic.Int32
	build := func() *graph.Graph[string, string] {
		g := graph.NewGraph[string, string]("member-chain-resume")
		if err := g.SetStateFactory(func(_ context.Context, _ string) (*memberChainState, error) {
			return &memberChainState{}, nil
		}); err != nil {
			t.Fatal(err)
		}
		planner := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
			return input, nil
		})
		if err := g.AddNode("planner", planner,
			graph.WithPreHandler(func(_ context.Context, _ *memberChainState) (string, error) { return "go", nil }),
		); err != nil {
			t.Fatal(err)
		}
		if err := graph.AddDispatch(g, "planner", []string{"fetch", "parse"}, "join",
			func(_ context.Context, _ *memberChainState) ([]graph.DispatchTask, error) {
				return []graph.DispatchTask{graph.Send("fetch", "a"), graph.Send("fetch", "b")}, nil
			}); err != nil {
			t.Fatal(err)
		}
		fetch := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
			fetchRuns.Add(1)
			return "fetched:" + input, nil
		})
		if err := g.AddNode("fetch", fetch,
			graph.WithPostHandler(func(_ context.Context, s *memberChainState, output string) error {
				s.Last = output
				return nil
			}),
		); err != nil {
			t.Fatal(err)
		}
		parse := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
			parseRuns.Add(1)
			if input == "fetched:b" {
				if _, err := graph.Interrupt(ctx, "approve-parse"); err != nil {
					return "", err
				}
			}
			return "parsed:" + input, nil
		})
		if err := g.AddNode("parse", parse,
			graph.WithPreHandler(func(_ context.Context, s *memberChainState) (string, error) { return s.Last, nil }),
			graph.WithPostHandler(func(_ context.Context, s *memberChainState, output string) error {
				s.Results = append(s.Results, output)
				return nil
			}),
		); err != nil {
			t.Fatal(err)
		}
		join := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
			return input, nil
		})
		if err := g.AddNode("join", join,
			graph.WithPreHandler(func(_ context.Context, s *memberChainState) (string, error) {
				return strings.Join(s.Results, ","), nil
			}),
		); err != nil {
			t.Fatal(err)
		}
		for _, edge := range [][2]string{
			{graph.Start, "planner"}, {"fetch", "parse"}, {"parse", "join"}, {"join", graph.End},
		} {
			if err := g.AddEdge(edge[0], edge[1]); err != nil {
				t.Fatal(err)
			}
		}
		if err := graph.SetStepMerger(g, func(_ context.Context, base *memberChainState, results []graph.StepResult[*memberChainState]) error {
			base.Results = nil
			for _, r := range results {
				base.Results = append(base.Results, r.State.Results...)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return g
	}
	store := state.NewMemoryStore()
	compiled := mustCompile(t, build())
	_, err := compiled.Invoke(context.Background(), "x", graph.WithCheckpoint(store, "conv"))
	var stopped *graph.InterruptedError
	if !errors.As(err, &stopped) {
		t.Fatalf("expected member interruption, got %v", err)
	}
	if fetchRuns.Load() != 2 || parseRuns.Load() != 2 {
		t.Fatalf("first run fetch=%d parse=%d", fetchRuns.Load(), parseRuns.Load())
	}
	questionID := ""
	for _, record := range stopped.PendingInterrupts() {
		if !record.Answered {
			questionID = record.ID
		}
	}
	if questionID == "" {
		t.Fatal("no pending question")
	}
	// 重建实例后按已保存清单恢复：b 的 parse 重放，a 的已完成记录复用。
	recompiled := mustCompile(t, build())
	out, err := recompiled.Resume(context.Background(),
		graph.WithCheckpoint(store, "conv"), graph.WithResume(questionID, "yes"))
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if out != "parsed:fetched:a,parsed:fetched:b" {
		t.Fatalf("resumed output=%q", out)
	}
	if fetchRuns.Load() != 2 || parseRuns.Load() != 3 {
		t.Fatalf("after resume fetch=%d parse=%d, want 2/3", fetchRuns.Load(), parseRuns.Load())
	}
}

// Codec 版本演进使整图指纹变化：round-1 提交后挂起的检查点在新版本
// 图上恢复，不再提供迁移通道，必须直接拒绝。
func TestDispatchResumeRejectsCodecChange(t *testing.T) {
	codecV1, err := graph.NewStateCodec[*memberChainState]("member-chain", 1,
		func(_ context.Context, s *memberChainState) ([]byte, error) { return json.Marshal(s) },
		func(_ context.Context, data []byte) (*memberChainState, error) {
			var s memberChainState
			return &s, json.Unmarshal(data, &s)
		})
	if err != nil {
		t.Fatal(err)
	}
	codecV2, err := graph.NewStateCodec[*memberChainState]("member-chain", 2,
		func(_ context.Context, s *memberChainState) ([]byte, error) {
			data, err := json.Marshal(s)
			return append([]byte("v2|"), data...), err
		},
		func(_ context.Context, data []byte) (*memberChainState, error) {
			text, ok := strings.CutPrefix(string(data), "v2|")
			if !ok {
				return nil, errors.New("not a v2 payload")
			}
			var s memberChainState
			return &s, json.Unmarshal([]byte(text), &s)
		})
	if err != nil {
		t.Fatal(err)
	}
	build := func(codec graph.StateCodec, pauseAfterFirstRound func()) *graph.Graph[string, string] {
		g := graph.NewGraph[string, string]("member-codec-change")
		if err := g.SetStateFactory(func(_ context.Context, _ string) (*memberChainState, error) {
			return &memberChainState{}, nil
		}); err != nil {
			t.Fatal(err)
		}
		planner := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
			return input, nil
		})
		if err := g.AddNode("planner", planner,
			graph.WithPreHandler(func(_ context.Context, _ *memberChainState) (string, error) { return "go", nil }),
		); err != nil {
			t.Fatal(err)
		}
		if err := graph.AddDispatch(g, "planner", []string{"fetch", "parse"}, "join",
			func(_ context.Context, _ *memberChainState) ([]graph.DispatchTask, error) {
				return []graph.DispatchTask{graph.Send("fetch", "a"), graph.Send("fetch", "b")}, nil
			}); err != nil {
			t.Fatal(err)
		}
		fetch := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
			return "fetched:" + input, nil
		})
		if err := g.AddNode("fetch", fetch,
			graph.WithPostHandler(func(_ context.Context, s *memberChainState, output string) error {
				s.Last = output
				return nil
			}),
		); err != nil {
			t.Fatal(err)
		}
		parse := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
			return "parsed:" + input, nil
		})
		var once atomic.Bool
		if err := g.AddNode("parse", parse,
			graph.WithPreHandler(func(_ context.Context, s *memberChainState) (string, error) {
				// 第一轮（两个 fetch 汇合）结束后挂起：此时成员已推进到
				// parse，member.State 落盘而 parse 任务记录尚未建立。
				if pauseAfterFirstRound != nil && once.CompareAndSwap(false, true) {
					pauseAfterFirstRound()
				}
				return s.Last, nil
			}),
			graph.WithPostHandler(func(_ context.Context, s *memberChainState, output string) error {
				s.Results = append(s.Results, output)
				return nil
			}),
		); err != nil {
			t.Fatal(err)
		}
		join := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
			return input, nil
		})
		if err := g.AddNode("join", join,
			graph.WithPreHandler(func(_ context.Context, s *memberChainState) (string, error) {
				return strings.Join(s.Results, ","), nil
			}),
		); err != nil {
			t.Fatal(err)
		}
		for _, edge := range [][2]string{
			{graph.Start, "planner"}, {"fetch", "parse"}, {"parse", "join"}, {"join", graph.End},
		} {
			if err := g.AddEdge(edge[0], edge[1]); err != nil {
				t.Fatal(err)
			}
		}
		if err := graph.SetStepMerger(g, func(_ context.Context, base *memberChainState, results []graph.StepResult[*memberChainState]) error {
			for _, r := range results {
				base.Results = append(base.Results, r.State.Results...)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := g.SetStateCodec(codec); err != nil {
			t.Fatal(err)
		}
		return g
	}
	runCtx, cancel := context.WithCancelCause(context.Background())
	store := state.NewMemoryStore()
	v1 := mustCompile(t, build(codecV1, func() {
		cancel(control.RequestInterrupt("pause before parse", nil))
	}))
	_, err = v1.Invoke(runCtx, "x", graph.WithCheckpoint(store, "conv"))
	var stopped *graph.InterruptedError
	if !errors.As(err, &stopped) {
		t.Fatalf("expected interruption between rounds, got %v", err)
	}
	v2 := mustCompile(t, build(codecV2, nil))
	if _, err := v2.Resume(context.Background(), graph.WithCheckpoint(store, "conv")); !errors.Is(err, graph.ErrGraphRevisionMismatch) {
		t.Fatalf("resume after codec change = %v, want ErrGraphRevisionMismatch", err)
	}
}

// 成员级条件路由按成员自己的局部 State 选路：hot/cold 两个成员走
// 不同的后继节点，汇合仍按清单序。
func TestDispatchMemberRoutingUsesLocalState(t *testing.T) {
	g := graph.NewGraph[string, string]("member-route")
	if err := g.SetStateFactory(func(_ context.Context, _ string) (*memberChainState, error) {
		return &memberChainState{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	planner := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
		return input, nil
	})
	if err := g.AddNode("planner", planner,
		graph.WithPreHandler(func(_ context.Context, _ *memberChainState) (string, error) { return "go", nil }),
	); err != nil {
		t.Fatal(err)
	}
	if err := graph.AddDispatch(g, "planner", []string{"fetch", "hot", "cold"}, "join",
		func(_ context.Context, _ *memberChainState) ([]graph.DispatchTask, error) {
			return []graph.DispatchTask{graph.Send("fetch", "hot"), graph.Send("fetch", "cold")}, nil
		}); err != nil {
		t.Fatal(err)
	}
	fetch := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
		return input, nil
	})
	if err := g.AddNode("fetch", fetch,
		graph.WithPostHandler(func(_ context.Context, s *memberChainState, output string) error {
			s.Last = output
			return nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"hot", "cold"} {
		branch := name
		node := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
			return branch + ":" + input, nil
		})
		if err := g.AddNode(branch, node,
			graph.WithPreHandler(func(_ context.Context, s *memberChainState) (string, error) { return s.Last, nil }),
			graph.WithPostHandler(func(_ context.Context, s *memberChainState, output string) error {
				s.Results = append(s.Results, output)
				return nil
			}),
		); err != nil {
			t.Fatal(err)
		}
	}
	join := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
		return input, nil
	})
	if err := g.AddNode("join", join,
		graph.WithPreHandler(func(_ context.Context, s *memberChainState) (string, error) {
			return strings.Join(s.Results, ","), nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "planner"); err != nil {
		t.Fatal(err)
	}
	// 成员局部 State 决定路由：含 "hot" 走 hot 分支，其余回落到 cold。
	if err := graph.AddBranch(g, "fetch", "hot", "is-hot", func(_ context.Context, s *memberChainState) (bool, error) {
		return strings.Contains(s.Last, "hot"), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("fetch", "cold"); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{
		{"hot", "join"}, {"cold", "join"}, {"join", graph.End},
	} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	if err := graph.SetStepMerger(g, func(_ context.Context, base *memberChainState, results []graph.StepResult[*memberChainState]) error {
		base.Results = nil
		for _, r := range results {
			base.Results = append(base.Results, r.State.Results...)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	out, err := mustCompile(t, g).Invoke(context.Background(), "x")
	if err != nil {
		t.Fatal(err)
	}
	if out != "hot:hot,cold:cold" {
		t.Fatalf("output=%q, want per-member routed results", out)
	}
}

// 分派输入为图 State 类型时，成员入口 StateNode 直接在该对象上
// 修改：输入、上下文与汇合结果是同一份局部 State。
func TestDispatchStateNodeMemberSharesEntryState(t *testing.T) {
	for _, mode := range []string{"memory", "checkpoint"} {
		t.Run(mode, func(t *testing.T) {
			g := graph.NewGraph[string, string]("member-statenode")
			if err := g.SetStateFactory(func(_ context.Context, _ string) (*memberChainState, error) {
				return &memberChainState{}, nil
			}); err != nil {
				t.Fatal(err)
			}
			planner := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
				return input, nil
			})
			if err := g.AddNode("planner", planner,
				graph.WithPreHandler(func(_ context.Context, _ *memberChainState) (string, error) { return "go", nil }),
			); err != nil {
				t.Fatal(err)
			}
			if err := graph.AddDispatch(g, "planner", []string{"bump", "bump2"}, "join",
				func(_ context.Context, _ *memberChainState) ([]graph.DispatchTask, error) {
					return []graph.DispatchTask{graph.Send("bump", &memberChainState{Results: []string{"seed:7"}})}, nil
				}); err != nil {
				t.Fatal(err)
			}
			if err := g.AddNode("bump", graph.StateNode(func(_ context.Context, s *memberChainState) error {
				s.Last = "bumped"
				return nil
			})); err != nil {
				t.Fatal(err)
			}
			if err := g.AddNode("bump2", graph.StateNode(func(_ context.Context, s *memberChainState) error {
				s.Last += ":again"
				return nil
			})); err != nil {
				t.Fatal(err)
			}
			join := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
				return input, nil
			})
			if err := g.AddNode("join", join,
				graph.WithPreHandler(func(_ context.Context, s *memberChainState) (string, error) {
					return s.Last + "|" + strings.Join(s.Results, ","), nil
				}),
			); err != nil {
				t.Fatal(err)
			}
			for _, edge := range [][2]string{
				{graph.Start, "planner"}, {"bump", "bump2"}, {"bump2", "join"}, {"join", graph.End},
			} {
				if err := g.AddEdge(edge[0], edge[1]); err != nil {
					t.Fatal(err)
				}
			}
			if err := graph.SetStepMerger(g, func(_ context.Context, base *memberChainState, results []graph.StepResult[*memberChainState]) error {
				base.Results = nil
				for _, r := range results {
					base.Results = append(base.Results, r.State.Results...)
					base.Last = r.State.Last
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			compiled := mustCompile(t, g)
			var opts []runnable.Option
			if mode == "checkpoint" {
				opts = append(opts, graph.WithCheckpoint(state.NewMemoryStore(), "sn"))
			}
			out, err := compiled.Invoke(context.Background(), "x", opts...)
			if err != nil {
				t.Fatal(err)
			}
			if out != "bumped:again|seed:7" {
				t.Fatalf("output=%q, want StateNode member state preserved", out)
			}
		})
	}
}

// 动态分派声明属于活跃组的恢复契约：成员审批挂起后，把汇合点改到
// 另一节点必须被拒绝，且不执行新路径。
func TestDispatchResumeRejectsJoinChange(t *testing.T) {
	var bRuns atomic.Int32
	build := func(joinName string) *graph.Graph[string, string] {
		g := graph.NewGraph[string, string]("dispatch-revision")
		if err := g.SetStateFactory(func(_ context.Context, _ string) (*dispatchState, error) {
			return &dispatchState{}, nil
		}); err != nil {
			t.Fatal(err)
		}
		planner := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
			return input, nil
		})
		if err := g.AddNode("planner", planner,
			graph.WithPreHandler(func(_ context.Context, _ *dispatchState) (string, error) { return "go", nil }),
		); err != nil {
			t.Fatal(err)
		}
		if err := graph.AddDispatch(g, "planner", []string{"worker"}, joinName,
			func(_ context.Context, _ *dispatchState) ([]graph.DispatchTask, error) {
				return []graph.DispatchTask{graph.Send("worker", "a"), graph.Send("worker", "b")}, nil
			}); err != nil {
			t.Fatal(err)
		}
		worker := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
			if input == "b" {
				bRuns.Add(1)
				if _, err := graph.Interrupt(ctx, "approve-b"); err != nil {
					return "", err
				}
			}
			return "w:" + input, nil
		})
		if err := g.AddNode("worker", worker,
			graph.WithPostHandler(func(_ context.Context, s *dispatchState, output string) error {
				s.Results = append(s.Results, output)
				return nil
			}),
		); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"join", "tail"} {
			n := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
				return input, nil
			})
			pre := func(_ context.Context, s *dispatchState) (string, error) {
				return strings.Join(s.Results, ","), nil
			}
			if err := g.AddNode(name, n, graph.WithPreHandler(pre)); err != nil {
				t.Fatal(err)
			}
		}
		// 两种构造的节点集合一致；汇合点声明不同意味着成员路径形状不同：
		// join 版 worker→join→End（tail 为永不走的旁路保持可达），tail 版
		// worker→join→tail→End——join 变成成员路径中段。
		for _, edge := range [][2]string{
			{graph.Start, "planner"}, {"worker", "join"}, {"tail", graph.End},
		} {
			if err := g.AddEdge(edge[0], edge[1]); err != nil {
				t.Fatal(err)
			}
		}
		if joinName == "join" {
			if err := g.AddEdge("join", graph.End); err != nil {
				t.Fatal(err)
			}
			if err := graph.AddBranch(g, "join", "tail", "never", func(_ context.Context, _ *dispatchState) (bool, error) {
				return false, nil
			}); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := g.AddEdge("join", "tail"); err != nil {
				t.Fatal(err)
			}
		}
		if err := graph.SetStepMerger(g, func(_ context.Context, base *dispatchState, results []graph.StepResult[*dispatchState]) error {
			base.Results = nil
			for _, r := range results {
				base.Results = append(base.Results, r.State.Results...)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return g
	}
	store := state.NewMemoryStore()
	compiled := mustCompile(t, build("join"))
	_, err := compiled.Invoke(context.Background(), "x", graph.WithCheckpoint(store, "conv"))
	var stopped *graph.InterruptedError
	if !errors.As(err, &stopped) {
		t.Fatalf("expected interruption, got %v", err)
	}
	questionID := ""
	for _, record := range stopped.PendingInterrupts() {
		if !record.Answered {
			questionID = record.ID
		}
	}
	if questionID == "" {
		t.Fatal("no pending question")
	}
	// 重建：节点集合与普通边不变，仅把分派汇合点 join 换成已有的 tail。
	recompiled := mustCompile(t, build("tail"))
	_, err = recompiled.Resume(context.Background(),
		graph.WithCheckpoint(store, "conv"), graph.WithResume(questionID, "yes"))
	if !errors.Is(err, graph.ErrGraphRevisionMismatch) {
		t.Fatalf("resume after join change = %v, want ErrGraphRevisionMismatch", err)
	}
}

// 来源节点挂起（清单尚未生成）时，成员路径上插入新节点也必须
// 被拒绝：分派来源到目标的连通关系参与恢复闭包。
func TestDispatchResumeRejectsMemberPathChange(t *testing.T) {
	build := func(withExtra bool) *graph.Graph[string, string] {
		g := graph.NewGraph[string, string]("dispatch-path")
		if err := g.SetStateFactory(func(_ context.Context, _ string) (*dispatchState, error) {
			return &dispatchState{}, nil
		}); err != nil {
			t.Fatal(err)
		}
		planner := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
			if _, err := graph.Interrupt(ctx, "approve-plan"); err != nil {
				return "", err
			}
			return input, nil
		})
		if err := g.AddNode("planner", planner,
			graph.WithPreHandler(func(_ context.Context, _ *dispatchState) (string, error) { return "go", nil }),
		); err != nil {
			t.Fatal(err)
		}
		if err := graph.AddDispatch(g, "planner", []string{"worker"}, "join",
			func(_ context.Context, _ *dispatchState) ([]graph.DispatchTask, error) {
				return []graph.DispatchTask{graph.Send("worker", "a")}, nil
			}); err != nil {
			t.Fatal(err)
		}
		worker := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
			return "w:" + input, nil
		})
		if err := g.AddNode("worker", worker,
			graph.WithPostHandler(func(_ context.Context, s *dispatchState, output string) error {
				s.Results = append(s.Results, output)
				return nil
			}),
		); err != nil {
			t.Fatal(err)
		}
		if withExtra {
			extra := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
				return input + ",new-step", nil
			})
			if err := g.AddNode("extra", extra,
				graph.WithPreHandler(func(_ context.Context, s *dispatchState) (string, error) {
					return s.Results[len(s.Results)-1], nil
				}),
				graph.WithPostHandler(func(_ context.Context, s *dispatchState, output string) error {
					s.Results = append(s.Results, output)
					return nil
				}),
			); err != nil {
				t.Fatal(err)
			}
		}
		join := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
			return input, nil
		})
		if err := g.AddNode("join", join,
			graph.WithPreHandler(func(_ context.Context, s *dispatchState) (string, error) {
				return strings.Join(s.Results, ","), nil
			}),
		); err != nil {
			t.Fatal(err)
		}
		edges := [][2]string{{graph.Start, "planner"}}
		if withExtra {
			edges = append(edges, [2]string{"worker", "extra"}, [2]string{"extra", "join"})
		} else {
			edges = append(edges, [2]string{"worker", "join"})
		}
		edges = append(edges, [2]string{"join", graph.End})
		for _, edge := range edges {
			if err := g.AddEdge(edge[0], edge[1]); err != nil {
				t.Fatal(err)
			}
		}
		if err := graph.SetStepMerger(g, func(_ context.Context, base *dispatchState, results []graph.StepResult[*dispatchState]) error {
			base.Results = nil
			for _, r := range results {
				base.Results = append(base.Results, r.State.Results...)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return g
	}
	store := state.NewMemoryStore()
	compiled := mustCompile(t, build(false))
	_, err := compiled.Invoke(context.Background(), "x", graph.WithCheckpoint(store, "conv"))
	var stopped *graph.InterruptedError
	if !errors.As(err, &stopped) {
		t.Fatalf("expected source interruption, got %v", err)
	}
	questionID := stopped.PendingInterrupts()[0].ID
	recompiled := mustCompile(t, build(true))
	_, err = recompiled.Resume(context.Background(),
		graph.WithCheckpoint(store, "conv"), graph.WithResume(questionID, "yes"))
	if !errors.Is(err, graph.ErrGraphRevisionMismatch) {
		t.Fatalf("resume after member path change = %v, want ErrGraphRevisionMismatch", err)
	}
}

func TestDispatchMemberChainUsesPreForLaterNodes(t *testing.T) {
	g := graph.NewGraph[string, string]("dispatch-chain")
	if err := g.SetStateFactory(func(_ context.Context, _ string) (*dispatchChainState, error) {
		return &dispatchChainState{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	planner := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
		return input, nil
	})
	if err := g.AddNode("planner", planner,
		graph.WithPreHandler(func(_ context.Context, s *dispatchChainState) (string, error) { return "go", nil }),
	); err != nil {
		t.Fatal(err)
	}
	if err := graph.AddDispatch(g, "planner", []string{"fetch", "parse"}, "join",
		func(_ context.Context, _ *dispatchChainState) ([]graph.DispatchTask, error) {
			return []graph.DispatchTask{graph.Send("fetch", "raw")}, nil
		}); err != nil {
		t.Fatal(err)
	}
	fetch := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
		return "fetched-" + input, nil
	})
	if err := g.AddNode("fetch", fetch,
		graph.WithPostHandler(func(_ context.Context, s *dispatchChainState, output string) error {
			s.Results = append(s.Results, output)
			return nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	parse := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
		return "parsed-" + input, nil
	})
	if err := g.AddNode("parse", parse,
		graph.WithPreHandler(func(_ context.Context, s *dispatchChainState) (string, error) {
			return s.Results[len(s.Results)-1], nil
		}),
		graph.WithPostHandler(func(_ context.Context, s *dispatchChainState, output string) error {
			s.Results = append(s.Results, output)
			return nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	join := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
		return input, nil
	})
	if err := g.AddNode("join", join,
		graph.WithPreHandler(func(_ context.Context, s *dispatchChainState) (string, error) {
			return strings.Join(s.Results, "|"), nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{
		{graph.Start, "planner"}, {"fetch", "parse"}, {"parse", "join"}, {"join", graph.End},
	} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	if err := graph.SetStepMerger(g, func(_ context.Context, base *dispatchChainState, results []graph.StepResult[*dispatchChainState]) error {
		base.Results = nil
		for _, r := range results {
			base.Results = append(base.Results, r.State.Results...)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	out, err := mustCompile(t, g).Invoke(context.Background(), "x")
	if err != nil {
		t.Fatal(err)
	}
	if out != "fetched-raw|parsed-fetched-raw" {
		t.Fatalf("output=%q", out)
	}
}

// 清单非法（未登记目标、输入类型不符、JSON 不可编码）时整份清单
// 被拒绝，一个成员也不会启动。
func TestDispatchRejectsInvalidManifest(t *testing.T) {
	for name, members := range map[string]func(*dispatchState) []graph.DispatchTask{
		"unknown-target": func(*dispatchState) []graph.DispatchTask {
			return []graph.DispatchTask{graph.Send("missing", "x")}
		},
		"wrong-input-type": func(*dispatchState) []graph.DispatchTask {
			return []graph.DispatchTask{graph.Send("worker", 42)}
		},
		"unencodable-input": func(*dispatchState) []graph.DispatchTask {
			return []graph.DispatchTask{graph.Send("worker", func() {})}
		},
	} {
		t.Run(name, func(t *testing.T) {
			g := newDispatchGraph(t, members, nil, nil)
			_, err := mustCompile(t, g).Invoke(context.Background(), "x")
			if err == nil {
				t.Fatal("expected manifest rejection")
			}
		})
	}
}

// 持久化开启：登记后崩溃恢复直接读取清单，不重新调用生成函数，
// 已完成成员复用结果。
func TestDispatchCheckpointPersistsManifest(t *testing.T) {
	var calls atomic.Int32
	g := newDispatchGraph(t, func(*dispatchState) []graph.DispatchTask {
		return []graph.DispatchTask{graph.Send("worker", "a"), graph.Send("worker", "b")}
	}, &calls, nil)
	store := state.NewMemoryStore()
	compiled := mustCompile(t, g)
	out, err := compiled.Invoke(context.Background(), "x", graph.WithCheckpoint(store, "conv"))
	if err != nil {
		t.Fatal(err)
	}
	if out != "w:a,w:b" || calls.Load() != 1 {
		t.Fatalf("output=%q calls=%d", out, calls.Load())
	}
}

// 编译期校验：分派来源不得混用普通出边、目标必须可达汇合点、
// 成员路径上的后续节点需要 PreHandler。
func TestDispatchCompileValidation(t *testing.T) {
	t.Run("mixed-edges", func(t *testing.T) {
		g := newDispatchGraph(t, func(*dispatchState) []graph.DispatchTask { return nil }, nil, nil)
		if err := g.AddEdge("planner", "join"); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Compile(); err == nil {
			t.Fatal("dispatch source with ordinary edge must fail")
		}
	})
	t.Run("unreachable-join", func(t *testing.T) {
		g := graph.NewGraph[string, string]("bad")
		if err := g.SetStateFactory(func(_ context.Context, _ string) (*dispatchChainState, error) {
			return &dispatchChainState{}, nil
		}); err != nil {
			t.Fatal(err)
		}
		noop := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
			return input, nil
		})
		if err := g.AddNode("planner", noop, graph.WithPreHandler(func(_ context.Context, _ *dispatchChainState) (string, error) { return "x", nil })); err != nil {
			t.Fatal(err)
		}
		if err := g.AddNode("worker", noop); err != nil {
			t.Fatal(err)
		}
		if err := g.AddNode("join", noop, graph.WithPreHandler(func(_ context.Context, _ *dispatchChainState) (string, error) { return "x", nil })); err != nil {
			t.Fatal(err)
		}
		if err := g.AddNode("sink", noop, graph.WithPreHandler(func(_ context.Context, _ *dispatchChainState) (string, error) { return "x", nil })); err != nil {
			t.Fatal(err)
		}
		// worker → sink 直接到 END，不到达 join。
		for _, edge := range [][2]string{
			{graph.Start, "planner"}, {"worker", "sink"}, {"sink", graph.End}, {"join", graph.End},
		} {
			if err := g.AddEdge(edge[0], edge[1]); err != nil {
				t.Fatal(err)
			}
		}
		if err := graph.AddDispatch(g, "planner", []string{"worker"}, "join",
			func(_ context.Context, _ *dispatchChainState) ([]graph.DispatchTask, error) {
				return []graph.DispatchTask{graph.Send("worker", "x")}, nil
			}); err != nil {
			t.Fatal(err)
		}
		if err := graph.SetStepMerger(g, func(_ context.Context, _ *dispatchChainState, _ []graph.StepResult[*dispatchChainState]) error {
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Compile(); err == nil || !strings.Contains(err.Error(), "reach join") {
			t.Fatalf("expected unreachable join error, got %v", err)
		}
	})
	t.Run("requires-merger", func(t *testing.T) {
		g := graph.NewGraph[string, string]("nomerger")
		if err := g.SetStateFactory(func(_ context.Context, _ string) (*dispatchChainState, error) {
			return &dispatchChainState{}, nil
		}); err != nil {
			t.Fatal(err)
		}
		noop := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
			return input, nil
		})
		if err := g.AddNode("planner", noop, graph.WithPreHandler(func(_ context.Context, _ *dispatchChainState) (string, error) { return "x", nil })); err != nil {
			t.Fatal(err)
		}
		if err := g.AddNode("worker", noop); err != nil {
			t.Fatal(err)
		}
		if err := g.AddNode("join", noop, graph.WithPreHandler(func(_ context.Context, _ *dispatchChainState) (string, error) { return "x", nil })); err != nil {
			t.Fatal(err)
		}
		for _, edge := range [][2]string{
			{graph.Start, "planner"}, {"worker", "join"}, {"join", graph.End},
		} {
			if err := g.AddEdge(edge[0], edge[1]); err != nil {
				t.Fatal(err)
			}
		}
		if err := graph.AddDispatch(g, "planner", []string{"worker"}, "join",
			func(_ context.Context, _ *dispatchChainState) ([]graph.DispatchTask, error) {
				return []graph.DispatchTask{graph.Send("worker", "x")}, nil
			}); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Compile(); err == nil || !strings.Contains(err.Error(), "merger") {
			t.Fatalf("expected merger requirement, got %v", err)
		}
	})
	t.Run("duplicate-registration", func(t *testing.T) {
		g := graph.NewGraph[string, string]("dup")
		err1 := graph.AddDispatch(g, "p", []string{"w"}, "j", func(_ context.Context, _ *dispatchChainState) ([]graph.DispatchTask, error) { return nil, nil })
		err2 := graph.AddDispatch(g, "p", []string{"w"}, "j", func(_ context.Context, _ *dispatchChainState) ([]graph.DispatchTask, error) { return nil, nil })
		if err1 != nil || err2 == nil {
			t.Fatalf("duplicate dispatch registration: first=%v second=%v", err1, err2)
		}
	})
}

// 恢复清单已持久化后，清单生成函数不再被调用——通过已保存的组实例
// 直接恢复执行。
func TestDispatchResumeReusesSavedManifest(t *testing.T) {
	var calls atomic.Int32
	g := newDispatchGraph(t, func(*dispatchState) []graph.DispatchTask {
		return []graph.DispatchTask{graph.Send("worker", "a"), graph.Send("worker", "b")}
	}, &calls, nil)
	store := state.NewMemoryStore()
	compiled := mustCompile(t, g)
	if _, err := compiled.Invoke(context.Background(), "x", graph.WithCheckpoint(store, "conv")); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("manifest calls=%d", calls.Load())
	}
	// 已保存的调度清单里应含动态组与两个成员。
	loaded, found, err := store.Load(context.Background(), "conv")
	if err != nil || !found {
		t.Fatalf("load found=%v err=%v", found, err)
	}
	if loaded.Execution == nil {
		t.Fatal("missing execution progress")
	}
}

// 成员内中断：清单在登记边界落盘，恢复时不再生成清单；已完成成员
// 不重跑，挂起成员从入口任务恢复。
func TestDispatchSuspendResumeKeepsManifest(t *testing.T) {
	var calls, aRuns, bRuns atomic.Int32
	g := newDispatchGraph(t, func(*dispatchState) []graph.DispatchTask {
		return []graph.DispatchTask{graph.Send("worker", "a"), graph.Send("worker", "b")}
	}, &calls, func(ctx context.Context, input string) (string, error) {
		switch input {
		case "a":
			aRuns.Add(1)
		case "b":
			bRuns.Add(1)
			if _, err := graph.Interrupt(ctx, "approve-"+input); err != nil {
				return "", err
			}
		}
		return "w:" + input, nil
	})
	store := state.NewMemoryStore()
	compiled := mustCompile(t, g)
	_, err := compiled.Invoke(context.Background(), "x", graph.WithCheckpoint(store, "conv"))
	var stopped *graph.InterruptedError
	if !errors.As(err, &stopped) {
		t.Fatalf("expected interruption, got %v", err)
	}
	if calls.Load() != 1 || aRuns.Load() != 1 || bRuns.Load() != 1 {
		t.Fatalf("first run calls=%d a=%d b=%d", calls.Load(), aRuns.Load(), bRuns.Load())
	}
	questionID := ""
	for _, record := range stopped.PendingInterrupts() {
		if !record.Answered {
			questionID = record.ID
		}
	}
	if questionID == "" {
		t.Fatal("no pending question recorded")
	}
	out, err := compiled.Resume(context.Background(),
		graph.WithCheckpoint(store, "conv"), graph.WithResume(questionID, "yes"))
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if out != "w:a,w:b" {
		t.Fatalf("resumed output=%q", out)
	}
	if calls.Load() != 1 {
		t.Fatalf("manifest regenerated after resume: calls=%d", calls.Load())
	}
	if aRuns.Load() != 1 || bRuns.Load() != 2 {
		t.Fatalf("member reruns a=%d b=%d, want a=1 b=2", aRuns.Load(), bRuns.Load())
	}
}
