package graph_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/state"
)

// 复核 P1-1：分支输出已消费进 State、后续只依赖 State 的路径不应强制
// 保存原输出——map 中的 int 经 JSON 往返变 float64，展开前同样用法正常。
// sync/async 各验一次；兄弟分支挂起恢复后完成分支不重跑。
func TestReviewParallelMemberMapOutputCompletes(t *testing.T) {
	type stateT struct {
		Text  string `json:"text"`
		Count int    `json:"count"`
	}
	for _, mode := range []graph.Durability{graph.DurabilitySync, graph.DurabilityAsync} {
		t.Run(string(mode), func(t *testing.T) {
			var aRuns atomic.Int32
			g := graph.NewGraph[string, string]("map-out")
			must := func(err error) {
				t.Helper()
				if err != nil {
					t.Fatal(err)
				}
			}
			must(g.SetStateFactory(func(_ context.Context, text string) (*stateT, error) {
				return &stateT{Text: text}, nil
			}))
			must(g.AddParallelNode("fan", []graph.ParallelBranch{
				{Name: "a", Node: runnable.Func[string, map[string]any](func(_ context.Context, s string, _ ...runnable.Option) (map[string]any, error) {
					aRuns.Add(1)
					return map[string]any{"count": len(s), "raw": s}, nil
				}), Pre: func(_ context.Context, s *stateT) (string, error) { return s.Text, nil },
					Post: func(_ context.Context, s *stateT, out map[string]any) error {
						s.Count = out["count"].(int)
						return nil
					}},
				{Name: "b", Node: runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
					return s + ":b", nil
				}), Pre: func(_ context.Context, s *stateT) (string, error) { return s.Text, nil },
					Post: func(_ context.Context, s *stateT, out string) error { s.Text = out; return nil }},
			}, graph.WithStateMerger(func(_ context.Context, s *stateT, branches []*stateT) error {
				s.Text = branches[1].Text
				s.Count = branches[0].Count + len(branches)
				return nil
			})))
			must(g.AddNode("sink", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
				return s, nil
			}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) {
				return fmt.Sprintf("%s#%d", s.Text, s.Count), nil
			})))
			must(g.AddEdge(graph.Start, "fan"))
			must(g.AddEdge("fan", "sink"))
			must(g.AddEdge("sink", graph.End))

			store := state.NewMemoryStore()
			out, err := mustCompile(t, g).Invoke(context.Background(), "base",
				graph.WithCheckpoint(store, "map-out"), graph.WithDurability(mode))
			if err != nil {
				t.Fatalf("invoke: %v", err)
			}
			// a 的局部态：Count=4；b 的局部态：Text="base:b"。合并后 Count=6。
			if out != "base:b#6" {
				t.Fatalf("out=%q", out)
			}
			if aRuns.Load() != 1 {
				t.Fatalf("aRuns=%d", aRuns.Load())
			}
		})
	}
}

// 复核 P1-1：输出不进 State 恢复的分支与挂起兄弟共存时，完成分支不重跑。
func TestReviewParallelMemberMapOutputSiblingSuspend(t *testing.T) {
	type stateT struct {
		Text  string `json:"text"`
		Count int    `json:"count"`
	}
	for _, mode := range []graph.Durability{graph.DurabilitySync, graph.DurabilityAsync} {
		t.Run(string(mode), func(t *testing.T) {
			var aRuns, bRuns atomic.Int32
			g := graph.NewGraph[string, string]("map-out-susp")
			must := func(err error) {
				t.Helper()
				if err != nil {
					t.Fatal(err)
				}
			}
			must(g.SetStateFactory(func(_ context.Context, text string) (*stateT, error) {
				return &stateT{Text: text}, nil
			}))
			must(g.AddParallelNode("fan", []graph.ParallelBranch{
				{Name: "a", Node: runnable.Func[string, map[string]any](func(_ context.Context, s string, _ ...runnable.Option) (map[string]any, error) {
					aRuns.Add(1)
					return map[string]any{"count": len(s)}, nil
				}), Pre: func(_ context.Context, s *stateT) (string, error) { return s.Text, nil },
					Post: func(_ context.Context, s *stateT, out map[string]any) error {
						s.Count = out["count"].(int)
						s.Text = "a-done"
						return nil
					}},
				{Name: "b", Node: runnable.Func[string, string](func(ctx context.Context, s string, _ ...runnable.Option) (string, error) {
					bRuns.Add(1)
					answer, err := graph.Interrupt(ctx, "approve b")
					if err != nil {
						return "", err
					}
					return s + ":" + fmt.Sprint(answer), nil
				}), Pre: func(_ context.Context, s *stateT) (string, error) { return s.Text, nil },
					Post: func(_ context.Context, s *stateT, out string) error { s.Text = out; return nil }},
			}, graph.WithStateMerger(func(_ context.Context, s *stateT, branches []*stateT) error {
				s.Text = branches[0].Text + "|" + branches[1].Text
				s.Count += len(branches)
				return nil
			})))
			must(g.AddNode("sink", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
				return s, nil
			}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) {
				return fmt.Sprintf("%s#%d", s.Text, s.Count), nil
			})))
			must(g.AddEdge(graph.Start, "fan"))
			must(g.AddEdge("fan", "sink"))
			must(g.AddEdge("sink", graph.End))

			store := state.NewMemoryStore()
			compiled := mustCompile(t, g)
			opt := graph.WithCheckpoint(store, "map-out-susp")
			_, err := compiled.Invoke(context.Background(), "base", opt, graph.WithDurability(mode))
			pending := interruptedPending(t, err)
			if len(pending) != 1 {
				t.Fatalf("pending=%v", pending)
			}
			out, err := compiled.Resume(context.Background(), opt,
				graph.WithResume(pending[0].ID, "yes"), graph.WithDurability(mode))
			if err != nil {
				t.Fatalf("resume: %v", err)
			}
			if out != "a-done|base:yes#2" {
				t.Fatalf("out=%q", out)
			}
			if aRuns.Load() != 1 || bRuns.Load() != 2 {
				t.Fatalf("a=%d b=%d (completed member must not re-run)", aRuns.Load(), bRuns.Load())
			}
		})
	}
}

// 复核 P1-1：出边条件读原输出时输出必须保存——恢复后按持久化的输出值
// 重新选路；兄弟挂起恢复走到正确分支且完成成员不重跑。
func TestReviewParallelMemberOutputConditionResumes(t *testing.T) {
	type stateT struct {
		Mark string `json:"mark"`
	}
	var aRuns atomic.Int32
	var sawOutput atomic.Bool
	g := graph.NewGraph[string, string]("out-cond")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(g.SetStateFactory(func(_ context.Context, _ string) (*stateT, error) { return &stateT{}, nil }))
	must(g.AddParallelNode("fan", []graph.ParallelBranch{
		{Name: "a", Node: runnable.Func[string, map[string]any](func(_ context.Context, s string, _ ...runnable.Option) (map[string]any, error) {
			aRuns.Add(1)
			return map[string]any{"verdict": "p"}, nil
		}), Pre: func(_ context.Context, s *stateT) (string, error) { return s.Mark, nil },
			Post: func(_ context.Context, s *stateT, _ map[string]any) error { s.Mark = "a-done"; return nil }},
		{Name: "b", Node: runnable.Func[string, string](func(ctx context.Context, s string, _ ...runnable.Option) (string, error) {
			if _, err := graph.Interrupt(ctx, "approve b"); err != nil {
				return "", err
			}
			return s, nil
		}), Pre: func(_ context.Context, s *stateT) (string, error) { return s.Mark, nil },
			Post: func(_ context.Context, s *stateT, _ string) error { s.Mark = "b-done"; return nil }},
	}, graph.WithStateMerger(func(_ context.Context, s *stateT, branches []*stateT) error {
		s.Mark = branches[0].Mark + "|" + branches[1].Mark
		return nil
	})))
	for _, id := range []string{"p", "q"} {
		id := id
		must(g.AddNode(id, runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
			return s, nil
		}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Mark, nil }),
			graph.WithPostHandler(func(_ context.Context, s *stateT, _ string) error { s.Mark += "|" + id; return nil })))
	}
	must(g.AddNode("sink", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
		return s, nil
	}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Mark, nil })))
	must(g.AddEdge(graph.Start, "fan"))
	must(g.AddBranch("fan/a", "p", "verdict", func(_ context.Context, out map[string]any) (bool, error) {
		matched := out != nil && out["verdict"] == "p"
		if matched {
			sawOutput.Store(true)
		}
		return matched, nil
	}))
	must(g.AddEdge("fan/a", "q"))
	must(g.AddEdge("p", "fan"))
	must(g.AddEdge("q", "fan"))
	must(g.AddEdge("fan", "sink"))
	must(g.AddEdge("sink", graph.End))

	store := state.NewMemoryStore()
	compiled := mustCompile(t, g)
	opt := graph.WithCheckpoint(store, "out-cond")
	_, err := compiled.Invoke(context.Background(), "base", opt)
	pending := interruptedPending(t, err)
	out, err := compiled.Resume(context.Background(), opt, graph.WithResume(pending[0].ID, "yes"))
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	// a 完成态经保存的输出值重选到 p；b 完成后合并。
	if out != "a-done|p|b-done" {
		t.Fatalf("out=%q", out)
	}
	if aRuns.Load() != 1 {
		t.Fatalf("a=%d (completed member must not re-run)", aRuns.Load())
	}
	if !sawOutput.Load() {
		t.Fatal("resume routing must re-evaluate the saved output value")
	}
}

// 复核 P1-1：出边条件读 State 时原输出不参与恢复——输出含 int 的成员
// 完成记录不保存输出，恢复后仍按局部 State 选到正确分支。
func TestReviewParallelMemberStateConditionResumes(t *testing.T) {
	type stateT struct {
		Verdict string `json:"verdict"`
		Mark    string `json:"mark"`
	}
	var aRuns atomic.Int32
	g := graph.NewGraph[string, string]("state-cond")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(g.SetStateFactory(func(_ context.Context, _ string) (*stateT, error) { return &stateT{}, nil }))
	must(g.AddParallelNode("fan", []graph.ParallelBranch{
		{Name: "a", Node: runnable.Func[string, map[string]any](func(_ context.Context, s string, _ ...runnable.Option) (map[string]any, error) {
			aRuns.Add(1)
			return map[string]any{"verdict": "p", "count": 1}, nil
		}), Pre: func(_ context.Context, s *stateT) (string, error) { return s.Mark, nil },
			Post: func(_ context.Context, s *stateT, _ map[string]any) error {
				s.Verdict = "p"
				s.Mark = "a-done"
				return nil
			}},
		{Name: "b", Node: runnable.Func[string, string](func(ctx context.Context, s string, _ ...runnable.Option) (string, error) {
			if _, err := graph.Interrupt(ctx, "approve b"); err != nil {
				return "", err
			}
			return s, nil
		}), Pre: func(_ context.Context, s *stateT) (string, error) { return s.Mark, nil },
			Post: func(_ context.Context, s *stateT, _ string) error { s.Mark = "b-done"; return nil }},
	}, graph.WithStateMerger(func(_ context.Context, s *stateT, branches []*stateT) error {
		s.Mark = branches[0].Mark + "|" + branches[1].Mark
		return nil
	})))
	for _, id := range []string{"p", "q"} {
		id := id
		must(g.AddNode(id, runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
			return s, nil
		}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Mark, nil }),
			graph.WithPostHandler(func(_ context.Context, s *stateT, _ string) error { s.Mark += "|" + id; return nil })))
	}
	must(g.AddNode("sink", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
		return s, nil
	}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Mark, nil })))
	must(g.AddEdge(graph.Start, "fan"))
	must(g.AddBranch("fan/a", "p", "verdict", func(_ context.Context, s *stateT) (bool, error) {
		return s.Verdict == "p", nil
	}))
	must(g.AddEdge("fan/a", "q"))
	must(g.AddEdge("p", "fan"))
	must(g.AddEdge("q", "fan"))
	must(g.AddEdge("fan", "sink"))
	must(g.AddEdge("sink", graph.End))

	store := state.NewMemoryStore()
	compiled := mustCompile(t, g)
	opt := graph.WithCheckpoint(store, "state-cond")
	_, err := compiled.Invoke(context.Background(), "base", opt)
	pending := interruptedPending(t, err)
	out, err := compiled.Resume(context.Background(), opt, graph.WithResume(pending[0].ID, "yes"))
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if out != "a-done|p|b-done" {
		t.Fatalf("out=%q", out)
	}
	if aRuns.Load() != 1 {
		t.Fatalf("a=%d", aRuns.Load())
	}
}

// 复核 P1-1：输出确实需要保存而类型无法稳定恢复时，错误必须给出输出侧
// 适配提示，不能误导使用者去改输入 PreHandler。
func TestReviewParallelMemberUnstableOutputFailsWithHint(t *testing.T) {
	type stateT struct {
		Mark string `json:"mark"`
	}
	g := graph.NewGraph[string, string]("unstable-out")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(g.SetStateFactory(func(_ context.Context, _ string) (*stateT, error) { return &stateT{}, nil }))
	must(g.AddParallelNode("fan", []graph.ParallelBranch{
		{Name: "a", Node: runnable.Func[string, map[string]any](func(_ context.Context, _ string, _ ...runnable.Option) (map[string]any, error) {
			return map[string]any{"verdict": "p", "count": 1}, nil
		}), Pre: func(_ context.Context, s *stateT) (string, error) { return s.Mark, nil },
			Post: func(_ context.Context, s *stateT, _ map[string]any) error { s.Mark = "a"; return nil }},
		{Name: "b", Node: runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
			return s, nil
		}), Pre: func(_ context.Context, s *stateT) (string, error) { return s.Mark, nil },
			Post: func(_ context.Context, s *stateT, _ string) error { return nil }},
	}, graph.WithStateMerger(func(_ context.Context, _ *stateT, _ []*stateT) error { return nil })))
	must(g.AddNode("p", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
		return s, nil
	}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Mark, nil })))
	must(g.AddNode("sink", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
		return s, nil
	}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Mark, nil })))
	must(g.AddEdge(graph.Start, "fan"))
	must(g.AddBranch("fan/a", "p", "verdict", func(_ context.Context, out map[string]any) (bool, error) {
		return out["verdict"] == "p", nil
	}))
	must(g.AddEdge("p", "fan"))
	must(g.AddEdge("fan/a", "fan"))
	must(g.AddEdge("fan", "sink"))
	must(g.AddEdge("sink", graph.End))

	_, err := mustCompile(t, g).Invoke(context.Background(), "base",
		graph.WithCheckpoint(state.NewMemoryStore(), "unstable"))
	if err == nil {
		t.Fatal("unstable output must fail when routing needs it")
	}
	msg := err.Error()
	if !strings.Contains(msg, "round-trip") || !strings.Contains(msg, "PostHandler") {
		t.Fatalf("err=%v (want output-side hint)", err)
	}
	if strings.Contains(msg, "add a PreHandler") {
		t.Fatalf("err=%v (must not point at input PreHandler)", err)
	}
}

// 复核 P1-2：组外边（静态与条件）进入成员路径的接续节点在编译期拒绝；
// 组外边进入公共组入口仍然合法。
func TestReviewParallelOutsideEdgeIntoContinuationRejected(t *testing.T) {
	type stateT struct {
		Text string `json:"text"`
	}
	build := func(extraEdge func(g *graph.Graph[string, string]) error) *graph.Graph[string, string] {
		g := graph.NewGraph[string, string]("outside")
		if err := g.SetStateFactory(func(_ context.Context, text string) (*stateT, error) {
			return &stateT{Text: text}, nil
		}); err != nil {
			t.Fatal(err)
		}
		must := func(err error) {
			t.Helper()
			if err != nil {
				t.Fatal(err)
			}
		}
		must(g.AddNode("route", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
			return s, nil
		}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Text, nil })))
		must(g.AddParallelNode("fan", []graph.ParallelBranch{
			{Name: "a", Node: runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
				return s, nil
			}), Pre: func(_ context.Context, s *stateT) (string, error) { return s.Text, nil },
				Post: func(_ context.Context, s *stateT, _ string) error { return nil }},
		}, graph.WithStateMerger(func(_ context.Context, _ *stateT, _ []*stateT) error { return nil })))
		must(g.AddNode("extra", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
			return s, nil
		}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Text, nil })))
		must(g.AddNode("sink", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
			return s, nil
		}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Text, nil })))
		must(g.AddEdge(graph.Start, "route"))
		must(g.AddEdge("route", "fan"))
		must(g.AddEdge("fan/a", "extra"))
		must(g.AddEdge("extra", "fan"))
		must(g.AddEdge("fan", "sink"))
		must(g.AddEdge("sink", graph.End))
		if err := extraEdge(g); err != nil {
			t.Fatal(err)
		}
		return g
	}

	// 静态边直接进入成员接续节点。
	g := build(func(g *graph.Graph[string, string]) error { return g.AddEdge("route", "extra") })
	if _, err := g.Compile(); err == nil ||
		!strings.Contains(err.Error(), "route") || !strings.Contains(err.Error(), "extra") ||
		!strings.Contains(err.Error(), "fan") {
		t.Fatalf("static edge compile err=%v", err)
	}

	// 条件边直接进入成员接续节点同样拒绝。
	g = build(func(g *graph.Graph[string, string]) error {
		return g.AddBranch("route", "extra", "pick", func(_ context.Context, s *stateT) (bool, error) {
			return s.Text == "x", nil
		})
	})
	if _, err := g.Compile(); err == nil || !strings.Contains(err.Error(), "extra") {
		t.Fatalf("conditional edge compile err=%v", err)
	}

	// 组外边进入公共组入口合法：route → fan 已在基线中，整体可编译运行。
	g = build(func(g *graph.Graph[string, string]) error { return nil })
	out, err := mustCompile(t, g).Invoke(context.Background(), "base")
	if err != nil {
		t.Fatalf("entry edge invoke: %v", err)
	}
	if out != "base" {
		t.Fatalf("out=%q", out)
	}
}

// 复核 P1-2：恢复数据或调度产生无活跃组的汇合任务时返回可识别的框架
// 错误，不能进入空组件调用。
func TestReviewParallelJoinTaskWithoutGroupFails(t *testing.T) {
	type stateT struct {
		Text string `json:"text"`
	}
	g := graph.NewGraph[string, string]("no-group-join")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(g.SetStateFactory(func(_ context.Context, text string) (*stateT, error) {
		return &stateT{Text: text}, nil
	}))
	must(g.AddParallelNode("fan", []graph.ParallelBranch{
		{Name: "a", Node: runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
			return s, nil
		}), Pre: func(_ context.Context, s *stateT) (string, error) { return s.Text, nil },
			Post: func(_ context.Context, s *stateT, _ string) error { return nil }},
		{Name: "b", Node: runnable.Func[string, string](func(ctx context.Context, s string, _ ...runnable.Option) (string, error) {
			if _, err := graph.Interrupt(ctx, "approve b"); err != nil {
				return "", err
			}
			return s, nil
		}), Pre: func(_ context.Context, s *stateT) (string, error) { return s.Text, nil },
			Post: func(_ context.Context, s *stateT, _ string) error { return nil }},
	}, graph.WithStateMerger(func(_ context.Context, _ *stateT, _ []*stateT) error { return nil })))
	must(g.AddNode("sink", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
		return s, nil
	}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Text, nil })))
	must(g.AddEdge(graph.Start, "fan"))
	must(g.AddEdge("fan", "sink"))
	must(g.AddEdge("sink", graph.End))

	store := state.NewMemoryStore()
	compiled := mustCompile(t, g)
	opt := graph.WithCheckpoint(store, "no-group-join")
	_, err := compiled.Invoke(context.Background(), "base", opt)
	interruptedPending(t, err)

	// 伪造损坏调度：任务直接指向内部汇合节点但不带组实例。
	record, found, err := store.Load(context.Background(), "no-group-join")
	if err != nil || !found {
		t.Fatalf("load: found=%v err=%v", found, err)
	}
	var progress map[string]json.RawMessage
	if err := json.Unmarshal(record.Execution, &progress); err != nil {
		t.Fatal(err)
	}
	progress["resume_node"] = mustJSON(t, "fan/__join__")
	progress["schedule"] = json.RawMessage(`{"tasks":[{"id":"task-9","node":"fan/__join__"}]}`)
	record.Execution = mustJSON(t, progress)
	if _, err := store.Save(context.Background(), record); err != nil {
		t.Fatal(err)
	}

	_, err = compiled.Resume(context.Background(), opt)
	if err == nil || !strings.Contains(err.Error(), "active group") {
		t.Fatalf("resume err=%v (want framework error, not panic)", err)
	}
}
