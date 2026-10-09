package graph_test

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/state"
)

// 复核：成员输出已消费进局部 State、后续只依赖 State 的路径不应强制
// 保存原输出——map 中的 int 经 JSON 往返变 float64，非成员任务同样用法
// 正常。sync/async 各验一次；兄弟分支挂起恢复后完成分支不重跑。
func TestReviewForkMemberMapOutputCompletes(t *testing.T) {
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
			must(g.AddNode("q", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
				return s, nil
			}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Text, nil })))
			must(g.AddNode("a", runnable.Func[string, map[string]any](func(_ context.Context, s string, _ ...runnable.Option) (map[string]any, error) {
				aRuns.Add(1)
				return map[string]any{"count": len(s), "raw": s}, nil
			}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Text, nil }),
				graph.WithPostHandler(func(_ context.Context, s *stateT, out map[string]any) error {
					s.Count = out["count"].(int)
					return nil
				})))
			must(g.AddNode("b", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
				return s + ":b", nil
			}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Text, nil }),
				graph.WithPostHandler(func(_ context.Context, s *stateT, out string) error { s.Text = out; return nil })))
			must(g.AddNode("sink", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
				return s, nil
			}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) {
				return fmt.Sprintf("%s#%d", s.Text, s.Count), nil
			})))
			must(g.AddEdge(graph.Start, "q"))
			must(g.AddFork("q", "a", "b"))
			must(g.AddJoin("q", "sink"))
			must(g.AddEdge("a", "sink"))
			must(g.AddEdge("b", "sink"))
			must(g.AddEdge("sink", graph.End))
			must(graph.SetForkMerger(g, "q", func(_ context.Context, s *stateT, rs []graph.StepResult[*stateT]) error {
				s.Text = rs[1].State.Text
				s.Count = rs[0].State.Count + len(rs)
				return nil
			}))

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

// 复核：输出不进 State 恢复的成员与挂起兄弟共存时，完成成员不重跑。
func TestReviewForkMemberMapOutputSiblingSuspend(t *testing.T) {
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
			must(g.AddNode("q", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
				return s, nil
			}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Text, nil })))
			must(g.AddNode("a", runnable.Func[string, map[string]any](func(_ context.Context, s string, _ ...runnable.Option) (map[string]any, error) {
				aRuns.Add(1)
				return map[string]any{"count": len(s)}, nil
			}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Text, nil }),
				graph.WithPostHandler(func(_ context.Context, s *stateT, out map[string]any) error {
					s.Count = out["count"].(int)
					s.Text = "a-done"
					return nil
				})))
			must(g.AddNode("b", runnable.Func[string, string](func(ctx context.Context, s string, _ ...runnable.Option) (string, error) {
				bRuns.Add(1)
				answer, err := graph.Interrupt(ctx, "approve b")
				if err != nil {
					return "", err
				}
				return s + ":" + fmt.Sprint(answer), nil
			}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Text, nil }),
				graph.WithPostHandler(func(_ context.Context, s *stateT, out string) error { s.Text = out; return nil })))
			must(g.AddNode("sink", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
				return s, nil
			}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) {
				return fmt.Sprintf("%s#%d", s.Text, s.Count), nil
			})))
			must(g.AddEdge(graph.Start, "q"))
			must(g.AddFork("q", "a", "b"))
			must(g.AddJoin("q", "sink"))
			must(g.AddEdge("a", "sink"))
			must(g.AddEdge("b", "sink"))
			must(g.AddEdge("sink", graph.End))
			must(graph.SetForkMerger(g, "q", func(_ context.Context, s *stateT, rs []graph.StepResult[*stateT]) error {
				s.Text = rs[0].State.Text + "|" + rs[1].State.Text
				s.Count += len(rs)
				return nil
			}))

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

// 复核：出边条件读原输出时输出必须保存——恢复后按持久化的输出值
// 重新选路；兄弟挂起恢复走到正确分支且完成成员不重跑。
func TestReviewForkMemberOutputConditionResumes(t *testing.T) {
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
	must(g.AddNode("q", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
		return s, nil
	}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Mark, nil })))
	must(g.AddNode("a", runnable.Func[string, map[string]any](func(_ context.Context, s string, _ ...runnable.Option) (map[string]any, error) {
		aRuns.Add(1)
		return map[string]any{"verdict": "p"}, nil
	}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Mark, nil }),
		graph.WithPostHandler(func(_ context.Context, s *stateT, _ map[string]any) error { s.Mark = "a-done"; return nil })))
	must(g.AddNode("b", runnable.Func[string, string](func(ctx context.Context, s string, _ ...runnable.Option) (string, error) {
		if _, err := graph.Interrupt(ctx, "approve b"); err != nil {
			return "", err
		}
		return s, nil
	}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Mark, nil }),
		graph.WithPostHandler(func(_ context.Context, s *stateT, _ string) error { s.Mark = "b-done"; return nil })))
	for _, id := range []string{"p", "x"} {
		id := id
		must(g.AddNode(id, runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
			return s, nil
		}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Mark, nil }),
			graph.WithPostHandler(func(_ context.Context, s *stateT, _ string) error { s.Mark += "|" + id; return nil })))
	}
	must(g.AddNode("sink", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
		return s, nil
	}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Mark, nil })))
	must(g.AddEdge(graph.Start, "q"))
	must(g.AddFork("q", "a", "b"))
	must(g.AddJoin("q", "sink"))
	// a 的条件边读原输出：verdict=p 走 p，否则走默认边到 x，二者同支内回到汇合。
	must(g.AddBranch("a", "p", "verdict", func(_ context.Context, out map[string]any) (bool, error) {
		matched := out != nil && out["verdict"] == "p"
		if matched {
			sawOutput.Store(true)
		}
		return matched, nil
	}))
	must(g.AddEdge("a", "x"))
	must(g.AddEdge("p", "sink"))
	must(g.AddEdge("x", "sink"))
	must(g.AddEdge("b", "sink"))
	must(g.AddEdge("sink", graph.End))
	must(graph.SetForkMerger(g, "q", func(_ context.Context, s *stateT, rs []graph.StepResult[*stateT]) error {
		s.Mark = rs[0].State.Mark + "|" + rs[1].State.Mark
		return nil
	}))

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

// 复核：出边条件读 State 时原输出不参与恢复——输出含 int 的成员
// 完成记录不保存输出，恢复后仍按局部 State 选到正确分支。
func TestReviewForkMemberStateConditionResumes(t *testing.T) {
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
	must(g.AddNode("q", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
		return s, nil
	}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Mark, nil })))
	must(g.AddNode("a", runnable.Func[string, map[string]any](func(_ context.Context, s string, _ ...runnable.Option) (map[string]any, error) {
		aRuns.Add(1)
		return map[string]any{"verdict": "p", "count": 1}, nil
	}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Mark, nil }),
		graph.WithPostHandler(func(_ context.Context, s *stateT, _ map[string]any) error {
			s.Verdict = "p"
			s.Mark = "a-done"
			return nil
		})))
	must(g.AddNode("b", runnable.Func[string, string](func(ctx context.Context, s string, _ ...runnable.Option) (string, error) {
		if _, err := graph.Interrupt(ctx, "approve b"); err != nil {
			return "", err
		}
		return s, nil
	}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Mark, nil }),
		graph.WithPostHandler(func(_ context.Context, s *stateT, _ string) error { s.Mark = "b-done"; return nil })))
	for _, id := range []string{"p", "x"} {
		id := id
		must(g.AddNode(id, runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
			return s, nil
		}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Mark, nil }),
			graph.WithPostHandler(func(_ context.Context, s *stateT, _ string) error { s.Mark += "|" + id; return nil })))
	}
	must(g.AddNode("sink", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
		return s, nil
	}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Mark, nil })))
	must(g.AddEdge(graph.Start, "q"))
	must(g.AddFork("q", "a", "b"))
	must(g.AddJoin("q", "sink"))
	must(g.AddBranch("a", "p", "verdict", func(_ context.Context, s *stateT) (bool, error) {
		return s.Verdict == "p", nil
	}))
	must(g.AddEdge("a", "x"))
	must(g.AddEdge("p", "sink"))
	must(g.AddEdge("x", "sink"))
	must(g.AddEdge("b", "sink"))
	must(g.AddEdge("sink", graph.End))
	must(graph.SetForkMerger(g, "q", func(_ context.Context, s *stateT, rs []graph.StepResult[*stateT]) error {
		s.Mark = rs[0].State.Mark + "|" + rs[1].State.Mark
		return nil
	}))

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

// 复核：输出确实需要保存而类型无法稳定恢复时，错误必须给出输出侧
// 适配提示，不能误导使用者去改输入 PreHandler。
func TestReviewForkMemberUnstableOutputFailsWithHint(t *testing.T) {
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
	must(g.AddNode("q", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
		return s, nil
	}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Mark, nil })))
	must(g.AddNode("a", runnable.Func[string, map[string]any](func(_ context.Context, _ string, _ ...runnable.Option) (map[string]any, error) {
		return map[string]any{"verdict": "p", "count": 1}, nil
	}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Mark, nil }),
		graph.WithPostHandler(func(_ context.Context, s *stateT, _ map[string]any) error { s.Mark = "a"; return nil })))
	must(g.AddNode("b", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
		return s, nil
	}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Mark, nil }),
		graph.WithPostHandler(func(_ context.Context, s *stateT, _ string) error { return nil })))
	must(g.AddNode("p", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
		return s, nil
	}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Mark, nil })))
	must(g.AddNode("sink", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
		return s, nil
	}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Mark, nil })))
	must(g.AddEdge(graph.Start, "q"))
	must(g.AddFork("q", "a", "b"))
	must(g.AddJoin("q", "sink"))
	must(g.AddBranch("a", "p", "verdict", func(_ context.Context, out map[string]any) (bool, error) {
		return out["verdict"] == "p", nil
	}))
	must(g.AddEdge("a", "sink"))
	must(g.AddEdge("p", "sink"))
	must(g.AddEdge("b", "sink"))
	must(g.AddEdge("sink", graph.End))
	must(graph.SetForkMerger(g, "q", func(_ context.Context, _ *stateT, _ []graph.StepResult[*stateT]) error { return nil }))

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

// 复核：组外边（静态与条件）进入成员路径的接续节点在编译期拒绝；
// 组外边进入扇出源节点仍然合法。
func TestReviewForkOutsideEdgeIntoContinuationRejected(t *testing.T) {
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
		must(g.AddNode("q", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
			return s, nil
		}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Text, nil })))
		must(g.AddNode("a", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
			return s, nil
		}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Text, nil }),
			graph.WithPostHandler(func(_ context.Context, s *stateT, _ string) error { return nil })))
		must(g.AddNode("b", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
			return s, nil
		}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Text, nil }),
			graph.WithPostHandler(func(_ context.Context, s *stateT, _ string) error { return nil })))
		must(g.AddNode("extra", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
			return s, nil
		}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Text, nil })))
		must(g.AddNode("sink", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
			return s, nil
		}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Text, nil })))
		must(g.AddEdge(graph.Start, "route"))
		must(g.AddEdge("route", "q"))
		must(g.AddFork("q", "a", "b"))
		must(g.AddJoin("q", "sink"))
		must(g.AddEdge("a", "extra"))
		must(g.AddEdge("extra", "sink"))
		must(g.AddEdge("b", "sink"))
		must(g.AddEdge("sink", graph.End))
		must(graph.SetForkMerger(g, "q", func(_ context.Context, _ *stateT, _ []graph.StepResult[*stateT]) error { return nil }))
		if err := extraEdge(g); err != nil {
			t.Fatal(err)
		}
		return g
	}

	// 静态边直接进入成员接续节点。
	g := build(func(g *graph.Graph[string, string]) error { return g.AddEdge("route", "extra") })
	if _, err := g.Compile(); err == nil ||
		!strings.Contains(err.Error(), "route") || !strings.Contains(err.Error(), "extra") {
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

	// 组外边进入扇出源节点合法：route → q 已在基线中，整体可编译运行。
	g = build(func(g *graph.Graph[string, string]) error { return nil })
	out, err := mustCompile(t, g).Invoke(context.Background(), "base")
	if err != nil {
		t.Fatalf("entry edge invoke: %v", err)
	}
	if out != "base" {
		t.Fatalf("out=%q", out)
	}
}
