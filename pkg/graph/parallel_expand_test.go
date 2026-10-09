package graph_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
)

// 阶段 4：并行组编译期展开——分支末端接续连线后仍回到组汇合。
func TestParallelBranchContinuationEdges(t *testing.T) {
	type stateT struct {
		Text   string `json:"text"`
		Extras int    `json:"extras"`
	}
	g := graph.NewGraph[string, string]("expand")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(g.SetStateFactory(func(_ context.Context, text string) (*stateT, error) { return &stateT{Text: text}, nil }))
	must(g.AddParallelNode("fan", []graph.ParallelBranch{
		{Name: "a", Node: runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
			return s + ":a", nil
		}), Pre: func(_ context.Context, s *stateT) (string, error) { return s.Text, nil },
			Post: func(_ context.Context, s *stateT, out string) error { s.Text = out; return nil }},
		{Name: "b", Node: runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
			return s + ":b", nil
		}), Pre: func(_ context.Context, s *stateT) (string, error) { return s.Text, nil },
			Post: func(_ context.Context, s *stateT, out string) error { s.Text = out; return nil }},
	}, graph.WithStateMerger(func(_ context.Context, s *stateT, branches []*stateT) error {
		var parts []string
		for _, b := range branches {
			parts = append(parts, b.Text)
		}
		s.Text = strings.Join(parts, "|")
		s.Extras += len(branches)
		return nil
	})))
	// 分支 a 增加接续节点；分支 b 直连汇合。
	must(g.AddNode("extra", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
		return s + ":extra", nil
	}), graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Text, nil }),
		graph.WithPostHandler(func(_ context.Context, s *stateT, out string) error {
			s.Text = out
			s.Extras++
			return nil
		})))
	must(g.AddEdge(graph.Start, "fan"))
	must(g.AddEdge("fan/a", "extra"))
	must(g.AddEdge("extra", "fan"))
	must(g.AddNode("sink", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) { return s, nil }),
		graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) {
			return fmt.Sprintf("%s#%d", s.Text, s.Extras), nil
		})))
	must(g.AddEdge("fan", "sink"))
	must(g.AddEdge("sink", graph.End))

	out, err := mustCompile(t, g).Invoke(context.Background(), "base")
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	// 声明序合并：a 走 extra，b 直连；组出口到 sink。
	if out != "base:a:extra|base:b#2" {
		t.Fatalf("out=%q", out)
	}
}

// 分支接续路径带条件路由——成员后的普通节点走 Branch 裁决。
func TestParallelBranchContinuationConditional(t *testing.T) {
	type stateT struct {
		Text string `json:"text"`
	}
	g := graph.NewGraph[string, string]("expand-cond")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(g.SetStateFactory(func(_ context.Context, text string) (*stateT, error) { return &stateT{Text: text}, nil }))
	must(g.AddParallelNode("fan", []graph.ParallelBranch{
		{Name: "a", Node: runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
			return s, nil
		}), Pre: func(_ context.Context, s *stateT) (string, error) { return s.Text, nil },
			Post: func(_ context.Context, s *stateT, _ string) error { return nil }},
		{Name: "b", Node: runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
			return s, nil
		}), Pre: func(_ context.Context, s *stateT) (string, error) { return s.Text, nil },
			Post: func(_ context.Context, s *stateT, _ string) error { return nil }},
	}, graph.WithStateMerger(func(_ context.Context, s *stateT, _ []*stateT) error { return nil })))
	hop := runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) { return s, nil })
	for _, id := range []string{"hop", "amid", "mid"} {
		must(g.AddNode(id, hop, graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Text, nil })))
	}
	must(g.AddNode("sink", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) { return s, nil }),
		graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Text, nil })))
	must(g.AddEdge(graph.Start, "fan"))
	must(g.AddEdge("fan/a", "hop"))
	must(g.AddEdge("fan/b", "mid"))
	must(g.AddBranch("hop", "amid", "skip", func(_ context.Context, s *stateT) (bool, error) {
		return s.Text == "skip", nil
	}))
	must(g.AddEdge("hop", "fan"))
	must(g.AddEdge("amid", "fan"))
	must(g.AddEdge("mid", "fan"))
	must(g.AddEdge("fan", "sink"))
	must(g.AddEdge("sink", graph.End))

	compiled := mustCompile(t, g)
	if _, err := compiled.Invoke(context.Background(), "direct"); err != nil {
		t.Fatalf("invoke direct: %v", err)
	}
	if _, err := compiled.Invoke(context.Background(), "skip"); err != nil {
		t.Fatalf("invoke skip: %v", err)
	}
}

// 分支路径离开组边界（不接回组汇合）必须拒绝。
func TestParallelBranchPathEscapesGroupRejected(t *testing.T) {
	type stateT struct {
		Text string `json:"text"`
	}
	g := graph.NewGraph[string, string]("escape")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(g.SetStateFactory(func(_ context.Context, text string) (*stateT, error) { return &stateT{Text: text}, nil }))
	must(g.AddParallelNode("fan", []graph.ParallelBranch{
		{Name: "a", Node: runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) { return s, nil }),
			Pre:  func(_ context.Context, s *stateT) (string, error) { return s.Text, nil },
			Post: func(_ context.Context, s *stateT, _ string) error { return nil }},
	}, graph.WithStateMerger(func(_ context.Context, _ *stateT, _ []*stateT) error { return nil })))
	must(g.AddNode("leak", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) { return s, nil }),
		graph.WithPreHandler(func(_ context.Context, s *stateT) (string, error) { return s.Text, nil })))
	must(g.AddEdge(graph.Start, "fan"))
	must(g.AddEdge("fan/a", "leak"))
	must(g.AddEdge("leak", graph.End))
	must(g.AddEdge("fan", graph.End))

	_, err := g.Compile()
	if err == nil || !strings.Contains(err.Error(), "does not reach") {
		t.Fatalf("compile err=%v", err)
	}
}

// 分支路径成环必须拒绝。
func TestParallelBranchPathCycleRejected(t *testing.T) {
	type stateT struct {
		Text string `json:"text"`
	}
	g := graph.NewGraph[string, string]("cycle")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(g.SetStateFactory(func(_ context.Context, text string) (*stateT, error) { return &stateT{Text: text}, nil }))
	must(g.AddParallelNode("fan", []graph.ParallelBranch{
		{Name: "a", Node: runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) { return s, nil }),
			Pre:  func(_ context.Context, s *stateT) (string, error) { return s.Text, nil },
			Post: func(_ context.Context, s *stateT, _ string) error { return nil }},
	}, graph.WithStateMerger(func(_ context.Context, _ *stateT, _ []*stateT) error { return nil })))
	must(g.AddEdge(graph.Start, "fan"))
	must(g.AddEdge("fan/a", "fan/a"))
	must(g.AddEdge("fan", graph.End))

	_, err := g.Compile()
	if err == nil {
		t.Fatal("cycle must be rejected")
	}
}

// 分支名与汇合身份冲突在登记期拒绝。
func TestParallelJoinNameCollisionRejected(t *testing.T) {
	type stateT struct {
		Text string `json:"text"`
	}
	g := graph.NewGraph[string, string]("collide")
	if err := g.SetStateFactory(func(_ context.Context, text string) (*stateT, error) { return &stateT{Text: text}, nil }); err != nil {
		t.Fatal(err)
	}
	err := g.AddParallelNode("fan", []graph.ParallelBranch{
		{Name: "__join__", Node: runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) { return s, nil }),
			Pre:  func(_ context.Context, s *stateT) (string, error) { return s.Text, nil },
			Post: func(_ context.Context, s *stateT, _ string) error { return nil }},
	}, graph.WithStateMerger(func(_ context.Context, _ *stateT, _ []*stateT) error { return nil }))
	if err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("err=%v", err)
	}
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
