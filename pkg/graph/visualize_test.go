package graph_test

import (
	"context"
	"strings"
	"testing"

	"github.com/goharness/goharness/pkg/graph"
)

type vizState struct {
	Done bool
}

func vizNode(_ context.Context, s *vizState) error {
	s.Done = true
	return nil
}

// 路由图：start→plan 经 Branch 分向 a/b，再各自到 end。
func routedSnapshot(t *testing.T) graph.Snapshot {
	t.Helper()
	app := graph.NewState[vizState]("routed").
		Node("plan", vizNode).
		Node("a", vizNode).
		Node("b", vizNode).
		Start("plan").
		Branch("plan", func(_ context.Context, _ *vizState) (string, error) {
			return "a", nil
		}, "a", "b").
		Edge("a", graph.End).
		Edge("b", graph.End)
	compiled, err := app.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return compiled.Snapshot()
}

func TestSnapshotMermaid(t *testing.T) {
	got := routedSnapshot(t).Mermaid()
	want := `flowchart TD
    plan["plan"]
    a["a"]
    b["b"]
    __start__((start))
    __end__((end))
    __start__ --> plan
    a --> __end__
    b --> __end__
    plan -.-> a
    plan -.-> b
`
	if got != want {
		t.Fatalf("Mermaid 输出不符：\n%s", got)
	}
}

func TestSnapshotDOT(t *testing.T) {
	got := routedSnapshot(t).DOT()
	want := `digraph "routed" {
    rankdir=TB;
    "plan" [label="plan", shape=box];
    "a" [label="a", shape=box];
    "b" [label="b", shape=box];
    "__start__" [label="start", shape=circle];
    "__end__" [label="end", shape=doublecircle];
    "__start__" -> "plan" [];
    "a" -> "__end__" [];
    "b" -> "__end__" [];
    "plan" -> "a" [style=dashed];
    "plan" -> "b" [style=dashed];
}
`
	if got != want {
		t.Fatalf("DOT 输出不符：\n%s", got)
	}
}

// Fork/Join 与动态分派的边必须投影进快照（条件边），可视化不漏边。
func TestSnapshotProjectsForkAndDispatchEdges(t *testing.T) {
	merge := func(_ context.Context, _ *vizState, _ []graph.StepResult[*vizState]) error { return nil }
	app := graph.NewState[vizState]("fanout").
		Node("prep", vizNode).
		Node("x", vizNode).
		Node("y", vizNode).
		Node("join", vizNode).
		Start("prep").
		Parallel("prep", []string{"x", "y"}, "join", merge).
		Edge("join", graph.End)
	if _, err := app.Compile(); err != nil {
		t.Fatal(err)
	}
	snap := app.Graph().Snapshot()
	assertEdge := func(from, to, label string, conditional bool) {
		t.Helper()
		for _, edge := range snap.Edges {
			if edge.From == from && edge.To == to {
				if edge.Conditional != conditional {
					t.Fatalf("边 %s→%s 条件标记=%v 期望 %v", from, to, edge.Conditional, conditional)
				}
				if label != "" && edge.Label != label {
					t.Fatalf("边 %s→%s 标签=%q 期望 %q", from, to, edge.Label, label)
				}
				return
			}
		}
		t.Fatalf("快照缺少边 %s→%s", from, to)
	}
	// Fork 扇出与 Compile 补齐的汇合边都是普通边。
	assertEdge("prep", "x", "", false)
	assertEdge("prep", "y", "", false)
	assertEdge("x", "join", "", false)
	assertEdge("y", "join", "", false)

	send := graph.NewState[vizState]("dispatch").
		Node("lead", vizNode).
		Node("w1", vizNode).
		Node("w2", vizNode).
		Node("gather", vizNode).
		Start("lead").
		Send("lead", []string{"w1", "w2"}, "gather",
			func(_ context.Context, s *vizState) ([]graph.DispatchTask, error) {
				return []graph.DispatchTask{graph.Send("w1", s)}, nil
			}).
		Merge(merge).
		Edge("w1", "gather").
		Edge("w2", "gather").
		Edge("gather", graph.End)
	if _, err := send.Compile(); err != nil {
		t.Fatal(err)
	}
	snap = send.Graph().Snapshot()
	// 分派源没有静态出边，投影为条件边；成员显式声明的汇合边保持普通边。
	assertEdge("lead", "w1", "dispatch", true)
	assertEdge("lead", "w2", "dispatch", true)
	assertEdge("w1", "gather", "", false)
	assertEdge("w2", "gather", "", false)

	// 未经 Compile 汇合接线时，缺失的 branch→join 也投影为条件边。
	rawFork := graph.NewState[vizState]("rawfork").Graph()
	for _, name := range []string{"p", "u", "v", "j"} {
		if err := rawFork.AddNode(name, graph.StateNode(vizNode)); err != nil {
			t.Fatal(err)
		}
	}
	if err := rawFork.AddFork("p", "u", "v"); err != nil {
		t.Fatal(err)
	}
	if err := rawFork.AddJoin("p", "j"); err != nil {
		t.Fatal(err)
	}
	snap = rawFork.Snapshot()
	found := false
	for _, edge := range snap.Edges {
		if edge.From == "u" && edge.To == "j" {
			found = true
			if !edge.Conditional || edge.Label != "join" {
				t.Fatalf("缺失汇合边投影不符：%+v", edge)
			}
		}
	}
	if !found {
		t.Fatal("快照缺少投影的 u→j 汇合边")
	}
}

// 节点 Kind 决定形状；特殊字符 ID/标签必须转义，不产生注入语法。
func TestSnapshotMermaidShapesAndEscaping(t *testing.T) {
	snap := graph.Snapshot{
		Name: `weird"name`,
		Nodes: []graph.NodeInfo{
			{ID: `model"x`, Kind: "chat_model", Label: `say "hi"`},
			{ID: "tools[1]", Kind: "tools"},
			{ID: "sub|g", Kind: "graph"},
		},
		Edges: []graph.EdgeInfo{
			{From: graph.Start, To: `model"x`},
			{From: `model"x`, To: "tools[1]", Label: `a|b"c`, Conditional: true},
		},
	}
	out := snap.Mermaid()
	for _, bad := range []string{`"x[`, `|b"`, `say "hi"`} {
		if strings.Contains(out, bad) {
			t.Fatalf("Mermaid 输出含未转义片段 %q：\n%s", bad, out)
		}
	}
	if !strings.Contains(out, "model_x([\"say #quot;hi#quot;\"])") {
		t.Fatalf("模型节点形状/转义不符：\n%s", out)
	}
	if !strings.Contains(out, "tools_1_[[\"tools#91;1#93;\"]]") {
		t.Fatalf("工具节点形状不符：\n%s", out)
	}
	if !strings.Contains(out, "sub_g{{\"sub#124;g\"}}") {
		t.Fatalf("子图节点形状不符：\n%s", out)
	}
	if !strings.Contains(out, "a#124;b#quot;c") {
		t.Fatalf("边标签未转义：\n%s", out)
	}

	dot := snap.DOT()
	if !strings.Contains(dot, `digraph "weird\"name"`) {
		t.Fatalf("DOT 图名未转义：\n%s", dot)
	}
	if !strings.Contains(dot, `"model\"x" [label="say \"hi\"", shape=oval]`) {
		t.Fatalf("DOT 模型节点转义不符：\n%s", dot)
	}
}
