package graph_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
)

type searchState struct {
	Query string
	Docs  []string
}

// State 节点零胶水串联：读取、调用组件、写回都在一个普通函数内完成。
func TestStateNodeChainWithoutHandlers(t *testing.T) {
	g := graph.NewGraph[string, string]("state-node")
	if err := graph.SetStateFactory(g, func(_ context.Context, input string) (*searchState, error) {
		return &searchState{Query: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("search", graph.StateNode(func(_ context.Context, s *searchState) error {
		s.Docs = []string{s.Query + ":doc1", s.Query + ":doc2"}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("summarize", graph.StateNode(func(_ context.Context, s *searchState) error {
		s.Docs = append(s.Docs, "summary")
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	// 结束节点仍用 Pre 声明输出。
	finish := runnable.InvokableLambda(func(_ context.Context, docs []string) (string, error) {
		return strings.Join(docs, "|"), nil
	})
	if err := g.AddNode("finish", finish, graph.WithPreHandler(func(_ context.Context, s *searchState) ([]string, error) {
		return s.Docs, nil
	})); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{{graph.Start, "search"}, {"search", "summarize"}, {"summarize", "finish"}, {"finish", graph.End}} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	out, err := mustCompileGraph(t, g).Invoke(context.Background(), "go")
	if err != nil || out != "go:doc1|go:doc2|summary" {
		t.Fatalf("output=%q err=%v", out, err)
	}
}

// 图输入类型与 State 节点可以不同：第一个 State 节点经恒等 Pre 拿 State。
// 若 State 类型不匹配图的 factory，Compile 报既有错误码。
func TestStateNodeStateMismatchFailsAtCompile(t *testing.T) {
	type other struct{ X int }
	g := graph.NewGraph[string, string]("mismatch")
	if err := graph.SetStateFactory(g, func(_ context.Context, input string) (*searchState, error) {
		return &searchState{Query: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("work", graph.StateNode(func(_ context.Context, s *other) error { return nil })); err != nil {
		t.Fatal(err)
	}
	_ = g.AddEdge(graph.Start, "work")
	_ = g.AddEdge("work", graph.End)
	if _, err := g.Compile(); err == nil || !strings.Contains(err.Error(), "node.pre.type_mismatch") {
		t.Fatalf("expected node.pre.type_mismatch, got %v", err)
	}
}

// State 节点内的 graph.Interrupt 照常挂起；恢复时不需要用户编写 Pre。
func TestStateNodeInterruptAndResume(t *testing.T) {
	build := func() *graph.Graph[string, string] {
		g := graph.NewGraph[string, string]("state-resume")
		if err := graph.SetStateFactory(g, func(_ context.Context, input string) (*searchState, error) {
			return &searchState{Query: input}, nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := g.AddNode("form", graph.StateNode(func(ctx context.Context, s *searchState) error {
			name, err := graph.Interrupt(ctx, "name")
			if err != nil {
				return err
			}
			s.Docs = []string{fmt.Sprintf("%v", name)}
			return nil
		})); err != nil {
			t.Fatal(err)
		}
		finish := runnable.InvokableLambda(func(_ context.Context, docs []string) (string, error) {
			return docs[0], nil
		})
		if err := g.AddNode("finish", finish, graph.WithPreHandler(func(_ context.Context, s *searchState) ([]string, error) {
			return s.Docs, nil
		})); err != nil {
			t.Fatal(err)
		}
		_ = g.AddEdge(graph.Start, "form")
		_ = g.AddEdge("form", "finish")
		_ = g.AddEdge("finish", graph.End)
		return g
	}
	store := &byteCheckpointStore{}
	checkpoint := graph.WithCheckpoint(store, "state-resume")
	compiled := mustCompileGraph(t, build())
	_, err := compiled.Invoke(context.Background(), "q", checkpoint)
	id := inputID(t, err)
	out, err := mustCompileGraph(t, build()).Resume(context.Background(), checkpoint, graph.WithResume(id, "Ada"))
	if err != nil || out != "Ada" {
		t.Fatalf("output=%q err=%v", out, err)
	}
}

// State 节点直接进入并行分支：不写 Pre/Post，分支在独立 State 副本上修改，
// 轮末由 State merger 合并。
func TestStateNodeInParallelBranch(t *testing.T) {
	g := graph.NewGraph[string, []string]("state-parallel")
	if err := graph.SetStateFactory(g, func(_ context.Context, input string) (*typedAPIState, error) {
		return &typedAPIState{Values: []string{input}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	branches := []graph.ParallelBranch{
		{Name: "first", Node: graph.StateNode(func(_ context.Context, s *typedAPIState) error {
			s.Values = append(s.Values, "one")
			return nil
		})},
		{Name: "second", Node: graph.StateNode(func(_ context.Context, s *typedAPIState) error {
			s.Values = append(s.Values, "two")
			return nil
		})},
	}
	if err := g.AddParallelNode("fanout", branches, graph.WithStateMerger(
		func(_ context.Context, base *typedAPIState, states []*typedAPIState) error {
			for _, s := range states {
				base.Values = append(base.Values, s.Values[len(s.Values)-1])
			}
			return nil
		},
	)); err != nil {
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
	out, err := mustCompileGraph(t, g).Invoke(context.Background(), "base")
	if err != nil || fmt.Sprint(out) != "[base one two]" {
		t.Fatalf("output=%v err=%v", out, err)
	}
}

// State 节点在 Stream 模式下按既有物化屏障规则执行。
func TestStateNodeUnderStreamMode(t *testing.T) {
	g := graph.NewGraph[string, string]("state-stream")
	if err := graph.SetStateFactory(g, func(_ context.Context, input string) (*searchState, error) {
		return &searchState{Query: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("work", graph.StateNode(func(_ context.Context, s *searchState) error {
		s.Docs = []string{s.Query + "!"}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	finish := runnable.InvokableLambda(func(_ context.Context, docs []string) (string, error) {
		return docs[0], nil
	})
	if err := g.AddNode("finish", finish, graph.WithPreHandler(func(_ context.Context, s *searchState) ([]string, error) {
		return s.Docs, nil
	})); err != nil {
		t.Fatal(err)
	}
	_ = g.AddEdge(graph.Start, "work")
	_ = g.AddEdge("work", "finish")
	_ = g.AddEdge("finish", graph.End)
	stream, err := mustCompileGraph(t, g).Stream(context.Background(), "x")
	if err != nil {
		t.Fatal(err)
	}
	out, err := stream.Collect(context.Background())
	if err != nil || out != "x!" {
		t.Fatalf("output=%q err=%v", out, err)
	}
}

// 节点内错误照常返回，不吞不掉。
func TestStateNodePropagatesError(t *testing.T) {
	// 图输出声明为 *S：State 节点直通 END 也合法。
	g := graph.NewGraph[string, *searchState]("state-error")
	if err := graph.SetStateFactory(g, func(_ context.Context, input string) (*searchState, error) {
		return &searchState{Query: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("search failed")
	if err := g.AddNode("work", graph.StateNode(func(_ context.Context, s *searchState) error {
		return sentinel
	})); err != nil {
		t.Fatal(err)
	}
	_ = g.AddEdge(graph.Start, "work")
	_ = g.AddEdge("work", graph.End)
	_, err := mustCompileGraph(t, g).Invoke(context.Background(), "x")
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}
}
