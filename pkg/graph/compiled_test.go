package graph_test

import (
	"context"
	"sync"
	"testing"

	"goharness/pkg/graph"
	"goharness/pkg/runnable"
)

func TestCompiledGraphIsImmutableBuilderSnapshot(t *testing.T) {
	g := graph.NewGraph[int, int]("snapshot")
	if err := g.AddNode("first", runnable.Func[int, int](func(_ context.Context, input int, _ ...runnable.Option) (int, error) { return input + 1, nil })); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "first"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("first", graph.End); err != nil {
		t.Fatal(err)
	}
	first, err := g.Compile()
	if err != nil {
		t.Fatal(err)
	}

	if err := g.AddNode("second", runnable.Func[int, int](func(_ context.Context, input int, _ ...runnable.Option) (int, error) { return input * 2, nil })); err != nil {
		t.Fatal(err)
	}
	if err := g.AddBranch("first", "second", "positive", func(_ context.Context, input int) (bool, error) { return input > 0, nil }); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("second", graph.End); err != nil {
		t.Fatal(err)
	}
	second, err := g.Compile()
	if err != nil {
		t.Fatal(err)
	}

	firstValue, err := first.Invoke(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	secondValue, err := second.Invoke(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if firstValue != 3 || secondValue != 6 {
		t.Fatalf("first/second = %d/%d", firstValue, secondValue)
	}
	if len(first.Snapshot().Nodes) != 1 || len(second.Snapshot().Nodes) != 2 {
		t.Fatalf("snapshots = %#v / %#v", first.Snapshot(), second.Snapshot())
	}
}

func TestCompiledGraphCanBeReusedConcurrently(t *testing.T) {
	g := graph.NewGraph[int, int]("concurrent")
	if err := g.AddNode("node", runnable.Func[int, int](func(_ context.Context, input int, _ ...runnable.Option) (int, error) { return input + 1, nil })); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "node"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("node", graph.End); err != nil {
		t.Fatal(err)
	}
	compiled, err := g.Compile()
	if err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	for index := range 32 {
		index := index
		wait.Add(1)
		go func() {
			defer wait.Done()
			value, err := compiled.Invoke(context.Background(), index)
			if err != nil || value != index+1 {
				t.Errorf("Invoke(%d) = %d, %v", index, value, err)
			}
		}()
	}
	wait.Wait()
}
