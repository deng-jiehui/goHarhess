package graph_test

import (
	"context"
	"errors"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/stream"
	"io"
	"testing"
)

func TestPublicStreamIncludesActivityLifecycleAndOneResult(t *testing.T) {
	g := graph.NewGraph[int, int]("orders")
	if err := g.AddNode("query", runnable.InvokableLambda(func(ctx context.Context, n int) (int, error) {
		a := stream.StartActivity(ctx, "search orders")
		a.End(nil)
		return n + 1, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "query"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("query", graph.End); err != nil {
		t.Fatal(err)
	}
	c, err := g.Compile()
	if err != nil {
		t.Fatal(err)
	}
	r, err := c.StreamEvents(context.Background(), 1, stream.Config{Buffer: 2, Lifecycle: true})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	counts := map[stream.Kind]int{}
	for {
		e, err := r.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		counts[e.Kind]++
		if e.Lifecycle != nil && e.Lifecycle.Phase == stream.PhaseCanceled {
			t.Fatal("successful graph reported cancellation")
		}
		if e.Activity != nil && (e.Meta.NodeID != "query" || len(e.Meta.Namespace) != 1 || e.Meta.Namespace[0] != "orders") {
			t.Fatalf("%+v", e.Meta)
		}
		if e.Result != nil && *e.Result != 2 {
			t.Fatal(e)
		}
	}
	if counts[stream.KindActivity] != 2 || counts[stream.KindResult] != 1 || counts[stream.KindLifecycle] < 4 {
		t.Fatal(counts)
	}
}

func TestFailedNodeGetsTerminalBeforeGraphFailure(t *testing.T) {
	want := errors.New("database failed")
	g := graph.NewGraph[int, int]("failing")
	_ = g.AddNode("query", runnable.InvokableLambda(func(ctx context.Context, n int) (int, error) { stream.StartActivity(ctx, "query"); return 0, want }))
	_ = g.AddEdge(graph.Start, "query")
	_ = g.AddEdge("query", graph.End)
	compiled, err := g.Compile()
	if err != nil {
		t.Fatal(err)
	}
	r, err := compiled.StreamEvents(context.Background(), 0, stream.Config{Buffer: 8, Lifecycle: true})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	nodeError, graphError, activityError := 0, 0, 0
	for {
		e, err := r.Recv()
		if err != nil {
			if !errors.Is(err, want) {
				t.Fatal(err)
			}
			break
		}
		if e.Result != nil {
			t.Fatal("unexpected success")
		}
		if e.Lifecycle != nil && e.Lifecycle.Phase == stream.PhaseError {
			if e.Lifecycle.Scope == stream.ScopeNode {
				nodeError++
			} else {
				if nodeError != 1 {
					t.Fatal("graph failed before node termination")
				}
				graphError++
			}
		}
		if e.Activity != nil && e.Activity.Status == stream.ActivityFailed {
			activityError++
		}
	}
	if nodeError != 1 || graphError != 1 || activityError != 1 {
		t.Fatalf("node=%d graph=%d activity=%d", nodeError, graphError, activityError)
	}
}

func TestNestedPublicStreamKeepsNamespaceAndSingleResult(t *testing.T) {
	child := graph.NewGraph[int, int]("child")
	_ = child.AddNode("work", runnable.InvokableLambda(func(ctx context.Context, n int) (int, error) {
		a := stream.StartActivity(ctx, "nested")
		a.End(nil)
		return n + 1, nil
	}))
	_ = child.AddEdge(graph.Start, "work")
	_ = child.AddEdge("work", graph.End)
	cc, err := child.Compile()
	if err != nil {
		t.Fatal(err)
	}
	parent := graph.NewGraph[int, int]("parent")
	_ = parent.AddNode("nested", cc)
	_ = parent.AddEdge(graph.Start, "nested")
	_ = parent.AddEdge("nested", graph.End)
	pc, err := parent.Compile()
	if err != nil {
		t.Fatal(err)
	}
	r, err := pc.StreamEvents(context.Background(), 1, stream.Config{Buffer: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	results, activities := 0, 0
	for {
		e, err := r.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if e.Result != nil {
			results++
		}
		if e.Activity != nil {
			activities++
			if len(e.Meta.Namespace) != 2 || e.Meta.Namespace[0] != "parent" || e.Meta.Namespace[1] != "child" || e.Meta.NodeID != "work" {
				t.Fatal(e.Meta)
			}
		}
	}
	if results != 1 || activities != 2 {
		t.Fatalf("%d %d", results, activities)
	}
}
