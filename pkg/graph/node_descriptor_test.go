package graph_test

import (
	"context"
	"testing"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
)

type describedNode struct{}

func (describedNode) Invoke(context.Context, string, ...runnable.Option) (string, error) {
	return "ok", nil
}

func (n describedNode) Stream(ctx context.Context, input string, options ...runnable.Option) (*runnable.StreamReader[string], error) {
	return runnable.StreamFromInvoke(ctx, input, n.Invoke, options...)
}

func (n describedNode) Collect(ctx context.Context, input *runnable.StreamReader[string], options ...runnable.Option) (string, error) {
	return runnable.CollectFromInvoke(ctx, input, n.Invoke, options...)
}

func (n describedNode) Transform(ctx context.Context, input *runnable.StreamReader[string], options ...runnable.Option) (*runnable.StreamReader[string], error) {
	return runnable.TransformFromInvoke(ctx, input, n.Invoke, options...)
}

func (describedNode) NodeKind() runnable.NodeKind { return runnable.NodeKindRetriever }

func TestGraphSnapshotUsesRunnableNodeDescriptor(t *testing.T) {
	g := graph.NewGraph[string, string]("described")
	if err := g.AddNode("lookup", describedNode{}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "lookup"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("lookup", graph.End); err != nil {
		t.Fatal(err)
	}
	compiled, err := g.Compile()
	if err != nil {
		t.Fatal(err)
	}
	nodes := compiled.Snapshot().Nodes
	if len(nodes) != 1 || nodes[0].Kind != runnable.NodeKindRetriever {
		t.Fatalf("nodes = %#v", nodes)
	}
}
