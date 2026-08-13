package graph_test

import (
	"context"
	"errors"
	"testing"

	"goharness/pkg/callback"
	"goharness/pkg/graph"
	"goharness/pkg/runnable"
)

func TestGraphCallbackClosesFailedNodeBeforeGraphError(t *testing.T) {
	var order []string
	observer := graph.CallbackFuncs{CallbackName: "trace", NodeEnd: func(_ context.Context, result graph.NodeResult) error {
		if result.Status != graph.NodeStatusError {
			t.Fatalf("status=%s", result.Status)
		}
		order = append(order, "node_end")
		return nil
	}, GraphError: func(context.Context, graph.RunFailure) error { order = append(order, "graph_error"); return nil }}
	manager, _ := callback.NewManager(callback.Config{}, []callback.Handler{observer})
	g := graph.NewGraph[int, int]("broken")
	if err := g.AddNode("fail", runnable.Func[int, int](func(context.Context, int, ...runnable.Option) (int, error) { return 0, errors.New("broken") })); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "fail"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("fail", graph.End); err != nil {
		t.Fatal(err)
	}
	if _, err := mustCompile(t, g).Invoke(callback.WithManager(context.Background(), manager), 1); err == nil {
		t.Fatal("Invoke succeeded")
	}
	if len(order) != 2 || order[0] != "node_end" || order[1] != "graph_error" {
		t.Fatalf("order=%#v", order)
	}
}
