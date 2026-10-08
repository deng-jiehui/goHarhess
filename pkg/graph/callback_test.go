package graph_test

import (
	"context"
	"errors"
	"testing"

	"github.com/goharness/goharness/pkg/callback"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
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

func TestGraphCallbackCarriesCompiledNodeMetadata(t *testing.T) {
	var execution graph.NodeExecution
	observer := graph.CallbackFuncs{CallbackName: "trace", NodeStart: func(_ context.Context, value graph.NodeExecution) error {
		execution = value
		return nil
	}}
	manager, err := callback.NewManager(callback.Config{}, []callback.Handler{observer})
	if err != nil {
		t.Fatal(err)
	}
	g := graph.NewGraph[string, string]("metadata")
	if err := g.AddNode("lookup", describedNode{}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "lookup"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("lookup", graph.End); err != nil {
		t.Fatal(err)
	}
	if _, err := mustCompile(t, g).Invoke(callback.WithManager(context.Background(), manager), "input"); err != nil {
		t.Fatal(err)
	}
	if execution.Kind != runnable.NodeKindRetriever || execution.InputType != "string" || execution.OutputType != "string" {
		t.Fatalf("execution = %#v", execution)
	}
}
