package graph_test

import (
	"context"
	"errors"
	"testing"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/runnable"
)

func TestGraphErrorEventClosesFailedNodeBeforeRunError(t *testing.T) {
	var order []string
	collector := observe.ObserverFunc(func(_ context.Context, event observe.Event) {
		if event.Phase != observe.PhaseError {
			return
		}
		switch event.Kind {
		case observe.KindNode:
			order = append(order, "node_error")
		case observe.KindRun:
			order = append(order, "run_error")
		}
	})
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
	if _, err := mustCompile(t, g).Invoke(observe.With(context.Background(), collector), 1); err == nil {
		t.Fatal("Invoke succeeded")
	}
	if len(order) != 2 || order[0] != "node_error" || order[1] != "run_error" {
		t.Fatalf("order=%#v", order)
	}
}

func TestGraphNodeEventCarriesCompiledNodeMetadata(t *testing.T) {
	var attrs map[string]any
	collector := observe.ObserverFunc(func(_ context.Context, event observe.Event) {
		if event.Kind == observe.KindNode && event.Phase == observe.PhaseStart {
			attrs = event.Attrs
		}
	})
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
	if _, err := mustCompile(t, g).Invoke(observe.With(context.Background(), collector), "input"); err != nil {
		t.Fatal(err)
	}
	if attrs["graph.node_kind"] != runnable.NodeKindRetriever || attrs["graph.input_type"] != "string" || attrs["graph.output_type"] != "string" {
		t.Fatalf("attrs = %#v", attrs)
	}
}
