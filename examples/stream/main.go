// Run with: go run ./examples/stream
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/stream"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	g := graph.NewGraph[string, []schema.Message]("orders")
	node := runnable.InvokableLambda(func(ctx context.Context, input string) ([]schema.Message, error) {
		activity := stream.StartActivity(ctx, "正在搜索订单库...")
		writer, err := stream.StartMessage(ctx, schema.RoleAssistant)
		if err != nil {
			activity.End(err)
			return nil, err
		}
		for _, text := range []string{"正在查询订单 ", input, "，查询完成。"} {
			if err := writer.AppendText(text); err != nil {
				activity.End(err)
				return nil, err
			}
		}
		message, err := writer.End()
		activity.End(err)
		if err != nil {
			return nil, err
		}
		return []schema.Message{message}, nil
	})
	if err := g.AddNode("query", node); err != nil {
		return err
	}
	if err := g.AddEdge(graph.Start, "query"); err != nil {
		return err
	}
	if err := g.AddEdge("query", graph.End); err != nil {
		return err
	}
	compiled, err := g.Compile()
	if err != nil {
		return err
	}
	events, err := compiled.StreamEvents(context.Background(), "123", stream.Config{Buffer: 16, Lifecycle: true})
	if err != nil {
		return err
	}
	defer events.Close()
	encoder := json.NewEncoder(os.Stdout)
	for {
		event, err := events.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := encoder.Encode(event); err != nil {
			return err
		}
	}
}
