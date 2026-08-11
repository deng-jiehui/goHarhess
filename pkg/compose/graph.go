// Package compose provides fluent builders for message-oriented typed graphs.
package compose

import (
	"context"
	"errors"
	"reflect"

	"goharness/pkg/graph"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

type GraphBuilder struct {
	value       *graph.MessageGraph
	err         error
	finishAdded bool
}

func NewGraph(name string) *GraphBuilder {
	return &GraphBuilder{value: graph.NewMessageGraph(name)}
}

func (b *GraphBuilder) Description(value string) *GraphBuilder {
	if b != nil && b.value != nil {
		b.value.SetDescription(value)
	}
	return b
}

// Node adds a typed state transformer to the message graph.
func (b *GraphBuilder) Node(id string, value runnable.Runnable[graph.MessageState, graph.MessageState]) *GraphBuilder {
	if b == nil || b.err != nil {
		return b
	}
	b.err = graph.AddNode(b.value, id, value)
	return b
}

func (b *GraphBuilder) Agent(id string, value runnable.NamedRunnable[[]schema.Message, []schema.Message]) *GraphBuilder {
	if b == nil || b.err != nil {
		return b
	}
	if isNilNamedRunnable(value) {
		b.err = errors.New("compose agent cannot be nil")
		return b
	}
	adapter := runnable.Func[graph.MessageState, graph.MessageState](func(ctx context.Context, state graph.MessageState, options ...runnable.Option) (graph.MessageState, error) {
		output, err := value.Invoke(ctx, state.Messages, options...)
		if err != nil {
			return graph.MessageState{}, err
		}
		state.Messages = append(schema.CloneMessages(state.Messages), schema.CloneMessages(output)...)
		state.Output = append(state.Output, schema.CloneMessages(output)...)
		return state, nil
	})
	b.err = graph.AddNode(b.value, id, adapter)
	return b
}

func isNilNamedRunnable(value any) bool {
	if value == nil {
		return true
	}
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}

func (b *GraphBuilder) NodeOptions(id string, options ...runnable.Option) *GraphBuilder {
	if b == nil || b.err != nil {
		return b
	}
	b.err = b.value.SetNodeOptions(id, options...)
	return b
}

func (b *GraphBuilder) Edge(from, to string) *GraphBuilder {
	if b == nil || b.err != nil {
		return b
	}
	if to == graph.End {
		b.ensureFinish()
		if b.err == nil {
			b.err = b.value.AddEdge(from, "__compose_finish__")
		}
		return b
	}
	b.err = b.value.AddEdge(from, to)
	return b
}

func (b *GraphBuilder) ensureFinish() {
	if b == nil || b.err != nil || b.finishAdded {
		return
	}
	finish := runnable.Func[graph.MessageState, []schema.Message](func(_ context.Context, state graph.MessageState, _ ...runnable.Option) ([]schema.Message, error) {
		return schema.CloneMessages(state.Output), nil
	})
	if err := graph.AddNode(b.value, "__compose_finish__", finish); err != nil {
		b.err = err
		return
	}
	b.err = b.value.AddEdge("__compose_finish__", graph.End)
	b.finishAdded = b.err == nil
}

func (b *GraphBuilder) Conditional(from, to, label string, condition func(context.Context, graph.MessageState) (bool, error)) *GraphBuilder {
	if b == nil || b.err != nil {
		return b
	}
	if to == graph.End {
		b.ensureFinish()
		if b.err == nil {
			b.err = graph.AddConditionalEdge(b.value, from, "__compose_finish__", label, condition)
		}
		return b
	}
	b.err = graph.AddConditionalEdge(b.value, from, to, label, condition)
	return b
}

func (b *GraphBuilder) MaxSteps(value int) *GraphBuilder {
	if b == nil || b.err != nil {
		return b
	}
	b.err = b.value.SetMaxSteps(value)
	return b
}

func (b *GraphBuilder) Build() (*graph.MessageGraph, error) {
	if b == nil {
		return nil, graph.ValidationError{Code: "compose.builder.nil", Path: "graph", Message: "builder cannot be nil"}
	}
	if b.err != nil {
		return nil, b.err
	}
	if err := b.value.Build(); err != nil {
		return nil, err
	}
	return b.value, nil
}

func (b *GraphBuilder) MustBuild() *graph.MessageGraph {
	value, err := b.Build()
	if err != nil {
		panic(err)
	}
	return value
}
