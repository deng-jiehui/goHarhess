// Package compose provides fluent builders for message-oriented Graphs.
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
	value       *graph.Graph[[]schema.Message, []schema.Message]
	err         error
	finishAdded bool
}

func NewGraph(name string) *GraphBuilder {
	g := graph.NewGraph[[]schema.Message, []schema.Message](name)
	_ = g.SetStateFactory(func(_ context.Context, input []schema.Message) (*graph.MessageState, error) {
		return &graph.MessageState{Messages: schema.CloneMessages(input)}, nil
	})
	return &GraphBuilder{value: g}
}

func (b *GraphBuilder) Description(value string) *GraphBuilder {
	if b != nil && b.value != nil {
		b.value.SetDescription(value)
	}
	return b
}

func (b *GraphBuilder) Node(id string, value runnable.Runnable[graph.MessageState, graph.MessageState]) *GraphBuilder {
	if b == nil || b.err != nil {
		return b
	}
	b.err = b.value.AddNode(id, value,
		graph.WithPreHandler(func(_ context.Context, state *graph.MessageState) (graph.MessageState, error) { return *state, nil }),
		graph.WithPostHandler(func(_ context.Context, state *graph.MessageState, output graph.MessageState) error {
			*state = output
			return nil
		}),
	)
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
	b.err = b.value.AddNode(id, value,
		graph.WithPreHandler(func(_ context.Context, state *graph.MessageState) ([]schema.Message, error) {
			return schema.CloneMessages(state.Messages), nil
		}),
		graph.WithPostHandler(func(_ context.Context, state *graph.MessageState, output []schema.Message) error {
			state.Messages = append(schema.CloneMessages(state.Messages), schema.CloneMessages(output)...)
			state.Output = append(state.Output, schema.CloneMessages(output)...)
			return nil
		}),
	)
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
	finish := runnable.Func[[]schema.Message, []schema.Message](func(_ context.Context, messages []schema.Message, _ ...runnable.Option) ([]schema.Message, error) {
		return schema.CloneMessages(messages), nil
	})
	b.err = b.value.AddNode("__compose_finish__", finish, graph.WithPreHandler(func(_ context.Context, state *graph.MessageState) ([]schema.Message, error) {
		return schema.CloneMessages(state.Output), nil
	}))
	if b.err != nil {
		return
	}
	b.err = b.value.AddEdge("__compose_finish__", graph.End)
	b.finishAdded = b.err == nil
}

func (b *GraphBuilder) Conditional(from, to, label string, condition func(context.Context, *graph.MessageState) (bool, error)) *GraphBuilder {
	if b == nil || b.err != nil {
		return b
	}
	if to == graph.End {
		b.ensureFinish()
		if b.err == nil {
			b.err = b.value.AddBranch(from, "__compose_finish__", label, condition)
		}
		return b
	}
	b.err = b.value.AddBranch(from, to, label, condition)
	return b
}

func (b *GraphBuilder) MaxSteps(value int) *GraphBuilder {
	if b == nil || b.err != nil {
		return b
	}
	b.err = b.value.SetMaxSteps(value)
	return b
}

func (b *GraphBuilder) Build() (*graph.Graph[[]schema.Message, []schema.Message], error) {
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

func (b *GraphBuilder) MustBuild() *graph.Graph[[]schema.Message, []schema.Message] {
	value, err := b.Build()
	if err != nil {
		panic(err)
	}
	return value
}
