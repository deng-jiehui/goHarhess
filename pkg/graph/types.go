package graph

import (
	"context"
	"fmt"
	"reflect"

	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

const (
	Start = "__start__"
	End   = "__end__"

	DefaultMaxSteps = 100
	DefaultMaxDepth = 32
)

// MessageState is the standard runtime State for message-oriented workflows.
// Graph owns it by pointer; users do not implement Clone methods.
type MessageState struct {
	Messages []schema.Message
	Output   []schema.Message
}

type MessageGraph = Graph[[]schema.Message, []schema.Message]

func NewMessageGraph(name string) *MessageGraph {
	g := NewGraph[[]schema.Message, []schema.Message](name)
	_ = g.SetStateFactory(func(_ context.Context, input []schema.Message) (*MessageState, error) {
		return &MessageState{Messages: schema.CloneMessages(input)}, nil
	})
	return g
}

type erasedValue struct {
	typ   reflect.Type
	value any
}

type graphComponent interface {
	compileGraph(map[uintptr]bool) error
	graphComponent()
}

type NodeInfo struct {
	ID          string `json:"id"`
	Kind        string `json:"kind"`
	Label       string `json:"label,omitempty"`
	Description string `json:"description,omitempty"`
	InputType   string `json:"input_type"`
	OutputType  string `json:"output_type"`
}

type EdgeInfo struct {
	From        string `json:"from"`
	To          string `json:"to"`
	Label       string `json:"label,omitempty"`
	Conditional bool   `json:"conditional"`
}

type Snapshot struct {
	Name        string     `json:"name"`
	Description string     `json:"description,omitempty"`
	Built       bool       `json:"built"`
	MaxSteps    int        `json:"max_steps"`
	InputType   string     `json:"input_type"`
	OutputType  string     `json:"output_type"`
	StateType   string     `json:"state_type"`
	Nodes       []NodeInfo `json:"nodes"`
	Edges       []EdgeInfo `json:"edges"`
}

type EventType string

const (
	EventGraphStart EventType = "graph_start"
	EventNodeStart  EventType = "node_start"
	EventNodeOutput EventType = "node_output"
	EventNodeEnd    EventType = "node_end"
	EventGraphEnd   EventType = "graph_end"
	EventGraphError EventType = "graph_error"
)

// Event is control-plane only. Runtime State and node values never escape
// through an any-typed observation API.
type Event struct {
	Type   EventType
	NodeID string
	Step   int
	Err    error
}

func typeOf[T any]() reflect.Type { return reflect.TypeFor[T]() }

func typeName(value reflect.Type) string {
	if value == nil {
		return "<nil>"
	}
	return value.String()
}

func nodeScopedOptions(options []runnable.Option) []runnable.Option {
	scoped := make([]runnable.Option, len(options))
	for index, option := range options {
		if runnable.ScopeOf(option) == runnable.OptionScopeNode {
			scoped[index] = option
		} else {
			scoped[index] = runnable.NodeOption(option)
		}
	}
	return scoped
}

func walkReverse(start string, reverse map[string][]string) map[string]bool {
	visited := map[string]bool{}
	queue := []string{start}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if visited[current] {
			continue
		}
		visited[current] = true
		queue = append(queue, reverse[current]...)
	}
	return visited
}

func edgePath(index int) string { return fmt.Sprintf("edges[%d]", index) }
