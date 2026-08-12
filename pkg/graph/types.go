package graph

import (
	"context"
	"reflect"
	"sync"

	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

const (
	Start = "__start__"
	End   = "__end__"

	DefaultMaxSteps = 100
	DefaultMaxDepth = 32
)

// Cloner makes state ownership explicit and keeps cloning in generated or
// hand-written native Go code. StateGraph never reflect-copies S.
type Cloner[S any] interface {
	Clone() S
}

// StateGenerator creates the strongly typed state placed on the graph's
// START data edge.
type StateGenerator[I any, S Cloner[S]] func(context.Context, I) S

// StatePreHandler derives a node's typed input from the current graph State.
// It is consumed by AddNode and is never forwarded to the underlying Runnable.
type StatePreHandler[S, NI any] func(context.Context, S) (NI, error)

// StatePostHandler applies a node's typed output to the current graph State.
// Returning the unchanged S is valid and makes the no-op explicit.
type StatePostHandler[S, NO any] func(context.Context, S, NO) (S, error)

type stateBindingOption interface {
	runnable.Option
	bindState(*nodeBinding) error
}

type nodeBinding struct {
	pre  *statePreBinding
	post *statePostBinding
}

type statePreBinding struct {
	stateType reflect.Type
	inputType reflect.Type
	invoke    func(context.Context, *stateScope) (erasedValue, error)
}

type statePostBinding struct {
	stateType  reflect.Type
	outputType reflect.Type
	invoke     func(context.Context, *stateScope, erasedValue) error
}

type stateScope struct {
	mu       sync.Mutex
	typ      reflect.Type
	snapshot func() (erasedValue, error)
	replace  func(erasedValue) error
}

type stateScopeContextKey struct{}

func withStateScope(ctx context.Context, scope *stateScope) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, stateScopeContextKey{}, scope)
}

func stateScopeFromContext(ctx context.Context) (*stateScope, bool) {
	if ctx == nil {
		return nil, false
	}
	scope, ok := ctx.Value(stateScopeContextKey{}).(*stateScope)
	return scope, ok && scope != nil
}

// MessageState is the standard typed state for message-oriented workflows.
type MessageState struct {
	Messages []schema.Message
	Output   []schema.Message
}

func (s MessageState) Clone() MessageState {
	s.Messages = schema.CloneMessages(s.Messages)
	s.Output = schema.CloneMessages(s.Output)
	return s
}

type MessageGraph = StateGraph[[]schema.Message, []schema.Message, MessageState]

func NewMessageGraph(name string) *MessageGraph {
	return NewStateGraph[[]schema.Message, []schema.Message, MessageState](name, func(_ context.Context, input []schema.Message) MessageState {
		return MessageState{Messages: schema.CloneMessages(input)}
	})
}

type erasedValue struct {
	typ   reflect.Type
	value any
}

type anyNode struct {
	id          string
	inputType   reflect.Type
	outputType  reflect.Type
	info        NodeInfo
	options     []runnable.Option
	pre         *statePreBinding
	post        *statePostBinding
	invoke      func(context.Context, erasedValue, ...runnable.Option) (erasedValue, error)
	compile     func(map[uintptr]bool) error
	nestedGraph bool
}

type anyCondition struct {
	inputType reflect.Type
	invoke    func(context.Context, erasedValue) (bool, error)
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

// Event is control-plane only. Strongly typed state and node values never
// escape through an any-typed observation API.
type Event struct {
	Type   EventType
	NodeID string
	Step   int
	Err    error
}

func typeOf[T any]() reflect.Type {
	return reflect.TypeFor[T]()
}

func typeName(value reflect.Type) string {
	if value == nil {
		return "<nil>"
	}
	return value.String()
}
