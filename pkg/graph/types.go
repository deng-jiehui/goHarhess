package graph

import (
	"fmt"
	"reflect"

	"github.com/goharness/goharness/pkg/runnable"
)

const (
	Start = "__start__"
	End   = "__end__"

	DefaultMaxSteps = 100
	DefaultMaxDepth = 32
)

type erasedValue struct {
	typ   reflect.Type
	value any
}

type graphComponent interface {
	compileGraph(map[uintptr]bool) error
	graphComponent()
}

type NodeInfo struct {
	ID          string            `json:"id"`
	Kind        runnable.NodeKind `json:"kind"`
	Label       string            `json:"label,omitempty"`
	Description string            `json:"description,omitempty"`
	InputType   string            `json:"input_type"`
	OutputType  string            `json:"output_type"`
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

type lifecycleEventType string

const (
	eventGraphStart lifecycleEventType = "graph_start"
	eventNodeStart  lifecycleEventType = "node_start"
	eventNodeOutput lifecycleEventType = "node_output"
	eventNodeEnd    lifecycleEventType = "node_end"
	eventGraphEnd   lifecycleEventType = "graph_end"
	eventGraphError lifecycleEventType = "graph_error"
)

// lifecycleEvent 是调度循环的控制面信封，只在包内流向 lifecycleSink；
// 运行时 State 与节点值永远不经过它外泄。
type lifecycleEvent struct {
	Type   lifecycleEventType
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
