package graph

import (
	"context"
	"errors"
	"sync"
	"time"

	"goharness/pkg/callback"
)

type ResumeMode string

const (
	ResumeModeInvoke ResumeMode = "invoke"
	ResumeModeResume ResumeMode = "resume"
)

type RunInfo struct {
	Name       string
	Revision   string
	InputType  string
	OutputType string
	ResumeMode ResumeMode
}

type NodeExecution struct {
	GraphName  string
	NodeID     string
	Kind       string
	Step       int
	Visit      int
	InputType  string
	OutputType string
}

type NodeStatus string

const (
	NodeStatusOK    NodeStatus = "ok"
	NodeStatusError NodeStatus = "error"
)

type NodeResult struct {
	NodeExecution
	Duration time.Duration
	Status   NodeStatus
	Err      error
}

type RunResult struct {
	RunInfo
	Steps int
}

type RunFailure struct {
	RunInfo
	NodeID string
	Step   int
	Err    error
}

type Callback interface {
	callback.Handler
	OnGraphStart(context.Context, RunInfo) error
	OnNodeStart(context.Context, NodeExecution) error
	OnNodeOutput(context.Context, NodeExecution) error
	OnNodeEnd(context.Context, NodeResult) error
	OnGraphEnd(context.Context, RunResult) error
	OnGraphError(context.Context, RunFailure) error
}

type CallbackFuncs struct {
	CallbackName string
	GraphStart   func(context.Context, RunInfo) error
	NodeStart    func(context.Context, NodeExecution) error
	NodeOutput   func(context.Context, NodeExecution) error
	NodeEnd      func(context.Context, NodeResult) error
	OnNode       func(context.Context, NodeExecution) error
	GraphEnd     func(context.Context, RunResult) error
	GraphError   func(context.Context, RunFailure) error
}

func (f CallbackFuncs) Name() string { return f.CallbackName }
func (f CallbackFuncs) OnGraphStart(ctx context.Context, value RunInfo) error {
	if f.GraphStart == nil {
		return nil
	}
	return f.GraphStart(ctx, value)
}
func (f CallbackFuncs) OnNodeStart(ctx context.Context, value NodeExecution) error {
	var first, second error
	if f.NodeStart != nil {
		first = f.NodeStart(ctx, value)
	}
	if f.OnNode != nil {
		second = f.OnNode(ctx, value)
	}
	return errors.Join(first, second)
}
func (f CallbackFuncs) OnNodeOutput(ctx context.Context, value NodeExecution) error {
	var first, second error
	if f.NodeOutput != nil {
		first = f.NodeOutput(ctx, value)
	}
	if f.OnNode != nil {
		second = f.OnNode(ctx, value)
	}
	return errors.Join(first, second)
}
func (f CallbackFuncs) OnNodeEnd(ctx context.Context, value NodeResult) error {
	if f.NodeEnd == nil {
		return nil
	}
	return f.NodeEnd(ctx, value)
}
func (f CallbackFuncs) OnGraphEnd(ctx context.Context, value RunResult) error {
	if f.GraphEnd == nil {
		return nil
	}
	return f.GraphEnd(ctx, value)
}
func (f CallbackFuncs) OnGraphError(ctx context.Context, value RunFailure) error {
	if f.GraphError == nil {
		return nil
	}
	return f.GraphError(ctx, value)
}

type lifecycleSink struct {
	mu            sync.Mutex
	run           RunInfo
	runInvocation callback.Invocation
	nodes         map[graphNodeKey]nodeLifecycle
}

type graphNodeKey struct {
	id   string
	step int
}
type nodeLifecycle struct {
	execution  NodeExecution
	invocation callback.Invocation
	ctx        context.Context
}

func bindCallbacks(ctx context.Context, run RunInfo) context.Context {
	if _, ok := callback.ManagerFromContext(ctx); !ok {
		return ctx
	}
	graphCtx, invocation := callback.Begin(ctx, "graph", run.Name, map[string]any{"graph.revision": run.Revision})
	sink := &lifecycleSink{run: run, runInvocation: invocation, nodes: map[graphNodeKey]nodeLifecycle{}}
	if existing, ok := EventSinkFromContext(ctx); ok {
		return WithEventSink(graphCtx, NewEventSink(sink, existing))
	}
	return WithEventSink(graphCtx, sink)
}

func (s *lifecycleSink) BindEventContext(ctx context.Context, event Event) context.Context {
	if event.Type != EventNodeStart {
		return ctx
	}
	key := graphNodeKey{id: event.NodeID, step: event.Step}
	s.mu.Lock()
	defer s.mu.Unlock()
	if node, ok := s.nodes[key]; ok {
		return callback.BindInvocation(ctx, node.invocation)
	}
	nodeCtx, invocation := callback.Begin(ctx, "graph.node", event.NodeID, map[string]any{"graph.node_id": event.NodeID, "graph.step": event.Step})
	execution := NodeExecution{GraphName: s.run.Name, NodeID: event.NodeID, Step: event.Step}
	s.nodes[key] = nodeLifecycle{execution: execution, invocation: invocation, ctx: nodeCtx}
	return nodeCtx
}

func (s *lifecycleSink) Publish(ctx context.Context, event Event) {
	if s == nil {
		return
	}
	s.mu.Lock()
	switch event.Type {
	case EventGraphStart:
		invocation := s.runInvocation
		s.mu.Unlock()
		_ = callback.Dispatch[Callback](callback.BindInvocation(ctx, invocation), invocation.WithPhase(callback.PhaseStart), func(ctx context.Context, h Callback) error { return h.OnGraphStart(ctx, s.run) })
	case EventNodeStart:
		key := graphNodeKey{id: event.NodeID, step: event.Step}
		node := s.nodes[key]
		if node.invocation.Operation == "" {
			nodeCtx, invocation := callback.Begin(ctx, "graph.node", event.NodeID, map[string]any{"graph.node_id": event.NodeID, "graph.step": event.Step})
			node = nodeLifecycle{execution: NodeExecution{GraphName: s.run.Name, NodeID: event.NodeID, Step: event.Step}, invocation: invocation, ctx: nodeCtx}
			s.nodes[key] = node
		}
		s.mu.Unlock()
		_ = callback.Dispatch[Callback](node.ctx, node.invocation.WithPhase(callback.PhaseStart), func(ctx context.Context, h Callback) error { return h.OnNodeStart(ctx, node.execution) })
	case EventNodeOutput:
		node := s.nodes[graphNodeKey{id: event.NodeID, step: event.Step}]
		s.mu.Unlock()
		_ = callback.Dispatch[Callback](node.ctx, node.invocation.WithPhase(callback.PhaseOutput), func(ctx context.Context, h Callback) error { return h.OnNodeOutput(ctx, node.execution) })
	case EventNodeEnd:
		key := graphNodeKey{id: event.NodeID, step: event.Step}
		node := s.nodes[key]
		delete(s.nodes, key)
		s.mu.Unlock()
		result := NodeResult{NodeExecution: node.execution, Duration: time.Since(node.invocation.StartedAt), Status: NodeStatusOK}
		_ = callback.Dispatch[Callback](node.ctx, node.invocation.WithPhase(callback.PhaseEnd), func(ctx context.Context, h Callback) error { return h.OnNodeEnd(ctx, result) })
	case EventGraphEnd:
		invocation := s.runInvocation
		s.mu.Unlock()
		result := RunResult{RunInfo: s.run, Steps: event.Step}
		_ = callback.Dispatch[Callback](callback.BindInvocation(ctx, invocation), invocation.WithPhase(callback.PhaseEnd), func(ctx context.Context, h Callback) error { return h.OnGraphEnd(ctx, result) })
	case EventGraphError:
		active := make([]nodeLifecycle, 0, len(s.nodes))
		for _, node := range s.nodes {
			active = append(active, node)
		}
		s.nodes = map[graphNodeKey]nodeLifecycle{}
		invocation := s.runInvocation
		s.mu.Unlock()
		for _, node := range active {
			result := NodeResult{NodeExecution: node.execution, Duration: time.Since(node.invocation.StartedAt), Status: NodeStatusError, Err: event.Err}
			_ = callback.Dispatch[Callback](node.ctx, node.invocation.WithPhase(callback.PhaseError), func(ctx context.Context, h Callback) error { return h.OnNodeEnd(ctx, result) })
		}
		failure := RunFailure{RunInfo: s.run, Err: event.Err}
		_ = callback.Dispatch[Callback](callback.BindInvocation(ctx, invocation), invocation.WithPhase(callback.PhaseError), func(ctx context.Context, h Callback) error { return h.OnGraphError(ctx, failure) })
	default:
		s.mu.Unlock()
	}
}
