package graph

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/goharness/goharness/pkg/callback"
	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/runnable"
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
	Kind       runnable.NodeKind
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
	nodeInfo      map[string]NodeInfo
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

func bindCallbacks(ctx context.Context, run RunInfo, nodes []NodeInfo) context.Context {
	// 旧 Manager 通道或新 observe.Observer 任一存在时都要安装生命周期
	// 汇聚器：两者共用同一套 span 身份，事件双发。
	if _, ok := callback.ManagerFromContext(ctx); !ok && !observe.Active(ctx) {
		return ctx
	}
	graphCtx, invocation := callback.Begin(ctx, "graph", run.Name, map[string]any{"graph.revision": run.Revision})
	nodeInfo := make(map[string]NodeInfo, len(nodes))
	for _, node := range nodes {
		nodeInfo[node.ID] = node
	}
	sink := &lifecycleSink{run: run, runInvocation: invocation, nodes: map[graphNodeKey]nodeLifecycle{}, nodeInfo: nodeInfo}
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
	execution := s.nodeExecution(event)
	nodeCtx, invocation := callback.Begin(ctx, "graph.node", event.NodeID, nodeExecutionMetadata(execution))
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
		start := invocation.WithPhase(callback.PhaseStart)
		bound := callback.BindInvocation(ctx, invocation)
		observe.Emit(bound, observe.EventOf(start, observe.KindRun, runAttrs(s.run)))
		_ = callback.Dispatch[Callback](bound, start, func(ctx context.Context, h Callback) error { return h.OnGraphStart(ctx, s.run) })
	case EventNodeStart:
		key := graphNodeKey{id: event.NodeID, step: event.Step}
		node := s.nodes[key]
		if node.invocation.Operation == "" {
			execution := s.nodeExecution(event)
			nodeCtx, invocation := callback.Begin(ctx, "graph.node", event.NodeID, nodeExecutionMetadata(execution))
			node = nodeLifecycle{execution: execution, invocation: invocation, ctx: nodeCtx}
			s.nodes[key] = node
		}
		s.mu.Unlock()
		start := node.invocation.WithPhase(callback.PhaseStart)
		observe.Emit(node.ctx, observe.EventOf(start, observe.KindNode, nil))
		_ = callback.Dispatch[Callback](node.ctx, start, func(ctx context.Context, h Callback) error { return h.OnNodeStart(ctx, node.execution) })
	case EventNodeOutput:
		node := s.nodes[graphNodeKey{id: event.NodeID, step: event.Step}]
		s.mu.Unlock()
		output := node.invocation.WithPhase(callback.PhaseOutput)
		observe.Emit(node.ctx, observe.EventOf(output, observe.KindNode, nil))
		_ = callback.Dispatch[Callback](node.ctx, output, func(ctx context.Context, h Callback) error { return h.OnNodeOutput(ctx, node.execution) })
	case EventNodeEnd:
		key := graphNodeKey{id: event.NodeID, step: event.Step}
		node := s.nodes[key]
		delete(s.nodes, key)
		s.mu.Unlock()
		result := NodeResult{NodeExecution: node.execution, Duration: time.Since(node.invocation.StartedAt), Status: NodeStatusOK}
		end := node.invocation.WithPhase(callback.PhaseEnd)
		observe.Emit(node.ctx, observe.EventOf(end, observe.KindNode, nil))
		_ = callback.Dispatch[Callback](node.ctx, end, func(ctx context.Context, h Callback) error { return h.OnNodeEnd(ctx, result) })
	case EventGraphEnd:
		invocation := s.runInvocation
		s.mu.Unlock()
		result := RunResult{RunInfo: s.run, Steps: event.Step}
		end := invocation.WithPhase(callback.PhaseEnd)
		bound := callback.BindInvocation(ctx, invocation)
		observe.Emit(bound, observe.EventOf(end, observe.KindRun, mergeAttrs(runAttrs(s.run), "graph.step", event.Step)))
		_ = callback.Dispatch[Callback](bound, end, func(ctx context.Context, h Callback) error { return h.OnGraphEnd(ctx, result) })
	case EventGraphError:
		active := make([]nodeLifecycle, 0, len(s.nodes))
		for _, node := range s.nodes {
			active = append(active, node)
		}
		// 扇出顺序按 step 排序，公共流里节点错误的出现顺序与旧路径一致。
		sort.Slice(active, func(i, j int) bool { return active[i].execution.Step < active[j].execution.Step })
		s.nodes = map[graphNodeKey]nodeLifecycle{}
		invocation := s.runInvocation
		s.mu.Unlock()
		for _, node := range active {
			result := NodeResult{NodeExecution: node.execution, Duration: time.Since(node.invocation.StartedAt), Status: NodeStatusError, Err: event.Err}
			failed := node.invocation.WithPhase(callback.PhaseError)
			nodeEvent := observe.EventOf(failed, observe.KindNode, nil)
			nodeEvent.Err = event.Err
			observe.Emit(node.ctx, nodeEvent)
			_ = callback.Dispatch[Callback](node.ctx, failed, func(ctx context.Context, h Callback) error { return h.OnNodeEnd(ctx, result) })
		}
		failure := RunFailure{RunInfo: s.run, Err: event.Err}
		failed := invocation.WithPhase(callback.PhaseError)
		bound := callback.BindInvocation(ctx, invocation)
		runEvent := observe.EventOf(failed, observe.KindRun, runAttrs(s.run))
		runEvent.Err = event.Err
		observe.Emit(bound, runEvent)
		_ = callback.Dispatch[Callback](bound, failed, func(ctx context.Context, h Callback) error { return h.OnGraphError(ctx, failure) })
	default:
		s.mu.Unlock()
	}
}

// runAttrs 是 run 级事件补充到 span 元数据上的运行属性。
func runAttrs(run RunInfo) map[string]any {
	return map[string]any{
		"graph.input_type":  run.InputType,
		"graph.output_type": run.OutputType,
		"graph.resume_mode": string(run.ResumeMode),
	}
}

// mergeAttrs 在 attrs 副本上补充键值，供一次性事件组装使用。
func mergeAttrs(attrs map[string]any, key string, value any) map[string]any {
	merged := make(map[string]any, len(attrs)+1)
	for k, v := range attrs {
		merged[k] = v
	}
	merged[key] = value
	return merged
}

func (s *lifecycleSink) nodeExecution(event Event) NodeExecution {
	info := s.nodeInfo[event.NodeID]
	return NodeExecution{
		GraphName:  s.run.Name,
		NodeID:     event.NodeID,
		Kind:       info.Kind,
		Step:       event.Step,
		InputType:  info.InputType,
		OutputType: info.OutputType,
	}
}

func nodeExecutionMetadata(execution NodeExecution) map[string]any {
	return map[string]any{
		"graph.node_id":     execution.NodeID,
		"graph.node_kind":   execution.Kind,
		"graph.step":        execution.Step,
		"graph.input_type":  execution.InputType,
		"graph.output_type": execution.OutputType,
	}
}
