package graph

import (
	"context"
	"sort"
	"sync"

	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/runnable"
)

type resumeMode string

const (
	resumeModeInvoke resumeMode = "invoke"
	resumeModeResume resumeMode = "resume"
)

type runInfo struct {
	Name       string
	Revision   string
	InputType  string
	OutputType string
	ResumeMode resumeMode
	// CheckpointKey 是本次运行关联的检查点键：恢复产生的新执行记录
	// 靠它把调用树接回被挂起的那次运行。
	CheckpointKey string
}

type nodeExec struct {
	GraphName  string
	NodeID     string
	Kind       runnable.NodeKind
	Step       int
	Visit      int
	InputType  string
	OutputType string
}

// lifecycleSink 把调度循环的控制面事件翻译成统一观测事件：为 run 与每个
// 节点维护 Invocation，经 BindEventContext 把节点 span 绑到执行 ctx 上，
// 使节点内的 llm/tool 调用在调用树中自动落到节点之下。
type lifecycleSink struct {
	mu            sync.Mutex
	run           runInfo
	runInvocation observe.Invocation
	nodes         map[graphNodeKey]nodeLifecycle
	nodeInfo      map[string]NodeInfo
}

type graphNodeKey struct {
	id   string
	step int
}
type nodeLifecycle struct {
	execution  nodeExec
	invocation observe.Invocation
	ctx        context.Context
}

// bindCallbacks 在有观测者时安装生命周期汇聚器；stream 适配器本身也是
// 观测者，所以开启公共流时同样会走这条路径。
func bindCallbacks(ctx context.Context, run runInfo, nodes []NodeInfo) context.Context {
	if !observe.Active(ctx) {
		return ctx
	}
	graphCtx, invocation := observe.Begin(ctx, "graph", run.Name, map[string]any{"graph.revision": run.Revision})
	nodeInfo := make(map[string]NodeInfo, len(nodes))
	for _, node := range nodes {
		nodeInfo[node.ID] = node
	}
	sink := &lifecycleSink{run: run, runInvocation: invocation, nodes: map[graphNodeKey]nodeLifecycle{}, nodeInfo: nodeInfo}
	return context.WithValue(graphCtx, lifecycleSinkKey{}, sink)
}

func (s *lifecycleSink) BindEventContext(ctx context.Context, event lifecycleEvent) context.Context {
	if event.Type != eventNodeStart {
		return ctx
	}
	key := graphNodeKey{id: event.NodeID, step: event.Step}
	s.mu.Lock()
	defer s.mu.Unlock()
	if node, ok := s.nodes[key]; ok {
		return observe.BindInvocation(ctx, node.invocation)
	}
	execution := s.nodeExecution(event)
	nodeCtx, invocation := observe.Begin(ctx, "graph.node", event.NodeID, nodeExecutionMetadata(execution))
	s.nodes[key] = nodeLifecycle{execution: execution, invocation: invocation, ctx: nodeCtx}
	return nodeCtx
}

func (s *lifecycleSink) Publish(ctx context.Context, event lifecycleEvent) {
	if s == nil {
		return
	}
	s.mu.Lock()
	switch event.Type {
	case eventGraphStart:
		invocation := s.runInvocation
		s.mu.Unlock()
		start := invocation.WithPhase(observe.PhaseStart)
		bound := observe.BindInvocation(ctx, invocation)
		observe.Emit(bound, observe.EventOf(start, observe.KindRun, runAttrs(s.run)))
	case eventNodeStart:
		key := graphNodeKey{id: event.NodeID, step: event.Step}
		node := s.nodes[key]
		if node.invocation.Operation == "" {
			execution := s.nodeExecution(event)
			nodeCtx, invocation := observe.Begin(ctx, "graph.node", event.NodeID, nodeExecutionMetadata(execution))
			node = nodeLifecycle{execution: execution, invocation: invocation, ctx: nodeCtx}
			s.nodes[key] = node
		}
		s.mu.Unlock()
		observe.Emit(node.ctx, observe.EventOf(node.invocation.WithPhase(observe.PhaseStart), observe.KindNode, nil))
	case eventNodeOutput:
		node := s.nodes[graphNodeKey{id: event.NodeID, step: event.Step}]
		s.mu.Unlock()
		observe.Emit(node.ctx, observe.EventOf(node.invocation.WithPhase(observe.PhaseOutput), observe.KindNode, nil))
	case eventNodeEnd:
		key := graphNodeKey{id: event.NodeID, step: event.Step}
		node := s.nodes[key]
		delete(s.nodes, key)
		s.mu.Unlock()
		observe.Emit(node.ctx, observe.EventOf(node.invocation.WithPhase(observe.PhaseEnd), observe.KindNode, nil))
	case eventGraphEnd:
		invocation := s.runInvocation
		s.mu.Unlock()
		end := invocation.WithPhase(observe.PhaseEnd)
		bound := observe.BindInvocation(ctx, invocation)
		observe.Emit(bound, observe.EventOf(end, observe.KindRun, mergeAttrs(runAttrs(s.run), "graph.step", event.Step)))
	case eventGraphError:
		active := make([]nodeLifecycle, 0, len(s.nodes))
		for _, node := range s.nodes {
			active = append(active, node)
		}
		// 扇出顺序按 step 排序，公共流里节点错误的出现顺序稳定。
		sort.Slice(active, func(i, j int) bool { return active[i].execution.Step < active[j].execution.Step })
		s.nodes = map[graphNodeKey]nodeLifecycle{}
		invocation := s.runInvocation
		s.mu.Unlock()
		for _, node := range active {
			failed := node.invocation.WithPhase(observe.PhaseError)
			nodeEvent := observe.EventOf(failed, observe.KindNode, nil)
			nodeEvent.Err = event.Err
			observe.Emit(node.ctx, nodeEvent)
		}
		failed := invocation.WithPhase(observe.PhaseError)
		bound := observe.BindInvocation(ctx, invocation)
		runEvent := observe.EventOf(failed, observe.KindRun, runAttrs(s.run))
		runEvent.Err = event.Err
		observe.Emit(bound, runEvent)
	case eventGraphSuspend:
		// 挂起收尾：活动节点按 step 序先结束，run 随后。结束事件带
		// graph.outcome=suspended，区别于正常完成与失败；同一 SpanID
		// 收尾，OTel/Slog/公共流据此闭合本次执行记录。
		active := make([]nodeLifecycle, 0, len(s.nodes))
		for _, node := range s.nodes {
			active = append(active, node)
		}
		sort.Slice(active, func(i, j int) bool { return active[i].execution.Step < active[j].execution.Step })
		s.nodes = map[graphNodeKey]nodeLifecycle{}
		invocation := s.runInvocation
		s.mu.Unlock()
		for _, node := range active {
			suspended := node.invocation.WithPhase(observe.PhaseEnd)
			observe.Emit(node.ctx, observe.EventOf(suspended, observe.KindNode, map[string]any{"graph.outcome": "suspended"}))
		}
		suspended := invocation.WithPhase(observe.PhaseEnd)
		bound := observe.BindInvocation(ctx, invocation)
		observe.Emit(bound, observe.EventOf(suspended, observe.KindRun, mergeAttrs(runAttrs(s.run), "graph.outcome", "suspended", "graph.step", event.Step)))
	default:
		s.mu.Unlock()
	}
}

func (s *lifecycleSink) nodeExecution(event lifecycleEvent) nodeExec {
	info := s.nodeInfo[event.NodeID]
	return nodeExec{
		GraphName:  s.run.Name,
		NodeID:     event.NodeID,
		Kind:       info.Kind,
		Step:       event.Step,
		InputType:  info.InputType,
		OutputType: info.OutputType,
	}
}

func nodeExecutionMetadata(execution nodeExec) map[string]any {
	return map[string]any{
		"graph.node_id":     execution.NodeID,
		"graph.node_kind":   execution.Kind,
		"graph.step":        execution.Step,
		"graph.input_type":  execution.InputType,
		"graph.output_type": execution.OutputType,
	}
}

// runAttrs 是 run 级事件补充到 span 元数据上的运行属性。
func runAttrs(run runInfo) map[string]any {
	attrs := map[string]any{
		"graph.input_type":  run.InputType,
		"graph.output_type": run.OutputType,
		"graph.resume_mode": string(run.ResumeMode),
	}
	if run.CheckpointKey != "" {
		attrs["checkpoint.key"] = run.CheckpointKey
	}
	return attrs
}

// mergeAttrs 在 attrs 副本上补充键值，供一次性事件组装使用。
func mergeAttrs(attrs map[string]any, pairs ...any) map[string]any {
	merged := make(map[string]any, len(attrs)+len(pairs)/2)
	for k, v := range attrs {
		merged[k] = v
	}
	for i := 0; i+1 < len(pairs); i += 2 {
		key, _ := pairs[i].(string)
		merged[key] = pairs[i+1]
	}
	return merged
}
