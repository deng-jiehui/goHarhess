package agent

import (
	"context"
	"errors"

	"goharness/pkg/graph"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

// AgentNode mounts an Agent as a public Graph component. Child failures are
// returned as data so the parent recipe can choose fail-fast or observation
// semantics in its PostHandler.
type AgentNode struct {
	agent Agent
}

func NewAgentNode(value Agent) (*AgentNode, error) {
	if isNilAgent(value) {
		return nil, errors.New("agent node requires an agent")
	}
	return &AgentNode{agent: value}, nil
}

func (n *AgentNode) Name() string {
	if n == nil || isNilAgent(n.agent) {
		return ""
	}
	return n.agent.Name()
}

func (n *AgentNode) Description() string {
	if n == nil || isNilAgent(n.agent) {
		return ""
	}
	return n.agent.Description()
}

func (*AgentNode) NodeKind() runnable.NodeKind { return runnable.NodeKindAgent }

func (n *AgentNode) Invoke(ctx context.Context, messages []schema.Message, invocationOptions ...runnable.Option) (runnable.Outcome[[]schema.Message], error) {
	if n == nil || isNilAgent(n.agent) {
		return runnable.Outcome[[]schema.Message]{}, errors.New("agent node is not configured")
	}
	if ctx == nil {
		return runnable.Outcome[[]schema.Message]{}, errors.New("agent node context cannot be nil")
	}
	runOptions, _ := resolvedRunOptionsFromContext(ctx)
	childOptions := []RunOption{WithCheckpointBypass()}
	if runOptions.DeltaStream {
		childOptions = append(childOptions, WithDeltaStream(true))
	}
	options := runOptionsAsRunnable(childOptions)
	options = append(options, runOptions.GlobalOptions...)
	options = append(options, runOptions.NodeOptions...)
	for _, option := range invocationOptions {
		if !runnable.IsForwardedOption(option) {
			options = append(options, option)
		}
	}
	childCtx := graph.WithEventSinkIsolated(ctx, nil)
	childCtx = runnable.WithOptionPlan(childCtx, runnable.OptionPlan{})
	reader, err := n.agent.Stream(childCtx, schema.CloneMessages(messages), options...)
	if err != nil {
		return runnable.Outcome[[]schema.Message]{Err: err}, nil
	}
	defer reader.Close()
	output, err := reader.Collect(childCtx)
	return runnable.Outcome[[]schema.Message]{Value: schema.CloneMessages(output), Err: err}, nil
}

func (n *AgentNode) Stream(ctx context.Context, input []schema.Message, options ...runnable.Option) (*runnable.StreamReader[runnable.Outcome[[]schema.Message]], error) {
	return runnable.StreamFromInvoke(ctx, input, n.Invoke, options...)
}

func (n *AgentNode) Collect(ctx context.Context, input *runnable.StreamReader[[]schema.Message], options ...runnable.Option) (runnable.Outcome[[]schema.Message], error) {
	return runnable.CollectFromInvoke(ctx, input, n.Invoke, options...)
}

func (n *AgentNode) Transform(ctx context.Context, input *runnable.StreamReader[[]schema.Message], options ...runnable.Option) (*runnable.StreamReader[runnable.Outcome[[]schema.Message]], error) {
	return runnable.TransformFromInvoke(ctx, input, n.Invoke, options...)
}

var _ runnable.NamedRunnable[[]schema.Message, runnable.Outcome[[]schema.Message]] = (*AgentNode)(nil)
var _ runnable.NodeDescriptor = (*AgentNode)(nil)
