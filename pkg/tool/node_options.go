package tool

import (
	"context"
	"errors"
	"time"

	"goharness/pkg/runnable"
)

const nodeExecutionOptionsKey = "tool.node.execution"

type nodeExecutionOptions struct {
	MaxConcurrency int
	Timeout        time.Duration
}

type nodeExecutionOption struct {
	maxConcurrency int
	timeout        time.Duration
}

func (o nodeExecutionOption) Apply(set *runnable.OptionSet) error {
	if set == nil {
		return errors.New("tool node option set is nil")
	}
	if o.maxConcurrency < 0 {
		return errors.New("tool node max concurrency cannot be negative")
	}
	if o.timeout < 0 {
		return errors.New("tool node timeout cannot be negative")
	}
	if set.Values == nil {
		set.Values = map[string]any{}
	}
	current, _ := set.Values[nodeExecutionOptionsKey].(nodeExecutionOptions)
	if o.maxConcurrency > 0 {
		current.MaxConcurrency = o.maxConcurrency
	}
	if o.timeout > 0 {
		current.Timeout = o.timeout
	}
	set.Values[nodeExecutionOptionsKey] = current
	return nil
}

func (nodeExecutionOption) OptionScope() runnable.OptionScope { return runnable.OptionScopeNode }

// WithMaxConcurrency tightens one ToolsNode invocation's batch parallelism.
func WithMaxConcurrency(max int) runnable.Option {
	return nodeExecutionOption{maxConcurrency: max}
}

// WithTimeout tightens one ToolsNode invocation's per-call timeout.
func WithTimeout(timeout time.Duration) runnable.Option {
	return nodeExecutionOption{timeout: timeout}
}

func nodeExecutionOptionsFromPlan(plan runnable.OptionPlan) nodeExecutionOptions {
	value, _ := plan.Set().Values[nodeExecutionOptionsKey].(nodeExecutionOptions)
	return value
}

type executionLimitsContextKey struct{}

// ExecutionLimits tighten one ToolsNode call without changing the immutable
// node configuration.
type ExecutionLimits struct {
	MaxConcurrency int
	Timeout        time.Duration
}

// WithExecutionLimits binds per-run limits for a ToolsNode reached through a
// compiled Graph, where invocation options cannot target one node by ID.
func WithExecutionLimits(ctx context.Context, limits ExecutionLimits) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, executionLimitsContextKey{}, limits)
}

func executionLimitsFromContext(ctx context.Context) ExecutionLimits {
	if ctx == nil {
		return ExecutionLimits{}
	}
	limits, _ := ctx.Value(executionLimitsContextKey{}).(ExecutionLimits)
	return limits
}
