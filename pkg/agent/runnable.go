package agent

import (
	"context"
	"fmt"

	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

func runOptionsFromRunnable(ctx context.Context, options []runnable.Option) ([]RunOption, error) {
	plan, planned := runnable.OptionPlanFromContext(ctx)
	if !planned {
		var err error
		plan, err = runnable.NewOptionPlan(options...)
		if err != nil {
			return nil, fmt.Errorf("resolve agent options: %w", err)
		}
	} else {
		local := make([]runnable.Option, 0, len(options))
		for index, option := range options {
			if option == nil {
				return nil, fmt.Errorf("agent option %d is nil", index)
			}
			if !runnable.IsForwardedOption(option) {
				local = append(local, option)
			}
		}
		if len(local) > 0 {
			var err error
			plan, err = plan.Cascade(local...)
			if err != nil {
				return nil, fmt.Errorf("cascade agent options: %w", err)
			}
		}
	}
	set := plan.Set()
	resolved := make([]RunOption, 0, len(options)+1)
	if values, ok := set.Values[runOptionsValueKey]; ok {
		runValues, valid := values.([]RunOption)
		if !valid {
			return nil, fmt.Errorf("agent option set has an invalid %q value", runOptionsValueKey)
		}
		for index, value := range runValues {
			if value == nil {
				return nil, fmt.Errorf("agent run option %d is nil", index)
			}
			resolved = append(resolved, value)
		}
	}
	if len(set.Metadata) > 0 {
		metadata := make(map[string]any, len(set.Metadata))
		for key, value := range set.Metadata {
			metadata[key] = value
		}
		resolved = append(resolved, func(opts *RunOptions) {
			for key, value := range metadata {
				opts.Metadata[key] = value
			}
		})
	}
	for index, option := range plan.ForwardOptions() {
		if option == nil {
			return nil, fmt.Errorf("agent option %d is nil", index)
		}
		inner := runnable.UnwrapOption(option)
		if runOption, ok := inner.(RunOption); ok {
			if runOption == nil {
				return nil, fmt.Errorf("agent run option %d is nil", index)
			}
			continue
		}
		if runnable.ScopeOf(option) == runnable.OptionScopeNode {
			resolved = append(resolved, forwardNodeOption(option))
			continue
		}
		// Unknown global options are intentionally forwarded. They may be
		// understood by a Context Engine, model, or extension component.
		resolved = append(resolved, preserveGlobalOption(option))
	}
	return resolved, nil
}

func preserveGlobalOption(option runnable.Option) RunOption {
	return func(opts *RunOptions) { opts.GlobalOptions = append(opts.GlobalOptions, option) }
}

// forwardNodeOption records an opaque node option in the run configuration.
// It is represented as a RunOption so the public execution entry remains
// Runnable-based while the private Agent pipeline retains the original value.
func forwardNodeOption(option runnable.Option) RunOption {
	forwarded := option
	if runnable.ScopeOf(option) != runnable.OptionScopeNode {
		forwarded = runnable.NodeOption(option)
	}
	return func(opts *RunOptions) { opts.NodeOptions = append(opts.NodeOptions, forwarded) }
}

func runOptionsAsRunnable(options []RunOption) []runnable.Option {
	converted := make([]runnable.Option, len(options))
	for index, option := range options {
		converted[index] = option
	}
	return converted
}

type agentStreamFunc func(context.Context, []schema.Message, ...RunOption) (*runnable.StreamReader[[]schema.Message], error)

func streamAgent(ctx context.Context, stream agentStreamFunc, messages []schema.Message, options ...runnable.Option) (*runnable.StreamReader[[]schema.Message], error) {
	resolved, err := runOptionsFromRunnable(ctx, options)
	if err != nil {
		return nil, err
	}
	return stream(ctx, messages, resolved...)
}

func invokeAgent(ctx context.Context, stream agentStreamFunc, messages []schema.Message, options ...runnable.Option) ([]schema.Message, error) {
	reader, err := streamAgent(ctx, stream, messages, options...)
	if err != nil {
		return nil, err
	}
	return reader.Collect(reader.Context())
}

func (a *BasicAgent) Invoke(ctx context.Context, messages []schema.Message, options ...runnable.Option) ([]schema.Message, error) {
	return invokeAgent(ctx, a.stream, messages, options...)
}

func (a *BasicAgent) Stream(ctx context.Context, messages []schema.Message, options ...runnable.Option) (*runnable.StreamReader[[]schema.Message], error) {
	return streamAgent(ctx, a.stream, messages, options...)
}

func (a *BasicAgent) Collect(ctx context.Context, input *runnable.StreamReader[[]schema.Message], options ...runnable.Option) ([]schema.Message, error) {
	return runnable.CollectFromInvoke(ctx, input, a.Invoke, options...)
}

func (a *BasicAgent) Transform(ctx context.Context, input *runnable.StreamReader[[]schema.Message], options ...runnable.Option) (*runnable.StreamReader[[]schema.Message], error) {
	return runnable.TransformFromInvoke(ctx, input, a.Invoke, options...)
}

func (a *ReactAgent) Invoke(ctx context.Context, messages []schema.Message, options ...runnable.Option) ([]schema.Message, error) {
	return invokeAgent(ctx, a.stream, messages, options...)
}

func (a *ReactAgent) Stream(ctx context.Context, messages []schema.Message, options ...runnable.Option) (*runnable.StreamReader[[]schema.Message], error) {
	return streamAgent(ctx, a.stream, messages, options...)
}

func (a *ReactAgent) Collect(ctx context.Context, input *runnable.StreamReader[[]schema.Message], options ...runnable.Option) ([]schema.Message, error) {
	return runnable.CollectFromInvoke(ctx, input, a.Invoke, options...)
}

func (a *ReactAgent) Transform(ctx context.Context, input *runnable.StreamReader[[]schema.Message], options ...runnable.Option) (*runnable.StreamReader[[]schema.Message], error) {
	return runnable.TransformFromInvoke(ctx, input, a.Invoke, options...)
}

func (a *RoutingAgent) Invoke(ctx context.Context, messages []schema.Message, options ...runnable.Option) ([]schema.Message, error) {
	return invokeAgent(ctx, a.stream, messages, options...)
}

func (a *RoutingAgent) Stream(ctx context.Context, messages []schema.Message, options ...runnable.Option) (*runnable.StreamReader[[]schema.Message], error) {
	return streamAgent(ctx, a.stream, messages, options...)
}

func (a *RoutingAgent) Collect(ctx context.Context, input *runnable.StreamReader[[]schema.Message], options ...runnable.Option) ([]schema.Message, error) {
	return runnable.CollectFromInvoke(ctx, input, a.Invoke, options...)
}

func (a *RoutingAgent) Transform(ctx context.Context, input *runnable.StreamReader[[]schema.Message], options ...runnable.Option) (*runnable.StreamReader[[]schema.Message], error) {
	return runnable.TransformFromInvoke(ctx, input, a.Invoke, options...)
}

func (a *GroupChat) Invoke(ctx context.Context, messages []schema.Message, options ...runnable.Option) ([]schema.Message, error) {
	return invokeAgent(ctx, a.stream, messages, options...)
}

func (a *GroupChat) Stream(ctx context.Context, messages []schema.Message, options ...runnable.Option) (*runnable.StreamReader[[]schema.Message], error) {
	return streamAgent(ctx, a.stream, messages, options...)
}

func (a *GroupChat) Collect(ctx context.Context, input *runnable.StreamReader[[]schema.Message], options ...runnable.Option) ([]schema.Message, error) {
	return runnable.CollectFromInvoke(ctx, input, a.Invoke, options...)
}

func (a *GroupChat) Transform(ctx context.Context, input *runnable.StreamReader[[]schema.Message], options ...runnable.Option) (*runnable.StreamReader[[]schema.Message], error) {
	return runnable.TransformFromInvoke(ctx, input, a.Invoke, options...)
}

var (
	_ Agent = (*BasicAgent)(nil)
	_ Agent = (*ReactAgent)(nil)
	_ Agent = (*RoutingAgent)(nil)
	_ Agent = (*GroupChat)(nil)
)
