package agent

import (
	"context"

	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
)

type agentStreamFunc func(context.Context, []schema.Message, ...runnable.Option) (*runnable.StreamReader[[]schema.Message], error)

// streamAgent/invokeAgent 是 Runnable 契约入口：所有调用选项原样进入
// stream，由 resolveRunOptions 按类型归属统一解析与分流。
func streamAgent(ctx context.Context, stream agentStreamFunc, messages []schema.Message, options ...runnable.Option) (*runnable.StreamReader[[]schema.Message], error) {
	return stream(ctx, messages, options...)
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
