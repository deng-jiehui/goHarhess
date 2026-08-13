package agent

import (
	"context"
	"errors"

	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

type AgentKind string

const (
	AgentKindBasic     AgentKind = "basic"
	AgentKindReact     AgentKind = "react"
	AgentKindRouting   AgentKind = "routing"
	AgentKindGroupChat AgentKind = "group_chat"
)

// Resume continues a Graph-owned execution checkpoint with the supplied Agent.
func Resume(ctx context.Context, value Agent, opts ...RunOption) ([]schema.Message, error) {
	if isNilAgent(value) {
		return nil, errors.New("resume agent cannot be nil")
	}
	options := append([]RunOption(nil), opts...)
	options = append(options, withResume())
	return value.Invoke(ctx, nil, runOptionsAsRunnable(options)...)
}

func ResumeStream(ctx context.Context, value Agent, opts ...RunOption) (runnable.Stream[schema.MessageEvent], error) {
	if isNilAgent(value) {
		return nil, errors.New("resume agent cannot be nil")
	}
	options := append([]RunOption(nil), opts...)
	options = append(options, withResume())
	return value.Stream(ctx, nil, runOptionsAsRunnable(options)...)
}

func withResume() RunOption {
	return func(opts *RunOptions) { opts.resume = true }
}
