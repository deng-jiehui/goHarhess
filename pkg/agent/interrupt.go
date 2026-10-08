package agent

import (
	"context"
	"errors"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
)

// WithResume 将外部输入交给指定的 Graph 中断，具体含义由发起节点解释。
// 恢复输入是 Graph 的入口配置，经 Agent 的转发通道直达配方图入口。
func WithResume(interruptID string, input any) runnable.Option {
	return runnable.WrapOption(func(opts *RunOptions) {
		opts.forwarded = append(opts.forwarded, graph.WithResume(interruptID, input))
	})
}

type AgentKind string

const (
	AgentKindBasic     AgentKind = "basic"
	AgentKindReact     AgentKind = "react"
	AgentKindRouting   AgentKind = "routing"
	AgentKindGroupChat AgentKind = "group_chat"
)

// Resume continues a Graph-owned execution checkpoint with the supplied Agent.
func Resume(ctx context.Context, value Agent, opts ...runnable.Option) ([]schema.Message, error) {
	if isNilAgent(value) {
		return nil, errors.New("resume agent cannot be nil")
	}
	options := append([]runnable.Option(nil), opts...)
	options = append(options, withResume())
	return value.Invoke(ctx, nil, options...)
}

func ResumeStream(ctx context.Context, value Agent, opts ...runnable.Option) (*runnable.StreamReader[[]schema.Message], error) {
	if isNilAgent(value) {
		return nil, errors.New("resume agent cannot be nil")
	}
	options := append([]runnable.Option(nil), opts...)
	options = append(options, withResume())
	return value.Stream(ctx, nil, options...)
}

func withResume() runnable.Option {
	return runnable.WrapOption(func(opts *RunOptions) { opts.resume = true })
}
