// Package react provides the React-specific construction entry point.
package react

import (
	"context"
	"errors"

	"goharness/pkg/agent"
)

// Config is the React Agent configuration. BaseConfig.Tools accepts any
// framework BaseTool, including MCP adapters and application-defined tools.
type Config = agent.ReactAgentConfig

// NewAgent builds a React Agent from config. The context is used as the
// construction lifecycle boundary and is validated here; request contexts
// continue to be supplied to Invoke/Stream. React's internal recipe always
// contains the bounded llm -> tools -> llm loop, and an empty tool list simply
// takes the direct model path.
func NewAgent(ctx context.Context, config Config, options ...agent.Option) (*agent.ReactAgent, error) {
	if ctx == nil {
		return nil, errors.New("react agent construction context cannot be nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return agent.NewReactAgent(config, options...)
}

// MustNewAgent is NewAgent with panic-on-configuration-error semantics.
func MustNewAgent(ctx context.Context, config Config, options ...agent.Option) *agent.ReactAgent {
	value, err := NewAgent(ctx, config, options...)
	if err != nil {
		panic(err)
	}
	return value
}
