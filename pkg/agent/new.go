package agent

import (
	"fmt"

	"github.com/goharness/goharness/pkg/llm"
)

// New creates an Agent and automatically selects its Graph recipe. Tool-backed
// configurations use the standard ReAct recipe; otherwise the minimal
// one-model graph is used. model 为 nil 时运行期首次调用返回 ErrMissingModel。
func New(model llm.ChatModel, opts ...Option) (Agent, error) {
	resolved, err := resolveBaseConfig(baseConfig{Model: model}, opts...)
	if err != nil {
		return nil, fmt.Errorf("configure agent: %w", err)
	}
	if len(resolved.Tools) > 0 {
		return newReactAgent(resolved)
	}
	return newBasicAgent(resolved)
}

func MustNew(model llm.ChatModel, opts ...Option) Agent {
	value, err := New(model, opts...)
	if err != nil {
		panic(err)
	}
	return value
}
