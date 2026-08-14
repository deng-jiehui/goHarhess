package agent

import "fmt"

// New creates an Agent and automatically selects its Graph recipe. Tool-backed
// configurations use the standard ReAct recipe; otherwise the minimal
// one-model graph is used.
func New(cfg BaseConfig, opts ...Option) (Agent, error) {
	resolved, err := resolveBaseConfig(cfg, opts...)
	if err != nil {
		return nil, fmt.Errorf("configure agent %q: %w", cfg.Name, err)
	}
	if len(resolved.Tools) > 0 {
		return NewReactAgent(ReactAgentConfig{BaseConfig: resolved})
	}
	return NewBasicAgent(resolved)
}

func MustNew(cfg BaseConfig, opts ...Option) Agent {
	value, err := New(cfg, opts...)
	if err != nil {
		panic(err)
	}
	return value
}
