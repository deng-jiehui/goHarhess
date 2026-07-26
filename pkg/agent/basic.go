package agent

import (
	"context"

	"goharness/pkg/schema"
)

type BasicAgent struct {
	*Base
}

func NewBasicAgent(cfg BaseConfig) (*BasicAgent, error) {
	base, err := NewBase(cfg)
	if err != nil {
		return nil, err
	}
	return &BasicAgent{Base: base}, nil
}

func (a *BasicAgent) Run(ctx context.Context, messages []schema.Message, opts ...RunOption) (Stream, error) {
	runOpts := resolveRunOptions(opts...)
	prepared := a.prepareMessages(messages, &runOpts)
	llmStream, err := a.callLLM(ctx, prepared, nil, runOpts)
	if err != nil {
		return nil, err
	}

	out := make(chan Chunk)
	go func() {
		defer close(out)
		for chunk := range llmStream {
			if chunk.Err != nil {
				out <- Chunk{Err: chunk.Err}
				return
			}
			select {
			case <-ctx.Done():
				out <- Chunk{Err: ctx.Err()}
				return
			case out <- Chunk{Messages: a.withAgentName(chunk.Messages)}:
			}
		}
	}()
	return out, nil
}
