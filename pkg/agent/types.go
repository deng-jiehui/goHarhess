package agent

import (
	"context"

	"goharness/pkg/schema"
)

type Chunk struct {
	Messages []schema.Message
	Err      error
}

type Stream <-chan Chunk

type Agent interface {
	Run(ctx context.Context, messages []schema.Message, opts ...RunOption) (Stream, error)
	Name() string
	Description() string
}

func Collect(stream Stream) ([]schema.Message, error) {
	var last []schema.Message
	for chunk := range stream {
		if chunk.Err != nil {
			return nil, chunk.Err
		}
		last = schema.CloneMessages(chunk.Messages)
	}
	return last, nil
}

func RunSync(ctx context.Context, a Agent, messages []schema.Message, opts ...RunOption) ([]schema.Message, error) {
	stream, err := a.Run(ctx, messages, opts...)
	if err != nil {
		return nil, err
	}
	return Collect(stream)
}
