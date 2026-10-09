package agent

import (
	"context"
	"errors"

	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/stream"
)

// StreamEvents publishes one result only after the Agent's own stream (including
// conversation commit) completes. It does not alter the Agent interface.
func StreamEvents(ctx context.Context, value Agent, input []schema.Message, config stream.Config, options ...runnable.Option) (*stream.Reader[[]schema.Message], error) {
	if value == nil {
		return nil, errors.New("agent is nil")
	}
	input = schema.CloneMessages(input)
	return stream.Run(ctx, func(run context.Context) ([]schema.Message, error) {
		run = stream.WithNamespace(run, agentName(value))
		run, seen := stream.TrackMessages(run)
		r, err := value.Stream(run, input, options...)
		if err != nil {
			return nil, err
		}
		defer r.Close()
		result, err := r.Collect(r.Context())
		if err != nil {
			return nil, err
		}
		stream.EnsureMessageIDs(result)
		if !seen.Load() {
			if err := stream.PublishSnapshots(run, result); err != nil {
				return nil, err
			}
		}
		return result, nil
	}, config)
}

// ResumeStreamEvents 使用公共事件流恢复 Agent，不修改 Agent 接口。
func ResumeStreamEvents(ctx context.Context, value Agent, config stream.Config, options ...runnable.Option) (*stream.Reader[[]schema.Message], error) {
	if isNilAgent(value) {
		return nil, errors.New("resume agent cannot be nil")
	}
	opts := append([]runnable.Option(nil), options...)
	opts = append(opts, withResume())
	return StreamEvents(ctx, value, nil, config, opts...)
}
