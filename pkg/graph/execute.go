package graph

import "context"

func graphFailure(ctx context.Context, err error) error {
	publish(ctx, lifecycleEvent{Type: eventGraphError, Err: err})
	return err
}
