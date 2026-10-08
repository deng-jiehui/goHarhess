package graph

import "context"

func graphFailure(ctx context.Context, err error) error {
	publish(ctx, Event{Type: EventGraphError, Err: err})
	return err
}
