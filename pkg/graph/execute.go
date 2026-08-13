package graph

import "context"

func publishControl(ctx context.Context, event Event) error {
	publish(ctx, event)
	return nil
}

func graphFailure(ctx context.Context, err error) error {
	publish(ctx, Event{Type: EventGraphError, Err: err})
	return err
}
