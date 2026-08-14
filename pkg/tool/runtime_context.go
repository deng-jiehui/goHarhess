package tool

import (
	"context"
	"time"
)

type executionState struct {
	depth      int
	heldGroups []string
}

type executionStateKey struct{}

func stateFromContext(ctx context.Context) executionState {
	if ctx == nil {
		return executionState{}
	}
	state, _ := ctx.Value(executionStateKey{}).(executionState)
	state.heldGroups = append([]string(nil), state.heldGroups...)
	return state
}

type controlledContext struct {
	control context.Context
	values  context.Context
	state   executionState
}

func (c *controlledContext) Deadline() (time.Time, bool) {
	controlDeadline, controlOK := c.control.Deadline()
	valueDeadline, valueOK := c.values.Deadline()
	switch {
	case controlOK && valueOK && valueDeadline.Before(controlDeadline):
		return valueDeadline, true
	case controlOK:
		return controlDeadline, true
	default:
		return valueDeadline, valueOK
	}
}

func (c *controlledContext) Done() <-chan struct{} {
	return c.control.Done()
}

func (c *controlledContext) Err() error {
	return c.control.Err()
}

func (c *controlledContext) Value(key any) any {
	if _, ok := key.(executionStateKey); ok {
		state := c.state
		state.heldGroups = append([]string(nil), state.heldGroups...)
		return state
	}
	if value := c.values.Value(key); value != nil {
		return value
	}
	return c.control.Value(key)
}

func bindContext(control context.Context, values context.Context, state executionState) (context.Context, func()) {
	base := &controlledContext{control: control, values: values, state: state}
	bound, cancel := context.WithCancelCause(base)
	stopValues := context.AfterFunc(values, func() {
		cancel(context.Cause(values))
	})
	cleanup := func() {
		stopValues()
		cancel(nil)
	}
	return bound, cleanup
}

func withHeldGroup(ctx context.Context, group string) context.Context {
	state := stateFromContext(ctx)
	state.heldGroups = append(state.heldGroups, group)
	return context.WithValue(ctx, executionStateKey{}, state)
}
