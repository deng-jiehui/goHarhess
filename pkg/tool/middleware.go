package tool

import (
	"context"
)

// Next is the only user-zone operation that can reach the actual tool.
// It is valid only for calls started before the current Middleware.Handle returns.
type Next func(ctx context.Context, input CallInput) (Result, error)

// Middleware adds business behavior around a tool invocation.
// It may reject, transform input/output, or call next to continue execution.
type Middleware interface {
	Handle(ctx context.Context, spec Spec, input CallInput, next Next) (Result, error)
}

// MiddlewareFunc adapts a function to Middleware.
type MiddlewareFunc func(ctx context.Context, spec Spec, input CallInput, next Next) (Result, error)

// Handle invokes f. A nil function returns a configuration error.
func (f MiddlewareFunc) Handle(ctx context.Context, spec Spec, input CallInput, next Next) (Result, error) {
	if f == nil {
		return Result{}, ErrInvalidMiddleware
	}
	return f(ctx, spec, input, next)
}

// NamedMiddleware optionally supplies a stable name for diagnostics.
type NamedMiddleware interface {
	Middleware
	Name() string
}

// MiddlewareFuncs is a named function-backed Middleware.
type MiddlewareFuncs struct {
	MiddlewareName string
	HandleFunc     MiddlewareFunc
}

// Name returns the diagnostic middleware name.
func (m MiddlewareFuncs) Name() string {
	return m.MiddlewareName
}

// Handle invokes HandleFunc.
func (m MiddlewareFuncs) Handle(ctx context.Context, spec Spec, input CallInput, next Next) (Result, error) {
	return m.HandleFunc.Handle(ctx, spec, input, next)
}
