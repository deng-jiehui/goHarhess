package callback

import "context"

// ErrorSink receives isolated Handler failures without changing the business
// result. OnError runs synchronously and must be safe for concurrent use.
type ErrorSink interface {
	OnError(context.Context, CallbackError)
}

// ErrorSinkFunc adapts a function to ErrorSink.
type ErrorSinkFunc func(context.Context, CallbackError)

func (f ErrorSinkFunc) OnError(ctx context.Context, callbackErr CallbackError) {
	if f != nil {
		f(ctx, callbackErr)
	}
}
