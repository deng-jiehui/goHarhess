package callback

import (
	"context"
	"errors"
)

// Handler is the synchronous lifecycle observation interface. Handle runs in
// the caller's goroutine and must not authorize, transform or otherwise
// control execution.
type Handler interface {
	Handle(context.Context, Event) error
}

// Callback is retained as a behavior-free alias for Handler.
type Callback = Handler

// CallbackFunc adapts a function to Handler.
type CallbackFunc func(context.Context, Event) error

func (f CallbackFunc) Handle(ctx context.Context, event Event) error {
	if f == nil {
		return nil
	}
	return f(ctx, event)
}

// NamedHandler optionally supplies a stable diagnostic name.
type NamedHandler interface {
	Handler
	Name() string
}

// NamedCallback is retained as a behavior-free alias for NamedHandler.
type NamedCallback = NamedHandler

// Funcs declares optional functions for the complete built-in lifecycle. An
// OnEvent function, when present, runs after the event-specific function.
type Funcs struct {
	CallbackName string
	OnEvent      CallbackFunc

	OnAgentStart CallbackFunc
	OnAgentChunk CallbackFunc
	OnAgentEnd   CallbackFunc
	OnAgentError CallbackFunc

	OnLLMStart CallbackFunc
	OnLLMChunk CallbackFunc
	OnLLMEnd   CallbackFunc
	OnLLMError CallbackFunc

	OnRetrieverStart CallbackFunc
	OnRetrieverEnd   CallbackFunc
	OnRetrieverError CallbackFunc

	OnToolStart CallbackFunc
	OnToolEnd   CallbackFunc
	OnToolError CallbackFunc

	OnGraphStart CallbackFunc
	OnGraphNode  CallbackFunc
	OnGraphEnd   CallbackFunc
	OnGraphError CallbackFunc
}

func (f Funcs) Name() string { return f.CallbackName }

func (f Funcs) Handle(ctx context.Context, event Event) error {
	var specific CallbackFunc
	switch event.Type {
	case EventAgentStart:
		specific = f.OnAgentStart
	case EventAgentChunk:
		specific = f.OnAgentChunk
	case EventAgentEnd:
		specific = f.OnAgentEnd
	case EventAgentError:
		specific = f.OnAgentError
	case EventLLMStart:
		specific = f.OnLLMStart
	case EventLLMChunk:
		specific = f.OnLLMChunk
	case EventLLMEnd:
		specific = f.OnLLMEnd
	case EventLLMError:
		specific = f.OnLLMError
	case EventRetrieverStart:
		specific = f.OnRetrieverStart
	case EventRetrieverEnd:
		specific = f.OnRetrieverEnd
	case EventRetrieverError:
		specific = f.OnRetrieverError
	case EventToolStart:
		specific = f.OnToolStart
	case EventToolEnd:
		specific = f.OnToolEnd
	case EventToolError:
		specific = f.OnToolError
	case EventGraphStart:
		specific = f.OnGraphStart
	case EventGraphNode:
		specific = f.OnGraphNode
	case EventGraphEnd:
		specific = f.OnGraphEnd
	case EventGraphError:
		specific = f.OnGraphError
	}
	var specificErr error
	if specific != nil {
		specificErr = specific(ctx, event)
	}
	if f.OnEvent == nil {
		return specificErr
	}
	return errors.Join(specificErr, f.OnEvent(ctx, event))
}
