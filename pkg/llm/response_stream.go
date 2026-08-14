package llm

import (
	"context"
	"errors"
	"io"

	"goharness/pkg/callback"
	"goharness/pkg/runnable"
)

// ObserveReader owns one ChatModel lifecycle while preserving the canonical
// Response data plane.
func ObserveReader(ctx context.Context, name string, request Request, reader *runnable.StreamReader[Response]) (*runnable.StreamReader[Response], error) {
	if ctx == nil {
		return nil, errors.New("model observe context cannot be nil")
	}
	if reader == nil {
		return nil, errors.New("model response reader is nil")
	}
	modelCtx, invocation := BeginCallback(ctx, name, nil, request)
	return ObserveReaderWithInvocation(modelCtx, invocation, reader)
}

// ObserveReaderWithInvocation wraps a Reader for callers that already opened
// the lifecycle invocation and must avoid a duplicate Start event.
func ObserveReaderWithInvocation(ctx context.Context, invocation callback.Invocation, upstream *runnable.StreamReader[Response]) (*runnable.StreamReader[Response], error) {
	if ctx == nil {
		return nil, errors.New("model observe context cannot be nil")
	}
	if upstream == nil {
		return nil, errors.New("model response reader is nil")
	}
	reader, writer := runnable.Pipe[Response](context.WithoutCancel(ctx))
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer upstream.Close()
		defer writer.Close()
		var final Response
		sawResponse := false
		for {
			value, err := upstream.Recv()
			if errors.Is(err, io.EOF) {
				if !sawResponse {
					ErrorCallback(ctx, invocation, ErrStreamTruncated)
					_ = writer.CloseWithError(ErrStreamTruncated)
					return
				}
				EndCallback(ctx, invocation, final)
				return
			}
			if err != nil {
				ErrorCallback(ctx, invocation, err)
				_ = writer.CloseWithError(err)
				return
			}
			final = value.Clone()
			sawResponse = true
			if err := writer.Send(final.Clone()); err != nil {
				ErrorCallback(ctx, invocation, err)
				return
			}
		}
	}()
	closeUpstreamWhenReaderStops(reader, upstream, done)
	return reader, nil
}

func closeUpstreamWhenReaderStops[T, U any](reader *runnable.StreamReader[T], upstream *runnable.StreamReader[U], done <-chan struct{}) {
	go func() {
		select {
		case <-reader.Context().Done():
			_ = upstream.Close()
		case <-done:
		}
	}()
}

// CollectResponse reduces a canonical Response Reader.
func CollectResponse(ctx context.Context, reader *runnable.StreamReader[Response]) (Response, error) {
	if ctx == nil {
		return Response{}, errors.New("model response collect context is nil")
	}
	if reader == nil {
		return Response{}, errors.New("model response reader is nil")
	}
	response, err := reader.Collect(ctx)
	if err != nil {
		return Response{}, err
	}
	if len(response.Messages) == 0 && response.Usage == nil && response.FinishReason == "" {
		return Response{}, ErrStreamTruncated
	}
	return response, nil
}
