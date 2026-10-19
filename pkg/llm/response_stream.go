package llm

import (
	"context"
	"errors"
	"io"

	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/stream"
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
	modelCtx, callback := beginModelCallback(ctx, name, nil, request)
	return observeReaderWithCallback(modelCtx, callback, reader)
}

// ObserveReaderWithInvocation wraps a Reader for callers that already opened
// the lifecycle invocation and must avoid a duplicate Start event.
func ObserveReaderWithInvocation(ctx context.Context, invocation observe.Invocation, upstream *runnable.StreamReader[Response]) (*runnable.StreamReader[Response], error) {
	return observeReaderWithCallback(ctx, newModelCallback(ctx, invocation), upstream)
}

func observeReaderWithCallback(ctx context.Context, callback *modelCallback, upstream *runnable.StreamReader[Response]) (*runnable.StreamReader[Response], error) {
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
		var final Response
		var terminalErr error
		// 先递交终止观测再关闭输出管道；调用方取消时仍由生产协程收尾。
		defer func() {
			callback.finish(final, terminalErr)
			if terminalErr != nil {
				_ = writer.CloseWithError(terminalErr)
				return
			}
			_ = writer.Close()
		}()
		sawResponse := false
		for {
			value, err := upstream.Recv()
			if errors.Is(err, io.EOF) {
				if !sawResponse {
					terminalErr = ErrStreamTruncated
					return
				}
				if stream.HasMessageTracking(ctx) && !stream.MessagesPublished(ctx) {
					if err := stream.PublishSnapshots(ctx, final.Messages); err != nil {
						terminalErr = err
						return
					}
				}
				return
			}
			if err != nil {
				terminalErr = err
				return
			}
			final = value.Clone()
			stream.EnsureMessageIDs(final.Messages)
			sawResponse = true
			if err := writer.Send(final.Clone()); err != nil {
				terminalErr = err
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
