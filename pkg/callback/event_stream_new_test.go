package callback_test

import (
	"context"
	"errors"
	"testing"

	"goharness/pkg/callback"
)

func TestEventStreamRoutesExactTypesInRegistrationOrder(t *testing.T) {
	var calls []string
	ctx := callback.WithStreamCallbacks(
		context.Background(),
		callback.StreamCallbackOf[string]("first", func(_ context.Context, value string) error {
			calls = append(calls, "first:"+value)
			return nil
		}),
		callback.StreamCallbackOf[int]("integer", func(context.Context, int) error {
			t.Fatal("wrong payload type was dispatched")
			return nil
		}),
		callback.StreamCallbackOf[string]("second", func(_ context.Context, value string) error {
			calls = append(calls, "second:"+value)
			return nil
		}),
	)
	callback.EmitStream(ctx, "chunk")
	if len(calls) != 2 || calls[0] != "first:chunk" || calls[1] != "second:chunk" {
		t.Fatalf("calls = %#v", calls)
	}
}

func TestEventStreamFailureUsesManagerErrorSink(t *testing.T) {
	sentinel := errors.New("ui failed")
	var reported []callback.CallbackError
	manager, err := callback.NewManager(callback.Config{}, nil, callback.ErrorSinkFunc(func(_ context.Context, failure callback.CallbackError) {
		reported = append(reported, failure)
	}))
	if err != nil {
		t.Fatal(err)
	}
	ctx := callback.WithManager(context.Background(), manager)
	ctx = callback.WithStreamCallbacks(ctx, callback.StreamCallbackOf[string]("ui", func(context.Context, string) error {
		return sentinel
	}))
	callback.EmitStream(ctx, "chunk")
	if len(reported) != 1 || reported[0].Handler != "ui" || !errors.Is(reported[0].Err, sentinel) {
		t.Fatalf("reported = %#v", reported)
	}
}
