package callback_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"goharness/pkg/callback"
)

type callbackContextKey struct{}

func TestSyncDispatcherRunsInlineAndAggregatesFailures(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var orderMu sync.Mutex
	var order []string
	sentinel := errors.New("telemetry backend failed")
	seenAfterPanic := atomic.Bool{}

	first := callback.Funcs{CallbackName: "blocking", OnEvent: func(ctx context.Context, _ callback.Event) error {
		if ctx.Err() != nil || ctx.Value(callbackContextKey{}) != "preserved" {
			return errors.New("callback context was not shared")
		}
		orderMu.Lock()
		order = append(order, "blocking")
		orderMu.Unlock()
		close(started)
		<-release
		return nil
	}}
	second := callback.Funcs{CallbackName: "error", OnEvent: func(context.Context, callback.Event) error {
		orderMu.Lock()
		order = append(order, "error")
		orderMu.Unlock()
		return sentinel
	}}
	third := callback.Funcs{CallbackName: "panic", OnEvent: func(context.Context, callback.Event) error {
		orderMu.Lock()
		order = append(order, "panic")
		orderMu.Unlock()
		panic("broken logger")
	}}
	fourth := callback.Funcs{CallbackName: "observer", OnEvent: func(context.Context, callback.Event) error {
		seenAfterPanic.Store(true)
		orderMu.Lock()
		order = append(order, "observer")
		orderMu.Unlock()
		return nil
	}}

	dispatcher, err := callback.New([]callback.Handler{first, second, third, fourth}, callback.Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(context.Background(), callbackContextKey{}, "preserved")
	returned := make(chan error, 1)
	go func() {
		returned <- dispatcher.Dispatch(ctx, callback.Event{Type: callback.EventAgentStart})
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("blocking Handler did not start")
	}
	select {
	case <-returned:
		t.Fatal("Dispatch returned before synchronous Handler released")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)

	var dispatchErr error
	select {
	case dispatchErr = <-returned:
	case <-time.After(time.Second):
		t.Fatal("Dispatch did not return after Handler released")
	}
	if !errors.Is(dispatchErr, sentinel) {
		t.Fatalf("Dispatch error = %v, want sentinel", dispatchErr)
	}
	var panicErr *callback.PanicError
	if !errors.As(dispatchErr, &panicErr) {
		t.Fatalf("Dispatch error = %v, want PanicError", dispatchErr)
	}
	if !seenAfterPanic.Load() {
		t.Fatal("Handler after panic was not invoked")
	}
	orderMu.Lock()
	defer orderMu.Unlock()
	if got, want := fmt.Sprint(order), "[blocking error panic observer]"; got != want {
		t.Fatalf("Handler order = %s, want %s", got, want)
	}
}

func TestSyncDispatcherTimeoutIsCooperative(t *testing.T) {
	dispatcher, err := callback.New([]callback.Handler{callback.CallbackFunc(func(ctx context.Context, _ callback.Event) error {
		<-ctx.Done()
		return ctx.Err()
	})}, callback.Config{Timeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	err = dispatcher.Dispatch(context.Background(), callback.Event{Type: callback.EventAgentStart})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Dispatch error = %v, want deadline exceeded", err)
	}
	var timeoutErr *callback.TimeoutError
	if !errors.As(err, &timeoutErr) {
		t.Fatalf("Dispatch error = %v, want TimeoutError", err)
	}
}

func TestSyncDispatcherDoesNotBlameParentDeadlineOnHandler(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()
	dispatcher, err := callback.New([]callback.Handler{callback.CallbackFunc(func(ctx context.Context, _ callback.Event) error {
		return ctx.Err()
	})}, callback.Config{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	dispatchErr := dispatcher.Dispatch(ctx, callback.Event{Type: callback.EventAgentStart})
	if !errors.Is(dispatchErr, context.DeadlineExceeded) {
		t.Fatalf("Dispatch error = %v, want context.DeadlineExceeded", dispatchErr)
	}
	var timeoutErr *callback.TimeoutError
	if errors.As(dispatchErr, &timeoutErr) {
		t.Fatalf("parent cancellation was reported as Handler timeout: %v", dispatchErr)
	}
}

func TestSyncDispatcherReportsFailuresToErrorSink(t *testing.T) {
	sentinel := errors.New("billing unavailable")
	reported := make(chan callback.CallbackError, 1)
	dispatcher, err := callback.New(
		[]callback.Handler{callback.CallbackFunc(func(context.Context, callback.Event) error { return sentinel })},
		callback.Config{},
		callback.ErrorSinkFunc(func(_ context.Context, callbackErr callback.CallbackError) {
			reported <- callbackErr
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.Dispatch(context.Background(), callback.Event{Type: callback.EventToolEnd}); !errors.Is(err, sentinel) {
		t.Fatalf("Dispatch error = %v, want sentinel", err)
	}
	select {
	case callbackErr := <-reported:
		if callbackErr.Event.Type != callback.EventToolEnd || !errors.Is(callbackErr.Err, sentinel) {
			t.Fatalf("reported error = %#v", callbackErr)
		}
	default:
		t.Fatal("ErrorSink was not called synchronously")
	}
}

func TestSyncDispatcherContainsErrorSinkPanic(t *testing.T) {
	sentinel := errors.New("audit unavailable")
	dispatcher, err := callback.New(
		[]callback.Handler{callback.CallbackFunc(func(context.Context, callback.Event) error { return sentinel })},
		callback.Config{},
		callback.ErrorSinkFunc(func(context.Context, callback.CallbackError) { panic("broken alarm") }),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.Dispatch(context.Background(), callback.Event{Type: callback.EventAgentEnd}); !errors.Is(err, sentinel) {
		t.Fatalf("Dispatch error = %v, want sentinel", err)
	}
}

func TestChildDispatcherSharesSequenceAndInheritsErrorSink(t *testing.T) {
	var reported atomic.Int32
	sink := callback.ErrorSinkFunc(func(context.Context, callback.CallbackError) {
		reported.Add(1)
	})
	parent, err := callback.New([]callback.Handler{callback.CallbackFunc(func(context.Context, callback.Event) error {
		return nil
	})}, callback.Config{}, sink)
	if err != nil {
		t.Fatal(err)
	}
	child, err := callback.NewChild(parent, []callback.Handler{callback.CallbackFunc(func(context.Context, callback.Event) error {
		return errors.New("child failed")
	})})
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Dispatch(context.Background(), callback.Event{Type: callback.EventToolEnd}); err == nil {
		t.Fatal("child Dispatch unexpectedly succeeded")
	}
	if reported.Load() != 1 {
		t.Fatalf("ErrorSink reports = %d, want 1", reported.Load())
	}
	var sequences []uint64
	collector := callback.CallbackFunc(func(_ context.Context, event callback.Event) error {
		sequences = append(sequences, event.Sequence)
		return nil
	})
	parent, err = callback.New([]callback.Handler{collector}, callback.Config{})
	if err != nil {
		t.Fatal(err)
	}
	child, err = callback.NewChild(parent, []callback.Handler{collector})
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Dispatch(context.Background(), callback.Event{Type: callback.EventToolEnd}); err != nil {
		t.Fatal(err)
	}
	if len(sequences) != 2 || sequences[0] == sequences[1] {
		t.Fatalf("parent/child sequences = %v", sequences)
	}
}

func TestSyncDispatcherFiltersChunksAndDeliversAllOtherEvents(t *testing.T) {
	var mu sync.Mutex
	var got []callback.EventType
	dispatcher, err := callback.New([]callback.Handler{callback.CallbackFunc(func(_ context.Context, event callback.Event) error {
		mu.Lock()
		got = append(got, event.Type)
		mu.Unlock()
		return nil
	})}, callback.Config{FilterChunks: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, eventType := range []callback.EventType{
		callback.EventAgentStart,
		callback.EventAgentChunk,
		callback.EventLLMChunk,
		callback.EventLLMEnd,
		callback.EventAgentEnd,
	} {
		if err := dispatcher.Dispatch(context.Background(), callback.Event{Type: eventType}); err != nil {
			t.Fatalf("Dispatch(%s) error = %v", eventType, err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	want := []callback.EventType{callback.EventAgentStart, callback.EventLLMEnd, callback.EventAgentEnd}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("delivered events = %v, want %v", got, want)
	}
}

func TestSyncDispatcherAllocatesUniqueSequencesConcurrently(t *testing.T) {
	const count = 64
	sequences := make(chan uint64, count)
	dispatcher, err := callback.New([]callback.Handler{callback.CallbackFunc(func(_ context.Context, event callback.Event) error {
		sequences <- event.Sequence
		return nil
	})}, callback.Config{})
	if err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	for index := 0; index < count; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if err := dispatcher.Dispatch(context.Background(), callback.Event{Type: callback.EventToolEnd}); err != nil {
				t.Errorf("Dispatch error = %v", err)
			}
		}()
	}
	wait.Wait()
	close(sequences)
	seen := map[uint64]bool{}
	for sequence := range sequences {
		if sequence == 0 || seen[sequence] {
			t.Fatalf("duplicate or zero sequence %d", sequence)
		}
		seen[sequence] = true
	}
	if len(seen) != count {
		t.Fatalf("sequence count = %d, want %d", len(seen), count)
	}
}

func TestSyncDispatcherDeepClonesEachHandlerEvent(t *testing.T) {
	event := callback.Event{
		Type: callback.EventAgentStart,
		Metadata: map[string]any{
			"nested": map[string]any{"value": "original"},
			"list":   []any{map[string]any{"value": "original"}},
			"typed":  map[string]string{"value": "original"},
		},
		Metrics: callback.Metrics{Custom: map[string]any{
			"nested": map[string]any{"value": "original"},
		}},
	}
	var secondSawOriginal atomic.Bool
	handlers := []callback.Handler{
		callback.CallbackFunc(func(_ context.Context, current callback.Event) error {
			current.Metadata["nested"].(map[string]any)["value"] = "changed"
			current.Metadata["list"].([]any)[0].(map[string]any)["value"] = "changed"
			current.Metadata["typed"].(map[string]string)["value"] = "changed"
			current.Metrics.Custom["nested"].(map[string]any)["value"] = "changed"
			return nil
		}),
		callback.CallbackFunc(func(_ context.Context, current callback.Event) error {
			metadataOK := current.Metadata["nested"].(map[string]any)["value"] == "original" &&
				current.Metadata["list"].([]any)[0].(map[string]any)["value"] == "original" &&
				current.Metadata["typed"].(map[string]string)["value"] == "original"
			metricsOK := current.Metrics.Custom["nested"].(map[string]any)["value"] == "original"
			secondSawOriginal.Store(metadataOK && metricsOK)
			return nil
		}),
	}
	dispatcher, err := callback.New(handlers, callback.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.Dispatch(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if !secondSawOriginal.Load() {
		t.Fatal("second Handler observed mutations from first Handler")
	}
}
