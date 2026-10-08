package llm_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/callback"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
)

func TestObserveReaderOwnsLifecycleUntilEOF(t *testing.T) {
	var starts, ends, failures int
	observer := llm.CallbackFuncs{
		CallbackName: "model",
		Start:        func(context.Context, llm.Request) error { starts++; return nil },
		End:          func(context.Context, llm.Response) error { ends++; return nil },
		Error:        func(context.Context, error) error { failures++; return nil },
	}
	manager, err := callback.NewManager(callback.Config{}, []callback.Handler{observer})
	if err != nil {
		t.Fatal(err)
	}
	upstream, writer := runnable.Pipe[llm.Response](context.Background())
	observed, err := llm.ObserveReader(callback.WithManager(context.Background(), manager), "test", llm.Request{}, upstream)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_ = writer.Send(llm.Response{Messages: []schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewTextBlock("ok"))}})
		_ = writer.Close()
	}()
	response, err := observed.Collect(context.Background())
	if err != nil || response.Messages[0].JoinedText("") != "ok" {
		t.Fatalf("response=%#v err=%v", response, err)
	}
	if starts != 1 || ends != 1 || failures != 0 {
		t.Fatalf("lifecycle=%d/%d/%d", starts, ends, failures)
	}
}

func TestObserveReaderReportsUpstreamError(t *testing.T) {
	seen := make(chan error, 1)
	observer := llm.CallbackFuncs{CallbackName: "model", Error: func(_ context.Context, err error) error { seen <- err; return nil }}
	manager, _ := callback.NewManager(callback.Config{}, []callback.Handler{observer})
	upstream, writer := runnable.Pipe[llm.Response](context.Background())
	observed, err := llm.ObserveReader(callback.WithManager(context.Background(), manager), "test", llm.Request{}, upstream)
	if err != nil {
		t.Fatal(err)
	}
	reason := errors.New("provider failed")
	_ = writer.CloseWithError(reason)
	if _, err := observed.Recv(); !errors.Is(err, reason) {
		t.Fatalf("Recv() error=%v", err)
	}
	if err := <-seen; !errors.Is(err, reason) {
		t.Fatalf("callback error=%v", err)
	}
}

func TestObserveReaderReportsEmptyUpstreamAsTruncated(t *testing.T) {
	var ends int
	seen := make(chan error, 1)
	observer := llm.CallbackFuncs{
		CallbackName: "model",
		End:          func(context.Context, llm.Response) error { ends++; return nil },
		Error:        func(_ context.Context, err error) error { seen <- err; return nil },
	}
	manager, _ := callback.NewManager(callback.Config{}, []callback.Handler{observer})
	upstream, writer := runnable.Pipe[llm.Response](context.Background())
	_ = writer.Close()
	observed, err := llm.ObserveReader(callback.WithManager(context.Background(), manager), "test", llm.Request{}, upstream)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := observed.Recv(); !errors.Is(err, llm.ErrStreamTruncated) {
		t.Fatalf("Recv() error=%v, want ErrStreamTruncated", err)
	}
	if err := <-seen; !errors.Is(err, llm.ErrStreamTruncated) {
		t.Fatalf("callback error=%v", err)
	}
	if ends != 0 {
		t.Fatalf("End callbacks=%d, want 0", ends)
	}
}

func TestObserveReaderCloseCancelsUpstream(t *testing.T) {
	upstream, _ := runnable.Pipe[llm.Response](context.Background())
	observed, err := llm.ObserveReader(context.Background(), "test", llm.Request{}, upstream)
	if err != nil {
		t.Fatal(err)
	}
	if err := observed.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-upstream.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("closing observed Reader did not cancel upstream")
	}
}

func TestCollectResponseUsesReaderEOF(t *testing.T) {
	reader, writer := runnable.Pipe[llm.Response](context.Background())
	go func() {
		_ = writer.Send(llm.Response{Messages: []schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewTextBlock("done"))}})
		_ = writer.Close()
	}()
	response, err := llm.CollectResponse(context.Background(), reader)
	if err != nil || response.Messages[0].JoinedText("") != "done" {
		t.Fatalf("response=%#v err=%v", response, err)
	}
	if _, err := reader.Recv(); !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
		t.Fatalf("reader terminal=%v", err)
	}
}

func TestCollectResponseRejectsEmptyReader(t *testing.T) {
	reader, writer := runnable.Pipe[llm.Response](context.Background())
	_ = writer.Close()
	if _, err := llm.CollectResponse(context.Background(), reader); !errors.Is(err, llm.ErrStreamTruncated) {
		t.Fatalf("CollectResponse error=%v, want %v", err, llm.ErrStreamTruncated)
	}
}

func TestCollectResponseHonorsCallerCancellation(t *testing.T) {
	reader, _ := runnable.Pipe[llm.Response](context.WithoutCancel(context.Background()))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := llm.CollectResponse(ctx, reader)
		done <- err
	}()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("CollectResponse error=%v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("CollectResponse ignored caller cancellation")
	}
}
