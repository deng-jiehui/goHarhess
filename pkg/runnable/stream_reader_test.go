package runnable_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"goharness/pkg/runnable"
)

func TestPipePreservesOrderAndEndsWithEOF(t *testing.T) {
	reader, writer := runnable.Pipe[int](context.Background())
	go func() {
		_ = writer.Send(1)
		_ = writer.Send(2)
		_ = writer.Close()
	}()

	first, err := reader.Recv()
	if err != nil || first != 1 {
		t.Fatalf("first Recv() = %d, %v", first, err)
	}
	second, err := reader.Recv()
	if err != nil || second != 2 {
		t.Fatalf("second Recv() = %d, %v", second, err)
	}
	if _, err = reader.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("terminal Recv() error = %v, want io.EOF", err)
	}
	if _, err = reader.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("repeated terminal Recv() error = %v, want io.EOF", err)
	}
}

func TestPipeDeliversTerminalErrorOnceThenEOF(t *testing.T) {
	sentinel := errors.New("upstream failed")
	reader, writer := runnable.Pipe[int](context.Background())
	if err := writer.CloseWithError(sentinel); err != nil {
		t.Fatal(err)
	}

	if _, err := reader.Recv(); !errors.Is(err, sentinel) {
		t.Fatalf("terminal Recv() error = %v, want sentinel", err)
	}
	if _, err := reader.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("second terminal Recv() error = %v, want io.EOF", err)
	}
}

func TestReaderCloseCancelsBlockedWriter(t *testing.T) {
	reader, writer := runnable.Pipe[int](context.Background())
	done := make(chan error, 1)
	go func() { done <- writer.Send(1) }()

	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Send() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked Send() did not observe Reader.Close")
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func TestPipeHonorsParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reader, writer := runnable.Pipe[int](ctx)
	cancel()

	if _, err := reader.Recv(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Recv() error = %v, want context.Canceled", err)
	}
	if err := writer.Send(1); !errors.Is(err, context.Canceled) {
		t.Fatalf("Send() error = %v, want context.Canceled", err)
	}
}

func TestPipeRejectsNilContext(t *testing.T) {
	reader, writer := runnable.Pipe[int](nil)
	if _, err := reader.Recv(); err == nil || err.Error() != "runnable pipe context is nil" {
		t.Fatalf("Recv() error = %v", err)
	}
	if err := writer.Send(1); err == nil || err.Error() != "runnable pipe context is nil" {
		t.Fatalf("Send() error = %v", err)
	}
}
