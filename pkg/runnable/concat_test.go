package runnable_test

import (
	"context"
	"errors"
	"testing"

	"goharness/pkg/runnable"
)

func TestConcatUsesDefaultStrategies(t *testing.T) {
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{name: "string", run: func(t *testing.T) {
			got, err := concatValues(context.Background(), "go", "Harness")
			if err != nil || got != "goHarness" {
				t.Fatalf("Concat() = %q, %v", got, err)
			}
		}},
		{name: "bytes", run: func(t *testing.T) {
			got, err := concatValues(context.Background(), []byte("go"), []byte("Harness"))
			if err != nil || string(got) != "goHarness" {
				t.Fatalf("Concat() = %q, %v", got, err)
			}
		}},
		{name: "slice", run: func(t *testing.T) {
			got, err := concatValues(context.Background(), []int{1, 2}, []int{3})
			if err != nil || len(got) != 3 || got[2] != 3 {
				t.Fatalf("Concat() = %#v, %v", got, err)
			}
		}},
		{name: "last value", run: func(t *testing.T) {
			got, err := concatValues(context.Background(), 1, 2, 3)
			if err != nil || got != 3 {
				t.Fatalf("Concat() = %d, %v", got, err)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, test.run)
	}
}

func TestConcatEmptyStreamReturnsZeroValue(t *testing.T) {
	reader, writer := runnable.Pipe[string](context.Background())
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := runnable.Concat(context.Background(), reader)
	if err != nil || got != "" {
		t.Fatalf("Concat() = %q, %v", got, err)
	}
}

func TestConcatReturnsUpstreamError(t *testing.T) {
	sentinel := errors.New("stream failed")
	reader, writer := runnable.Pipe[int](context.Background())
	if err := writer.CloseWithError(sentinel); err != nil {
		t.Fatal(err)
	}
	if _, err := runnable.Concat(context.Background(), reader); !errors.Is(err, sentinel) {
		t.Fatalf("Concat() error = %v, want sentinel", err)
	}
}

func TestConcatHonorsConsumerContext(t *testing.T) {
	reader, _ := runnable.Pipe[int](context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := runnable.Concat(ctx, reader); !errors.Is(err, context.Canceled) {
		t.Fatalf("Concat() error = %v, want context.Canceled", err)
	}
}

func TestConcatUsesReaderDomainStrategy(t *testing.T) {
	reader, writer := runnable.Pipe[int](context.Background(), runnable.WithConcat(func(total, next int) (int, error) {
		return total + next, nil
	}))
	go func() {
		_ = writer.Send(2)
		_ = writer.Send(3)
		_ = writer.Close()
	}()
	got, err := runnable.Concat(context.Background(), reader)
	if err != nil || got != 5 {
		t.Fatalf("Concat() = %d, %v", got, err)
	}
}

func concatValues[T any](ctx context.Context, values ...T) (T, error) {
	reader, writer := runnable.Pipe[T](ctx)
	go func() {
		for _, value := range values {
			if err := writer.Send(value); err != nil {
				return
			}
		}
		_ = writer.Close()
	}()
	return runnable.Concat(ctx, reader)
}
