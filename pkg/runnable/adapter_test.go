package runnable_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"goharness/pkg/runnable"
)

type invokeNative struct{}

func (invokeNative) Invoke(_ context.Context, input int, _ ...runnable.Option) (string, error) {
	return strings.Repeat("x", input), nil
}

type streamNative struct{}

func (streamNative) Stream(ctx context.Context, input int, _ ...runnable.Option) (*runnable.StreamReader[string], error) {
	reader, writer := runnable.Pipe[string](ctx)
	go func() {
		_ = writer.Send(strings.Repeat("x", input))
		_ = writer.Send("!")
		_ = writer.Close()
	}()
	return reader, nil
}

type transformNative struct{}

func (transformNative) Transform(ctx context.Context, input *runnable.StreamReader[int], _ ...runnable.Option) (*runnable.StreamReader[string], error) {
	output, writer := runnable.Pipe[string](ctx)
	go func() {
		defer input.Close()
		for {
			value, err := input.Recv()
			if errors.Is(err, io.EOF) {
				_ = writer.Close()
				return
			}
			if err != nil {
				_ = writer.CloseWithError(err)
				return
			}
			if err := writer.Send(strings.Repeat("x", value)); err != nil {
				return
			}
		}
	}()
	return output, nil
}

type invokeAndStreamNative struct {
	invokeCalls int
	streamCalls int
}

type collectNative struct {
	calls int
}

func (n *collectNative) Collect(ctx context.Context, input *runnable.StreamReader[int], _ ...runnable.Option) (string, error) {
	n.calls++
	value, err := runnable.Concat(ctx, input)
	if err != nil {
		return "", err
	}
	return strings.Repeat("c", value), nil
}

func (n *invokeAndStreamNative) Invoke(context.Context, int, ...runnable.Option) (string, error) {
	n.invokeCalls++
	return "invoke", nil
}

func (n *invokeAndStreamNative) Stream(ctx context.Context, _ int, _ ...runnable.Option) (*runnable.StreamReader[string], error) {
	n.streamCalls++
	reader, writer := runnable.Pipe[string](ctx)
	go func() {
		_ = writer.Send("stream")
		_ = writer.Close()
	}()
	return reader, nil
}

func TestAdaptInvokeNativeSuppliesAllExecutionModes(t *testing.T) {
	component, err := runnable.Adapt[int, string](invokeNative{})
	if err != nil {
		t.Fatal(err)
	}
	assertInvokeAndStream(t, component, 2, "xx")
	input := intReader(t, 1, 2)
	collected, err := component.Collect(context.Background(), input)
	if err != nil || collected != "xx" {
		t.Fatalf("Collect() = %q, %v", collected, err)
	}
	transformed, err := component.Transform(context.Background(), intReader(t, 1, 2))
	if err != nil {
		t.Fatal(err)
	}
	got, err := runnable.Concat(context.Background(), transformed)
	if err != nil || got != "xx" {
		t.Fatalf("Transform() concat = %q, %v", got, err)
	}
}

func TestAdaptStreamNativeSuppliesAllExecutionModes(t *testing.T) {
	component, err := runnable.Adapt[int, string](streamNative{})
	if err != nil {
		t.Fatal(err)
	}
	assertInvokeAndStream(t, component, 2, "xx!")
	got, err := component.Collect(context.Background(), intReader(t, 1, 2))
	if err != nil || got != "xx!" {
		t.Fatalf("Collect() = %q, %v", got, err)
	}
	stream, err := component.Transform(context.Background(), intReader(t, 1, 2))
	if err != nil {
		t.Fatal(err)
	}
	got, err = runnable.Concat(context.Background(), stream)
	if err != nil || got != "xx!" {
		t.Fatalf("Transform() concat = %q, %v", got, err)
	}
}

func TestAdaptTransformNativeSuppliesAllExecutionModes(t *testing.T) {
	component, err := runnable.Adapt[int, string](transformNative{})
	if err != nil {
		t.Fatal(err)
	}
	assertInvokeAndStream(t, component, 2, "xx")
	got, err := component.Collect(context.Background(), intReader(t, 1, 2))
	if err != nil || got != "xxx" {
		t.Fatalf("Collect() = %q, %v", got, err)
	}
	stream, err := component.Transform(context.Background(), intReader(t, 1, 2))
	if err != nil {
		t.Fatal(err)
	}
	got, err = runnable.Concat(context.Background(), stream)
	if err != nil || got != "xxx" {
		t.Fatalf("Transform() concat = %q, %v", got, err)
	}
}

func TestAdaptUsesNativeMethodWhenAvailable(t *testing.T) {
	native := &invokeAndStreamNative{}
	component, err := runnable.Adapt[int, string](native)
	if err != nil {
		t.Fatal(err)
	}
	value, err := component.Invoke(context.Background(), 1)
	if err != nil || value != "invoke" || native.invokeCalls != 1 || native.streamCalls != 0 {
		t.Fatalf("Invoke() = %q, %v; calls = %d/%d", value, err, native.invokeCalls, native.streamCalls)
	}
	stream, err := component.Stream(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	value, err = runnable.Concat(context.Background(), stream)
	if err != nil || value != "stream" || native.streamCalls != 1 {
		t.Fatalf("Stream() = %q, %v; calls = %d", value, err, native.streamCalls)
	}
}

func TestAdaptUsesNativeCollectWithoutRecursion(t *testing.T) {
	native := &collectNative{}
	component, err := runnable.Adapt[int, string](native)
	if err != nil {
		t.Fatal(err)
	}
	value, err := component.Invoke(context.Background(), 2)
	if err != nil || value != "cc" || native.calls != 1 {
		t.Fatalf("Invoke() = %q, %v; Collect calls = %d", value, err, native.calls)
	}
	value, err = component.Collect(context.Background(), intReader(t, 1, 3))
	if err != nil || value != "ccc" || native.calls != 2 {
		t.Fatalf("Collect() = %q, %v; calls = %d", value, err, native.calls)
	}
}

func TestAdaptTransformBarrierReturnsInputErrorDirectly(t *testing.T) {
	component, err := runnable.Adapt[int, string](invokeNative{})
	if err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("input stream failed")
	input, writer := runnable.Pipe[int](context.Background())
	if err := writer.CloseWithError(sentinel); err != nil {
		t.Fatal(err)
	}
	output, err := component.Transform(context.Background(), input)
	if output != nil || !errors.Is(err, sentinel) {
		t.Fatalf("Transform() = %v, %v; want nil, sentinel", output, err)
	}
}

func TestAdaptRejectsNilAndUnsupportedValues(t *testing.T) {
	if _, err := runnable.Adapt[int, string](nil); err == nil || !strings.Contains(err.Error(), "nil") {
		t.Fatalf("nil Adapt() error = %v", err)
	}
	var typedNil *invokeAndStreamNative
	if _, err := runnable.Adapt[int, string](typedNil); err == nil || !strings.Contains(err.Error(), "nil") {
		t.Fatalf("typed nil Adapt() error = %v", err)
	}
	if _, err := runnable.Adapt[int, string](struct{}{}); err == nil || !strings.Contains(err.Error(), "execution capability") {
		t.Fatalf("unsupported Adapt() error = %v", err)
	}
}

func TestTransformableLambdaIsCompleteAdapter(t *testing.T) {
	component := runnable.TransformableLambda(func(ctx context.Context, input *runnable.StreamReader[int]) (*runnable.StreamReader[string], error) {
		return transformNative{}.Transform(ctx, input)
	})
	value, err := component.Invoke(context.Background(), 3)
	if err != nil || value != "xxx" {
		t.Fatalf("Invoke() = %q, %v", value, err)
	}
}

type completeRunnable interface {
	Invoke(context.Context, int, ...runnable.Option) (string, error)
	Stream(context.Context, int, ...runnable.Option) (*runnable.StreamReader[string], error)
	Collect(context.Context, *runnable.StreamReader[int], ...runnable.Option) (string, error)
	Transform(context.Context, *runnable.StreamReader[int], ...runnable.Option) (*runnable.StreamReader[string], error)
}

func assertInvokeAndStream(t *testing.T, component completeRunnable, input int, want string) {
	t.Helper()
	value, err := component.Invoke(context.Background(), input)
	if err != nil || value != want {
		t.Fatalf("Invoke() = %q, %v", value, err)
	}
	stream, err := component.Stream(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	value, err = runnable.Concat(context.Background(), stream)
	if err != nil || value != want {
		t.Fatalf("Stream() concat = %q, %v", value, err)
	}
}

func intReader(t *testing.T, values ...int) *runnable.StreamReader[int] {
	t.Helper()
	reader, writer := runnable.Pipe[int](context.Background())
	go func() {
		for _, value := range values {
			if err := writer.Send(value); err != nil {
				return
			}
		}
		_ = writer.Close()
	}()
	return reader
}
