package runnable

import (
	"context"
	"errors"
	"reflect"
)

type nativeInvoker[I, O any] interface {
	Invoke(context.Context, I, ...Option) (O, error)
}

type nativeStreamable[I, O any] interface {
	Stream(context.Context, I, ...Option) (*StreamReader[O], error)
}

type nativeCollector[I, O any] interface {
	Collect(context.Context, *StreamReader[I], ...Option) (O, error)
}

type nativeTransformer[I, O any] interface {
	Transform(context.Context, *StreamReader[I], ...Option) (*StreamReader[O], error)
}

// Adapter promotes one or more native execution capabilities to all four
// discrete and streaming execution forms.
type Adapter[I, O any] struct {
	invoke    func(context.Context, I, ...Option) (O, error)
	stream    func(context.Context, I, ...Option) (*StreamReader[O], error)
	collect   func(context.Context, *StreamReader[I], ...Option) (O, error)
	transform func(context.Context, *StreamReader[I], ...Option) (*StreamReader[O], error)
}

// Adapt inspects a component once and binds all missing execution forms.
func Adapt[I, O any](value any) (*Adapter[I, O], error) {
	if isNilAdapterValue(value) {
		return nil, errors.New("runnable adapter value is nil")
	}
	invoker, hasInvoke := value.(nativeInvoker[I, O])
	streamable, hasStream := value.(nativeStreamable[I, O])
	collector, hasCollect := value.(nativeCollector[I, O])
	transformer, hasTransform := value.(nativeTransformer[I, O])
	if !hasInvoke && !hasStream && !hasCollect && !hasTransform {
		return nil, errors.New("runnable adapter value has no supported execution capability")
	}

	adapter := &Adapter[I, O]{}
	if hasInvoke {
		adapter.invoke = invoker.Invoke
	}
	if hasStream {
		adapter.stream = streamable.Stream
	}
	if hasCollect {
		adapter.collect = collector.Collect
	}
	if hasTransform {
		adapter.transform = transformer.Transform
	}
	adapter.bindMissing()
	return adapter, nil
}

func (a *Adapter[I, O]) bindMissing() {
	if a.invoke == nil {
		switch {
		case a.stream != nil:
			a.invoke = func(ctx context.Context, input I, options ...Option) (O, error) {
				output, err := a.stream(ctx, input, options...)
				if err != nil {
					var zero O
					return zero, err
				}
				return Concat(ctx, output)
			}
		case a.transform != nil:
			a.invoke = func(ctx context.Context, input I, options ...Option) (O, error) {
				output, err := a.transform(ctx, box(ctx, input), options...)
				if err != nil {
					var zero O
					return zero, err
				}
				return Concat(ctx, output)
			}
		case a.collect != nil:
			a.invoke = func(ctx context.Context, input I, options ...Option) (O, error) {
				return a.collect(ctx, box(ctx, input), options...)
			}
		}
	}
	if a.stream == nil {
		switch {
		case a.transform != nil:
			a.stream = func(ctx context.Context, input I, options ...Option) (*StreamReader[O], error) {
				return a.transform(ctx, box(ctx, input), options...)
			}
		default:
			a.stream = func(ctx context.Context, input I, options ...Option) (*StreamReader[O], error) {
				output, err := a.invoke(ctx, input, options...)
				return boxResult(ctx, output, err), nil
			}
		}
	}
	if a.collect == nil {
		switch {
		case a.transform != nil:
			a.collect = func(ctx context.Context, input *StreamReader[I], options ...Option) (O, error) {
				output, err := a.transform(ctx, input, options...)
				if err != nil {
					var zero O
					return zero, err
				}
				return Concat(ctx, output)
			}
		default:
			a.collect = func(ctx context.Context, input *StreamReader[I], options ...Option) (O, error) {
				value, err := Concat(ctx, input)
				if err != nil {
					var zero O
					return zero, err
				}
				return a.invoke(ctx, value, options...)
			}
		}
	}
	if a.transform == nil {
		a.transform = func(ctx context.Context, input *StreamReader[I], options ...Option) (*StreamReader[O], error) {
			value, err := Concat(ctx, input)
			if err != nil {
				return nil, err
			}
			return a.stream(ctx, value, options...)
		}
	}
}

func (a *Adapter[I, O]) Invoke(ctx context.Context, input I, options ...Option) (O, error) {
	var zero O
	if a == nil || a.invoke == nil {
		return zero, errors.New("runnable adapter Invoke is unavailable")
	}
	return a.invoke(ctx, input, options...)
}

func (a *Adapter[I, O]) Stream(ctx context.Context, input I, options ...Option) (*StreamReader[O], error) {
	if a == nil || a.stream == nil {
		return nil, errors.New("runnable adapter Stream is unavailable")
	}
	return a.stream(ctx, input, options...)
}

func (a *Adapter[I, O]) Collect(ctx context.Context, input *StreamReader[I], options ...Option) (O, error) {
	var zero O
	if a == nil || a.collect == nil {
		return zero, errors.New("runnable adapter Collect is unavailable")
	}
	return a.collect(ctx, input, options...)
}

func (a *Adapter[I, O]) Transform(ctx context.Context, input *StreamReader[I], options ...Option) (*StreamReader[O], error) {
	if a == nil || a.transform == nil {
		return nil, errors.New("runnable adapter Transform is unavailable")
	}
	return a.transform(ctx, input, options...)
}

func box[T any](ctx context.Context, value T) *StreamReader[T] {
	return boxResult(ctx, value, nil)
}

func boxResult[T any](ctx context.Context, value T, resultErr error) *StreamReader[T] {
	reader, writer := Pipe[T](ctx)
	go func() {
		if resultErr != nil {
			_ = writer.CloseWithError(resultErr)
			return
		}
		if err := writer.Send(value); err != nil {
			return
		}
		_ = writer.Close()
	}()
	return reader
}

func isNilAdapterValue(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
