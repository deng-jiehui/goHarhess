package runnable

import (
	"context"
	"errors"
	"io"
	"reflect"
)

// Concat consumes a stream with the strategy bound when its Pipe was created.
func Concat[T any](ctx context.Context, reader *StreamReader[T]) (T, error) {
	var accumulated T
	if ctx == nil {
		return accumulated, errors.New("runnable concat context is nil")
	}
	if reader == nil || reader.state == nil {
		return accumulated, errors.New("runnable stream reader is nil")
	}
	defer reader.Close()
	concat := reader.state.concat
	if concat == nil {
		concat = defaultConcat[T]()
	}
	for {
		if err := ctx.Err(); err != nil {
			return accumulated, err
		}
		value, err := reader.recv(ctx)
		if errors.Is(err, io.EOF) {
			return accumulated, nil
		}
		if err != nil {
			return accumulated, err
		}
		accumulated, err = concat(accumulated, value)
		if err != nil {
			return accumulated, err
		}
	}
}

func defaultConcat[T any]() ConcatFunc[T] {
	typeOfValue := reflect.TypeFor[T]()
	if typeOfValue == nil {
		return func(_ T, next T) (T, error) { return next, nil }
	}
	switch typeOfValue.Kind() {
	case reflect.String:
		return func(accumulated, next T) (T, error) {
			left := reflect.ValueOf(accumulated)
			right := reflect.ValueOf(next)
			combined := reflect.New(typeOfValue).Elem()
			combined.SetString(left.String() + right.String())
			return combined.Interface().(T), nil
		}
	case reflect.Slice:
		return func(accumulated, next T) (T, error) {
			left := reflect.ValueOf(accumulated)
			right := reflect.ValueOf(next)
			combined := reflect.MakeSlice(typeOfValue, left.Len()+right.Len(), left.Len()+right.Len())
			reflect.Copy(combined, left)
			reflect.Copy(combined.Slice(left.Len(), combined.Len()), right)
			return combined.Interface().(T), nil
		}
	default:
		return func(_ T, next T) (T, error) { return next, nil }
	}
}
