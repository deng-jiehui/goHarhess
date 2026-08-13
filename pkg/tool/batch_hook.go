package tool

import (
	"context"
	"fmt"
	"reflect"
)

// BatchCall is an immutable view of one parsed tool call presented to a
// control-plane admission hook.
type BatchCall struct {
	ID       string
	Spec     Spec
	RawArgs  string
	Args     map[string]any
	Metadata map[string]any
}

// Batch is the complete set of executable calls awaiting admission.
type Batch struct {
	Calls   []BatchCall
	Subject Subject
}

// BeforeBatchHook runs synchronously after the complete batch is parsed and
// before any tool execution is scheduled.
type BeforeBatchHook interface {
	BeforeBatch(context.Context, Batch) error
}

// BeforeBatchFunc adapts a function to BeforeBatchHook.
type BeforeBatchFunc func(context.Context, Batch) error

func (f BeforeBatchFunc) BeforeBatch(ctx context.Context, batch Batch) error {
	return f(ctx, batch)
}

// RunBeforeBatchHooks dispatches hooks in registration order. Each hook
// receives an independent snapshot and cannot rewrite the admitted batch.
func RunBeforeBatchHooks(ctx context.Context, hooks []BeforeBatchHook, batch Batch) error {
	for index, hook := range hooks {
		if hook == nil || isNilHook(hook) {
			return fmt.Errorf("before tool batch hook %d is nil", index)
		}
		if err := invokeBeforeBatchHook(ctx, hook, cloneBatch(batch)); err != nil {
			return fmt.Errorf("before tool batch hook %d: %w", index, err)
		}
	}
	return nil
}

func invokeBeforeBatchHook(ctx context.Context, hook BeforeBatchHook, batch Batch) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panicked: %v", recovered)
		}
	}()
	return hook.BeforeBatch(ctx, batch)
}

func isNilHook(hook BeforeBatchHook) bool {
	value := reflect.ValueOf(hook)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func cloneBatch(batch Batch) Batch {
	cloned := Batch{
		Calls:   make([]BatchCall, len(batch.Calls)),
		Subject: cloneSubject(batch.Subject),
	}
	for index, call := range batch.Calls {
		cloned.Calls[index] = BatchCall{
			ID:       call.ID,
			Spec:     cloneSpec(call.Spec),
			RawArgs:  call.RawArgs,
			Args:     cloneAnyMap(call.Args),
			Metadata: cloneAnyMap(call.Metadata),
		}
	}
	return cloned
}

func cloneSpec(spec Spec) Spec {
	spec.Parameters = cloneAnyMap(spec.Parameters)
	return spec
}

func cloneAnyMap(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	cloned := make(map[string]any, len(source))
	for key, value := range source {
		cloned[key] = cloneAnyValue(reflect.ValueOf(value))
	}
	return cloned
}

func cloneAnyValue(value reflect.Value) any {
	if !value.IsValid() {
		return nil
	}
	if value.Kind() == reflect.Interface {
		if value.IsNil() {
			return nil
		}
		return cloneAnyValue(value.Elem())
	}
	switch value.Kind() {
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type()).Interface()
		}
		cloned := reflect.MakeMapWithSize(value.Type(), value.Len())
		iterator := value.MapRange()
		for iterator.Next() {
			item := cloneAnyValue(iterator.Value())
			cloned.SetMapIndex(iterator.Key(), cloneReflectValue(item, value.Type().Elem()))
		}
		return cloned.Interface()
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type()).Interface()
		}
		cloned := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for index := 0; index < value.Len(); index++ {
			item := cloneAnyValue(value.Index(index))
			cloned.Index(index).Set(cloneReflectValue(item, value.Type().Elem()))
		}
		return cloned.Interface()
	case reflect.Array:
		cloned := reflect.New(value.Type()).Elem()
		for index := 0; index < value.Len(); index++ {
			item := cloneAnyValue(value.Index(index))
			cloned.Index(index).Set(cloneReflectValue(item, value.Type().Elem()))
		}
		return cloned.Interface()
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type()).Interface()
		}
		cloned := reflect.New(value.Type().Elem())
		item := cloneAnyValue(value.Elem())
		cloned.Elem().Set(cloneReflectValue(item, value.Type().Elem()))
		return cloned.Interface()
	default:
		return value.Interface()
	}
}

func cloneReflectValue(value any, target reflect.Type) reflect.Value {
	if value == nil {
		return reflect.Zero(target)
	}
	result := reflect.ValueOf(value)
	if result.Type().AssignableTo(target) {
		return result
	}
	if target.Kind() == reflect.Interface && result.Type().Implements(target) {
		return result
	}
	return reflect.Zero(target)
}
