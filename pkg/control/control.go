// Package control defines the provider-neutral execution control protocol
// shared by graphs, tools, and higher-level orchestration.
package control

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

var ErrInterruptRequested = errors.New("execution interrupt requested")

// InterruptRequest is a typed cancellation cause that asks an execution
// engine to persist a checkpoint at its next safe boundary.
type InterruptRequest struct {
	Reason   string         `json:"reason,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

func (r InterruptRequest) Error() string {
	return fmt.Sprintf("%v: %s", ErrInterruptRequested, r.Reason)
}

func (InterruptRequest) Unwrap() error { return ErrInterruptRequested }

// Clone returns an independent request snapshot.
func (r InterruptRequest) Clone() InterruptRequest {
	r.Metadata = cloneMetadata(r.Metadata)
	return r
}

// RequestInterrupt creates a typed interruption error suitable for
// context.WithCancelCause or a synchronous control hook.
func RequestInterrupt(reason string, metadata map[string]any) error {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "interrupted"
	}
	return InterruptRequest{Reason: reason, Metadata: cloneMetadata(metadata)}
}

// AsInterrupt extracts a defensive interruption snapshot from an error chain.
func AsInterrupt(err error) (InterruptRequest, bool) {
	var request InterruptRequest
	if !errors.As(err, &request) {
		return InterruptRequest{}, false
	}
	return request.Clone(), true
}

func cloneMetadata(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	cloned := make(map[string]any, len(source))
	for key, value := range source {
		cloned[key] = cloneValue(reflect.ValueOf(value))
	}
	return cloned
}

func cloneValue(value reflect.Value) any {
	if !value.IsValid() {
		return nil
	}
	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return nil
		}
		return cloneValue(value.Elem())
	case reflect.Map:
		if value.IsNil() || value.Type().Key().Kind() != reflect.String {
			return value.Interface()
		}
		copy := reflect.MakeMapWithSize(value.Type(), value.Len())
		iterator := value.MapRange()
		for iterator.Next() {
			item := cloneValue(iterator.Value())
			copy.SetMapIndex(iterator.Key(), reflectForType(item, value.Type().Elem()))
		}
		return copy.Interface()
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type()).Interface()
		}
		copy := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for index := 0; index < value.Len(); index++ {
			item := cloneValue(value.Index(index))
			copy.Index(index).Set(reflectForType(item, value.Type().Elem()))
		}
		return copy.Interface()
	case reflect.Array:
		copy := reflect.New(value.Type()).Elem()
		for index := 0; index < value.Len(); index++ {
			item := cloneValue(value.Index(index))
			copy.Index(index).Set(reflectForType(item, value.Type().Elem()))
		}
		return copy.Interface()
	default:
		return value.Interface()
	}
}

func reflectForType(value any, target reflect.Type) reflect.Value {
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
