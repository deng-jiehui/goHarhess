package callback

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"time"
)

type traceIDContextKey struct{}
type traceContextKey struct{}
type metadataContextKey struct{}
type managerContextKey struct{}
type invocationContextKey struct{}
type scopeContextKey struct{}

var fallbackID atomic.Uint64

type Scope struct {
	Depth int
	Root  bool
}

// EnterScope establishes one callback ownership tree.
func EnterScope(ctx context.Context) (context.Context, Scope) {
	if ctx == nil {
		ctx = context.Background()
	}
	parent, ok := ctx.Value(scopeContextKey{}).(Scope)
	scope := Scope{Root: !ok, Depth: 0}
	if ok {
		scope.Depth = parent.Depth + 1
	}
	return context.WithValue(ctx, scopeContextKey{}, scope), scope
}

func ScopeFromContext(ctx context.Context) (Scope, bool) {
	if ctx == nil {
		return Scope{}, false
	}
	scope, ok := ctx.Value(scopeContextKey{}).(Scope)
	return scope, ok
}

// WithTraceID seeds a framework trace with an upstream request or trace ID.
// Blank IDs are ignored.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if traceID = strings.TrimSpace(traceID); traceID == "" {
		return ctx
	}
	return context.WithValue(ctx, traceIDContextKey{}, traceID)
}

// TraceFromContext returns the current run/span identity.
func TraceFromContext(ctx context.Context) (Trace, bool) {
	if ctx == nil {
		return Trace{}, false
	}
	trace, ok := ctx.Value(traceContextKey{}).(Trace)
	return trace, ok && trace.TraceID != "" && trace.SpanID != ""
}

// WithMetadata adds trace-wide metadata. Child operations inherit a cloned
// snapshot, and values already present in metadata are overwritten.
func WithMetadata(ctx context.Context, metadata map[string]any) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	merged := MetadataFromContext(ctx)
	for key, value := range metadata {
		merged[key] = cloneValue(value)
	}
	return context.WithValue(ctx, metadataContextKey{}, merged)
}

// WithoutMetadata 删除指定的元数据键，返回新的派生上下文；删除不存在的键
// 为无操作。父调用与其他分支的上下文不受影响。
func WithoutMetadata(ctx context.Context, keys ...string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(keys) == 0 {
		return ctx
	}
	merged := MetadataFromContext(ctx)
	for _, key := range keys {
		delete(merged, key)
	}
	return context.WithValue(ctx, metadataContextKey{}, merged)
}

// MetadataFromContext returns an independent metadata snapshot.
func MetadataFromContext(ctx context.Context) map[string]any {
	if ctx == nil {
		return map[string]any{}
	}
	metadata, _ := ctx.Value(metadataContextKey{}).(map[string]any)
	return CloneMetadata(metadata)
}

// Begin creates a child operation and returns the context that must be passed
// to nested model, retriever and tool calls.
func Begin(ctx context.Context, component, name string, metadata map[string]any) (context.Context, Invocation) {
	if ctx == nil {
		ctx = context.Background()
	}
	parent, hasParent := TraceFromContext(ctx)
	traceID, _ := ctx.Value(traceIDContextKey{}).(string)
	if hasParent {
		traceID = parent.TraceID
	}
	if traceID == "" {
		traceID = newID()
	}
	spanID := newID()
	trace := Trace{TraceID: traceID, SpanID: spanID}
	if hasParent {
		trace.RunID = parent.RunID
		trace.ParentSpanID = parent.SpanID
	} else {
		trace.RunID = spanID
	}
	merged := MetadataFromContext(ctx)
	for key, value := range metadata {
		merged[key] = cloneValue(value)
	}
	invocation := Invocation{
		Component: component,
		Operation: strings.TrimSpace(name),
		Trace:     trace,
		StartedAt: time.Now(),
		Timestamp: time.Now(),
		Phase:     PhaseStart,
		Metadata:  merged,
	}
	ctx = context.WithValue(ctx, traceContextKey{}, trace)
	ctx = context.WithValue(ctx, invocationContextKey{}, invocation)
	ctx = context.WithValue(ctx, metadataContextKey{}, CloneMetadata(merged))
	return ctx, invocation
}

// bindOperation overlays an already-created operation onto an execution
// context without discarding graph cursor, option, cancellation or data-sink
// values installed by other packages.
func bindOperation(ctx context.Context, invocation Invocation) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = context.WithValue(ctx, traceContextKey{}, invocation.Trace)
	ctx = context.WithValue(ctx, invocationContextKey{}, invocation.Clone())
	return context.WithValue(ctx, metadataContextKey{}, CloneMetadata(invocation.Metadata))
}

func withInvocation(ctx context.Context, invocation Invocation) context.Context {
	return bindOperation(ctx, invocation)
}

// InvocationFromContext returns the current lifecycle snapshot.
func InvocationFromContext(ctx context.Context) (Invocation, bool) {
	if ctx == nil {
		return Invocation{}, false
	}
	invocation, ok := ctx.Value(invocationContextKey{}).(Invocation)
	if !ok {
		return Invocation{}, false
	}
	return invocation.Clone(), true
}

// WithManager exposes one synchronous Manager to nested framework components.
func WithManager(ctx context.Context, manager *Manager) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, managerContextKey{}, manager)
}

// ManagerFromContext resolves the callback Manager for the current run.
func ManagerFromContext(ctx context.Context) (*Manager, bool) {
	if ctx == nil {
		return nil, false
	}
	manager, ok := ctx.Value(managerContextKey{}).(*Manager)
	return manager, ok && manager != nil
}

// CloneMetadata recursively clones common mutable metadata containers.
func CloneMetadata(metadata map[string]any) map[string]any {
	if metadata == nil {
		return map[string]any{}
	}
	cloned := make(map[string]any, len(metadata))
	for key, value := range metadata {
		cloned[key] = cloneValue(value)
	}
	return cloned
}

func cloneValue(value any) any {
	cloned := cloneReflectValue(reflect.ValueOf(value), 0)
	if !cloned.IsValid() {
		return nil
	}
	return cloned.Interface()
}

// CloneValue recursively clones common mutable maps, slices, arrays, and
// interface containers while preserving their concrete Go types.
func CloneValue(value any) any { return cloneValue(value) }

// BindInvocation overlays an existing operation on the current runtime
// Context while preserving values installed by other framework packages.
func BindInvocation(ctx context.Context, invocation Invocation) context.Context {
	return bindOperation(ctx, invocation)
}

const maxMetadataCloneDepth = 64

func cloneReflectValue(value reflect.Value, depth int) reflect.Value {
	if !value.IsValid() || depth > maxMetadataCloneDepth {
		return value
	}
	switch value.Kind() {
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		cloned := cloneReflectValue(value.Elem(), depth+1)
		if !cloned.IsValid() || !cloned.Type().AssignableTo(value.Type().Elem()) {
			return value
		}
		pointer := reflect.New(value.Type().Elem())
		pointer.Elem().Set(cloned)
		return pointer
	case reflect.Struct:
		cloned := reflect.New(value.Type()).Elem()
		for index := 0; index < value.NumField(); index++ {
			source := value.Field(index)
			target := cloned.Field(index)
			if !source.CanInterface() || !target.CanSet() {
				return value
			}
			field := cloneReflectValue(source, depth+1)
			if !field.IsValid() || !field.Type().AssignableTo(target.Type()) {
				return value
			}
			target.Set(field)
		}
		return cloned
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		cloned := cloneReflectValue(value.Elem(), depth+1)
		wrapped := reflect.New(value.Type()).Elem()
		if cloned.IsValid() && cloned.Type().AssignableTo(value.Type()) {
			wrapped.Set(cloned)
		} else if cloned.IsValid() && cloned.Type().Implements(value.Type()) {
			wrapped.Set(cloned)
		} else {
			return value
		}
		return wrapped
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		cloned := reflect.MakeMapWithSize(value.Type(), value.Len())
		iterator := value.MapRange()
		for iterator.Next() {
			item := cloneReflectValue(iterator.Value(), depth+1)
			if !item.IsValid() || !item.Type().AssignableTo(value.Type().Elem()) {
				return value
			}
			cloned.SetMapIndex(iterator.Key(), item)
		}
		return cloned
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		cloned := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for index := 0; index < value.Len(); index++ {
			item := cloneReflectValue(value.Index(index), depth+1)
			if !item.IsValid() || !item.Type().AssignableTo(value.Type().Elem()) {
				return value
			}
			cloned.Index(index).Set(item)
		}
		return cloned
	case reflect.Array:
		cloned := reflect.New(value.Type()).Elem()
		for index := 0; index < value.Len(); index++ {
			item := cloneReflectValue(value.Index(index), depth+1)
			if !item.IsValid() || !item.Type().AssignableTo(value.Type().Elem()) {
				return value
			}
			cloned.Index(index).Set(item)
		}
		return cloned
	default:
		return value
	}
}

func newID() string {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err == nil {
		return hex.EncodeToString(bytes)
	}
	return fmt.Sprintf("%x-%x", time.Now().UnixNano(), fallbackID.Add(1))
}
