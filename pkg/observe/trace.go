// Package observe 提供统一的运行事件观测：一个事件类型、一个 Observer
// 接口、一处注册（With）。Trace/Invocation/Begin 承载 span 级别的调用树
// 身份，沿用原 callback 包的 ID 体系。
package observe

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
type invocationContextKey struct{}
type scopeContextKey struct{}

var fallbackID atomic.Uint64

// Phase identifies a lifecycle boundary.
type Phase string

const (
	PhaseStart  Phase = "start"
	PhaseOutput Phase = "output"
	PhaseEnd    Phase = "end"
	PhaseError  Phase = "error"
	// PhaseEvent 标记无 start/end 配对的瞬时事件（interrupt、checkpoint）。
	PhaseEvent Phase = "event"
)

// Trace identifies a root run and one nested operation.
type Trace struct {
	TraceID      string `json:"trace_id"`
	RunID        string `json:"run_id"`
	SpanID       string `json:"span_id"`
	ParentSpanID string `json:"parent_span_id,omitempty"`
}

// Invocation is payload-free lifecycle and tracing metadata.
type Invocation struct {
	Component string         `json:"component"`
	Operation string         `json:"operation"`
	Phase     Phase          `json:"phase"`
	Trace     Trace          `json:"trace"`
	StartedAt time.Time      `json:"started_at"`
	Timestamp time.Time      `json:"timestamp"`
	Duration  time.Duration  `json:"duration,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

func (i Invocation) Clone() Invocation {
	i.Metadata = CloneMetadata(i.Metadata)
	return i
}

// WithPhase returns a terminal or progress snapshot of the same operation.
func (i Invocation) WithPhase(phase Phase) Invocation {
	now := time.Now()
	i.Phase = phase
	i.Timestamp = now
	if phase == PhaseEnd || phase == PhaseError {
		i.Duration = now.Sub(i.StartedAt)
	}
	return i.Clone()
}

// Scope marks one observation ownership tree.
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

// cloneReflectValue 是元数据/CloneValue 的宽松复制：失败回退共享原值
// （元数据只读可接受），且纯反射实现——它可能被领域 Clone() 反向调用，
// 若再反向调 Clone() 会无限递归。
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

// cloneReflectValueStrict 是脱敏载荷的严格复制：报告隔离是否成功。
// 领域类型带 Clone() 方法时按其约定复制（反射处理不了的共享视图、
// 未导出句柄由领域实现负责）；反射路径遇到不可复制形态（未导出字段、
// 不可赋值项）返回 ok=false，调用方不得拿原值当副本使用。
func cloneReflectValueStrict(value reflect.Value, depth int) (reflect.Value, bool) {
	if !value.IsValid() || depth > maxMetadataCloneDepth {
		return value, false
	}
	// 递归遵循领域 Clone 约定：llm.Request/Response、retrieval 载荷、
	// mcp 载荷等都实现了 Clone()，反射复制可能漏掉内部共享结构。
	if method := value.MethodByName("Clone"); method.IsValid() {
		t := method.Type()
		if t.NumIn() == 0 && t.NumOut() == 1 && t.Out(0).AssignableTo(value.Type()) {
			return method.Call(nil)[0], true
		}
	}
	switch value.Kind() {
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type()), true
		}
		cloned, ok := cloneReflectValueStrict(value.Elem(), depth+1)
		if !ok || !cloned.Type().AssignableTo(value.Type().Elem()) {
			return value, false
		}
		pointer := reflect.New(value.Type().Elem())
		pointer.Elem().Set(cloned)
		return pointer, true
	case reflect.Struct:
		cloned := reflect.New(value.Type()).Elem()
		for index := 0; index < value.NumField(); index++ {
			source := value.Field(index)
			target := cloned.Field(index)
			if !source.CanInterface() || !target.CanSet() {
				return value, false
			}
			field, ok := cloneReflectValueStrict(source, depth+1)
			if !ok || !field.Type().AssignableTo(target.Type()) {
				return value, false
			}
			target.Set(field)
		}
		return cloned, true
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type()), true
		}
		cloned, ok := cloneReflectValueStrict(value.Elem(), depth+1)
		if !ok {
			return value, false
		}
		wrapped := reflect.New(value.Type()).Elem()
		if cloned.Type().AssignableTo(value.Type()) || cloned.Type().Implements(value.Type()) {
			wrapped.Set(cloned)
			return wrapped, true
		}
		return value, false
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type()), true
		}
		cloned := reflect.MakeMapWithSize(value.Type(), value.Len())
		iterator := value.MapRange()
		for iterator.Next() {
			item, ok := cloneReflectValueStrict(iterator.Value(), depth+1)
			if !ok || !item.Type().AssignableTo(value.Type().Elem()) {
				return value, false
			}
			cloned.SetMapIndex(iterator.Key(), item)
		}
		return cloned, true
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type()), true
		}
		cloned := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for index := 0; index < value.Len(); index++ {
			item, ok := cloneReflectValueStrict(value.Index(index), depth+1)
			if !ok || !item.Type().AssignableTo(value.Type().Elem()) {
				return value, false
			}
			cloned.Index(index).Set(item)
		}
		return cloned, true
	case reflect.Array:
		cloned := reflect.New(value.Type()).Elem()
		for index := 0; index < value.Len(); index++ {
			item, ok := cloneReflectValueStrict(value.Index(index), depth+1)
			if !ok || !item.Type().AssignableTo(value.Type().Elem()) {
				return value, false
			}
			cloned.Index(index).Set(item)
		}
		return cloned, true
	default:
		// 基本类型不可变，原值即安全副本。
		return value, true
	}
}

func newID() string {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err == nil {
		return hex.EncodeToString(bytes)
	}
	return fmt.Sprintf("%x-%x", time.Now().UnixNano(), fallbackID.Add(1))
}
