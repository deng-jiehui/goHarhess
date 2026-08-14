// Package runnable defines the single provider-neutral execution contract used
// to compose framework components. Components may additionally expose streaming
// or batch capabilities without forcing every component to implement them.
package runnable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

// Option is an invocation option. Components may define package-specific
// options, but every option must explicitly implement Apply so invalid values
// cannot enter a Runnable call silently.
type Option interface {
	Apply(*OptionSet) error
}

// OptionScope identifies the layer that owns an invocation option. Unscoped
// options default to the global framework scope.
type OptionScope string

const (
	OptionScopeGlobal OptionScope = "global"
	OptionScopeNode   OptionScope = "node"
)

// ScopedOption optionally declares the component layer that consumes an
// option. Options without this method use OptionScopeGlobal by default.
type ScopedOption interface {
	Option
	OptionScope() OptionScope
}

type scopedOption struct {
	Option
	scope OptionScope
}

func (o scopedOption) OptionScope() OptionScope { return o.scope }

// UnwrapOption returns the option carried by a scope wrapper. Components that
// route options may use this to identify their own option type without
// executing Apply again. Unrecognized wrappers are returned unchanged.
func UnwrapOption(option Option) Option {
	if option == nil {
		return nil
	}
	for depth := 0; depth < 16; depth++ {
		wrapped, ok := option.(interface{ UnwrapOption() Option })
		if !ok {
			return option
		}
		inner := wrapped.UnwrapOption()
		if inner == nil {
			return option
		}
		option = inner
	}
	return option
}

func (o scopedOption) UnwrapOption() Option { return o.Option }

// NodeOption marks an option for a downstream node or provider. The wrapped
// option still applies normally when the downstream component resolves it.
func NodeOption(option Option) Option {
	if option == nil {
		return nil
	}
	return scopedOption{Option: option, scope: OptionScopeNode}
}

// GlobalOption marks an option for framework-level composition. It is also
// the default for options that do not declare a scope.
func GlobalOption(option Option) Option {
	if option == nil {
		return nil
	}
	return scopedOption{Option: option, scope: OptionScopeGlobal}
}

// ScopeOf returns an option's declared scope. Unknown scopes are treated as
// global so components can safely forward options they do not own.
func ScopeOf(option Option) OptionScope {
	if scoped, ok := option.(ScopedOption); ok {
		if scoped.OptionScope() == OptionScopeNode {
			return OptionScopeNode
		}
	}
	return OptionScopeGlobal
}

// OptionSet carries execution metadata shared by composed components.
type OptionSet struct {
	Metadata map[string]any
	Values   map[string]any
}

// OptionPlan is the immutable result of applying invocation options once.
// Components read the resolved set from context and use ForwardOptions when
// invoking children, avoiding repeated Apply side effects across layers.
type OptionPlan struct {
	options  []Option
	planned  []plannedOption
	resolved OptionSet
}

type plannedOption struct {
	raw             Option
	scope           OptionScope
	resolved        OptionSet
	deletedMetadata []string
	deletedValues   []string
}

// IsForwardedOption reports whether an option is an Apply-safe Plan snapshot.
// It is useful when a child invocation adds genuinely local options to an
// inherited context plan.
func IsForwardedOption(option Option) bool {
	_, ok := option.(plannedOption)
	return ok
}

func (o plannedOption) Apply(target *OptionSet) error {
	if target == nil {
		return errors.New("runnable option set is nil")
	}
	if target.Metadata == nil {
		target.Metadata = map[string]any{}
	}
	if target.Values == nil {
		target.Values = map[string]any{}
	}
	for _, key := range o.deletedMetadata {
		delete(target.Metadata, key)
	}
	for _, key := range o.deletedValues {
		delete(target.Values, key)
	}
	for key, value := range o.resolved.Metadata {
		target.Metadata[key] = value
	}
	for key, value := range o.resolved.Values {
		target.Values[key] = value
	}
	return nil
}

func (o plannedOption) OptionScope() OptionScope { return o.scope }
func (o plannedOption) UnwrapOption() Option     { return o.raw }

func NewOptionPlan(options ...Option) (OptionPlan, error) {
	resolved := OptionSet{Metadata: map[string]any{}, Values: map[string]any{}}
	planned := make([]plannedOption, 0, len(options))
	for index, option := range options {
		if option == nil {
			return OptionPlan{}, fmt.Errorf("apply runnable option %d: option is nil", index)
		}
		before := snapshotOptionSet(resolved)
		if err := option.Apply(&resolved); err != nil {
			return OptionPlan{}, fmt.Errorf("apply runnable option %d: %w", index, err)
		}
		resolvedDelta, deletedMetadata, deletedValues := optionDelta(before, resolved)
		planned = append(planned, plannedOption{
			raw:      option,
			scope:    ScopeOf(option),
			resolved: resolvedDelta, deletedMetadata: deletedMetadata, deletedValues: deletedValues,
		})
	}
	return OptionPlan{options: append([]Option(nil), options...), planned: planned, resolved: resolved}, nil
}

func (p OptionPlan) Options() []Option {
	return append([]Option(nil), p.options...)
}

// ForwardOptions returns options safe to pass to a downstream Runnable. Each
// option merges the already resolved set and never calls the original Apply.
func (p OptionPlan) ForwardOptions() []Option {
	forwarded := make([]Option, 0, len(p.planned))
	for _, option := range p.planned {
		forwarded = append(forwarded, option)
	}
	return forwarded
}

// ForwardOptionsFor returns only options owned by scope. The returned plan
// snapshots are Apply-safe and never execute the original option twice.
func (p OptionPlan) ForwardOptionsFor(scope OptionScope) []Option {
	forwarded := make([]Option, 0, len(p.planned))
	for _, option := range p.planned {
		if option.scope == scope {
			forwarded = append(forwarded, option)
		}
	}
	return forwarded
}

func (p OptionPlan) Set() OptionSet {
	set := OptionSet{Metadata: map[string]any{}, Values: map[string]any{}}
	for key, value := range p.resolved.Metadata {
		set.Metadata[key] = value
	}
	for key, value := range p.resolved.Values {
		set.Values[key] = value
	}
	return set
}

// Cascade applies only local options to an already resolved parent plan.
func (p OptionPlan) Cascade(local ...Option) (OptionPlan, error) {
	set := p.Set()
	planned := append([]plannedOption(nil), p.planned...)
	for index, option := range local {
		if option == nil {
			return OptionPlan{}, fmt.Errorf("local option %d is nil", index)
		}
		before := snapshotOptionSet(set)
		if err := option.Apply(&set); err != nil {
			return OptionPlan{}, fmt.Errorf("apply local option %d: %w", index, err)
		}
		resolvedDelta, deletedMetadata, deletedValues := optionDelta(before, set)
		planned = append(planned, plannedOption{
			raw:      option,
			scope:    ScopeOf(option),
			resolved: resolvedDelta, deletedMetadata: deletedMetadata, deletedValues: deletedValues,
		})
	}
	options := append(p.Options(), local...)
	return OptionPlan{options: options, planned: planned, resolved: set}, nil
}

type optionSnapshot struct {
	metadata map[string]string
	values   map[string]string
}

func snapshotOptionSet(set OptionSet) optionSnapshot {
	return optionSnapshot{
		metadata: snapshotOptionMap(set.Metadata),
		values:   snapshotOptionMap(set.Values),
	}
}

func snapshotOptionMap(values map[string]any) map[string]string {
	snapshot := make(map[string]string, len(values))
	for key, value := range values {
		snapshot[key] = snapshotOptionValue(value)
	}
	return snapshot
}

func snapshotOptionValue(value any) string {
	if encoded, err := json.Marshal(value); err == nil {
		return "json:" + string(encoded)
	}
	return fmt.Sprintf("go:%T:%#v", value, value)
}

func optionDelta(before optionSnapshot, after OptionSet) (OptionSet, []string, []string) {
	deletedMetadata := deletedOptionKeys(before.metadata, after.Metadata)
	deletedValues := deletedOptionKeys(before.values, after.Values)
	return OptionSet{
		Metadata: changedOptionMap(before.metadata, after.Metadata),
		Values:   changedOptionMap(before.values, after.Values),
	}, deletedMetadata, deletedValues
}

func changedOptionMap(before map[string]string, after map[string]any) map[string]any {
	changed := make(map[string]any)
	for key, value := range after {
		current := snapshotOptionValue(value)
		previous, existed := before[key]
		if !existed || previous != current {
			changed[key] = value
		}
	}
	return changed
}

func deletedOptionKeys(before map[string]string, after map[string]any) []string {
	deleted := make([]string, 0)
	for key := range before {
		if _, exists := after[key]; !exists {
			deleted = append(deleted, key)
		}
	}
	sort.Strings(deleted)
	return deleted
}

type optionPlanContextKey struct{}

func WithOptionPlan(ctx context.Context, plan OptionPlan) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, optionPlanContextKey{}, plan)
}

func OptionPlanFromContext(ctx context.Context) (OptionPlan, bool) {
	if ctx == nil {
		return OptionPlan{}, false
	}
	plan, ok := ctx.Value(optionPlanContextKey{}).(OptionPlan)
	return plan, ok
}

// Cascade appends local options after inherited options. Since OptionSet is
// resolved in order, local metadata and scalar settings override inherited
// values while unrelated options continue downstream.
func Cascade(inherited []Option, local ...Option) ([]Option, error) {
	combined := make([]Option, 0, len(inherited)+len(local))
	for index, option := range inherited {
		if option == nil {
			return nil, fmt.Errorf("inherited option %d is nil", index)
		}
		combined = append(combined, option)
	}
	for index, option := range local {
		if option == nil {
			return nil, fmt.Errorf("local option %d is nil", index)
		}
		combined = append(combined, option)
	}
	return combined, nil
}

// OptionFunc adapts a function to Option.
type OptionFunc func(*OptionSet)

func (f OptionFunc) Apply(options *OptionSet) error {
	if f == nil {
		return errors.New("runnable option function is nil")
	}
	if options == nil {
		return errors.New("runnable option set is nil")
	}
	f(options)
	return nil
}

func (OptionFunc) OptionScope() OptionScope { return OptionScopeGlobal }

// WithMetadata adds one metadata field to a composed invocation.
func WithMetadata(key string, value any) OptionFunc {
	return func(option *OptionSet) {
		if option.Metadata == nil {
			option.Metadata = map[string]any{}
		}
		option.Metadata[key] = value
	}
}

// ResolveOptions applies options to a defensive zero-value configuration.
func ResolveOptions(options ...Option) (OptionSet, error) {
	resolved := OptionSet{Metadata: map[string]any{}, Values: map[string]any{}}
	for _, option := range options {
		if option == nil {
			return OptionSet{}, errors.New("runnable option is nil")
		}
		if err := option.Apply(&resolved); err != nil {
			return OptionSet{}, fmt.Errorf("apply runnable option: %w", err)
		}
	}
	return resolved, nil
}

// Runnable is the complete provider-neutral execution contract. Components
// normally implement one native form and use Adapt to bind the other forms.
type Runnable[I, O any] interface {
	Invoke(context.Context, I, ...Option) (O, error)
	Stream(context.Context, I, ...Option) (*StreamReader[O], error)
	Collect(context.Context, *StreamReader[I], ...Option) (O, error)
	Transform(context.Context, *StreamReader[I], ...Option) (*StreamReader[O], error)
}

// Outcome carries a child component result without forcing the Graph engine to
// decide whether that component's error is fatal or domain-visible state.
type Outcome[T any] struct {
	Value T
	Err   error
}

// Batchable is an optional bulk capability for a Runnable.
type Batchable[I, O any] interface {
	Batch(context.Context, []I, ...Option) ([]O, error)
}

// Named provides diagnostic metadata without coupling composition to Agent or
// Provider-specific interfaces.
type Named interface {
	Name() string
	Description() string
}

// NodeKind identifies a component's role in a compiled Graph snapshot without
// coupling the Graph package to domain packages.
type NodeKind string

const (
	NodeKindRunnable  NodeKind = "runnable"
	NodeKindGraph     NodeKind = "graph"
	NodeKindParallel  NodeKind = "parallel"
	NodeKindChatModel NodeKind = "chat_model"
	NodeKindTools     NodeKind = "tools"
	NodeKindRetriever NodeKind = "retriever"
	NodeKindMCP       NodeKind = "mcp"
	NodeKindAgent     NodeKind = "agent"
)

// NodeDescriptor provides Graph diagnostic metadata for a Runnable.
type NodeDescriptor interface {
	NodeKind() NodeKind
}

// NamedRunnable combines execution and diagnostic metadata for registry and
// application entry points without defining an Agent-specific interface.
type NamedRunnable[I, O any] interface {
	Runnable[I, O]
	Named
}

// Func is a function-backed Runnable.
type Func[I, O any] func(context.Context, I, ...Option) (O, error)

// Invoke calls the function-backed Runnable.
func (f Func[I, O]) Invoke(ctx context.Context, input I, options ...Option) (O, error) {
	var zero O
	if f == nil {
		return zero, errors.New("runnable function is nil")
	}
	return f(ctx, input, options...)
}

func (f Func[I, O]) Stream(ctx context.Context, input I, options ...Option) (*StreamReader[O], error) {
	return StreamFromInvoke(ctx, input, f.Invoke, options...)
}

func (f Func[I, O]) Collect(ctx context.Context, input *StreamReader[I], options ...Option) (O, error) {
	return CollectFromInvoke(ctx, input, f.Invoke, options...)
}

func (f Func[I, O]) Transform(ctx context.Context, input *StreamReader[I], options ...Option) (*StreamReader[O], error) {
	return TransformFromInvoke(ctx, input, f.Invoke, options...)
}

// Batch invokes a Runnable once per input when it does not provide a native
// Batchable implementation.
func Batch[I, O any](ctx context.Context, component Runnable[I, O], inputs []I, options ...Option) ([]O, error) {
	if component == nil {
		return nil, errors.New("runnable is nil")
	}
	if ctx == nil {
		return nil, errors.New("runnable context is nil")
	}
	if batchable, ok := component.(Batchable[I, O]); ok {
		return batchable.Batch(ctx, inputs, options...)
	}
	outputs := make([]O, len(inputs))
	for index, input := range inputs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		output, err := component.Invoke(ctx, input, options...)
		if err != nil {
			return nil, err
		}
		outputs[index] = output
	}
	return outputs, nil
}
