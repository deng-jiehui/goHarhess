package graph

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"goharness/pkg/runnable"
)

type graphComponent interface {
	Compile() error
	compileGraph(map[uintptr]bool) error
	graphComponent()
}

type statePreOption[S Cloner[S], NI any] struct {
	handler StatePreHandler[S, NI]
}

func (o statePreOption[S, NI]) Apply(_ *runnable.OptionSet) error {
	if o.handler == nil {
		return fmt.Errorf("state pre handler cannot be nil")
	}
	return nil
}

func (statePreOption[S, NI]) OptionScope() runnable.OptionScope { return runnable.OptionScopeNode }

func (o statePreOption[S, NI]) bindState(binding *nodeBinding) error {
	if o.handler == nil {
		return fmt.Errorf("state pre handler cannot be nil")
	}
	binding.pre = &statePreBinding{
		stateType: typeOf[S](),
		inputType: typeOf[NI](),
		invoke: func(ctx context.Context, scope *stateScope) (erasedValue, error) {
			if scope == nil || scope.typ != typeOf[S]() {
				return erasedValue{}, fmt.Errorf("state pre handler requires state %s", typeName(typeOf[S]()))
			}
			stateValue, err := scope.snapshot()
			if err != nil {
				return erasedValue{}, err
			}
			var state S
			if stateValue.value != nil {
				value, ok := stateValue.value.(S)
				if !ok {
					return erasedValue{}, fmt.Errorf("state pre handler received %T, want %s", stateValue.value, typeName(typeOf[S]()))
				}
				state = value
			}
			input, err := o.handler(ctx, state)
			if err != nil {
				return erasedValue{}, err
			}
			return erasedValue{typ: typeOf[NI](), value: any(input)}, nil
		},
	}
	return nil
}

// WithStatePreHandler derives NI from the current strongly typed State before
// invoking a heterogeneous node.
func WithStatePreHandler[S Cloner[S], NI any](handler StatePreHandler[S, NI]) runnable.Option {
	return statePreOption[S, NI]{handler: handler}
}

type statePostOption[S Cloner[S], NO any] struct {
	handler StatePostHandler[S, NO]
}

func (o statePostOption[S, NO]) Apply(_ *runnable.OptionSet) error {
	if o.handler == nil {
		return fmt.Errorf("state post handler cannot be nil")
	}
	return nil
}

func (statePostOption[S, NO]) OptionScope() runnable.OptionScope { return runnable.OptionScopeNode }

func (o statePostOption[S, NO]) bindState(binding *nodeBinding) error {
	if o.handler == nil {
		return fmt.Errorf("state post handler cannot be nil")
	}
	binding.post = &statePostBinding{
		stateType:  typeOf[S](),
		outputType: typeOf[NO](),
		invoke: func(ctx context.Context, scope *stateScope, output erasedValue) error {
			if scope == nil || scope.typ != typeOf[S]() {
				return fmt.Errorf("state post handler requires state %s", typeName(typeOf[S]()))
			}
			if output.typ == nil || !output.typ.AssignableTo(typeOf[NO]()) {
				return fmt.Errorf("state post handler received output type %s, want %s", typeName(output.typ), typeName(typeOf[NO]()))
			}
			stateValue, err := scope.snapshot()
			if err != nil {
				return err
			}
			var state S
			if stateValue.value != nil {
				value, ok := stateValue.value.(S)
				if !ok {
					return fmt.Errorf("state post handler received state %T, want %s", stateValue.value, typeName(typeOf[S]()))
				}
				state = value
			}
			var typedOutput NO
			if output.value != nil {
				value, ok := output.value.(NO)
				if !ok {
					return fmt.Errorf("state post handler received runtime output %T, want %s", output.value, typeName(typeOf[NO]()))
				}
				typedOutput = value
			}
			updated, err := o.handler(ctx, state, typedOutput)
			if err != nil {
				return err
			}
			return scope.replace(erasedValue{typ: typeOf[S](), value: any(updated)})
		},
	}
	return nil
}

// WithStatePostHandler applies a heterogeneous node output back to State.
func WithStatePostHandler[S Cloner[S], NO any](handler StatePostHandler[S, NO]) runnable.Option {
	return statePostOption[S, NO]{handler: handler}
}

// AddNode binds one strongly typed Runnable to a graph. The wrapper performs
// type erasure once at configuration time; user code never receives any.
func AddNode[I, O any, S Cloner[S], NI, NO any](
	g *StateGraph[I, O, S],
	name string,
	node runnable.Runnable[NI, NO],
	options ...runnable.Option,
) error {
	if g == nil {
		return validationError("graph.nil", "graph", "graph cannot be nil")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return validationError("node.id.empty", "node.id", "node ID cannot be empty")
	}
	if name == Start || name == End {
		return validationError("node.id.reserved", fmt.Sprintf("nodes[%q].id", name), "ID is reserved")
	}
	if isNilRunnable(node) {
		return validationError("node.nil", fmt.Sprintf("nodes[%q]", name), "node cannot be nil")
	}
	for index, option := range options {
		if option == nil {
			return validationError("node.options.nil", fmt.Sprintf("nodes[%q].options[%d]", name, index), "option cannot be nil")
		}
	}
	binding := nodeBinding{}
	forwardedOptions := make([]runnable.Option, 0, len(options))
	for _, option := range options {
		if bindingOption, ok := option.(stateBindingOption); ok {
			if err := bindingOption.bindState(&binding); err != nil {
				return err
			}
			continue
		}
		forwardedOptions = append(forwardedOptions, option)
	}
	if binding.pre != nil {
		if binding.pre.stateType != typeOf[S]() || binding.pre.inputType != typeOf[NI]() {
			return validationError("node.state_pre.type_mismatch", fmt.Sprintf("nodes[%q]", name), "handler state/input types %s/%s do not match node %s/%s", typeName(binding.pre.stateType), typeName(binding.pre.inputType), typeName(typeOf[S]()), typeName(typeOf[NI]()))
		}
	}
	if binding.post != nil {
		if binding.post.stateType != typeOf[S]() || binding.post.outputType != typeOf[NO]() {
			return validationError("node.state_post.type_mismatch", fmt.Sprintf("nodes[%q]", name), "handler state/output types %s/%s do not match node %s/%s", typeName(binding.post.stateType), typeName(binding.post.outputType), typeName(typeOf[S]()), typeName(typeOf[NO]()))
		}
	}

	info := NodeInfo{
		ID:         name,
		Kind:       "runnable",
		Label:      name,
		InputType:  typeName(typeOf[NI]()),
		OutputType: typeName(typeOf[NO]()),
	}
	if named, ok := any(node).(runnable.Named); ok {
		info.Label = named.Name()
		info.Description = named.Description()
	}
	entry := anyNode{
		id:         name,
		inputType:  typeOf[NI](),
		outputType: typeOf[NO](),
		info:       info,
		options:    nodeScopedOptions(forwardedOptions),
		pre:        binding.pre,
		post:       binding.post,
		invoke: func(ctx context.Context, input erasedValue, opts ...runnable.Option) (erasedValue, error) {
			nodeInput := input
			if binding.pre != nil {
				scope, ok := stateScopeFromContext(ctx)
				if !ok {
					return erasedValue{}, fmt.Errorf("node %q state pre handler has no State scope", name)
				}
				var err error
				nodeInput, err = binding.pre.invoke(ctx, scope)
				if err != nil {
					return erasedValue{}, fmt.Errorf("node %q state pre handler: %w", name, err)
				}
			} else if input.typ == nil || !input.typ.AssignableTo(typeOf[NI]()) {
				return erasedValue{}, fmt.Errorf("node %q received erased type %s, want %s", name, typeName(input.typ), typeName(typeOf[NI]()))
			}
			var typedInput NI
			if nodeInput.value != nil {
				value, ok := nodeInput.value.(NI)
				if !ok {
					return erasedValue{}, fmt.Errorf("node %q received runtime type %T, want %s", name, nodeInput.value, typeName(typeOf[NI]()))
				}
				typedInput = value
			}
			output, err := node.Invoke(ctx, typedInput, opts...)
			if err != nil {
				return erasedValue{}, err
			}
			erasedOutput := erasedValue{typ: typeOf[NO](), value: any(output)}
			if binding.post != nil {
				scope, ok := stateScopeFromContext(ctx)
				if !ok {
					return erasedValue{}, fmt.Errorf("node %q state post handler has no State scope", name)
				}
				if err := binding.post.invoke(ctx, scope, erasedOutput); err != nil {
					return erasedValue{}, fmt.Errorf("node %q state post handler: %w", name, err)
				}
				// A Post handler makes graph State the node's effective output.
				// The heterogeneous NO is consumed by the handler; downstream
				// ordinary State nodes continue on the canonical State data edge.
				updated, err := scope.snapshot()
				if err != nil {
					return erasedValue{}, fmt.Errorf("node %q state snapshot: %w", name, err)
				}
				return updated, nil
			} else if typeOf[NO]() == typeOf[S]() {
				if scope, ok := stateScopeFromContext(ctx); ok {
					if err := scope.replace(erasedOutput); err != nil {
						return erasedValue{}, fmt.Errorf("node %q state update: %w", name, err)
					}
				}
			}
			return erasedOutput, nil
		},
	}
	if component, ok := any(node).(graphComponent); ok {
		entry.compile = component.compileGraph
		entry.nestedGraph = true
		entry.info.Kind = "graph"
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	if g.built {
		return validationError("graph.built", "nodes", "built graph cannot be modified")
	}
	if _, exists := g.nodes[name]; exists {
		return validationError("node.id.duplicate", fmt.Sprintf("nodes[%q].id", name), "node ID is already registered")
	}
	if entry.nestedGraph && reflect.ValueOf(node).Pointer() == reflect.ValueOf(g).Pointer() {
		return validationError("node.graph.self", fmt.Sprintf("nodes[%q]", name), "graph cannot contain itself")
	}
	g.nodes[name] = entry
	g.nodeOrder = append(g.nodeOrder, name)
	return nil
}

func MustAddNode[I, O any, S Cloner[S], NI, NO any](
	g *StateGraph[I, O, S],
	name string,
	node runnable.Runnable[NI, NO],
	options ...runnable.Option,
) {
	if err := AddNode(g, name, node, options...); err != nil {
		panic(err)
	}
}

func isNilRunnable(value any) bool {
	if value == nil {
		return true
	}
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}
