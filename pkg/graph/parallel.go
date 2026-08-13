package graph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"goharness/pkg/runnable"
)

// ParallelBranch describes one pure Runnable and its State adapters.
type ParallelBranch struct {
	Name    string
	Node    any
	Pre     any
	Post    any
	Options []runnable.Option
}

type runtimeParallelBranch struct {
	name        string
	inputType   reflect.Type
	outputType  reflect.Type
	invoke      func(context.Context, any, []runnable.Option) (any, error)
	pre         *runtimePreHandler
	post        *runtimePostHandler
	options     []runnable.Option
	nestedGraph bool
	compile     func(map[uintptr]bool) error
}

type runtimeParallelNode struct {
	branches []runtimeParallelBranch
	merger   runtimeStateMerger
}

type runtimeStateMerger struct {
	stateType reflect.Type
	invoke    func(context.Context, any, []any) error
}

type ParallelOption interface {
	applyParallel(*runtimeParallelConfig) error
}

type runtimeParallelOption func(*runtimeParallelConfig) error

func (f runtimeParallelOption) applyParallel(config *runtimeParallelConfig) error {
	if f == nil {
		return errors.New("parallel option is nil")
	}
	return f(config)
}

type runtimeParallelConfig struct {
	merger runtimeStateMerger
}

// WithStateMerger applies branch State snapshots to the base State in
// declaration order. The handler signature is
// func(context.Context, *State, []*State) error.
func WithStateMerger(handler any) ParallelOption {
	return runtimeParallelOption(func(config *runtimeParallelConfig) error {
		merger, err := bindRuntimeStateMerger(handler)
		if err != nil {
			return err
		}
		if config.merger.invoke != nil {
			return errors.New("parallel node has multiple State mergers")
		}
		config.merger = merger
		return nil
	})
}

// AddParallelNode adds a Fan-Out/Fan-In node. State is copied only at this
// ownership boundary by the configured State Codec.
func (g *Graph[I, O]) AddParallelNode(name string, branches []ParallelBranch, options ...ParallelOption) error {
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
	if len(branches) == 0 {
		return validationError("parallel.branches.empty", fmt.Sprintf("nodes[%q].branches", name), "parallel node requires at least one branch")
	}
	config := runtimeParallelConfig{}
	for index, option := range options {
		if option == nil {
			return validationError("parallel.options.nil", fmt.Sprintf("nodes[%q].options[%d]", name, index), "option cannot be nil")
		}
		if err := option.applyParallel(&config); err != nil {
			return validationError("parallel.options.invalid", fmt.Sprintf("nodes[%q].options[%d]", name, index), "%v", err)
		}
	}
	if config.merger.invoke == nil {
		return validationError("parallel.merger.nil", fmt.Sprintf("nodes[%q].merger", name), "parallel node requires a State merger")
	}
	owned := make([]runtimeParallelBranch, len(branches))
	for index, branch := range branches {
		branchName := strings.TrimSpace(branch.Name)
		if branchName == "" {
			branchName = fmt.Sprintf("branch[%d]", index)
		}
		invoker, inputType, outputType, _, component, err := bindRuntimeNode(branchName, branch.Node)
		if err != nil {
			return validationError("parallel.branch.invalid", fmt.Sprintf("nodes[%q].branches[%d]", name, index), "%v", err)
		}
		pre, err := bindOptionalRuntimePreHandler(branch.Pre)
		if err != nil {
			return validationError("parallel.branch.pre.invalid", fmt.Sprintf("nodes[%q].branches[%d].pre", name, index), "%v", err)
		}
		post, err := bindOptionalRuntimePostHandler(branch.Post)
		if err != nil {
			return validationError("parallel.branch.post.invalid", fmt.Sprintf("nodes[%q].branches[%d].post", name, index), "%v", err)
		}
		for optionIndex, option := range branch.Options {
			if option == nil {
				return validationError("parallel.branch.options.nil", fmt.Sprintf("nodes[%q].branches[%d].options[%d]", name, index, optionIndex), "option cannot be nil")
			}
		}
		if component != nil && reflect.ValueOf(branch.Node).Pointer() == reflect.ValueOf(g).Pointer() {
			return validationError("parallel.branch.graph.self", fmt.Sprintf("nodes[%q].branches[%d]", name, index), "graph cannot contain itself")
		}
		owned[index] = runtimeParallelBranch{
			name: branchName, inputType: inputType, outputType: outputType, invoke: invoker,
			pre: pre, post: post, options: nodeScopedOptions(branch.Options), nestedGraph: component != nil,
		}
		if component != nil {
			owned[index].compile = component.compileGraph
		}
	}
	compilers := make([]func(map[uintptr]bool) error, 0, len(owned))
	for _, branch := range owned {
		if branch.compile != nil {
			compilers = append(compilers, branch.compile)
		}
	}
	entry := runtimeNode{
		id: name, inputType: config.merger.stateType, outputType: config.merger.stateType,
		info:     NodeInfo{ID: name, Kind: "parallel", Label: name, InputType: typeName(config.merger.stateType), OutputType: typeName(config.merger.stateType)},
		parallel: &runtimeParallelNode{branches: owned, merger: config.merger},
		compile: func(stack map[uintptr]bool) error {
			for _, compile := range compilers {
				if err := compile(stack); err != nil {
					return err
				}
			}
			return nil
		},
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.built {
		return validationError("graph.built", "nodes", "built graph cannot be modified")
	}
	if _, exists := g.nodes[name]; exists {
		return validationError("node.id.duplicate", fmt.Sprintf("nodes[%q].id", name), "node ID is already registered")
	}
	g.nodes[name] = entry
	g.nodeOrder = append(g.nodeOrder, name)
	return nil
}

func bindOptionalRuntimePreHandler(handler any) (*runtimePreHandler, error) {
	if handler == nil {
		return nil, nil
	}
	return bindRuntimePreHandler(handler)
}

func bindOptionalRuntimePostHandler(handler any) (*runtimePostHandler, error) {
	if handler == nil {
		return nil, nil
	}
	return bindRuntimePostHandler(handler)
}

func bindRuntimeStateMerger(handler any) (runtimeStateMerger, error) {
	value := reflect.ValueOf(handler)
	if !value.IsValid() || isNilReflectValue(value) || value.Kind() != reflect.Func {
		return runtimeStateMerger{}, errors.New("State merger must be a non-nil function")
	}
	typeOfHandler := value.Type()
	if typeOfHandler.NumIn() != 3 || typeOfHandler.In(0) != contextType || typeOfHandler.In(1).Kind() != reflect.Pointer || typeOfHandler.In(1).Elem().Kind() != reflect.Struct || typeOfHandler.In(2).Kind() != reflect.Slice || typeOfHandler.In(2).Elem() != typeOfHandler.In(1) || typeOfHandler.NumOut() != 1 || typeOfHandler.Out(0) != errorType {
		return runtimeStateMerger{}, errors.New("State merger must have signature func(context.Context, *State, []*State) error")
	}
	stateType := typeOfHandler.In(1)
	return runtimeStateMerger{stateType: stateType, invoke: func(ctx context.Context, base any, branches []any) (err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("State merger panicked: %v", recovered)
			}
		}()
		branchValues := reflect.MakeSlice(typeOfHandler.In(2), len(branches), len(branches))
		for index, branch := range branches {
			branchValues.Index(index).Set(reflect.ValueOf(branch))
		}
		outputs := value.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(base), branchValues})
		if !outputs[0].IsNil() {
			return outputs[0].Interface().(error)
		}
		return nil
	}}, nil
}

func (g *Graph[I, O]) invokeRuntimeParallel(ctx context.Context, node *runtimeParallelNode, base any, plan runnable.OptionPlan) error {
	codec := g.runtimeCodec()
	if codec == nil {
		return errors.New("parallel node requires a State factory and State Codec")
	}
	data, err := callStateCodecEncode(ctx, codec, base)
	if err != nil {
		return fmt.Errorf("snapshot parallel base State: %w", err)
	}
	inputs := make([]any, len(node.branches))
	seenPointers := map[uintptr]string{reflect.ValueOf(base).Pointer(): "base State"}
	for index := range node.branches {
		inputs[index], err = callStateCodecDecode(ctx, codec, data)
		if err != nil {
			return fmt.Errorf("restore parallel branch %q State: %w", node.branches[index].name, err)
		}
		if reflect.TypeOf(inputs[index]) != g.stateType {
			return fmt.Errorf("parallel branch %q State type %T, want %s", node.branches[index].name, inputs[index], typeName(g.stateType))
		}
		pointer := reflect.ValueOf(inputs[index]).Pointer()
		if owner, exists := seenPointers[pointer]; exists {
			return fmt.Errorf("parallel branch %q Codec did not create an independent State pointer; it shares ownership with %s", node.branches[index].name, owner)
		}
		seenPointers[pointer] = fmt.Sprintf("branch %q", node.branches[index].name)
		if g.codec == nil {
			if err := compareDynamicTypes(reflect.ValueOf(base), reflect.ValueOf(inputs[index]), g.stateType.String(), map[visitPair]bool{}); err != nil {
				return fmt.Errorf("default State Codec lost dynamic type: %w; configure a custom State Codec", err)
			}
			if index == 0 {
				roundTrip, err := json.Marshal(inputs[index])
				if err != nil {
					return fmt.Errorf("encode parallel State JSON round trip: %w", err)
				}
				if !bytes.Equal(data, roundTrip) {
					return errors.New("default State Codec produced an unstable JSON round trip; configure a custom State Codec")
				}
			}
		}
	}
	control, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var workers sync.WaitGroup
	var firstErr error
	var errorOnce sync.Once
	workers.Add(len(node.branches))
	for index := range node.branches {
		go func(index int) {
			defer workers.Done()
			branch := node.branches[index]
			branchCtx := withRuntimeState(control, inputs[index])
			branchPlan, err := plan.Cascade(branch.options...)
			if err != nil {
				errorOnce.Do(func() {
					firstErr = fmt.Errorf("parallel branch %q options: %w", branch.name, err)
					cancel(firstErr)
				})
				return
			}
			branchCtx = runnable.WithOptionPlan(branchCtx, branchPlan)
			input := inputs[index]
			if branch.pre != nil {
				input, err = branch.pre.invoke(branchCtx, inputs[index])
				if err != nil {
					errorOnce.Do(func() {
						firstErr = fmt.Errorf("parallel branch %q pre handler: %w", branch.name, err)
						cancel(firstErr)
					})
					return
				}
			}
			invokeCtx := branchCtx
			if branch.nestedGraph {
				invokeCtx = WithDataSinkIsolated(invokeCtx, nil)
				invokeCtx = withoutRuntimeStateCheckpointProvider(invokeCtx)
			}
			output, err := branch.invoke(invokeCtx, input, branchPlan.ForwardOptionsFor(runnable.OptionScopeNode))
			if err != nil {
				errorOnce.Do(func() {
					firstErr = fmt.Errorf("parallel branch %q: %w", branch.name, err)
					cancel(firstErr)
				})
				return
			}
			if branch.post != nil {
				if err := branch.post.invoke(branchCtx, inputs[index], output); err != nil {
					errorOnce.Do(func() {
						firstErr = fmt.Errorf("parallel branch %q post handler: %w", branch.name, err)
						cancel(firstErr)
					})
				}
			}
		}(index)
	}
	workers.Wait()
	if firstErr != nil {
		return firstErr
	}
	if err := node.merger.invoke(ctx, base, inputs); err != nil {
		return fmt.Errorf("merge parallel State: %w", err)
	}
	return nil
}
