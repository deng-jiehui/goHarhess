package graph

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/goharness/goharness/pkg/runnable"
)

type erasedStream struct {
	value    reflect.Value
	elemType reflect.Type
	collect  func(context.Context) (any, error)
	close    func() error
}

func compileRuntimePlans[I, O any](runtime *Graph[I, O]) (*CompiledGraph[I, O], error) {
	for id, node := range runtime.nodes {
		stream, collect, transform, err := bindRuntimeStreamCapabilities(id, node.component, node.inputType, node.outputType)
		if err != nil {
			return nil, err
		}
		node.stream = stream
		node.collect = collect
		node.transform = transform
		runtime.nodes[id] = node
	}
	compiled := &CompiledGraph[I, O]{runtime: runtime}
	compiled.invokePlan = runtime.invokeRuntime
	compiled.streamPlan = runtime.streamRuntime
	return compiled, nil
}

func bindRuntimeStreamCapabilities(
	name string,
	component any,
	inputType reflect.Type,
	outputType reflect.Type,
) (
	func(context.Context, any, []runnable.Option) (erasedStream, error),
	func(context.Context, erasedStream, []runnable.Option) (any, error),
	func(context.Context, erasedStream, []runnable.Option) (erasedStream, error),
	error,
) {
	value := reflect.ValueOf(component)
	if !value.IsValid() || isNilReflectValue(value) {
		return nil, nil, nil, validationError("node.nil", fmt.Sprintf("nodes[%q]", name), "node cannot be nil")
	}
	stream, err := bindStreamMethod(name, value.MethodByName("Stream"), inputType, outputType)
	if err != nil {
		return nil, nil, nil, err
	}
	collect, err := bindCollectMethod(name, value.MethodByName("Collect"), inputType, outputType)
	if err != nil {
		return nil, nil, nil, err
	}
	transform, err := bindTransformMethod(name, value.MethodByName("Transform"), inputType, outputType)
	if err != nil {
		return nil, nil, nil, err
	}
	return stream, collect, transform, nil
}

func bindStreamMethod(name string, method reflect.Value, inputType, outputType reflect.Type) (func(context.Context, any, []runnable.Option) (erasedStream, error), error) {
	if !method.IsValid() {
		return nil, nil
	}
	typ := method.Type()
	if !validVariadicMethod(typ, inputType) || !isStreamReaderType(typ.Out(0), outputType) {
		return nil, validationError("node.stream.signature", fmt.Sprintf("nodes[%q]", name), "Stream must have signature func(context.Context, %s, ...runnable.Option) (*runnable.StreamReader[%s], error)", typeName(inputType), typeName(outputType))
	}
	return func(ctx context.Context, input any, options []runnable.Option) (result erasedStream, err error) {
		defer recoverNodePanic(name, &err)
		outputs := method.CallSlice([]reflect.Value{reflect.ValueOf(ctx), reflectArgument(inputType, input), reflect.ValueOf(options)})
		if !outputs[1].IsNil() {
			return erasedStream{}, outputs[1].Interface().(error)
		}
		return eraseStreamReader(outputs[0], outputType)
	}, nil
}

func bindCollectMethod(name string, method reflect.Value, inputType, outputType reflect.Type) (func(context.Context, erasedStream, []runnable.Option) (any, error), error) {
	if !method.IsValid() {
		return nil, nil
	}
	typ := method.Type()
	if !typ.IsVariadic() || typ.NumIn() != 3 || typ.In(0) != contextType || !isStreamReaderType(typ.In(1), inputType) || typ.In(2) != optionSliceType || typ.NumOut() != 2 || typ.Out(0) != outputType || typ.Out(1) != errorType {
		return nil, validationError("node.collect.signature", fmt.Sprintf("nodes[%q]", name), "Collect has an incompatible signature")
	}
	return func(ctx context.Context, input erasedStream, options []runnable.Option) (result any, err error) {
		if input.elemType != inputType {
			return nil, fmt.Errorf("node %q Collect received stream of %s, want %s", name, typeName(input.elemType), typeName(inputType))
		}
		defer recoverNodePanic(name, &err)
		outputs := method.CallSlice([]reflect.Value{reflect.ValueOf(ctx), input.value, reflect.ValueOf(options)})
		if !outputs[1].IsNil() {
			return nil, outputs[1].Interface().(error)
		}
		return outputs[0].Interface(), nil
	}, nil
}

func bindTransformMethod(name string, method reflect.Value, inputType, outputType reflect.Type) (func(context.Context, erasedStream, []runnable.Option) (erasedStream, error), error) {
	if !method.IsValid() {
		return nil, nil
	}
	typ := method.Type()
	if !typ.IsVariadic() || typ.NumIn() != 3 || typ.In(0) != contextType || !isStreamReaderType(typ.In(1), inputType) || typ.In(2) != optionSliceType || typ.NumOut() != 2 || !isStreamReaderType(typ.Out(0), outputType) || typ.Out(1) != errorType {
		return nil, validationError("node.transform.signature", fmt.Sprintf("nodes[%q]", name), "Transform has an incompatible signature")
	}
	return func(ctx context.Context, input erasedStream, options []runnable.Option) (result erasedStream, err error) {
		if input.elemType != inputType {
			return erasedStream{}, fmt.Errorf("node %q Transform received stream of %s, want %s", name, typeName(input.elemType), typeName(inputType))
		}
		defer recoverNodePanic(name, &err)
		outputs := method.CallSlice([]reflect.Value{reflect.ValueOf(ctx), input.value, reflect.ValueOf(options)})
		if !outputs[1].IsNil() {
			return erasedStream{}, outputs[1].Interface().(error)
		}
		return eraseStreamReader(outputs[0], outputType)
	}, nil
}

func validVariadicMethod(typ reflect.Type, inputType reflect.Type) bool {
	return typ.IsVariadic() && typ.NumIn() == 3 && typ.In(0) == contextType && typ.In(1) == inputType && typ.In(2) == optionSliceType && typ.NumOut() == 2 && typ.Out(1) == errorType
}

func isStreamReaderType(typ, elemType reflect.Type) bool {
	if typ == nil || typ.Kind() != reflect.Pointer {
		return false
	}
	recv, ok := typ.MethodByName("Recv")
	return ok && recv.Type.NumIn() == 1 && recv.Type.NumOut() == 2 && recv.Type.Out(0) == elemType && recv.Type.Out(1) == errorType
}

func eraseStreamReader(value reflect.Value, elemType reflect.Type) (erasedStream, error) {
	if !value.IsValid() || value.IsNil() {
		return erasedStream{}, errors.New("node returned a nil StreamReader")
	}
	if !isStreamReaderType(value.Type(), elemType) {
		return erasedStream{}, fmt.Errorf("stream reader element type is %s, want %s", typeName(value.Type()), typeName(elemType))
	}
	collect := value.MethodByName("Collect")
	closeMethod := value.MethodByName("Close")
	return erasedStream{
		value: value, elemType: elemType,
		collect: func(ctx context.Context) (any, error) {
			outputs := collect.Call([]reflect.Value{reflect.ValueOf(ctx)})
			if !outputs[1].IsNil() {
				return nil, outputs[1].Interface().(error)
			}
			return outputs[0].Interface(), nil
		},
		close: func() error {
			outputs := closeMethod.Call(nil)
			if len(outputs) > 0 && !outputs[0].IsNil() {
				return outputs[0].Interface().(error)
			}
			return nil
		},
	}, nil
}

func recoverNodePanic(name string, err *error) {
	if recovered := recover(); recovered != nil {
		*err = fmt.Errorf("node %q panicked: %v", name, recovered)
	}
}
