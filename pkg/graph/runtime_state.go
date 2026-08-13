package graph

import (
	"bytes"
	"context"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	statepkg "goharness/pkg/state"
)

var (
	jsonMarshalerType   = reflect.TypeFor[json.Marshaler]()
	jsonUnmarshalerType = reflect.TypeFor[json.Unmarshaler]()
	textMarshalerType   = reflect.TypeFor[encoding.TextMarshaler]()
)

// StateCodec creates owned snapshots of one exact runtime State pointer type.
type StateCodec interface {
	ID() string
	Version() int
	StateType() reflect.Type
	Encode(context.Context, any) ([]byte, error)
	Decode(context.Context, []byte) (any, error)
}

type typedStateCodec[S any] struct {
	id      string
	version int
	encode  func(context.Context, S) ([]byte, error)
	decode  func(context.Context, []byte) (S, error)
}

// NewStateCodec adapts a typed domain codec to the runtime Graph State codec.
func NewStateCodec[S any](
	id string,
	version int,
	encode func(context.Context, S) ([]byte, error),
	decode func(context.Context, []byte) (S, error),
) (StateCodec, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, errors.New("State Codec ID cannot be empty")
	}
	if version <= 0 {
		return nil, fmt.Errorf("State Codec version must be positive, got %d", version)
	}
	if encode == nil || decode == nil {
		return nil, errors.New("State Codec encode and decode functions are required")
	}
	stateType := typeOf[S]()
	if stateType.Kind() != reflect.Pointer || stateType.Elem().Kind() != reflect.Struct {
		return nil, fmt.Errorf("State Codec type must be a pointer to struct, got %s", typeName(stateType))
	}
	return typedStateCodec[S]{id: id, version: version, encode: encode, decode: decode}, nil
}

func (c typedStateCodec[S]) ID() string              { return c.id }
func (c typedStateCodec[S]) Version() int            { return c.version }
func (c typedStateCodec[S]) StateType() reflect.Type { return typeOf[S]() }

func (c typedStateCodec[S]) Encode(ctx context.Context, state any) (data []byte, err error) {
	value, ok := state.(S)
	if !ok {
		return nil, fmt.Errorf("State Codec received %T, want %s", state, typeName(typeOf[S]()))
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("State Codec encode panicked: %v", recovered)
		}
	}()
	return c.encode(ctx, value)
}

func (c typedStateCodec[S]) Decode(ctx context.Context, data []byte) (state any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("State Codec decode panicked: %v", recovered)
		}
	}()
	value, err := c.decode(ctx, append([]byte(nil), data...))
	if err != nil {
		return nil, err
	}
	reflected := reflect.ValueOf(value)
	if !reflected.IsValid() || reflected.IsNil() {
		return nil, errors.New("State Codec decoded nil State")
	}
	return value, nil
}

type strictJSONStateCodec struct{ stateType reflect.Type }

func (c strictJSONStateCodec) ID() string              { return "json:" + c.stateType.String() }
func (strictJSONStateCodec) Version() int              { return 1 }
func (c strictJSONStateCodec) StateType() reflect.Type { return c.stateType }

func (c strictJSONStateCodec) Encode(_ context.Context, state any) ([]byte, error) {
	if state == nil || reflect.TypeOf(state) != c.stateType {
		return nil, fmt.Errorf("default State Codec received %T, want %s", state, typeName(c.stateType))
	}
	data, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("encode State as JSON: %w", err)
	}
	return data, nil
}

func (c strictJSONStateCodec) Decode(_ context.Context, data []byte) (any, error) {
	value := reflect.New(c.stateType.Elem())
	if err := json.Unmarshal(append([]byte(nil), data...), value.Interface()); err != nil {
		return nil, fmt.Errorf("decode State from JSON: %w", err)
	}
	return value.Interface(), nil
}

// SetStateCodec replaces the strict default JSON State snapshot codec.
func (g *Graph[I, O]) SetStateCodec(codec StateCodec) error {
	if g == nil {
		return validationError("graph.nil", "graph", "graph cannot be nil")
	}
	if codec == nil || isNilReflectValue(reflect.ValueOf(codec)) {
		return validationError("graph.state_codec.nil", "state_codec", "State Codec cannot be nil")
	}
	id, version, stateType, err := readStateCodecMetadata(codec)
	if err != nil {
		return validationError("graph.state_codec.invalid", "state_codec", "%v", err)
	}
	if id == "" || version <= 0 {
		return validationError("graph.state_codec.invalid", "state_codec", "State Codec requires a non-empty ID and positive version")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.built {
		return validationError("graph.built", "state_codec", "built graph cannot be modified")
	}
	g.codec = codec
	g.codecID = id
	g.codecVersion = version
	g.codecType = stateType
	return nil
}

func readStateCodecMetadata(codec StateCodec) (id string, version int, stateType reflect.Type, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("State Codec metadata panicked: %v", recovered)
		}
	}()
	id = strings.TrimSpace(codec.ID())
	version = codec.Version()
	stateType = codec.StateType()
	if stateType == nil || stateType.Kind() != reflect.Pointer || stateType.Elem().Kind() != reflect.Struct {
		return "", 0, nil, fmt.Errorf("State Codec type must be a pointer to struct, got %s", typeName(stateType))
	}
	return id, version, stateType, nil
}

type runtimeStateCheckpointProvider func(context.Context) (statepkg.GraphStateCheckpoint, error)
type runtimeStateCheckpointProviderKey struct{}
type runtimeStateContextKey struct{}

func withRuntimeState(ctx context.Context, state any) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, runtimeStateContextKey{}, state)
}

func runtimeStateFromContext(ctx context.Context) (any, bool) {
	if ctx == nil {
		return nil, false
	}
	state := ctx.Value(runtimeStateContextKey{})
	return state, state != nil
}

func withRuntimeStateCheckpointProvider(ctx context.Context, provider runtimeStateCheckpointProvider) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, runtimeStateCheckpointProviderKey{}, provider)
}

func withoutRuntimeStateCheckpointProvider(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, runtimeStateCheckpointProviderKey{}, runtimeStateCheckpointProvider(nil))
}

// StateCheckpointFromContext lazily snapshots the current runtime Graph State.
func StateCheckpointFromContext(ctx context.Context) (statepkg.GraphStateCheckpoint, bool, error) {
	if ctx == nil {
		return statepkg.GraphStateCheckpoint{}, false, nil
	}
	provider, ok := ctx.Value(runtimeStateCheckpointProviderKey{}).(runtimeStateCheckpointProvider)
	if !ok || provider == nil {
		return statepkg.GraphStateCheckpoint{}, false, nil
	}
	checkpoint, err := provider(ctx)
	if err != nil {
		return statepkg.GraphStateCheckpoint{}, true, err
	}
	return checkpoint.Clone(), true, nil
}

func (g *Graph[I, O]) runtimeCodec() StateCodec {
	if g.codec != nil {
		return g.codec
	}
	if g.stateType == nil {
		return nil
	}
	return strictJSONStateCodec{stateType: g.stateType}
}

func (g *Graph[I, O]) validateRuntimeCodec() error {
	if g.stateType == nil {
		if g.codec != nil {
			return validationError("graph.state_codec.without_state", "state_codec", "State Codec requires a State factory")
		}
		return nil
	}
	codecType := g.stateType
	if g.codec != nil {
		codecType = g.codecType
	}
	if codecType != g.stateType {
		return validationError("graph.state_codec.type_mismatch", "state_codec", "State Codec type %s does not match factory State %s", typeName(codecType), typeName(g.stateType))
	}
	if g.codec == nil {
		if err := validateDefaultJSONStateType(g.stateType); err != nil {
			return validationError("graph.state_codec.default_unsafe", "state_factory", "%v", err)
		}
	}
	return nil
}

func validateDefaultJSONStateType(stateType reflect.Type) error {
	if stateType == nil || stateType.Kind() != reflect.Pointer || stateType.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("default State Codec requires a pointer to struct, got %s", typeName(stateType))
	}
	return validateDefaultJSONType(stateType.Elem(), stateType.String(), map[reflect.Type]bool{})
}

func validateDefaultJSONType(valueType reflect.Type, path string, visiting map[reflect.Type]bool) error {
	if hasCompleteJSONContract(valueType) {
		return nil
	}
	for valueType.Kind() == reflect.Pointer {
		valueType = valueType.Elem()
		if hasCompleteJSONContract(valueType) {
			return nil
		}
	}
	if visiting[valueType] {
		return nil
	}
	visiting[valueType] = true
	defer delete(visiting, valueType)
	switch valueType.Kind() {
	case reflect.Struct:
		for index := 0; index < valueType.NumField(); index++ {
			field := valueType.Field(index)
			fieldPath := path + "." + field.Name
			if !field.IsExported() {
				return fmt.Errorf("default State Codec cannot preserve unexported field %s; configure a custom State Codec", fieldPath)
			}
			if strings.Split(field.Tag.Get("json"), ",")[0] == "-" {
				return fmt.Errorf("default State Codec cannot preserve json:\"-\" field %s; configure a custom State Codec", fieldPath)
			}
			if err := validateDefaultJSONType(field.Type, fieldPath, visiting); err != nil {
				return err
			}
		}
	case reflect.Array, reflect.Slice:
		return validateDefaultJSONType(valueType.Elem(), path+"[]", visiting)
	case reflect.Map:
		if valueType.Key().Kind() != reflect.String && !valueType.Key().Implements(textMarshalerType) {
			return fmt.Errorf("default State Codec cannot preserve map key %s at %s", typeName(valueType.Key()), path)
		}
		return validateDefaultJSONType(valueType.Elem(), path+"{}", visiting)
	case reflect.Chan, reflect.Func, reflect.UnsafePointer, reflect.Complex64, reflect.Complex128:
		return fmt.Errorf("default State Codec cannot preserve %s at %s; configure a custom State Codec", valueType.Kind(), path)
	}
	return nil
}

func hasCompleteJSONContract(valueType reflect.Type) bool {
	marshal := valueType.Implements(jsonMarshalerType) || (valueType.Kind() != reflect.Pointer && reflect.PointerTo(valueType).Implements(jsonMarshalerType))
	unmarshal := valueType.Implements(jsonUnmarshalerType) || (valueType.Kind() != reflect.Pointer && reflect.PointerTo(valueType).Implements(jsonUnmarshalerType))
	return marshal && unmarshal
}

func (g *Graph[I, O]) snapshotRuntimeState(ctx context.Context, state any) (statepkg.GraphStateCheckpoint, error) {
	codec := g.runtimeCodec()
	if codec == nil {
		return statepkg.GraphStateCheckpoint{}, errors.New("runtime Graph has no State Codec")
	}
	data, err := callStateCodecEncode(ctx, codec, state)
	if err != nil {
		return statepkg.GraphStateCheckpoint{}, err
	}
	if g.codec == nil {
		copyState, err := callStateCodecDecode(ctx, codec, data)
		if err != nil {
			return statepkg.GraphStateCheckpoint{}, err
		}
		if err := compareDynamicTypes(reflect.ValueOf(state), reflect.ValueOf(copyState), g.stateType.String(), map[visitPair]bool{}); err != nil {
			return statepkg.GraphStateCheckpoint{}, fmt.Errorf("default State Codec lost dynamic type: %w; configure a custom State Codec", err)
		}
		roundTrip, err := callStateCodecEncode(ctx, codec, copyState)
		if err != nil {
			return statepkg.GraphStateCheckpoint{}, err
		}
		if !bytes.Equal(data, roundTrip) {
			return statepkg.GraphStateCheckpoint{}, errors.New("default State Codec produced an unstable JSON round trip; configure a custom State Codec")
		}
	}
	id, version := g.runtimeCodecMetadata()
	checkpoint := statepkg.GraphStateCheckpoint{CodecID: id, Version: version, Data: append([]byte(nil), data...)}
	if err := checkpoint.Validate(); err != nil {
		return statepkg.GraphStateCheckpoint{}, err
	}
	return checkpoint, nil
}

func (g *Graph[I, O]) restoreRuntimeState(ctx context.Context, checkpoint statepkg.GraphStateCheckpoint) (any, error) {
	codec := g.runtimeCodec()
	if codec == nil {
		return nil, errors.New("Graph State checkpoint requires a State factory")
	}
	if err := checkpoint.Validate(); err != nil {
		return nil, err
	}
	id, version := g.runtimeCodecMetadata()
	if checkpoint.CodecID != id || checkpoint.Version != version {
		return nil, fmt.Errorf("Graph State checkpoint Codec %q/%d does not match %q/%d", checkpoint.CodecID, checkpoint.Version, id, version)
	}
	state, err := callStateCodecDecode(ctx, codec, checkpoint.Data)
	if err != nil {
		return nil, err
	}
	if reflect.TypeOf(state) != g.stateType {
		return nil, fmt.Errorf("Graph State checkpoint decoded %T, want %s", state, typeName(g.stateType))
	}
	return state, nil
}

func (g *Graph[I, O]) runtimeCodecMetadata() (string, int) {
	if g.codec != nil {
		return g.codecID, g.codecVersion
	}
	return "json:" + g.stateType.String(), 1
}

func callStateCodecEncode(ctx context.Context, codec StateCodec, state any) (data []byte, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("State Codec encode panicked: %v", recovered)
		}
	}()
	return codec.Encode(ctx, state)
}

func callStateCodecDecode(ctx context.Context, codec StateCodec, data []byte) (state any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("State Codec decode panicked: %v", recovered)
		}
	}()
	state, err = codec.Decode(ctx, append([]byte(nil), data...))
	if err != nil {
		return nil, err
	}
	value := reflect.ValueOf(state)
	if !value.IsValid() || (value.Kind() == reflect.Pointer && value.IsNil()) {
		return nil, errors.New("State Codec decoded nil State")
	}
	return state, nil
}

type visitPair struct {
	source uintptr
	target uintptr
	typeOf reflect.Type
}

func compareDynamicTypes(source, target reflect.Value, path string, visited map[visitPair]bool) error {
	if !source.IsValid() || !target.IsValid() {
		if source.IsValid() != target.IsValid() {
			return fmt.Errorf("%s validity changed", path)
		}
		return nil
	}
	if source.Type() != target.Type() {
		return fmt.Errorf("%s changed from %s to %s", path, typeName(source.Type()), typeName(target.Type()))
	}
	if source.Kind() == reflect.Interface {
		if source.IsNil() || target.IsNil() {
			if source.IsNil() != target.IsNil() {
				return fmt.Errorf("%s nil interface changed", path)
			}
			return nil
		}
		if source.Elem().Type() != target.Elem().Type() {
			return fmt.Errorf("%s changed from %s to %s", path, typeName(source.Elem().Type()), typeName(target.Elem().Type()))
		}
		return compareDynamicTypes(source.Elem(), target.Elem(), path, visited)
	}
	if source.Kind() == reflect.Pointer {
		if source.IsNil() || target.IsNil() {
			if source.IsNil() != target.IsNil() {
				return fmt.Errorf("%s pointer nil state changed", path)
			}
			return nil
		}
		pair := visitPair{source: source.Pointer(), target: target.Pointer(), typeOf: source.Type()}
		if visited[pair] {
			return nil
		}
		visited[pair] = true
		return compareDynamicTypes(source.Elem(), target.Elem(), path, visited)
	}
	if hasCompleteJSONContract(source.Type()) {
		return nil
	}
	switch source.Kind() {
	case reflect.Struct:
		for index := 0; index < source.NumField(); index++ {
			field := source.Type().Field(index)
			if field.IsExported() {
				if err := compareDynamicTypes(source.Field(index), target.Field(index), path+"."+field.Name, visited); err != nil {
					return err
				}
			}
		}
	case reflect.Slice, reflect.Array:
		if source.Len() != target.Len() {
			return fmt.Errorf("%s length changed", path)
		}
		for index := 0; index < source.Len(); index++ {
			if err := compareDynamicTypes(source.Index(index), target.Index(index), fmt.Sprintf("%s[%d]", path, index), visited); err != nil {
				return err
			}
		}
	case reflect.Map:
		iterator := source.MapRange()
		for iterator.Next() {
			key := iterator.Key()
			targetValue := target.MapIndex(key)
			if !targetValue.IsValid() {
				return fmt.Errorf("%s map key disappeared", path)
			}
			if err := compareDynamicTypes(iterator.Value(), targetValue, fmt.Sprintf("%s[%v]", path, key.Interface()), visited); err != nil {
				return err
			}
		}
	}
	return nil
}
