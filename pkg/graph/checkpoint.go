package graph

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	statepkg "goharness/pkg/state"
)

// StateCodec gives a graph owner explicit control over durable State
// serialization. Graphs never fall back to JSON because JSON cannot preserve
// unexported fields, interface invariants, or provider-specific pointers.
type StateCodec[S Cloner[S]] interface {
	ID() string
	Version() int
	Marshal(context.Context, S) ([]byte, error)
	Unmarshal(context.Context, []byte) (S, error)
}

// SetStateCodec configures the codec used when a graph State checkpoint is
// present in the invocation Context. It must be configured before Compile.
func (g *StateGraph[I, O, S]) SetStateCodec(codec StateCodec[S]) error {
	if g == nil {
		return validationError("graph.nil", "graph", "graph cannot be nil")
	}
	if isNilStateCodec(codec) {
		return validationError("graph.state_codec.nil", "state_codec", "state codec cannot be nil")
	}
	id, version, err := stateCodecMetadata(codec)
	if err != nil {
		return validationError("graph.state_codec.invalid", "state_codec", "%v", err)
	}
	if id == "" {
		return validationError("graph.state_codec.id.empty", "state_codec", "codec ID cannot be empty")
	}
	if version <= 0 {
		return validationError("graph.state_codec.version.invalid", "state_codec", "codec version must be positive, got %d", version)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.built {
		return validationError("graph.built", "state_codec", "built graph cannot be modified")
	}
	g.codec = codec
	return nil
}

func isNilStateCodec[S Cloner[S]](codec StateCodec[S]) bool {
	if codec == nil {
		return true
	}
	value := reflect.ValueOf(codec)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func stateCodecMetadata[S Cloner[S]](codec StateCodec[S]) (id string, version int, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("codec metadata panicked: %v", recovered)
		}
	}()
	id = strings.TrimSpace(codec.ID())
	version = codec.Version()
	return id, version, nil
}

func (g *StateGraph[I, O, S]) stateCodec() StateCodec[S] {
	if g == nil {
		return nil
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.codec
}

func (g *StateGraph[I, O, S]) decodeState(ctx context.Context, checkpoint statepkg.GraphStateCheckpoint) (erasedValue, error) {
	codec := g.stateCodec()
	if isNilStateCodec(codec) {
		return erasedValue{}, fmt.Errorf("graph State checkpoint requires a configured state codec")
	}
	if err := checkpoint.Validate(); err != nil {
		return erasedValue{}, err
	}
	id, version, err := stateCodecMetadata(codec)
	if err != nil {
		return erasedValue{}, err
	}
	if checkpoint.CodecID != id {
		return erasedValue{}, fmt.Errorf("graph State checkpoint codec ID %q does not match configured codec ID %q", checkpoint.CodecID, id)
	}
	if checkpoint.Version != version {
		return erasedValue{}, fmt.Errorf("graph State checkpoint version %d does not match configured codec version %d", checkpoint.Version, version)
	}
	data := append([]byte(nil), checkpoint.Data...)
	value, err := codec.Unmarshal(ctx, data)
	if err != nil {
		return erasedValue{}, fmt.Errorf("decode graph State with codec %q: %w", id, err)
	}
	cloned, err := cloneStateValue(value)
	if err != nil {
		return erasedValue{}, fmt.Errorf("clone decoded graph State: %w", err)
	}
	return erasedValue{typ: g.stateType, value: any(cloned)}, nil
}

func (g *StateGraph[I, O, S]) encodeState(ctx context.Context, scope *stateScope) (statepkg.GraphStateCheckpoint, error) {
	codec := g.stateCodec()
	if isNilStateCodec(codec) {
		return statepkg.GraphStateCheckpoint{}, nil
	}
	stateValue, err := scope.snapshot()
	if err != nil {
		return statepkg.GraphStateCheckpoint{}, fmt.Errorf("snapshot graph State for checkpoint: %w", err)
	}
	var value S
	if stateValue.value != nil {
		converted, ok := stateValue.value.(S)
		if !ok {
			return statepkg.GraphStateCheckpoint{}, fmt.Errorf("snapshot graph State runtime type %T, want %s", stateValue.value, typeName(g.stateType))
		}
		value = converted
	}
	id, version, err := stateCodecMetadata(codec)
	if err != nil {
		return statepkg.GraphStateCheckpoint{}, err
	}
	data, err := codec.Marshal(ctx, value)
	if err != nil {
		return statepkg.GraphStateCheckpoint{}, fmt.Errorf("encode graph State with codec %q: %w", id, err)
	}
	checkpoint := statepkg.GraphStateCheckpoint{CodecID: id, Version: version, Data: append([]byte(nil), data...)}
	if err := checkpoint.Validate(); err != nil {
		return statepkg.GraphStateCheckpoint{}, err
	}
	return checkpoint, nil
}

func bindGraphStateCheckpoint(ctx context.Context, checkpoint statepkg.GraphStateCheckpoint) context.Context {
	return statepkg.WithGraphStateCheckpoint(ctx, checkpoint)
}
