package graph_test

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
)

type strictState struct {
	Text    string
	Payload any
}

type privateState struct {
	Text   string
	hidden string
}

type ignoredState struct {
	Text  string
	Cache string `json:"-"`
}

type unsupportedState struct {
	Value complex64
}

type metadataCountingCodec struct {
	metadataCalls *atomic.Int32
}

func (c metadataCountingCodec) ID() string   { c.metadataCalls.Add(1); return "metadata-counting" }
func (c metadataCountingCodec) Version() int { c.metadataCalls.Add(1); return 1 }
func (c metadataCountingCodec) StateType() reflect.Type {
	c.metadataCalls.Add(1)
	return reflect.TypeFor[*strictState]()
}
func (metadataCountingCodec) Encode(_ context.Context, current any) ([]byte, error) {
	return json.Marshal(current)
}
func (metadataCountingCodec) Decode(_ context.Context, data []byte) (any, error) {
	var current strictState
	return &current, json.Unmarshal(data, &current)
}

func TestDefaultStateCodecRejectsPrivateFields(t *testing.T) {
	g := graph.NewGraph[string, string]("private-state")
	if err := g.SetStateFactory(func(_ context.Context, input string) (*privateState, error) {
		return &privateState{Text: input, hidden: "secret"}, nil
	}); err != nil {
		t.Fatal(err)
	}
	node := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) { return input, nil })
	if err := g.AddNode("echo", node, graph.WithPreHandler(func(_ context.Context, state *privateState) (string, error) {
		return state.Text, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "echo"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("echo", graph.End); err != nil {
		t.Fatal(err)
	}
	_, err := g.Compile()
	if err == nil || !strings.Contains(err.Error(), "unexported") {
		t.Fatalf("Compile error = %v", err)
	}
}

func TestDefaultStateCodecRejectsIgnoredFields(t *testing.T) {
	g := graph.NewGraph[string, string]("ignored-state")
	if err := g.SetStateFactory(func(_ context.Context, input string) (*ignoredState, error) {
		return &ignoredState{Text: input, Cache: "cached"}, nil
	}); err != nil {
		t.Fatal(err)
	}
	node := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) { return input, nil })
	if err := g.AddNode("echo", node, graph.WithPreHandler(func(_ context.Context, state *ignoredState) (string, error) {
		return state.Text, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "echo"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("echo", graph.End); err != nil {
		t.Fatal(err)
	}
	_, err := g.Compile()
	if err == nil || !strings.Contains(err.Error(), `json:"-"`) {
		t.Fatalf("Compile error = %v", err)
	}
}

func TestDefaultStateCodecRejectsUnsupportedJSONTypes(t *testing.T) {
	g := graph.NewGraph[string, string]("unsupported-state")
	if err := g.SetStateFactory(func(_ context.Context, _ string) (*unsupportedState, error) {
		return &unsupportedState{Value: 1 + 2i}, nil
	}); err != nil {
		t.Fatal(err)
	}
	node := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) { return input, nil })
	if err := g.AddNode("echo", node, graph.WithPreHandler(func(_ context.Context, _ *unsupportedState) (string, error) { return "ok", nil })); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "echo"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("echo", graph.End); err != nil {
		t.Fatal(err)
	}
	_, err := g.Compile()
	if err == nil || !strings.Contains(err.Error(), "complex64") {
		t.Fatalf("Compile error = %v", err)
	}
}

func TestDefaultStateCodecRejectsDynamicTypeLoss(t *testing.T) {
	g := graph.NewGraph[string, string]("dynamic-state")
	if err := g.SetStateFactory(func(_ context.Context, input string) (*strictState, error) {
		return &strictState{Text: input, Payload: int(1)}, nil
	}); err != nil {
		t.Fatal(err)
	}
	checkpoint := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		_, _, err := graph.StateCheckpointFromContext(ctx)
		return input, err
	})
	if err := g.AddNode("checkpoint", checkpoint, graph.WithPreHandler(func(_ context.Context, state *strictState) (string, error) {
		return state.Text, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "checkpoint"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("checkpoint", graph.End); err != nil {
		t.Fatal(err)
	}
	_, err := mustCompile(t, g).Invoke(context.Background(), "hello")
	if err == nil || !strings.Contains(err.Error(), "dynamic type") || !strings.Contains(err.Error(), "custom State Codec") {
		t.Fatalf("Invoke error = %v", err)
	}
}

func TestSequentialGraphDoesNotSnapshotState(t *testing.T) {
	var encodes atomic.Int32
	var decodes atomic.Int32
	codec, err := graph.NewStateCodec[*strictState](
		"strict-state",
		1,
		func(_ context.Context, state *strictState) ([]byte, error) {
			encodes.Add(1)
			return json.Marshal(state)
		},
		func(_ context.Context, data []byte) (*strictState, error) {
			decodes.Add(1)
			var state strictState
			return &state, json.Unmarshal(data, &state)
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	g := graph.NewGraph[string, string]("no-sequential-snapshot")
	if err := g.SetStateFactory(func(_ context.Context, input string) (*strictState, error) {
		return &strictState{Text: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.SetStateCodec(codec); err != nil {
		t.Fatal(err)
	}
	first := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
		return input + ":first", nil
	})
	second := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
		return input + ":second", nil
	})
	if err := g.AddNode("first", first, graph.WithPreHandler(func(_ context.Context, state *strictState) (string, error) {
		return state.Text, nil
	}), graph.WithPostHandler(func(_ context.Context, state *strictState, output string) error {
		state.Text = output
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("second", second, graph.WithPreHandler(func(_ context.Context, state *strictState) (string, error) {
		return state.Text, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "first"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("first", "second"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("second", graph.End); err != nil {
		t.Fatal(err)
	}
	output, err := mustCompile(t, g).Invoke(context.Background(), "hello")
	if err != nil {
		t.Fatal(err)
	}
	if output != "hello:first:second" {
		t.Fatalf("output = %q", output)
	}
	if encodes.Load() != 0 || decodes.Load() != 0 {
		t.Fatalf("codec calls = encode:%d decode:%d", encodes.Load(), decodes.Load())
	}
}

func TestLazyCheckpointWithCustomCodecEncodesWithoutDecoding(t *testing.T) {
	var encodes atomic.Int32
	var decodes atomic.Int32
	codec, err := graph.NewStateCodec[*strictState](
		"lazy-checkpoint", 1,
		func(_ context.Context, current *strictState) ([]byte, error) {
			encodes.Add(1)
			return json.Marshal(current)
		},
		func(_ context.Context, data []byte) (*strictState, error) {
			decodes.Add(1)
			var current strictState
			return &current, json.Unmarshal(data, &current)
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	g := graph.NewGraph[string, string]("lazy-checkpoint")
	if err := g.SetStateFactory(func(_ context.Context, input string) (*strictState, error) { return &strictState{Text: input}, nil }); err != nil {
		t.Fatal(err)
	}
	if err := g.SetStateCodec(codec); err != nil {
		t.Fatal(err)
	}
	node := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		_, ok, err := graph.StateCheckpointFromContext(ctx)
		if !ok {
			t.Fatal("missing lazy State checkpoint provider")
		}
		return input, err
	})
	if err := g.AddNode("node", node, graph.WithPreHandler(func(_ context.Context, current *strictState) (string, error) { return current.Text, nil })); err != nil {
		t.Fatal(err)
	}
	_ = g.AddEdge(graph.Start, "node")
	_ = g.AddEdge("node", graph.End)
	if _, err := mustCompile(t, g).Invoke(context.Background(), "value"); err != nil {
		t.Fatal(err)
	}
	if encodes.Load() != 1 || decodes.Load() != 0 {
		t.Fatalf("codec calls = encode:%d decode:%d", encodes.Load(), decodes.Load())
	}
}

func TestCompileDoesNotCallStateCodecMetadataUnderGraphLock(t *testing.T) {
	var calls atomic.Int32
	g := graph.NewGraph[string, string]("codec-metadata")
	if err := g.SetStateFactory(func(_ context.Context, input string) (*strictState, error) { return &strictState{Text: input}, nil }); err != nil {
		t.Fatal(err)
	}
	if err := g.SetStateCodec(metadataCountingCodec{metadataCalls: &calls}); err != nil {
		t.Fatal(err)
	}
	afterSet := calls.Load()
	node := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) { return input, nil })
	if err := g.AddNode("node", node, graph.WithPreHandler(func(_ context.Context, current *strictState) (string, error) { return current.Text, nil })); err != nil {
		t.Fatal(err)
	}
	_ = g.AddEdge(graph.Start, "node")
	_ = g.AddEdge("node", graph.End)
	if _, err := g.Compile(); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != afterSet {
		t.Fatalf("Codec metadata calls after SetStateCodec = %d, after Compile = %d", afterSet, calls.Load())
	}
}
