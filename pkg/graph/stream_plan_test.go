package graph_test

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"goharness/pkg/callback"
	"goharness/pkg/graph"
	"goharness/pkg/runnable"
)

type streamSource struct {
	invokes atomic.Int32
}

func (s *streamSource) Invoke(_ context.Context, input int, _ ...runnable.Option) (int, error) {
	s.invokes.Add(1)
	return input, nil
}

func (s *streamSource) Stream(ctx context.Context, input int, _ ...runnable.Option) (*runnable.StreamReader[int], error) {
	reader, writer := runnable.Pipe[int](ctx)
	go func() {
		_ = writer.Send(input)
		_ = writer.Send(input + 1)
		_ = writer.Send(input + 2)
		_ = writer.Close()
	}()
	return reader, nil
}

type streamTransform struct {
	invokes    atomic.Int32
	transforms atomic.Int32
	items      atomic.Int32
}

func (s *streamTransform) Invoke(_ context.Context, input int, _ ...runnable.Option) (int, error) {
	s.invokes.Add(1)
	return input * 10, nil
}

func (s *streamTransform) Transform(ctx context.Context, input *runnable.StreamReader[int], _ ...runnable.Option) (*runnable.StreamReader[int], error) {
	s.transforms.Add(1)
	reader, writer := runnable.Pipe[int](ctx)
	go func() {
		for {
			value, err := input.Recv()
			if err == io.EOF {
				_ = writer.Close()
				return
			}
			if err != nil {
				_ = writer.CloseWithError(err)
				return
			}
			s.items.Add(1)
			if err := writer.Send(value * 10); err != nil {
				return
			}
		}
	}()
	return reader, nil
}

func TestCompiledGraphStreamUsesDirectTransformPipeline(t *testing.T) {
	source := &streamSource{}
	transform := &streamTransform{}
	g := graph.NewGraph[int, int]("stream-pipeline")
	if err := g.AddNode("source", source); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("transform", transform); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "source"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("source", "transform"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("transform", graph.End); err != nil {
		t.Fatal(err)
	}
	compiled, err := g.Compile()
	if err != nil {
		t.Fatal(err)
	}

	output, err := compiled.Stream(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	value, err := output.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if value != 30 {
		t.Fatalf("Stream value = %d, want 30", value)
	}
	if _, err := output.Recv(); err != io.EOF {
		t.Fatalf("Stream end = %v, want EOF", err)
	}
	if source.invokes.Load() != 0 || transform.invokes.Load() != 0 {
		t.Fatalf("Invoke calls = source %d, transform %d; want zero", source.invokes.Load(), transform.invokes.Load())
	}
	if transform.transforms.Load() != 1 || transform.items.Load() != 3 {
		t.Fatalf("Transform calls/items = %d/%d, want 1/3", transform.transforms.Load(), transform.items.Load())
	}
}

func TestCompiledGraphStreamCollectsBeforeBranch(t *testing.T) {
	source := &streamSource{}
	var branchInput atomic.Int32
	g := graph.NewGraph[int, int]("stream-branch")
	if err := g.AddNode("source", source); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("selected", runnable.Func[int, int](func(_ context.Context, input int, _ ...runnable.Option) (int, error) {
		return input + 10, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("fallback", runnable.Func[int, int](func(_ context.Context, input int, _ ...runnable.Option) (int, error) {
		return input - 10, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "source"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddBranch("source", "selected", "last_is_three", func(_ context.Context, input int) (bool, error) {
		branchInput.Store(int32(input))
		return input == 3, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("source", "fallback"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("selected", graph.End); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("fallback", graph.End); err != nil {
		t.Fatal(err)
	}
	compiled, err := g.Compile()
	if err != nil {
		t.Fatal(err)
	}

	output, err := compiled.Stream(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	value, err := output.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if value != 13 || branchInput.Load() != 3 {
		t.Fatalf("value/branch input = %d/%d, want 13/3", value, branchInput.Load())
	}
	if _, err := output.Recv(); err != io.EOF {
		t.Fatalf("Stream end = %v, want EOF", err)
	}
}

func TestCompiledGraphTransformAndCollectUseFinalOutputContract(t *testing.T) {
	g := graph.NewGraph[int, int]("stream-boundary")
	if err := g.AddNode("source", &streamSource{}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "source"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("source", graph.End); err != nil {
		t.Fatal(err)
	}
	compiled, err := g.Compile()
	if err != nil {
		t.Fatal(err)
	}

	input, writer := runnable.Pipe[int](context.Background())
	go func() {
		_ = writer.Send(1)
		_ = writer.Close()
	}()
	transformed, err := compiled.Transform(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	value, err := transformed.Recv()
	if err != nil || value != 3 {
		t.Fatalf("Transform value = %d, %v; want 3", value, err)
	}
	if _, err := transformed.Recv(); err != io.EOF {
		t.Fatalf("Transform end = %v, want EOF", err)
	}

	collectInput, collectWriter := runnable.Pipe[int](context.Background())
	go func() {
		_ = collectWriter.Send(4)
		_ = collectWriter.Close()
	}()
	collected, err := compiled.Collect(context.Background(), collectInput)
	if err != nil || collected != 4 {
		t.Fatalf("Collect = %d, %v; want 4", collected, err)
	}
}

func TestCompiledGraphStreamTreatsStateHandlersAsLocalBarriers(t *testing.T) {
	type runState struct {
		Value int
	}
	source := &streamSource{}
	g := graph.NewGraph[int, int]("stream-state-barrier")
	if err := g.SetStateFactory(func(_ context.Context, input int) (*runState, error) {
		return &runState{Value: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("source", source,
		graph.WithPreHandler(func(_ context.Context, state *runState) (int, error) {
			return state.Value, nil
		}),
		graph.WithPostHandler(func(_ context.Context, state *runState, output int) error {
			state.Value = output
			return nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("finish", runnable.Func[int, int](func(_ context.Context, input int, _ ...runnable.Option) (int, error) {
		return input, nil
	}), graph.WithPreHandler(func(_ context.Context, state *runState) (int, error) {
		return state.Value, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "source"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("source", "finish"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("finish", graph.End); err != nil {
		t.Fatal(err)
	}
	compiled, err := g.Compile()
	if err != nil {
		t.Fatal(err)
	}

	output, err := compiled.Stream(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	value, err := output.Recv()
	if err != nil || value != 3 {
		t.Fatalf("Stream value = %d, %v; want 3", value, err)
	}
	if source.invokes.Load() != 0 {
		t.Fatalf("source Invoke calls = %d, want zero", source.invokes.Load())
	}
}

func TestCompiledGraphStreamCommitsStateBeforeNodeEnd(t *testing.T) {
	type runState struct {
		Value int
	}
	var state *runState
	var observed atomic.Int32
	manager, err := callback.NewManager(callback.Config{}, []callback.Handler{graph.CallbackFuncs{
		CallbackName: "state-order",
		NodeEnd: func(_ context.Context, result graph.NodeResult) error {
			if result.NodeID == "source" && state != nil {
				observed.Store(int32(state.Value))
			}
			return nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	g := graph.NewGraph[int, int]("stream-state-order")
	if err := g.SetStateFactory(func(_ context.Context, input int) (*runState, error) {
		state = &runState{Value: input}
		return state, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("source", &streamSource{},
		graph.WithPreHandler(func(_ context.Context, state *runState) (int, error) { return state.Value, nil }),
		graph.WithPostHandler(func(_ context.Context, state *runState, output int) error {
			state.Value = output
			return nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "source"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("source", graph.End); err != nil {
		t.Fatal(err)
	}
	compiled, err := g.Compile()
	if err != nil {
		t.Fatal(err)
	}

	reader, err := compiled.Stream(callback.WithManager(context.Background(), manager), 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Recv(); err != nil {
		t.Fatal(err)
	}
	if observed.Load() != 3 {
		t.Fatalf("state observed at NodeEnd = %d, want 3", observed.Load())
	}
}

type cancelAwareStreamSource struct {
	canceled chan error
	started  chan struct{}
}

func (s *cancelAwareStreamSource) Invoke(_ context.Context, input int, _ ...runnable.Option) (int, error) {
	return input, nil
}

func (s *cancelAwareStreamSource) Stream(ctx context.Context, _ int, _ ...runnable.Option) (*runnable.StreamReader[int], error) {
	reader, writer := runnable.Pipe[int](ctx)
	close(s.started)
	go func() {
		<-ctx.Done()
		s.canceled <- ctx.Err()
		_ = writer.CloseWithError(ctx.Err())
	}()
	return reader, nil
}

func TestCompiledGraphStreamCloseCancelsUpstream(t *testing.T) {
	source := &cancelAwareStreamSource{canceled: make(chan error, 1), started: make(chan struct{})}
	g := graph.NewGraph[int, int]("stream-close")
	if err := g.AddNode("source", source); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "source"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("source", graph.End); err != nil {
		t.Fatal(err)
	}
	compiled, err := g.Compile()
	if err != nil {
		t.Fatal(err)
	}

	reader, err := compiled.Stream(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-source.started:
	case <-time.After(time.Second):
		t.Fatal("upstream did not start")
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-source.canceled:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("upstream error = %v, want canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("upstream was not canceled")
	}
}

type errorStreamSource struct {
	err error
}

func (s errorStreamSource) Invoke(_ context.Context, input int, _ ...runnable.Option) (int, error) {
	return input, nil
}

func (s errorStreamSource) Stream(ctx context.Context, _ int, _ ...runnable.Option) (*runnable.StreamReader[int], error) {
	reader, writer := runnable.Pipe[int](ctx)
	go func() { _ = writer.CloseWithError(s.err) }()
	return reader, nil
}

func TestCompiledGraphStreamPreservesUpstreamError(t *testing.T) {
	reason := errors.New("stream failed")
	g := graph.NewGraph[int, int]("stream-error")
	if err := g.AddNode("source", errorStreamSource{err: reason}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "source"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("source", graph.End); err != nil {
		t.Fatal(err)
	}
	compiled, err := g.Compile()
	if err != nil {
		t.Fatal(err)
	}
	reader, err := compiled.Stream(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Recv(); !errors.Is(err, reason) {
		t.Fatalf("Stream error = %v, want %v", err, reason)
	}
}
