package graph_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"goharness/pkg/control"
	"goharness/pkg/graph"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

type byteCheckpointStore struct {
	mu      sync.Mutex
	data    map[string][]byte
	loads   int
	saves   int
	saveErr error
}

type streamReceiveResult[T any] struct {
	value T
	err   error
}

func receiveStreamMessage[T any](stream *runnable.StreamReader[T]) <-chan streamReceiveResult[T] {
	result := make(chan streamReceiveResult[T], 1)
	go func() {
		value, err := stream.Recv()
		result <- streamReceiveResult[T]{value: value, err: err}
	}()
	return result
}

func (s *byteCheckpointStore) Save(_ context.Context, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saves++
	if s.saveErr != nil {
		return s.saveErr
	}
	if s.data == nil {
		s.data = map[string][]byte{}
	}
	s.data[key] = append([]byte(nil), data...)
	return nil
}

func (s *byteCheckpointStore) Load(_ context.Context, key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loads++
	data, ok := s.data[key]
	return append([]byte(nil), data...), ok, nil
}

func (s *byteCheckpointStore) counts() (loads, saves int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loads, s.saves
}

type continuationState struct {
	Value string
	Runs  int
}

func newInterruptibleGraph(
	t *testing.T,
	started chan<- struct{},
	firstCalls *atomic.Int32,
	firstPostCalls *atomic.Int32,
) *graph.Graph[string, string] {
	t.Helper()
	g := graph.NewGraph[string, string]("interruptible")
	if err := g.SetStateFactory(func(_ context.Context, input string) (*continuationState, error) {
		return &continuationState{Value: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	first := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		call := firstCalls.Add(1)
		if call == 1 && started != nil {
			started <- struct{}{}
			<-ctx.Done()
			return "discarded", ctx.Err()
		}
		return input + "-first", nil
	})
	if err := g.AddNode("first", first,
		graph.WithPreHandler(func(_ context.Context, state *continuationState) (string, error) {
			return state.Value, nil
		}),
		graph.WithPostHandler(func(_ context.Context, state *continuationState, output string) error {
			firstPostCalls.Add(1)
			state.Value = output
			state.Runs++
			return nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	second := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
		return fmt.Sprintf("%s-second-%d", input, firstCalls.Load()), nil
	})
	if err := g.AddNode("second", second,
		graph.WithPreHandler(func(_ context.Context, state *continuationState) (string, error) {
			return state.Value, nil
		}),
		graph.WithPostHandler(func(_ context.Context, state *continuationState, output string) error {
			state.Value = output
			state.Runs++
			return nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{{graph.Start, "first"}, {"first", "second"}, {"second", graph.End}} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	return g
}

func TestGraphInterruptDuringNodeSkipsCommitAndResumeReplaysNode(t *testing.T) {
	store := &byteCheckpointStore{}
	started := make(chan struct{}, 1)
	var firstCalls atomic.Int32
	var firstPostCalls atomic.Int32
	g := newInterruptibleGraph(t, started, &firstCalls, &firstPostCalls)
	compiled := mustCompile(t, g)

	ctx, cancel := context.WithCancelCause(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := compiled.Invoke(ctx, "input", graph.WithCheckpoint(store, "run-1"))
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first node did not start")
	}
	cancel(control.RequestInterrupt("await approval", map[string]any{"ticket": "A-1"}))

	err := <-done
	if !errors.Is(err, graph.ErrInterrupted) {
		t.Fatalf("Invoke error=%v", err)
	}
	var interrupted *graph.InterruptedError
	if !errors.As(err, &interrupted) || interrupted.Continuation.ResumeNode != "first" {
		t.Fatalf("InterruptedError=%#v", interrupted)
	}
	if firstPostCalls.Load() != 0 {
		t.Fatalf("first PostHandler calls=%d", firstPostCalls.Load())
	}

	output, err := compiled.Resume(context.Background(), graph.WithCheckpoint(store, "run-1"))
	if err != nil {
		t.Fatal(err)
	}
	if output != "input-first-second-2" || firstCalls.Load() != 2 || firstPostCalls.Load() != 1 {
		t.Fatalf("output=%q first calls=%d post calls=%d", output, firstCalls.Load(), firstPostCalls.Load())
	}
	loads, saves := store.counts()
	if loads != 1 || saves != 1 {
		t.Fatalf("store loads=%d saves=%d", loads, saves)
	}
}

func TestGraphStreamDeliversPersistedInterruptionAfterContextCancellation(t *testing.T) {
	store := &byteCheckpointStore{}
	started := make(chan struct{}, 1)
	type streamState struct{ Messages []schema.Message }
	g := graph.NewGraph[[]schema.Message, []schema.Message]("interruptible-message-stream")
	if err := g.SetStateFactory(func(_ context.Context, input []schema.Message) (*streamState, error) {
		return &streamState{Messages: schema.CloneMessages(input)}, nil
	}); err != nil {
		t.Fatal(err)
	}
	node := runnable.Func[[]schema.Message, []schema.Message](func(ctx context.Context, _ []schema.Message, _ ...runnable.Option) ([]schema.Message, error) {
		started <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	})
	if err := g.AddNode("model", node,
		graph.WithPreHandler(func(_ context.Context, state *streamState) ([]schema.Message, error) {
			return schema.CloneMessages(state.Messages), nil
		}),
		graph.WithPostHandler(func(_ context.Context, state *streamState, output []schema.Message) error {
			state.Messages = schema.CloneMessages(output)
			return nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "model"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("model", graph.End); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	stream, err := mustCompile(t, g).Stream(ctx, []schema.Message{schema.UserMessage("input")}, graph.WithCheckpoint(store, "stream-interrupt"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("graph node did not start")
	}
	cancel(control.RequestInterrupt("pause stream", nil))
	select {
	case result := <-receiveStreamMessage(stream):
		if !errors.Is(result.err, graph.ErrInterrupted) {
			t.Fatalf("stream error=%v", result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("stream did not deliver interruption")
	}
}

func TestGraphStreamDeliversCheckpointSaveFailureAfterInterruptRequest(t *testing.T) {
	saveErr := errors.New("stream checkpoint unavailable")
	store := &byteCheckpointStore{saveErr: saveErr}
	started := make(chan struct{}, 1)
	type streamState struct{ Messages []schema.Message }
	g := graph.NewGraph[[]schema.Message, []schema.Message]("failed-message-checkpoint")
	if err := g.SetStateFactory(func(_ context.Context, input []schema.Message) (*streamState, error) {
		return &streamState{Messages: schema.CloneMessages(input)}, nil
	}); err != nil {
		t.Fatal(err)
	}
	node := runnable.Func[[]schema.Message, []schema.Message](func(ctx context.Context, _ []schema.Message, _ ...runnable.Option) ([]schema.Message, error) {
		started <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	})
	if err := g.AddNode("model", node,
		graph.WithPreHandler(func(_ context.Context, state *streamState) ([]schema.Message, error) {
			return schema.CloneMessages(state.Messages), nil
		}),
		graph.WithPostHandler(func(_ context.Context, state *streamState, output []schema.Message) error {
			state.Messages = schema.CloneMessages(output)
			return nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(graph.Start, "model"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("model", graph.End); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	stream, err := mustCompile(t, g).Stream(ctx, []schema.Message{schema.UserMessage("input")}, graph.WithCheckpoint(store, "failed-stream"))
	if err != nil {
		t.Fatal(err)
	}
	<-started
	cancel(control.RequestInterrupt("pause", nil))
	select {
	case result := <-receiveStreamMessage(stream):
		if !errors.Is(result.err, saveErr) || errors.Is(result.err, graph.ErrInterrupted) {
			t.Fatalf("stream error=%v", result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("stream did not deliver checkpoint failure")
	}
}

func TestGraphOrdinaryCancellationDoesNotSaveCheckpoint(t *testing.T) {
	store := &byteCheckpointStore{}
	started := make(chan struct{}, 1)
	var firstCalls atomic.Int32
	var firstPostCalls atomic.Int32
	g := newInterruptibleGraph(t, started, &firstCalls, &firstPostCalls)
	compiled := mustCompile(t, g)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := compiled.Invoke(ctx, "input", graph.WithCheckpoint(store, "run-2"))
		done <- err
	}()
	<-started
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) || errors.Is(err, graph.ErrInterrupted) {
		t.Fatalf("Invoke error=%v", err)
	}
	loads, saves := store.counts()
	if loads != 0 || saves != 0 {
		t.Fatalf("store loads=%d saves=%d", loads, saves)
	}
}

func TestGraphOrdinaryInvokeDoesNotTouchCheckpointStore(t *testing.T) {
	store := &byteCheckpointStore{}
	var firstCalls atomic.Int32
	var firstPostCalls atomic.Int32
	g := newInterruptibleGraph(t, nil, &firstCalls, &firstPostCalls)
	output, err := mustCompile(t, g).Invoke(context.Background(), "input", graph.WithCheckpoint(store, "run-3"))
	if err != nil || output != "input-first-second-1" {
		t.Fatalf("output=%q err=%v", output, err)
	}
	loads, saves := store.counts()
	if loads != 0 || saves != 0 {
		t.Fatalf("store loads=%d saves=%d", loads, saves)
	}
}

func TestGraphCheckpointSaveFailureIsNotInterrupted(t *testing.T) {
	saveErr := errors.New("storage unavailable")
	store := &byteCheckpointStore{saveErr: saveErr}
	var firstCalls atomic.Int32
	var firstPostCalls atomic.Int32
	g := newInterruptibleGraph(t, nil, &firstCalls, &firstPostCalls)
	compiled := mustCompile(t, g)
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(control.RequestInterrupt("pause", nil))

	_, err := compiled.Invoke(ctx, "input", graph.WithCheckpoint(store, "save-failure"))
	if !errors.Is(err, saveErr) || errors.Is(err, graph.ErrInterrupted) {
		t.Fatalf("Invoke error=%v", err)
	}
	if firstCalls.Load() != 0 || firstPostCalls.Load() != 0 {
		t.Fatalf("first calls=%d post calls=%d", firstCalls.Load(), firstPostCalls.Load())
	}
}

func TestGraphResumeRejectsMissingAndMalformedCheckpointsBeforeNodes(t *testing.T) {
	store := &byteCheckpointStore{data: map[string][]byte{"malformed": []byte(`{"schema_version":99}`)}}
	var firstCalls atomic.Int32
	var firstPostCalls atomic.Int32
	g := newInterruptibleGraph(t, nil, &firstCalls, &firstPostCalls)
	compiled := mustCompile(t, g)

	if _, err := compiled.Resume(context.Background(), graph.WithCheckpoint(store, "missing")); !errors.Is(err, graph.ErrCheckpointNotFound) {
		t.Fatalf("missing Resume error=%v", err)
	}
	if _, err := compiled.Resume(context.Background(), graph.WithCheckpoint(store, "malformed")); !errors.Is(err, graph.ErrInvalidContinuation) {
		t.Fatalf("malformed Resume error=%v", err)
	}
	if firstCalls.Load() != 0 || firstPostCalls.Load() != 0 {
		t.Fatalf("first calls=%d post calls=%d", firstCalls.Load(), firstPostCalls.Load())
	}
}

func TestGraphResumeRejectsRevisionMismatchBeforeNodes(t *testing.T) {
	store := &byteCheckpointStore{}
	var firstCalls atomic.Int32
	var firstPostCalls atomic.Int32
	g := newInterruptibleGraph(t, nil, &firstCalls, &firstPostCalls)
	compiled := mustCompile(t, g)
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(control.RequestInterrupt("pause", nil))
	if _, err := compiled.Invoke(ctx, "input", graph.WithCheckpoint(store, "revision")); !errors.Is(err, graph.ErrInterrupted) {
		t.Fatalf("Invoke error=%v", err)
	}
	data, found, err := store.Load(context.Background(), "revision")
	if err != nil || !found {
		t.Fatalf("Load found=%t err=%v", found, err)
	}
	continuation, err := graph.DecodeContinuation(data)
	if err != nil {
		t.Fatal(err)
	}
	continuation.GraphRevision = "different"
	tampered, err := continuation.JSON()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(context.Background(), "revision", tampered); err != nil {
		t.Fatal(err)
	}

	if _, err := compiled.Resume(context.Background(), graph.WithCheckpoint(store, "revision")); !errors.Is(err, graph.ErrGraphRevisionMismatch) {
		t.Fatalf("Resume error=%v", err)
	}
	if firstCalls.Load() != 0 || firstPostCalls.Load() != 0 {
		t.Fatalf("first calls=%d post calls=%d", firstCalls.Load(), firstPostCalls.Load())
	}
}

func TestGraphCheckpointCanBeResumedMoreThanOnce(t *testing.T) {
	store := &byteCheckpointStore{}
	var firstCalls atomic.Int32
	var firstPostCalls atomic.Int32
	g := newInterruptibleGraph(t, nil, &firstCalls, &firstPostCalls)
	compiled := mustCompile(t, g)
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(control.RequestInterrupt("pause", nil))
	if _, err := compiled.Invoke(ctx, "input", graph.WithCheckpoint(store, "repeat")); !errors.Is(err, graph.ErrInterrupted) {
		t.Fatalf("Invoke error=%v", err)
	}

	first, err := compiled.Resume(context.Background(), graph.WithCheckpoint(store, "repeat"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := compiled.Resume(context.Background(), graph.WithCheckpoint(store, "repeat"))
	if err != nil {
		t.Fatal(err)
	}
	if first != "input-first-second-1" || second != "input-first-second-2" {
		t.Fatalf("first=%q second=%q", first, second)
	}
	if firstCalls.Load() != 2 || firstPostCalls.Load() != 2 {
		t.Fatalf("first calls=%d post calls=%d", firstCalls.Load(), firstPostCalls.Load())
	}
}

func TestCompiledGraphResumeStreamReturnsOnlyFinalOutput(t *testing.T) {
	store := &byteCheckpointStore{}
	var firstCalls atomic.Int32
	var firstPostCalls atomic.Int32
	g := newInterruptibleGraph(t, nil, &firstCalls, &firstPostCalls)
	compiled := mustCompile(t, g)
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(control.RequestInterrupt("pause", nil))
	if _, err := compiled.Invoke(ctx, "input", graph.WithCheckpoint(store, "resume-stream")); !errors.Is(err, graph.ErrInterrupted) {
		t.Fatalf("Invoke error=%v", err)
	}

	reader, err := compiled.ResumeStream(context.Background(), graph.WithCheckpoint(store, "resume-stream"))
	if err != nil {
		t.Fatal(err)
	}
	value, err := reader.Recv()
	if err != nil || value != "input-first-second-1" {
		t.Fatalf("ResumeStream value=%q err=%v", value, err)
	}
	if _, err := reader.Recv(); err != io.EOF {
		t.Fatalf("ResumeStream end=%v, want EOF", err)
	}
}
