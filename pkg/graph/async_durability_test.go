package graph_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/state"
)

// gatedStore 在放行前阻塞所有进度写入（running），用于观察调度循环是否等待存储。
type gatedStore struct {
	*state.MemoryStore
	gate    chan struct{}
	blocked atomic.Int32
}

func newGatedStore() *gatedStore {
	return &gatedStore{MemoryStore: state.NewMemoryStore(), gate: make(chan struct{})}
}

func (s *gatedStore) Save(ctx context.Context, c state.Checkpoint) (state.Checkpoint, error) {
	if c.Status == state.StatusRunning {
		s.blocked.Add(1)
		<-s.gate
	}
	return s.MemoryStore.Save(ctx, c)
}

// newSignalGraph 构建 START -> a -> b -> END，b 开始执行时发出信号。
func newSignalGraph(t *testing.T, bStarted chan<- struct{}) *graph.CompiledGraph[string, string] {
	t.Helper()
	g := graph.NewGraph[string, string]("signal")
	if err := g.SetStateFactory(func(_ context.Context, input string) (*checkpointTestState, error) {
		return &checkpointTestState{Value: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "b"} {
		n := name
		if err := g.AddNode(n, runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
			if n == "b" {
				bStarted <- struct{}{}
			}
			return input + n, nil
		}),
			graph.WithPreHandler(func(_ context.Context, s *checkpointTestState) (string, error) { return s.Value, nil }),
			graph.WithPostHandler(func(_ context.Context, s *checkpointTestState, out string) error { s.Value = out; return nil }),
		); err != nil {
			t.Fatal(err)
		}
	}
	for _, e := range [][2]string{{graph.Start, "a"}, {"a", "b"}, {"b", graph.End}} {
		if err := g.AddEdge(e[0], e[1]); err != nil {
			t.Fatal(err)
		}
	}
	return mustCompile(t, g)
}

// 默认 async：进度写入阻塞时下一节点照常执行；运行返回前所有写入已完成。
func TestAsyncDurabilityOverlapsNodesWithCheckpointWrites(t *testing.T) {
	for _, mode := range execModes {
		t.Run(string(mode), func(t *testing.T) {
			store := newGatedStore()
			bStarted := make(chan struct{}, 1)
			compiled := newSignalGraph(t, bStarted)
			done := make(chan error, 1)
			go func() {
				_, err := runGraph(context.Background(), compiled, mode, "x", graph.WithCheckpoint(store, "async"))
				done <- err
			}()
			select {
			case <-bStarted:
			case <-time.After(2 * time.Second):
				t.Fatal("node b waited for a blocked checkpoint write")
			}
			select {
			case err := <-done:
				t.Fatalf("run returned before pending writes finished: %v", err)
			default:
			}
			close(store.gate)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			latest, found, err := store.Load(context.Background(), "async")
			if err != nil || !found || latest.Status != state.StatusCompleted {
				t.Fatalf("latest status=%q found=%t err=%v", latest.Status, found, err)
			}
			history, err := store.List(context.Background(), "async", state.ListOptions{})
			if err != nil || len(history) != 3 {
				t.Fatalf("history=%d err=%v", len(history), err)
			}
		})
	}
}

// sync 模式：进度写入成功后才开始下一节点。
func TestSyncDurabilityWaitsForCheckpointWrites(t *testing.T) {
	store := newGatedStore()
	bStarted := make(chan struct{}, 1)
	compiled := newSignalGraph(t, bStarted)
	done := make(chan error, 1)
	go func() {
		_, err := compiled.Invoke(context.Background(), "x", graph.WithCheckpoint(store, "sync"), graph.WithDurability(graph.DurabilitySync))
		done <- err
	}()
	select {
	case <-bStarted:
		t.Fatal("node b started before its predecessor checkpoint was written")
	case <-time.After(100 * time.Millisecond):
	}
	close(store.gate)
	<-bStarted
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestDurabilityOptionRejectsUnknownMode(t *testing.T) {
	compiled := newSignalGraph(t, make(chan struct{}, 1))
	_, err := compiled.Invoke(context.Background(), "x", graph.WithDurability("eventually"))
	if err == nil || !strings.Contains(err.Error(), "durability") {
		t.Fatalf("err=%v", err)
	}
}

// async 模式下后台写入失败会在下一个节点边界终止运行，之后的写入不再执行。
func TestAsyncWriteFailureStopsAtNextBoundary(t *testing.T) {
	failure := errors.New("write failed")
	store := &byteCheckpointStore{saveErr: failure, failAfter: 1}
	var first, post atomic.Int32
	compiled := mustCompile(t, newInterruptibleGraph(t, nil, &first, &post))
	_, err := compiled.Invoke(context.Background(), "input", graph.WithCheckpoint(store, "async-failure"))
	if !errors.Is(err, failure) || errors.Is(err, graph.ErrInterrupted) {
		t.Fatalf("err=%v", err)
	}
	history, err := store.List(context.Background(), "async-failure", state.ListOptions{})
	if err != nil || len(history) != 1 || history[0].Status == state.StatusCompleted {
		t.Fatalf("history=%d err=%v", len(history), err)
	}
	store.mu.Lock()
	store.saveErr = nil
	store.mu.Unlock()
	out, err := compiled.Resume(context.Background(), graph.WithCheckpoint(store, "async-failure"))
	if err != nil || !strings.HasPrefix(out, "input-first-second-") {
		t.Fatalf("resume=%q err=%v", out, err)
	}
}

// async 提交边界必须观测已失败的写入：首个进度写落盘失败后，后续提交
// 的票据已携带错误，在节点边界返回 CheckpointError，更后的节点不执行。
func TestAsyncWriteFailureObservedAtNextBoundary(t *testing.T) {
	var secondCalls atomic.Int32
	g := graph.NewGraph[string, string]("async-boundary")
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(g.SetStateFactory(func(_ context.Context, input string) (*checkpointTestState, error) {
		return &checkpointTestState{Value: input}, nil
	}))
	must(g.AddNode("first", runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
		return input + "-first", nil
	}), graph.WithPreHandler(func(_ context.Context, s *checkpointTestState) (string, error) { return s.Value, nil }),
		graph.WithPostHandler(func(_ context.Context, s *checkpointTestState, out string) error { s.Value = out; return nil })))
	must(g.AddNode("ask", runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		answer, err := graph.Interrupt(ctx, "approve")
		if err != nil {
			return "", err
		}
		return input + "-" + fmt.Sprint(answer), nil
	}), graph.WithPreHandler(func(_ context.Context, s *checkpointTestState) (string, error) { return s.Value, nil })))
	must(g.AddNode("second", runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
		secondCalls.Add(1)
		return input + "-second", nil
	}), graph.WithPreHandler(func(_ context.Context, s *checkpointTestState) (string, error) { return s.Value, nil })))
	must(g.AddEdge(graph.Start, "first"))
	must(g.AddEdge("first", "ask"))
	must(g.AddEdge("ask", "second"))
	must(g.AddEdge("second", graph.End))
	compiled := mustCompile(t, g)

	// 所有保存都失败：first 的进度写入在后台失败，ask 的挂起写入同步等待
	// 时必然拿到 writer 的失败态——无论 first 的提交是否抢在失败之前放行。
	store := &byteCheckpointStore{saveErr: errors.New("store down")}
	_, err := compiled.Invoke(context.Background(), "in", graph.WithCheckpoint(store, "async-boundary"), graph.WithDurability(graph.DurabilityAsync))
	var checkpointErr *graph.CheckpointError
	if !errors.As(err, &checkpointErr) {
		t.Fatalf("err=%v, want CheckpointError", err)
	}
	if secondCalls.Load() != 0 {
		t.Fatalf("node after failed boundary executed %d times", secondCalls.Load())
	}
}

// countingCodec 统计 State 编码次数。
type countingCodec struct{ encodes atomic.Int32 }

func (c *countingCodec) codec(t *testing.T) graph.StateCodec {
	t.Helper()
	codec, err := graph.NewStateCodec[*checkpointTestState]("counting", 1,
		func(_ context.Context, s *checkpointTestState) ([]byte, error) {
			c.encodes.Add(1)
			return json.Marshal(s)
		},
		func(_ context.Context, data []byte) (*checkpointTestState, error) {
			var s checkpointTestState
			return &s, json.Unmarshal(data, &s)
		})
	if err != nil {
		t.Fatal(err)
	}
	return codec
}

// 节点入口快照复用上一次进度快照：每个节点只编码一次 State，外加初始快照。
func TestCheckpointEncodesStateOncePerNode(t *testing.T) {
	var counter countingCodec
	var first, post atomic.Int32
	g := newInterruptibleGraph(t, nil, &first, &post)
	if err := g.SetStateCodec(counter.codec(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := mustCompile(t, g).Invoke(context.Background(), "input", graph.WithCheckpoint(state.NewMemoryStore(), "encodes")); err != nil {
		t.Fatal(err)
	}
	if got := counter.encodes.Load(); got != 3 {
		t.Fatalf("state encodes=%d, want 3 (initial + one per node)", got)
	}
}
