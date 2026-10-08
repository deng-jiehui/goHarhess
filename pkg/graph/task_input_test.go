package graph_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
)

type taskInputState struct {
	Seen []string
}

// 无 Pre 的顺序节点在提交后暂停，恢复输入来自调度时冻结的任务输入。
func TestPlainNodeResumesWithPersistedInput(t *testing.T) {
	g := graph.NewGraph[string, string]("plain-resume")
	if err := graph.SetStateFactory(g, func(_ context.Context, input string) (*taskInputState, error) {
		return &taskInputState{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	pause := runnable.InvokableLambda(func(_ context.Context, input string) (string, error) {
		return input + "-a", nil
	})
	var received string
	plain := runnable.InvokableLambda(func(_ context.Context, input string) (string, error) {
		received = input
		return input + "-b", nil
	})
	if err := g.AddNode("a", pause, graph.WithPostHandler(func(_ context.Context, s *taskInputState, out string) error {
		s.Seen = append(s.Seen, out)
		cancel(control.RequestInterrupt("pause after a", nil))
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	// b 没有 PreHandler：输入只来自 a 的输出边。
	if err := g.AddNode("b", plain); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{{graph.Start, "a"}, {"a", "b"}, {"b", graph.End}} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	compiled := mustCompileGraph(t, g)
	checkpoint := graph.WithCheckpoint(&byteCheckpointStore{}, "plain")
	_, err := compiled.Invoke(ctx, "in", checkpoint)
	progress := interruptedProgress(t, err)
	if progress.Point != graph.InterruptPointAfterCommit || progress.ResumeNode != "b" {
		t.Fatalf("progress=%+v", progress)
	}
	if len(progress.Schedule.Tasks) != 1 || progress.Schedule.Tasks[0].Node != "b" {
		t.Fatalf("tasks=%+v", progress.Schedule.Tasks)
	}
	if string(progress.Schedule.Tasks[0].Input) != `"in-a"` {
		t.Fatalf("persisted input=%s", progress.Schedule.Tasks[0].Input)
	}
	if received != "" {
		t.Fatalf("b must not run before resume, received=%q", received)
	}
	out, err := compiled.Resume(context.Background(), checkpoint)
	if err != nil || out != "in-a-b" || received != "in-a" {
		t.Fatalf("output=%q received=%q err=%v", out, received, err)
	}
}

// 无 Pre 节点执行中被外部暂停，恢复时重放同一冻结输入。
func TestPlainNodeReplaysPersistedInputAfterDuringPause(t *testing.T) {
	g := graph.NewGraph[string, string]("plain-during")
	if err := graph.SetStateFactory(g, func(_ context.Context, input string) (*taskInputState, error) {
		return &taskInputState{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("a", runnable.InvokableLambda(func(_ context.Context, input string) (string, error) {
		return input + "-a", nil
	})); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	started := make(chan string, 1)
	if err := g.AddNode("b", runnable.InvokableLambda(func(ctx context.Context, input string) (string, error) {
		if calls.Add(1) == 1 {
			started <- input
			<-ctx.Done()
			return "", ctx.Err()
		}
		return input + "-b", nil
	})); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{{graph.Start, "a"}, {"a", "b"}, {"b", graph.End}} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	compiled := mustCompileGraph(t, g)
	store := &byteCheckpointStore{}
	ctx, cancel := context.WithCancelCause(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := compiled.Invoke(ctx, "in", graph.WithCheckpoint(store, "plain-during"))
		done <- err
	}()
	select {
	case got := <-started:
		if got != "in-a" {
			t.Fatalf("first call input=%q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("b did not start")
	}
	cancel(control.RequestInterrupt("pause", nil))
	progress := interruptedProgress(t, <-done)
	if progress.Point != graph.InterruptPointDuringNode || progress.ResumeNode != "b" {
		t.Fatalf("progress=%+v", progress)
	}
	out, err := compiled.Resume(context.Background(), graph.WithCheckpoint(store, "plain-during"))
	if err != nil || out != "in-a-b" || calls.Load() != 2 {
		t.Fatalf("output=%q calls=%d err=%v", out, calls.Load(), err)
	}
}

// 接口与不可编码输入类型让图在编译期失去可恢复性；不配置检查点时照常运行。
func TestUnencodableTaskInputIsNotResumable(t *testing.T) {
	newGraph := func(name string, aOut func(context.Context, any) (any, error), bIn func(context.Context, any) (string, error)) *graph.Graph[any, string] {
		g := graph.NewGraph[any, string](name)
		if err := graph.SetStateFactory(g, func(_ context.Context, input any) (*taskInputState, error) {
			return &taskInputState{}, nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := g.AddNode("a", runnable.InvokableLambda(aOut)); err != nil {
			t.Fatal(err)
		}
		if err := g.AddNode("b", runnable.InvokableLambda(bIn)); err != nil {
			t.Fatal(err)
		}
		_ = g.AddEdge(graph.Start, "a")
		_ = g.AddEdge("a", "b")
		_ = g.AddEdge("b", graph.End)
		return g
	}
	// any 输入：无法保留具体类型。
	g := newGraph("any-input",
		func(_ context.Context, input any) (any, error) { return input, nil },
		func(_ context.Context, input any) (string, error) { return "ok", nil })
	compiled := mustCompileGraph(t, g)
	_, err := compiled.Invoke(context.Background(), "x", graph.WithCheckpoint(&byteCheckpointStore{}, "k1"))
	if !errors.Is(err, graph.ErrGraphNotResumable) {
		t.Fatalf("expected ErrGraphNotResumable, got %v", err)
	}
	if out, err := compiled.Invoke(context.Background(), "x"); err != nil || out != "ok" {
		t.Fatalf("uncheckpointed output=%q err=%v", out, err)
	}

	// chan 输入：资源类型不可持久化。
	g2 := graph.NewGraph[string, string]("chan-input")
	if err := graph.SetStateFactory(g2, func(_ context.Context, input string) (*taskInputState, error) {
		return &taskInputState{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := g2.AddNode("a", runnable.InvokableLambda(func(_ context.Context, input string) (chan int, error) {
		return make(chan int), nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := g2.AddNode("b", runnable.InvokableLambda(func(_ context.Context, input chan int) (string, error) {
		return "ok", nil
	})); err != nil {
		t.Fatal(err)
	}
	_ = g2.AddEdge(graph.Start, "a")
	_ = g2.AddEdge("a", "b")
	_ = g2.AddEdge("b", graph.End)
	compiled2 := mustCompileGraph(t, g2)
	_, err = compiled2.Invoke(context.Background(), "x", graph.WithCheckpoint(&byteCheckpointStore{}, "k2"))
	if !errors.Is(err, graph.ErrGraphNotResumable) {
		t.Fatalf("expected ErrGraphNotResumable, got %v", err)
	}
}
