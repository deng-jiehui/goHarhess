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

type parallelCheckpointState struct {
	Values []string
}

// 并行中断跳过合并、恢复时整节点重放：扇出成员在节点内请求中断时，
// 不提交中间态、不执行合并；恢复后全部成员任务整体重跑。
func TestParallelInterruptSkipsMergeAndResumeReplaysWholeNode(t *testing.T) {
	store := &byteCheckpointStore{}
	var interrupt atomic.Bool
	interrupt.Store(true)
	var firstRuns atomic.Int32
	var secondRuns atomic.Int32
	var postCalls atomic.Int32
	var mergeCalls atomic.Int32
	secondStarted := make(chan struct{})
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	g := graph.NewGraph[string, string]("parallel-interrupt")
	must(g.SetStateFactory(func(_ context.Context, input string) (*parallelCheckpointState, error) {
		return &parallelCheckpointState{Values: []string{input}}, nil
	}))
	first := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
		firstRuns.Add(1)
		if interrupt.Load() {
			// 先确认另一个分支已经执行，再验证中断会取消它。
			<-secondStarted
			return "", control.RequestInterrupt("approve fanout", nil)
		}
		return input + "-first", nil
	})
	second := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		secondRuns.Add(1)
		if interrupt.Load() {
			close(secondStarted)
			<-ctx.Done()
			return "", ctx.Err()
		}
		return input + "-second", nil
	})
	post := func(_ context.Context, state *parallelCheckpointState, output string) error {
		postCalls.Add(1)
		state.Values = append(state.Values, output)
		return nil
	}
	member := func(name string, node runnable.Func[string, string]) {
		must(g.AddNode(name, node,
			graph.WithPreHandler(func(_ context.Context, state *parallelCheckpointState) (string, error) {
				return state.Values[0], nil
			}),
			graph.WithPostHandler(post)))
	}
	member("first", first)
	member("second", second)
	must(g.AddNode("q", runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
		return input, nil
	}), graph.WithPreHandler(func(_ context.Context, state *parallelCheckpointState) (string, error) {
		return state.Values[0], nil
	})))
	finish := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
		return input, nil
	})
	must(g.AddNode("finish", finish,
		graph.WithPreHandler(func(_ context.Context, state *parallelCheckpointState) (string, error) {
			return state.Values[1] + "+" + state.Values[2], nil
		}),
		graph.WithPostHandler(func(_ context.Context, _ *parallelCheckpointState, _ string) error { return nil }),
	))
	must(g.AddEdge(graph.Start, "q"))
	must(g.AddFork("q", "first", "second"))
	must(g.AddJoin("q", "finish"))
	must(g.AddEdge("first", "finish"))
	must(g.AddEdge("second", "finish"))
	must(g.AddEdge("finish", graph.End))
	must(graph.SetForkMerger(g, "q",
		func(_ context.Context, base *parallelCheckpointState, rs []graph.StepResult[*parallelCheckpointState]) error {
			mergeCalls.Add(1)
			base.Values = []string{base.Values[0], rs[0].State.Values[1], rs[1].State.Values[1]}
			return nil
		}))
	compiled := mustCompile(t, g)

	done := make(chan error, 1)
	go func() {
		_, err := compiled.Invoke(context.Background(), "base", graph.WithCheckpoint(store, "parallel"))
		done <- err
	}()
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("second branch did not start")
	}
	err := <-done
	if !errors.Is(err, graph.ErrInterrupted) {
		t.Fatalf("Invoke error=%v", err)
	}
	var interrupted *graph.InterruptedError
	// 扇出组内挂起现场落在成员任务上：续点是成员入口节点。
	if !errors.As(err, &interrupted) || inspectCheckpoint(t, interrupted.Checkpoint).ResumeNode != "first" {
		t.Fatalf("InterruptedError=%#v", interrupted)
	}
	if postCalls.Load() != 0 || mergeCalls.Load() != 0 {
		t.Fatalf("post calls=%d merge calls=%d", postCalls.Load(), mergeCalls.Load())
	}

	interrupt.Store(false)
	output, err := compiled.Resume(context.Background(), graph.WithCheckpoint(store, "parallel"))
	if err != nil {
		t.Fatal(err)
	}
	if output != "base-first+base-second" {
		t.Fatalf("output=%q", output)
	}
	if firstRuns.Load() != 2 || secondRuns.Load() != 2 || postCalls.Load() != 2 || mergeCalls.Load() != 1 {
		t.Fatalf("runs=%d/%d post=%d merge=%d", firstRuns.Load(), secondRuns.Load(), postCalls.Load(), mergeCalls.Load())
	}
}
