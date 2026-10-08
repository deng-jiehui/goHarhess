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

func TestParallelInterruptSkipsMergeAndResumeReplaysWholeNode(t *testing.T) {
	store := &byteCheckpointStore{}
	var interrupt atomic.Bool
	interrupt.Store(true)
	var firstRuns atomic.Int32
	var secondRuns atomic.Int32
	var postCalls atomic.Int32
	var mergeCalls atomic.Int32
	secondStarted := make(chan struct{})

	g := graph.NewGraph[string, string]("parallel-interrupt")
	if err := g.SetStateFactory(func(_ context.Context, input string) (*parallelCheckpointState, error) {
		return &parallelCheckpointState{Values: []string{input}}, nil
	}); err != nil {
		t.Fatal(err)
	}
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
	branch := func(name string, node runnable.Func[string, string]) graph.ParallelBranch {
		return graph.ParallelBranch{
			Name: name,
			Node: node,
			Pre: func(_ context.Context, state *parallelCheckpointState) (string, error) {
				return state.Values[0], nil
			},
			Post: func(_ context.Context, state *parallelCheckpointState, output string) error {
				postCalls.Add(1)
				state.Values = append(state.Values, output)
				return nil
			},
		}
	}
	if err := g.AddParallelNode("fanout", []graph.ParallelBranch{branch("first", first), branch("second", second)},
		graph.WithStateMerger(func(_ context.Context, base *parallelCheckpointState, branches []*parallelCheckpointState) error {
			mergeCalls.Add(1)
			base.Values = []string{base.Values[0], branches[0].Values[1], branches[1].Values[1]}
			return nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	finish := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
		return input, nil
	})
	if err := g.AddNode("finish", finish,
		graph.WithPreHandler(func(_ context.Context, state *parallelCheckpointState) (string, error) {
			return state.Values[1] + "+" + state.Values[2], nil
		}),
		graph.WithPostHandler(func(_ context.Context, _ *parallelCheckpointState, _ string) error { return nil }),
	); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{{graph.Start, "fanout"}, {"fanout", "finish"}, {"finish", graph.End}} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
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
	// 展开拓扑中挂起现场落在成员任务上：续点是「组名/分支名」身份。
	if !errors.As(err, &interrupted) || inspectCheckpoint(t, interrupted.Checkpoint).ResumeNode != "fanout/first" {
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
