package graph_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/state"
)

func TestOrdinaryFailureResumesLastCommitAndCompletedResult(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		t.Run(map[bool]string{false: "invoke", true: "stream"}[streaming], func(t *testing.T) {
			ctx := context.Background()
			store := state.NewMemoryStore()
			first, second := 0, 0
			g := graph.NewGraph[string, string]("durable")
			if err := g.SetStateFactory(func(_ context.Context, input string) (*checkpointTestState, error) {
				return &checkpointTestState{Value: input}, nil
			}); err != nil {
				t.Fatal(err)
			}
			boom := errors.New("ordinary failure")
			for _, name := range []string{"a", "b"} {
				n := name
				err := g.AddNode(n, runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
					if n == "a" {
						first++
					} else {
						second++
						if second == 1 {
							return "", boom
						}
					}
					return input + n, nil
				}), graph.WithPreHandler(func(_ context.Context, s *checkpointTestState) (string, error) { return s.Value, nil }), graph.WithPostHandler(func(_ context.Context, s *checkpointTestState, out string) error { s.Value = out; return nil }))
				if err != nil {
					t.Fatal(err)
				}
			}
			for _, e := range [][2]string{{graph.Start, "a"}, {"a", "b"}, {"b", graph.End}} {
				if err := g.AddEdge(e[0], e[1]); err != nil {
					t.Fatal(err)
				}
			}
			c := mustCompile(t, g)
			option := graph.WithCheckpoint(store, "thread")
			var err error
			if streaming {
				var r *runnable.StreamReader[string]
				r, err = c.Stream(ctx, "x", option)
				if err == nil {
					_, err = r.Collect(ctx)
				}
			} else {
				_, err = c.Invoke(ctx, "x", option)
			}
			if !errors.Is(err, boom) {
				t.Fatalf("failure=%v", err)
			}
			out, err := c.Resume(ctx, option)
			if err != nil || out != "xab" || first != 1 || second != 2 {
				t.Fatalf("resume=%q %v calls=%d/%d", out, err, first, second)
			}
			out, err = c.Resume(ctx, option)
			if err != nil || out != "xab" || first != 1 || second != 2 {
				t.Fatalf("completed replay=%q %v calls=%d/%d", out, err, first, second)
			}
			records, err := store.List(ctx, "thread", state.ListOptions{})
			if err != nil || len(records) != 3 || records[0].Status != state.StatusCompleted {
				t.Fatalf("history=%+v %v", records, err)
			}
		})
	}
}

// sync 模式下写入失败时不会推进到下一节点；async 语义见 TestAsyncWriteFailureStopsAtNextBoundary。
func TestCheckpointWriteFailureStopsAdvancement(t *testing.T) {
	for _, failAfter := range []int{0, 1, 2} {
		t.Run(fmt.Sprint(failAfter), func(t *testing.T) {
			ctx := context.Background()
			failure := errors.New("write failed")
			store := &byteCheckpointStore{saveErr: failure, failAfter: failAfter}
			var first, post atomic.Int32
			c := mustCompile(t, newInterruptibleGraph(t, nil, &first, &post))
			option := graph.WithCheckpoint(store, "write-failure")
			_, err := c.Invoke(ctx, "input", option, graph.WithDurability(graph.DurabilitySync))
			if !errors.Is(err, failure) || errors.Is(err, graph.ErrInterrupted) {
				t.Fatalf("failure=%v", err)
			}
			history, err := store.List(ctx, "write-failure", state.ListOptions{})
			if err != nil || len(history) != failAfter {
				t.Fatalf("saved history length=%d err=%v", len(history), err)
			}
			if failAfter == 0 {
				if first.Load() != 0 {
					t.Fatal("node ran before initial save")
				}
				return
			}
			if history[0].Status == state.StatusCompleted || len(history[0].Result) > 0 {
				t.Fatal("failed write exposed completion")
			}
			store.mu.Lock()
			store.saveErr = nil
			store.mu.Unlock()
			out, err := c.Resume(ctx, option)
			want := "input-first-second-1"
			wantCalls := int32(1)
			if failAfter == 1 {
				want = "input-first-second-2"
				wantCalls = 2
			}
			if err != nil || out != want || first.Load() != wantCalls {
				t.Fatalf("resume=%q %v calls=%d", out, err, first.Load())
			}
		})
	}
}
