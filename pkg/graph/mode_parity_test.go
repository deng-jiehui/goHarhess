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

	"github.com/goharness/goharness/pkg/callback"
	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
)

// 同一张图分别以 Invoke 与 Stream 执行时，控制面语义必须完全一致。
type execMode string

const (
	execInvoke execMode = "invoke"
	execStream execMode = "stream"
)

var execModes = []execMode{execInvoke, execStream}

func runGraph[I, O any](ctx context.Context, compiled *graph.CompiledGraph[I, O], mode execMode, input I, options ...runnable.Option) (O, error) {
	if mode == execInvoke {
		return compiled.Invoke(ctx, input, options...)
	}
	reader, err := compiled.Stream(ctx, input, options...)
	return drainSingle(reader, err)
}

func resumeGraph[I, O any](ctx context.Context, compiled *graph.CompiledGraph[I, O], mode execMode, options ...runnable.Option) (O, error) {
	if mode == execInvoke {
		return compiled.Resume(ctx, options...)
	}
	reader, err := compiled.ResumeStream(ctx, options...)
	return drainSingle(reader, err)
}

func drainSingle[O any](reader *runnable.StreamReader[O], err error) (O, error) {
	var zero O
	if err != nil {
		return zero, err
	}
	defer reader.Close()
	value, err := reader.Recv()
	if err != nil {
		return zero, err
	}
	if _, err := reader.Recv(); !errors.Is(err, io.EOF) {
		return value, fmt.Errorf("stream did not end after the final result: %v", err)
	}
	return value, nil
}

type parityState struct {
	Value string
	Trail []string
}

type parityHooks struct {
	first     func(context.Context, string) (string, error)
	firstPost func(context.Context, *parityState) error
	secondPre func(context.Context, *parityState) error
}

type parityCounters struct {
	first      atomic.Int32
	firstPost  atomic.Int32
	second     atomic.Int32
	secondPost atomic.Int32
}

// newParityGraph 构建可恢复的两节点图：START -> first -> second -> END。
func newParityGraph(t *testing.T, name string, hooks parityHooks, counters *parityCounters) *graph.CompiledGraph[string, string] {
	t.Helper()
	g := graph.NewGraph[string, string](name)
	if err := g.SetStateFactory(func(_ context.Context, input string) (*parityState, error) {
		return &parityState{Value: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	first := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		counters.first.Add(1)
		if hooks.first != nil {
			return hooks.first(ctx, input)
		}
		return input + "-first", nil
	})
	if err := g.AddNode("first", first,
		graph.WithPreHandler(func(_ context.Context, state *parityState) (string, error) { return state.Value, nil }),
		graph.WithPostHandler(func(ctx context.Context, state *parityState, output string) error {
			counters.firstPost.Add(1)
			state.Value = output
			state.Trail = append(state.Trail, "first")
			if hooks.firstPost != nil {
				return hooks.firstPost(ctx, state)
			}
			return nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	second := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
		counters.second.Add(1)
		return input + "-second", nil
	})
	if err := g.AddNode("second", second,
		graph.WithPreHandler(func(ctx context.Context, state *parityState) (string, error) {
			if hooks.secondPre != nil {
				if err := hooks.secondPre(ctx, state); err != nil {
					return "", err
				}
			}
			return state.Value, nil
		}),
		graph.WithPostHandler(func(_ context.Context, state *parityState, output string) error {
			counters.secondPost.Add(1)
			state.Value = output
			state.Trail = append(state.Trail, "second")
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
	return mustCompile(t, g)
}

func interruptedProgress(t *testing.T, err error) checkpointProgress {
	t.Helper()
	var interrupted *graph.InterruptedError
	if !errors.Is(err, graph.ErrInterrupted) || !errors.As(err, &interrupted) {
		t.Fatalf("error=%v, want persisted InterruptedError", err)
	}
	return inspectCheckpoint(t, interrupted.Checkpoint)
}

func executionCode(err error) string {
	var execution *graph.ExecutionError
	if errors.As(err, &execution) {
		return execution.Code
	}
	return ""
}

func TestModeParitySuccessAndLifecycleOrder(t *testing.T) {
	sequences := map[execMode][]string{}
	for _, mode := range execModes {
		var mu sync.Mutex
		var events []string
		record := func(kind, node string) {
			mu.Lock()
			events = append(events, kind+":"+node)
			mu.Unlock()
		}
		manager, err := callback.NewManager(callback.Config{}, []callback.Handler{graph.CallbackFuncs{
			CallbackName: "parity",
			NodeStart:    func(_ context.Context, value graph.NodeExecution) error { record("start", value.NodeID); return nil },
			NodeEnd:      func(_ context.Context, value graph.NodeResult) error { record("end", value.NodeID); return nil },
		}})
		if err != nil {
			t.Fatal(err)
		}
		counters := &parityCounters{}
		compiled := newParityGraph(t, "parity-success", parityHooks{}, counters)
		output, err := runGraph(callback.WithManager(context.Background(), manager), compiled, mode, "in")
		if err != nil || output != "in-first-second" {
			t.Fatalf("%s output=%q err=%v", mode, output, err)
		}
		sequences[mode] = events
	}
	if fmt.Sprint(sequences[execInvoke]) != fmt.Sprint(sequences[execStream]) {
		t.Fatalf("lifecycle order differs: invoke=%v stream=%v", sequences[execInvoke], sequences[execStream])
	}
}

func TestModeParityPreHandlerInterruptPersistsAndResumes(t *testing.T) {
	for _, mode := range execModes {
		t.Run(string(mode), func(t *testing.T) {
			store := &byteCheckpointStore{}
			var interrupt atomic.Bool
			interrupt.Store(true)
			counters := &parityCounters{}
			compiled := newParityGraph(t, "parity-pre", parityHooks{secondPre: func(context.Context, *parityState) error {
				if interrupt.Load() {
					return control.RequestInterrupt("approve second", nil)
				}
				return nil
			}}, counters)

			_, err := runGraph(context.Background(), compiled, mode, "in", graph.WithCheckpoint(store, "k"))
			progress := interruptedProgress(t, err)
			if progress.Point != graph.InterruptPointBeforeNode || progress.ResumeNode != "second" {
				t.Fatalf("progress=%+v", progress)
			}
			if _, saves := store.counts(); saves != 3 {
				t.Fatalf("saves=%d", saves)
			}

			interrupt.Store(false)
			output, err := resumeGraph(context.Background(), compiled, mode, graph.WithCheckpoint(store, "k"))
			if err != nil || output != "in-first-second" {
				t.Fatalf("resume output=%q err=%v", output, err)
			}
			if counters.first.Load() != 1 || counters.second.Load() != 1 {
				t.Fatalf("first=%d second=%d", counters.first.Load(), counters.second.Load())
			}
		})
	}
}

func TestModeParityDuringNodeInterruptReplaysNode(t *testing.T) {
	for _, mode := range execModes {
		t.Run(string(mode), func(t *testing.T) {
			store := &byteCheckpointStore{}
			started := make(chan struct{}, 1)
			counters := &parityCounters{}
			compiled := newParityGraph(t, "parity-during", parityHooks{first: func(ctx context.Context, input string) (string, error) {
				if counters.first.Load() == 1 {
					started <- struct{}{}
					<-ctx.Done()
					return "", ctx.Err()
				}
				return input + "-first", nil
			}}, counters)

			ctx, cancel := context.WithCancelCause(context.Background())
			done := make(chan error, 1)
			go func() {
				_, err := runGraph(ctx, compiled, mode, "in", graph.WithCheckpoint(store, "k"))
				done <- err
			}()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("first node did not start")
			}
			cancel(control.RequestInterrupt("pause", nil))
			progress := interruptedProgress(t, <-done)
			if progress.Point != graph.InterruptPointDuringNode || progress.ResumeNode != "first" || counters.firstPost.Load() != 0 {
				t.Fatalf("progress=%+v post=%d", progress, counters.firstPost.Load())
			}

			output, err := resumeGraph(context.Background(), compiled, mode, graph.WithCheckpoint(store, "k"))
			if err != nil || output != "in-first-second" || counters.first.Load() != 2 {
				t.Fatalf("resume output=%q err=%v first=%d", output, err, counters.first.Load())
			}
		})
	}
}

func TestModeParityAfterCommitInterruptResumesAtNextNode(t *testing.T) {
	for _, mode := range execModes {
		t.Run(string(mode), func(t *testing.T) {
			store := &byteCheckpointStore{}
			counters := &parityCounters{}
			var cancel context.CancelCauseFunc
			compiled := newParityGraph(t, "parity-after-commit", parityHooks{firstPost: func(context.Context, *parityState) error {
				if counters.firstPost.Load() == 1 {
					cancel(control.RequestInterrupt("review first", nil))
				}
				return nil
			}}, counters)

			var ctx context.Context
			ctx, cancel = context.WithCancelCause(context.Background())
			defer cancel(nil)
			_, err := runGraph(ctx, compiled, mode, "in", graph.WithCheckpoint(store, "k"))
			progress := interruptedProgress(t, err)
			if progress.Point != graph.InterruptPointAfterCommit || progress.ResumeNode != "second" || progress.Step != 1 {
				t.Fatalf("progress=%+v", progress)
			}

			output, err := resumeGraph(context.Background(), compiled, mode, graph.WithCheckpoint(store, "k"))
			if err != nil || output != "in-first-second" {
				t.Fatalf("resume output=%q err=%v", output, err)
			}
			if counters.first.Load() != 1 || counters.firstPost.Load() != 1 {
				t.Fatalf("first=%d firstPost=%d", counters.first.Load(), counters.firstPost.Load())
			}
		})
	}
}

func TestModeParityInterruptWithoutCheckpointRequiresCheckpoint(t *testing.T) {
	cases := map[string]func(counters *parityCounters, cancel *context.CancelCauseFunc) parityHooks{
		"during node": func(_ *parityCounters, cancel *context.CancelCauseFunc) parityHooks {
			return parityHooks{first: func(_ context.Context, input string) (string, error) {
				(*cancel)(control.RequestInterrupt("pause", nil))
				return input + "-first", nil
			}}
		},
		"after commit": func(_ *parityCounters, cancel *context.CancelCauseFunc) parityHooks {
			return parityHooks{firstPost: func(context.Context, *parityState) error {
				(*cancel)(control.RequestInterrupt("pause", nil))
				return nil
			}}
		},
	}
	for name, build := range cases {
		for _, mode := range execModes {
			t.Run(name+"/"+string(mode), func(t *testing.T) {
				counters := &parityCounters{}
				var cancel context.CancelCauseFunc
				compiled := newParityGraph(t, "parity-no-checkpoint", build(counters, &cancel), counters)
				var ctx context.Context
				ctx, cancel = context.WithCancelCause(context.Background())
				defer cancel(nil)
				_, err := runGraph(ctx, compiled, mode, "in")
				if !errors.Is(err, graph.ErrCheckpointRequired) || errors.Is(err, graph.ErrInterrupted) {
					t.Fatalf("error=%v, want ErrCheckpointRequired", err)
				}
				if counters.second.Load() != 0 {
					t.Fatalf("second ran %d times after interruption", counters.second.Load())
				}
			})
		}
	}
}

func TestModeParityPostHandlerInterruptIsRejected(t *testing.T) {
	for _, withCheckpoint := range []bool{true, false} {
		for _, mode := range execModes {
			t.Run(fmt.Sprintf("%s/checkpoint=%v", mode, withCheckpoint), func(t *testing.T) {
				store := &byteCheckpointStore{}
				counters := &parityCounters{}
				compiled := newParityGraph(t, "parity-post-interrupt", parityHooks{firstPost: func(context.Context, *parityState) error {
					return control.RequestInterrupt("not allowed", nil)
				}}, counters)
				var options []runnable.Option
				if withCheckpoint {
					options = append(options, graph.WithCheckpoint(store, "k"))
				}
				_, err := runGraph(context.Background(), compiled, mode, "in", options...)
				if errors.Is(err, graph.ErrInterrupted) || errors.Is(err, graph.ErrCheckpointRequired) {
					t.Fatalf("error=%v, PostHandler interrupts must fail the run", err)
				}
				if code := executionCode(err); code != "node.post.interrupt" {
					t.Fatalf("error code=%q err=%v", code, err)
				}
				wantSaves := 0
				if withCheckpoint {
					wantSaves = 1
				}
				if _, saves := store.counts(); saves != wantSaves || counters.second.Load() != 0 {
					t.Fatalf("saves=%d second=%d", saves, counters.second.Load())
				}
			})
		}
	}
}

func TestModeParityMaxDepth(t *testing.T) {
	stack := make(graph.CursorStack, graph.DefaultMaxDepth)
	for index := range stack {
		stack[index] = graph.CursorFrame{Graph: fmt.Sprintf("outer-%d", index), Node: "child"}
	}
	for _, mode := range execModes {
		t.Run(string(mode), func(t *testing.T) {
			counters := &parityCounters{}
			compiled := newParityGraph(t, "parity-depth", parityHooks{}, counters)
			_, err := runGraph(graph.WithCursorStack(context.Background(), stack), compiled, mode, "in")
			if code := executionCode(err); code != "graph.max_depth" || counters.first.Load() != 0 {
				t.Fatalf("code=%q first=%d err=%v", code, counters.first.Load(), err)
			}
		})
	}
}

func TestModeParityCursorResumeStartsAtFrameNode(t *testing.T) {
	for _, mode := range execModes {
		t.Run(string(mode), func(t *testing.T) {
			counters := &parityCounters{}
			compiled := newParityGraph(t, "parity-cursor", parityHooks{}, counters)
			ctx := graph.WithCursorStack(context.Background(), graph.CursorStack{{Graph: "parity-cursor", Node: "second", Step: 1}})
			output, err := runGraph(ctx, compiled, mode, "in")
			if err != nil || output != "in-second" || counters.first.Load() != 0 {
				t.Fatalf("output=%q err=%v first=%d", output, err, counters.first.Load())
			}
		})
	}
}

func TestModeParityMaxSteps(t *testing.T) {
	for _, mode := range execModes {
		t.Run(string(mode), func(t *testing.T) {
			var runs atomic.Int32
			g := graph.NewGraph[string, string]("parity-steps")
			if err := g.SetStateFactory(func(_ context.Context, input string) (*parityState, error) {
				return &parityState{Value: input}, nil
			}); err != nil {
				t.Fatal(err)
			}
			loop := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
				runs.Add(1)
				return input, nil
			})
			if err := g.AddNode("loop", loop,
				graph.WithPreHandler(func(_ context.Context, state *parityState) (string, error) { return state.Value, nil }),
				graph.WithPostHandler(func(_ context.Context, state *parityState, _ string) error { return nil }),
			); err != nil {
				t.Fatal(err)
			}
			_ = g.AddEdge(graph.Start, "loop")
			_ = g.AddBranch("loop", "loop", "again", func(context.Context, *parityState) (bool, error) { return true, nil })
			_ = g.AddEdge("loop", graph.End)
			if err := g.SetMaxSteps(3); err != nil {
				t.Fatal(err)
			}
			_, err := runGraph(context.Background(), mustCompile(t, g), mode, "in")
			if code := executionCode(err); code != "graph.max_steps" || runs.Load() != 3 {
				t.Fatalf("code=%q runs=%d err=%v", code, runs.Load(), err)
			}
		})
	}
}

func TestModeParityParallelNode(t *testing.T) {
	for _, withCheckpoint := range []bool{false, true} {
		for _, mode := range execModes {
			t.Run(fmt.Sprintf("%s/checkpoint=%v", mode, withCheckpoint), func(t *testing.T) {
				g := graph.NewGraph[string, string]("parity-parallel")
				if err := g.SetStateFactory(func(_ context.Context, input string) (*parallelCheckpointState, error) {
					return &parallelCheckpointState{Values: []string{input}}, nil
				}); err != nil {
					t.Fatal(err)
				}
				branch := func(name string) graph.ParallelBranch {
					return graph.ParallelBranch{
						Name: name,
						Node: runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
							return input + "-" + name, nil
						}),
						Pre: func(_ context.Context, state *parallelCheckpointState) (string, error) { return state.Values[0], nil },
						Post: func(_ context.Context, state *parallelCheckpointState, output string) error {
							state.Values = append(state.Values, output)
							return nil
						},
					}
				}
				if err := g.AddParallelNode("fanout", []graph.ParallelBranch{branch("a"), branch("b")},
					graph.WithStateMerger(func(_ context.Context, base *parallelCheckpointState, branches []*parallelCheckpointState) error {
						base.Values = []string{base.Values[0], branches[0].Values[1], branches[1].Values[1]}
						return nil
					}),
				); err != nil {
					t.Fatal(err)
				}
				finish := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) { return input, nil })
				if err := g.AddNode("finish", finish, graph.WithPreHandler(func(_ context.Context, state *parallelCheckpointState) (string, error) {
					return state.Values[1] + "+" + state.Values[2], nil
				})); err != nil {
					t.Fatal(err)
				}
				for _, edge := range [][2]string{{graph.Start, "fanout"}, {"fanout", "finish"}, {"finish", graph.End}} {
					_ = g.AddEdge(edge[0], edge[1])
				}
				var options []runnable.Option
				if withCheckpoint {
					options = append(options, graph.WithCheckpoint(&byteCheckpointStore{}, "k"))
				}
				output, err := runGraph(context.Background(), mustCompile(t, g), mode, "in", options...)
				if err != nil || output != "in-a+in-b" {
					t.Fatalf("output=%q err=%v", output, err)
				}
			})
		}
	}
}

// 配置 checkpoint 后 Stream 仍以节点 Stream 形态执行，不再回退到 Invoke。
func TestStreamWithCheckpointUsesNodeStream(t *testing.T) {
	type sourceState struct{ Value int }
	source := &streamSource{}
	g := graph.NewGraph[int, int]("stream-checkpoint")
	if err := g.SetStateFactory(func(_ context.Context, input int) (*sourceState, error) {
		return &sourceState{Value: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("source", source,
		graph.WithPreHandler(func(_ context.Context, state *sourceState) (int, error) { return state.Value, nil }),
		graph.WithPostHandler(func(_ context.Context, state *sourceState, output int) error {
			state.Value = output
			return nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	_ = g.AddEdge(graph.Start, "source")
	_ = g.AddEdge("source", graph.End)
	output, err := runGraph(context.Background(), mustCompile(t, g), execStream, 1, graph.WithCheckpoint(&byteCheckpointStore{}, "k"))
	if err != nil || output != 3 || source.invokes.Load() != 0 {
		t.Fatalf("output=%d err=%v invokes=%d", output, err, source.invokes.Load())
	}
}
