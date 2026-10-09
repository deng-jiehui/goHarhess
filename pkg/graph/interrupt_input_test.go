package graph_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/goharness/goharness/pkg/state"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/stream"
)

type inputState struct {
	Text    string
	PreRuns int
}

func inputGraph(t *testing.T, body func(context.Context, string) (string, error)) *graph.CompiledGraph[string, string] {
	t.Helper()
	g := graph.NewGraph[string, string]("input-form")
	if err := g.SetStateFactory(func(_ context.Context, text string) (*inputState, error) { return &inputState{Text: text}, nil }); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("form", runnable.Func[string, string](func(ctx context.Context, text string, _ ...runnable.Option) (string, error) { return body(ctx, text) }),
		graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) {
			s.PreRuns++
			return fmt.Sprintf("%s:%d", s.Text, s.PreRuns), nil
		}),
		graph.WithPostHandler(func(_ context.Context, s *inputState, out string) error { s.Text = out; return nil })); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{{graph.Start, "form"}, {"form", graph.End}} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	return mustCompile(t, g)
}

func inputID(t *testing.T, err error) string {
	t.Helper()
	var interrupted *graph.InterruptedError
	if !errors.As(err, &interrupted) {
		t.Fatalf("expected saved interruption, got %v", err)
	}
	for _, record := range interrupted.PendingInterrupts() {
		if !record.Answered {
			return record.ID
		}
	}
	t.Fatal("missing pending input")
	return ""
}

func TestInputResumeReplaysAnswersAndEntryStateAcrossGraphInstances(t *testing.T) {
	ctx := context.Background()
	store := &byteCheckpointStore{}
	checkpoint := graph.WithCheckpoint(store, "form")
	body := func(ctx context.Context, text string) (string, error) {
		name, err := graph.Interrupt(ctx, map[string]any{"field": "name"})
		if err != nil {
			return "", err
		}
		choice, err := graph.Interrupt(ctx, map[string]any{"field": "language"})
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s/%v/%v", text, name, choice), nil
	}
	_, err := inputGraph(t, body).Invoke(ctx, "hello", checkpoint)
	first := inputID(t, err)
	g := inputGraph(t, body)
	_, err = g.Resume(ctx, checkpoint, graph.WithResume(first, "Ada"))
	second := inputID(t, err)
	if second == first {
		t.Fatal("two questions share ID")
	}
	records, err := g.PendingInterrupts(ctx, checkpoint)
	if err != nil || len(records) != 1 || records[0].ID != second {
		t.Fatalf("pending=%+v err=%v", records, err)
	}
	_, err = g.Resume(ctx, checkpoint, graph.WithResume(first, "wrong"))
	if !errors.Is(err, graph.ErrInvalidResume) {
		t.Fatalf("duplicate answer=%v", err)
	}
	out, err := inputGraph(t, body).Resume(ctx, checkpoint, graph.WithResume(second, "zh"))
	if err != nil || out != "hello:1/Ada/zh" {
		t.Fatalf("out=%q err=%v", out, err)
	}
	_, err = g.Resume(ctx, checkpoint, graph.WithResume(second, "zh"))
	if !errors.Is(err, graph.ErrInvalidResume) {
		t.Fatalf("completed resume=%v", err)
	}
	records, err = g.PendingInterrupts(ctx, checkpoint)
	if err != nil || len(records) != 0 {
		t.Fatalf("completed pending=%v err=%v", records, err)
	}
}

func TestInputResumeValidationBeforeNodeAndNullAnswer(t *testing.T) {
	calls := 0
	g := inputGraph(t, func(ctx context.Context, text string) (string, error) {
		calls++
		value, err := graph.Interrupt(ctx, "optional note")
		if err != nil {
			return "", err
		}
		if value != nil {
			return "", fmt.Errorf("expected null, got %v", value)
		}
		return text, nil
	})
	store := &byteCheckpointStore{}
	option := graph.WithCheckpoint(store, "null")
	_, err := g.Invoke(context.Background(), "x", option)
	id := inputID(t, err)
	// 无输入恢复且没有可推进工作时返回当前挂起结果，原来的问题 ID 不变。
	_, err = g.Resume(context.Background(), option)
	var suspended *graph.InterruptedError
	if !errors.As(err, &suspended) || len(suspended.PendingInterrupts()) != 1 || suspended.PendingInterrupts()[0].ID != id {
		t.Fatalf("empty resume=%v", err)
	}
	for _, opts := range [][]runnable.Option{{option, graph.WithResume("unknown", true)}, {option, graph.WithResume(id, true), graph.WithResume(id, false)}} {
		_, err = g.Resume(context.Background(), opts...)
		if !errors.Is(err, graph.ErrInvalidResume) {
			t.Fatalf("invalid input=%v", err)
		}
	}
	if calls != 1 {
		t.Fatalf("invalid answers ran node %d times", calls)
	}
	out, err := g.Resume(context.Background(), option, graph.WithResume(id, nil))
	if err != nil || out != "x:1" {
		t.Fatalf("null answer out=%q err=%v", out, err)
	}
}

func TestInputRejectsChangedQuestionAndUnserializablePayload(t *testing.T) {
	payload := any("original")
	g := inputGraph(t, func(ctx context.Context, text string) (string, error) {
		_, err := graph.Interrupt(ctx, payload)
		return text, err
	})
	option := graph.WithCheckpoint(&byteCheckpointStore{}, "changed")
	_, err := g.Invoke(context.Background(), "x", option)
	id := inputID(t, err)
	payload = "different"
	_, err = g.Resume(context.Background(), option, graph.WithResume(id, "answer"))
	if !errors.Is(err, graph.ErrInterruptMismatch) {
		t.Fatalf("changed question=%v", err)
	}
	payload = make(chan int)
	_, err = g.Invoke(context.Background(), "x", option)
	if err == nil || errors.Is(err, graph.ErrInterrupted) {
		t.Fatalf("invalid payload=%v", err)
	}
}

func TestInputStreamEventsSuspendsWithoutResultAndResumes(t *testing.T) {
	ctx := context.Background()
	g := inputGraph(t, func(ctx context.Context, text string) (string, error) {
		v, err := graph.Interrupt(ctx, "pick a color")
		return fmt.Sprintf("%s/%v", text, v), err
	})
	option := graph.WithCheckpoint(&byteCheckpointStore{}, "events")
	r, err := g.StreamEvents(ctx, "color", stream.Config{}, option)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var id string
	for {
		e, err := r.Recv()
		if err != nil {
			if inputID(t, err) != id {
				t.Fatal("event and error IDs differ")
			}
			break
		}
		if e.Result != nil {
			t.Fatal("suspended run emitted result")
		}
		if e.Lifecycle != nil && e.Lifecycle.Phase == stream.PhaseInterrupted {
			if len(e.Lifecycle.Interrupts) > 0 {
				id = e.Lifecycle.Interrupts[0].ID
			}
		}
	}
	if id == "" {
		t.Fatal("default stream hid input request")
	}
	resumed, err := g.ResumeStreamEvents(ctx, stream.Config{}, option, graph.WithResume(id, "blue"))
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.Close()
	results := 0
	for {
		e, err := resumed.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if e.Result != nil {
			results++
			if *e.Result != "color:1/blue" {
				t.Fatal(*e.Result)
			}
		}
	}
	if results != 1 {
		t.Fatalf("results=%d", results)
	}
}

func TestInputRequiresCheckpointAndNeverReportsFailedSaveAsSuspended(t *testing.T) {
	g := inputGraph(t, func(ctx context.Context, text string) (string, error) {
		_, err := graph.Interrupt(ctx, "question")
		return text, err
	})
	_, err := g.Invoke(context.Background(), "x")
	if !errors.Is(err, graph.ErrCheckpointRequired) {
		t.Fatal(err)
	}
	failure := errors.New("disk unavailable")
	_, err = g.Invoke(context.Background(), "x", graph.WithCheckpoint(&byteCheckpointStore{saveErr: failure}, "fail"))
	if !errors.Is(err, failure) || errors.Is(err, graph.ErrInterrupted) {
		t.Fatal(err)
	}
}

func TestInputInPreHandlerReplaysBeforeMutation(t *testing.T) {
	for _, mode := range execModes {
		t.Run(string(mode), func(t *testing.T) {
			g := graph.NewGraph[string, string]("pre-input")
			_ = g.SetStateFactory(func(context.Context, string) (*inputState, error) { return &inputState{}, nil })
			_ = g.AddNode("form", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) { return s, nil }), graph.WithPreHandler(func(ctx context.Context, s *inputState) (string, error) {
				s.PreRuns++
				answer, err := graph.Interrupt(ctx, "supply an integer")
				if err != nil {
					return "", err
				}
				number, ok := answer.(json.Number)
				if !ok {
					return "", fmt.Errorf("number type %T", answer)
				}
				return fmt.Sprintf("%d/%s", s.PreRuns, number), nil
			}))
			_ = g.AddEdge(graph.Start, "form")
			_ = g.AddEdge("form", graph.End)
			c := mustCompile(t, g)
			option := graph.WithCheckpoint(&byteCheckpointStore{}, "pre")
			_, err := runGraph(context.Background(), c, mode, "", option)
			id := inputID(t, err)
			out, err := resumeGraph(context.Background(), c, mode, option, graph.WithResume(id, int64(9007199254740993)))
			if err != nil || out != "1/9007199254740993" {
				t.Fatalf("out=%q err=%v", out, err)
			}
		})
	}
}

type inputStreamNode struct{}

func (inputStreamNode) Invoke(context.Context, string, ...runnable.Option) (string, error) {
	return "", errors.New("stream unexpectedly fell back to invoke")
}
func (inputStreamNode) Stream(ctx context.Context, text string, _ ...runnable.Option) (*runnable.StreamReader[string], error) {
	reader, writer := runnable.Pipe[string](ctx)
	go func() {
		defer writer.Close()
		answer, err := graph.Interrupt(ctx, "stream question")
		if err != nil {
			_ = writer.CloseWithError(err)
			return
		}
		_ = writer.Send(text + "/" + fmt.Sprint(answer))
	}()
	return reader, nil
}

func TestInputFromStreamProducerResumesItsOwnNode(t *testing.T) {
	g := graph.NewGraph[string, string]("producer")
	_ = g.SetStateFactory(func(_ context.Context, s string) (*inputState, error) { return &inputState{Text: s}, nil })
	_ = g.AddNode("source", inputStreamNode{}, graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil }))
	downstream := 0
	_ = g.AddNode("next", runnable.Func[string, string](func(_ context.Context, text string, _ ...runnable.Option) (string, error) {
		downstream++
		return text, nil
	}), graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil }))
	_ = g.AddEdge(graph.Start, "source")
	_ = g.AddEdge("source", "next")
	_ = g.AddEdge("next", graph.End)
	c := mustCompile(t, g)
	option := graph.WithCheckpoint(&byteCheckpointStore{}, "producer")
	_, err := runGraph(context.Background(), c, execStream, "value", option)
	id := inputID(t, err)
	var suspended *graph.InterruptedError
	if !errors.As(err, &suspended) || inspectCheckpoint(t, suspended.Checkpoint).ResumeNode != "source" || downstream != 0 {
		t.Fatalf("err=%v downstream=%d", err, downstream)
	}
	_, err = resumeGraph(context.Background(), c, execStream, option, graph.WithResume(id, "external"))
	if err != nil || downstream != 1 {
		t.Fatalf("err=%v downstream=%d", err, downstream)
	}
}

func TestInputLoopCreatesFreshQuestionsAndRejectsOldAnswer(t *testing.T) {
	g := graph.NewGraph[string, string]("loop-input")
	_ = g.SetStateFactory(func(context.Context, string) (*inputState, error) { return &inputState{}, nil })
	_ = g.AddNode("form", runnable.Func[string, string](func(ctx context.Context, text string, _ ...runnable.Option) (string, error) {
		v, err := graph.Interrupt(ctx, "next choice")
		return fmt.Sprint(v), err
	}),
		graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil }),
		graph.WithPostHandler(func(_ context.Context, s *inputState, text string) error { s.PreRuns++; s.Text = text; return nil }))
	_ = g.AddEdge(graph.Start, "form")
	_ = g.AddBranch("form", "form", "again", func(_ context.Context, s *inputState) (bool, error) { return s.PreRuns < 2, nil })
	_ = g.AddEdge("form", graph.End)
	c := mustCompile(t, g)
	option := graph.WithCheckpoint(&byteCheckpointStore{}, "loop")
	_, err := c.Invoke(context.Background(), "", option)
	first := inputID(t, err)
	_, err = c.Resume(context.Background(), option, graph.WithResume(first, "one"))
	second := inputID(t, err)
	if first == second {
		t.Fatal("loop reused answered question")
	}
	_, err = c.Resume(context.Background(), option, graph.WithResume(first, "stale"))
	if !errors.Is(err, graph.ErrInvalidResume) {
		t.Fatal(err)
	}
	out, err := c.Resume(context.Background(), option, graph.WithResume(second, "two"))
	if err != nil || out != "two" {
		t.Fatalf("%q %v", out, err)
	}
}

func TestInputCannotBeSilentlySwallowedOrSkipped(t *testing.T) {
	skip := false
	g := inputGraph(t, func(ctx context.Context, s string) (string, error) {
		if !skip {
			_, _ = graph.Interrupt(ctx, "required")
		}
		return s, nil
	})
	option := graph.WithCheckpoint(&byteCheckpointStore{}, "skip")
	_, err := g.Invoke(context.Background(), "x", option)
	id := inputID(t, err)
	skip = true
	_, err = g.Resume(context.Background(), option, graph.WithResume(id, "answer"))
	if !errors.Is(err, graph.ErrInterruptMismatch) {
		t.Fatal(err)
	}
}

func TestInputNestedGraphSuspendsAsChildExecution(t *testing.T) {
	child := inputGraph(t, func(ctx context.Context, s string) (string, error) {
		_, err := graph.Interrupt(ctx, "child")
		return s, err
	})
	parent := inputGraph(t, func(ctx context.Context, s string) (string, error) {
		_, err := graph.Interrupt(ctx, "parent")
		if err != nil {
			return "", err
		}
		return child.Invoke(ctx, s)
	})
	option := graph.WithCheckpoint(&byteCheckpointStore{}, "nested")
	_, err := parent.Invoke(context.Background(), "x", option)
	id := inputID(t, err)
	// 父问题回答后恢复执行到子图：子图的提问挂起整棵执行树，由根级
	// InterruptedError 返回，待答列表包含子 frame 的记录。
	_, err = parent.Resume(context.Background(), option, graph.WithResume(id, "go"))
	var interrupted *graph.InterruptedError
	if !errors.As(err, &interrupted) {
		t.Fatalf("nested suspend=%v", err)
	}
	childID := inputID(t, err)
	out, err := parent.Resume(context.Background(), option, graph.WithResume(childID, "go"))
	if err != nil || out == "" {
		t.Fatalf("nested resume out=%q err=%v", out, err)
	}
}

func TestInputResumeAdvancesCheckpointBeforeLaterFailure(t *testing.T) {
	firstRuns := 0
	laterFails := true
	g := graph.NewGraph[string, string]("advance-input")
	_ = g.SetStateFactory(func(context.Context, string) (*inputState, error) { return &inputState{}, nil })
	_ = g.AddNode("form", runnable.Func[string, string](func(ctx context.Context, _ string, _ ...runnable.Option) (string, error) {
		firstRuns++
		answer, err := graph.Interrupt(ctx, "supply text")
		if err != nil {
			return "", err
		}
		return answer.(string), nil
	}), graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil }), graph.WithPostHandler(func(_ context.Context, s *inputState, out string) error { s.Text = out; return nil }))
	failure := errors.New("transient downstream failure")
	_ = g.AddNode("later", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) {
		if laterFails {
			return "", failure
		}
		return s + "/finished", nil
	}), graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil }))
	_ = g.AddEdge(graph.Start, "form")
	_ = g.AddEdge("form", "later")
	_ = g.AddEdge("later", graph.End)
	c := mustCompile(t, g)
	option := graph.WithCheckpoint(&byteCheckpointStore{}, "advance")
	_, err := c.Invoke(context.Background(), "", option)
	id := inputID(t, err)
	_, err = c.Resume(context.Background(), option, graph.WithResume(id, "external"))
	if !errors.Is(err, failure) {
		t.Fatal(err)
	}
	laterFails = false
	out, err := c.Resume(context.Background(), option)
	if err != nil || out != "external/finished" || firstRuns != 2 {
		t.Fatalf("out=%q runs=%d err=%v", out, firstRuns, err)
	}
	_, err = c.Resume(context.Background(), option)
	if err != nil {
		t.Fatalf("completed input run=%v", err)
	}
}

func TestInputSuspensionLifecycleAndFailedSave(t *testing.T) {
	for _, saveFails := range []bool{false, true} {
		label := "saved"
		if saveFails {
			label = "failed"
		}
		t.Run(label, func(t *testing.T) {
			store := &byteCheckpointStore{}
			if saveFails {
				store.saveErr = errors.New("save failed")
			}
			g := inputGraph(t, func(ctx context.Context, s string) (string, error) {
				stream.StartActivity(ctx, "preparing")
				_, err := graph.Interrupt(ctx, "input")
				return s, err
			})
			r, err := g.StreamEvents(context.Background(), "x", stream.Config{Lifecycle: true}, graph.WithCheckpoint(store, "lifecycle"))
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			notices := 0
			for {
				e, err := r.Recv()
				if err != nil {
					if saveFails {
						if !errors.Is(err, store.saveErr) {
							t.Fatal(err)
						}
					} else {
						inputID(t, err)
					}
					break
				}
				if e.Result != nil {
					t.Fatal("suspended run emitted result")
				}
				if e.Activity != nil && !saveFails && e.Activity.Status == stream.ActivityFailed {
					t.Fatal("input wait treated as failure")
				}
				if e.Lifecycle != nil {
					if e.Lifecycle.Phase == stream.PhaseInterrupted {
						notices++
						if e.Lifecycle.Scope == stream.ScopeGraph && len(e.Lifecycle.Interrupts) != 1 {
							t.Fatal(e)
						}
					}
					if !saveFails && e.Lifecycle.Phase == stream.PhaseError {
						t.Fatal("input wait treated as error")
					}
				}
			}
			if (!saveFails && notices != 2) || (saveFails && notices != 0) {
				t.Fatalf("notices=%d", notices)
			}
		})
	}
}

func TestInputParallelBranchSuspendsAndResumes(t *testing.T) {
	g := graph.NewGraph[string, string]("parallel-input")
	_ = g.SetStateFactory(func(context.Context, string) (*inputState, error) { return &inputState{}, nil })
	_ = g.AddNode("q", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) { return s, nil }),
		graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil }))
	_ = g.AddNode("ask", runnable.Func[string, string](func(ctx context.Context, s string, _ ...runnable.Option) (string, error) {
		v, err := graph.Interrupt(ctx, "parallel question")
		if err != nil {
			return "", err
		}
		return v.(string), nil
	}), graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil }),
		graph.WithPostHandler(func(_ context.Context, s *inputState, out string) error { s.Text = out; return nil }))
	_ = g.AddNode("other", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) { return s, nil }),
		graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil }),
		graph.WithPostHandler(func(_ context.Context, s *inputState, out string) error { s.Text = out; return nil }))
	_ = g.AddNode("end", runnable.Func[string, string](func(_ context.Context, s string, _ ...runnable.Option) (string, error) { return s, nil }), graph.WithPreHandler(func(_ context.Context, s *inputState) (string, error) { return s.Text, nil }))
	_ = g.AddEdge(graph.Start, "q")
	_ = g.AddFork("q", "ask", "other")
	_ = g.AddJoin("q", "end")
	_ = g.AddEdge("ask", "end")
	_ = g.AddEdge("other", "end")
	_ = g.AddEdge("end", graph.End)
	_ = graph.SetForkMerger(g, "q", func(_ context.Context, s *inputState, rs []graph.StepResult[*inputState]) error {
		s.Text = rs[0].State.Text
		return nil
	})
	c := mustCompile(t, g)
	option := graph.WithCheckpoint(&byteCheckpointStore{}, "parallel")
	_, err := c.Invoke(context.Background(), "", option)
	var suspended *graph.InterruptedError
	if !errors.As(err, &suspended) {
		t.Fatal(err)
	}
	pending := suspended.PendingInterrupts()
	if len(pending) != 1 {
		t.Fatalf("pending=%v", pending)
	}
	out, err := c.Resume(context.Background(), option, graph.WithResume(pending[0].ID, "answered"))
	if err != nil || out != "answered" {
		t.Fatalf("resume=%q %v", out, err)
	}
}

func TestInputCheckpointCopiesRecordsAndRejectsLegacyFormat(t *testing.T) {
	g := inputGraph(t, func(ctx context.Context, s string) (string, error) {
		_, err := graph.Interrupt(ctx, map[string]any{"field": "text"})
		return s, err
	})
	option := graph.WithCheckpoint(&byteCheckpointStore{}, "clone")
	_, err := g.Invoke(context.Background(), "", option)
	var suspended *graph.InterruptedError
	if !errors.As(err, &suspended) {
		t.Fatal(err)
	}
	copy := suspended.PendingInterrupts()
	copy[0].Payload[0] = '!'
	records, err := g.PendingInterrupts(context.Background(), option)
	if err != nil || !json.Valid(records[0].Payload) || !json.Valid(suspended.PendingInterrupts()[0].Payload) {
		t.Fatalf("records=%v err=%v", records, err)
	}
	legacy := suspended.Checkpoint.Clone()
	legacy.SchemaVersion = 2
	data, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.DecodeCheckpoint(data); !errors.Is(err, state.ErrInvalidCheckpoint) {
		t.Fatalf("legacy format: %v", err)
	}
}
