package graph_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
)

// convState 是会话外部化 State：序列化时按 convExt 用 history_len 引用
// 代替完整历史，恢复时需由检查点回填。模拟 agentrecipe baseState 行为。
type convState struct {
	History    []schema.Message
	Out        string
	convExt    bool
	historyLen int
}

func (s *convState) CheckpointMessages() []schema.Message { return s.History }

func (s *convState) MarkConversationExternal() {
	s.convExt = true
	s.historyLen = len(s.History)
}

func (s *convState) NeedsConversation() bool { return s.convExt }

func (s *convState) RestoreConversation(messages []schema.Message) error {
	if s.historyLen > len(messages) {
		return fmt.Errorf("history_len=%d exceeds session=%d", s.historyLen, len(messages))
	}
	s.History = append([]schema.Message(nil), messages[:s.historyLen]...)
	s.convExt = false
	return nil
}

func (s *convState) MarshalJSON() ([]byte, error) {
	type wire struct {
		History    []schema.Message `json:"history,omitempty"`
		ConvExt    bool             `json:"conv_ext,omitempty"`
		HistoryLen int              `json:"history_len,omitempty"`
		Out        string           `json:"out,omitempty"`
	}
	w := wire{Out: s.Out}
	if s.convExt {
		w.ConvExt, w.HistoryLen = true, s.historyLen
	} else {
		w.History = s.History
	}
	return json.Marshal(w)
}

func (s *convState) UnmarshalJSON(data []byte) error {
	var w struct {
		History    []schema.Message `json:"history,omitempty"`
		ConvExt    bool             `json:"conv_ext,omitempty"`
		HistoryLen int              `json:"history_len,omitempty"`
		Out        string           `json:"out,omitempty"`
	}
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	s.History, s.Out = w.History, w.Out
	s.convExt, s.historyLen = w.ConvExt, w.HistoryLen
	return nil
}

// 分支复制的 StepStates 在持久化开启后会被外部化：成员解码副本必须
// 被回填会话，否则成员看到空历史。该缺陷只在 checkpoint 开启时出现。
func TestParallelBranchStatesGetHydratedConversation(t *testing.T) {
	store := state.NewMemoryStore()
	var seen atomic.Int32
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	g := graph.NewGraph[string, string]("parallel-conv")
	must(g.SetStateFactory(func(_ context.Context, input string) (*convState, error) {
		return &convState{History: []schema.Message{schema.UserMessage(input)}}, nil
	}))
	member := func(name string) {
		must(g.AddNode(name,
			runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
				return input + "-" + name, nil
			}),
			graph.WithPreHandler(func(_ context.Context, s *convState) (string, error) {
				if len(s.History) != 1 {
					return "", fmt.Errorf("branch %s saw %d history", name, len(s.History))
				}
				seen.Add(1)
				return "hi", nil
			}),
			graph.WithPostHandler(func(_ context.Context, s *convState, output string) error {
				s.Out += ";" + output
				return nil
			})))
	}
	member("a")
	member("b")
	must(g.AddNode("q", runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
		return input, nil
	}), graph.WithPreHandler(func(_ context.Context, _ *convState) (string, error) {
		return "hi", nil
	})))
	finish := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
		return input, nil
	})
	must(g.AddNode("finish", finish,
		graph.WithPreHandler(func(_ context.Context, s *convState) (string, error) {
			return s.Out, nil
		}),
	))
	must(g.AddEdge(graph.Start, "q"))
	must(g.AddFork("q", "a", "b"))
	must(g.AddJoin("q", "finish"))
	must(g.AddEdge("a", "finish"))
	must(g.AddEdge("b", "finish"))
	must(g.AddEdge("finish", graph.End))
	must(graph.SetForkMerger(g, "q",
		func(_ context.Context, base *convState, rs []graph.StepResult[*convState]) error {
			base.Out = rs[0].State.Out + rs[1].State.Out
			return nil
		}))
	compiled := mustCompile(t, g)
	for _, persist := range []bool{false, true} {
		var opts []runnable.Option
		name := "conv-nopersist"
		if persist {
			name = "conv-persist"
			opts = append(opts, graph.WithCheckpoint(store, name))
		}
		out, err := compiled.Invoke(context.Background(), "hi", opts...)
		if err != nil {
			t.Fatalf("persist=%v: %v", persist, err)
		}
		if out != ";hi-a;hi-b" {
			t.Fatalf("persist=%v output=%q", persist, out)
		}
	}
	if seen.Load() != 4 {
		t.Fatalf("branch Pre calls=%d", seen.Load())
	}
}
