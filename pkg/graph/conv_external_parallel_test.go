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

// 并行分支复制的 StepStates 在持久化开启后会被外部化：分支解码副本必须
// 被回填会话，否则分支看到空历史。该缺陷只在 checkpoint 开启时出现。
func TestParallelBranchStatesGetHydratedConversation(t *testing.T) {
	store := state.NewMemoryStore()
	var seen atomic.Int32

	g := graph.NewGraph[string, string]("parallel-conv")
	if err := g.SetStateFactory(func(_ context.Context, input string) (*convState, error) {
		return &convState{History: []schema.Message{schema.UserMessage(input)}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	branch := func(name string) graph.ParallelBranch {
		return graph.ParallelBranch{
			Name: name,
			Node: runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
				return input + "-" + name, nil
			}),
			Pre: func(_ context.Context, s *convState) (string, error) {
				if len(s.History) != 1 {
					return "", fmt.Errorf("branch %s saw %d history", name, len(s.History))
				}
				seen.Add(1)
				return "hi", nil
			},
			Post: func(_ context.Context, s *convState, output string) error {
				s.Out += ";" + output
				return nil
			},
		}
	}
	if err := g.AddParallelNode("fanout", []graph.ParallelBranch{branch("a"), branch("b")},
		graph.WithStateMerger(func(_ context.Context, base *convState, branches []*convState) error {
			base.Out = branches[0].Out + branches[1].Out
			return nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	finish := runnable.Func[string, string](func(_ context.Context, input string, _ ...runnable.Option) (string, error) {
		return input, nil
	})
	if err := g.AddNode("finish", finish,
		graph.WithPreHandler(func(_ context.Context, s *convState) (string, error) {
			return s.Out, nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{{graph.Start, "fanout"}, {"fanout", "finish"}, {"finish", graph.End}} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
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
