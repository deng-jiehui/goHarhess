package agent_test

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/stream"
)

type decision struct {
	Approved bool   `json:"approved"`
	Reason   string `json:"reason"`
}

const goodDecisionJSON = `{"approved":true,"reason":"ok"}`

func newDecisionOutput(t *testing.T) *llm.StructuredOutput[decision] {
	t.Helper()
	output, err := llm.NewStructuredOutput[decision](llm.WithOutputName("decision"))
	if err != nil {
		t.Fatal(err)
	}
	return output
}

// 模型 Option 经 Agent 现有分发到达模型，不进入解码组件。
func TestResponseFormatReachesModelThroughAgent(t *testing.T) {
	var got *llm.ResponseFormat
	model := llm.NewHandlerModel(func(_ context.Context, _ llm.Request, opts llm.Options) ([]schema.Message, error) {
		got = opts.ResponseFormat
		return []schema.Message{schema.AssistantMessage(goodDecisionJSON)}, nil
	})
	assistant, err := agent.New(model)
	if err != nil {
		t.Fatal(err)
	}
	output := newDecisionOutput(t)
	messages, err := assistant.Invoke(context.Background(),
		[]schema.Message{schema.UserMessage("决定")},
		llm.WithResponseFormat(output.Format()))
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Name != "decision" || !got.Strict || len(got.Schema) == 0 {
		t.Fatalf("model saw format %#v", got)
	}
	value, err := output.Decode(messages)
	if err != nil || !value.Approved {
		t.Fatalf("value = %#v, err = %v", value, err)
	}
}

func wireDecisionGraph(t *testing.T, assistant agent.Agent, output *llm.StructuredOutput[decision]) *graph.Graph[[]schema.Message, decision] {
	t.Helper()
	g := graph.NewGraph[[]schema.Message, decision]("decision-graph")
	if err := g.AddNode("answer", assistant); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("decode", output); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{
		{graph.Start, "answer"}, {"answer", "decode"}, {"decode", graph.End},
	} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	return g
}

// 结构化输出作为普通图节点：无 State 的最小接线，类型化输出直达 END。
func TestStructuredOutputAsGraphEnd(t *testing.T) {
	model := llm.NewScriptedModel([]schema.Message{schema.AssistantMessage(goodDecisionJSON)})
	assistant, err := agent.New(model)
	if err != nil {
		t.Fatal(err)
	}
	output := newDecisionOutput(t)
	compiled, err := wireDecisionGraph(t, assistant, output).Compile()
	if err != nil {
		t.Fatal(err)
	}
	value, err := compiled.Invoke(context.Background(),
		[]schema.Message{schema.UserMessage("决定")},
		runnable.ForNode("answer", llm.WithResponseFormat(output.Format())))
	if err != nil {
		t.Fatal(err)
	}
	if !value.Approved || value.Reason != "ok" {
		t.Fatalf("value = %#v", value)
	}
}

// 广播的模型 Option 只到声明了 llm.Options 的节点，不进入解码节点。
func TestBroadcastModelOptionSkipsDecodeNode(t *testing.T) {
	var got *llm.ResponseFormat
	model := llm.NewHandlerModel(func(_ context.Context, _ llm.Request, opts llm.Options) ([]schema.Message, error) {
		got = opts.ResponseFormat
		return []schema.Message{schema.AssistantMessage(goodDecisionJSON)}, nil
	})
	assistant, err := agent.New(model)
	if err != nil {
		t.Fatal(err)
	}
	output := newDecisionOutput(t)
	compiled, err := wireDecisionGraph(t, assistant, output).Compile()
	if err != nil {
		t.Fatal(err)
	}
	// 不带定向的广播选项：decode 声明不接受任何配置类型，不因它报错。
	value, err := compiled.Invoke(context.Background(),
		[]schema.Message{schema.UserMessage("决定")},
		llm.WithResponseFormat(output.Format()))
	if err != nil {
		t.Fatal(err)
	}
	if !value.Approved || got == nil || got.Name != "decision" {
		t.Fatalf("value = %#v, model format = %#v", value, got)
	}
}

type decisionState struct {
	Messages []schema.Message
	Decision *decision
}

// 有 State 与检查点的完整接线：模型消息持久化后，在 decode 前的边界挂起；
// 恢复只重放解码节点，不重复调用模型。
func TestStructuredOutputResumeReplaysDecodeOnly(t *testing.T) {
	var calls atomic.Int32
	model := llm.NewHandlerModel(func(_ context.Context, _ llm.Request, _ llm.Options) ([]schema.Message, error) {
		calls.Add(1)
		return []schema.Message{schema.AssistantMessage(goodDecisionJSON)}, nil
	})
	assistant, err := agent.New(model)
	if err != nil {
		t.Fatal(err)
	}
	output := newDecisionOutput(t)

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	g := graph.NewGraph[[]schema.Message, decision]("checkpointed")
	if err := g.SetStateFactory(func(_ context.Context, _ []schema.Message) (*decisionState, error) {
		return &decisionState{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("answer", assistant,
		graph.WithPostHandler(func(_ context.Context, s *decisionState, messages []schema.Message) error {
			s.Messages = schema.CloneMessages(messages)
			// 模型输出落进 State 后立刻挂起，恢复点应停在 decode。
			cancel(control.RequestInterrupt("await approval", nil))
			return nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("decode", output,
		graph.WithPreHandler(func(_ context.Context, s *decisionState) ([]schema.Message, error) {
			return schema.CloneMessages(s.Messages), nil
		}),
		graph.WithPostHandler(func(_ context.Context, s *decisionState, value decision) error {
			s.Decision = &value
			return nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	for _, edge := range [][2]string{{graph.Start, "answer"}, {"answer", "decode"}, {"decode", graph.End}} {
		if err := g.AddEdge(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	store := state.NewMemoryStore()
	compiled, err := g.Compile(graph.Checkpointer(store))
	if err != nil {
		t.Fatal(err)
	}
	input := []schema.Message{schema.UserMessage("决定")}
	option := graph.WithCheckpoint(store, "structured-run")

	if _, err := compiled.Invoke(ctx, input, option); !errors.Is(err, graph.ErrInterrupted) {
		t.Fatalf("first invoke err = %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("model calls = %d", calls.Load())
	}
	value, err := compiled.Resume(context.Background(), option)
	if err != nil {
		t.Fatal(err)
	}
	if !value.Approved || calls.Load() != 1 {
		t.Fatalf("resume re-invoked model or wrong value: %#v calls=%d", value, calls.Load())
	}
}

// decode 失败不产生公共流 KindResult。
func TestStructuredOutputFailureEmitsNoKindResult(t *testing.T) {
	model := llm.NewScriptedModel([]schema.Message{schema.AssistantMessage(`{"reason":"ok"}`)})
	assistant, err := agent.New(model)
	if err != nil {
		t.Fatal(err)
	}
	output := newDecisionOutput(t)
	compiled, err := wireDecisionGraph(t, assistant, output).Compile()
	if err != nil {
		t.Fatal(err)
	}
	events, err := compiled.StreamEvents(context.Background(),
		[]schema.Message{schema.UserMessage("决定")}, stream.Config{Buffer: 16})
	if err != nil {
		t.Fatal(err)
	}
	sawResult := false
	for {
		event, recvErr := events.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			if !errors.Is(recvErr, llm.ErrInvalidStructuredOutput) {
				t.Fatalf("stream err = %v", recvErr)
			}
			break
		}
		if event.Kind == stream.KindResult {
			sawResult = true
		}
	}
	if sawResult {
		t.Fatal("KindResult emitted for failed decode")
	}
}
