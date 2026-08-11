package agent_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"goharness/pkg/agent"
	"goharness/pkg/llm"
	"goharness/pkg/schema"
	"goharness/pkg/state"
	"goharness/pkg/tool"
)

func TestInterruptedCheckpointPreservesCallerSystemMessage(t *testing.T) {
	store := state.NewMemoryStore()
	first, err := agent.NewBasicAgent(agent.BaseConfig{
		SystemMessage: "old policy", Model: llm.NewScriptedModel(), CheckpointStore: store,
		Breakpointers: []agent.Breakpointer{agent.BreakpointFunc(func(context.Context, agent.Breakpoint) (agent.InterruptDecision, error) {
			return agent.InterruptDecision{Interrupt: true, Reason: "move"}, nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	input := []schema.Message{schema.SystemMessage("caller policy"), schema.UserMessage("continue")}
	if _, err := first.Invoke(context.Background(), input, agent.WithConversationID("caller-system-resume")); !errors.Is(err, agent.ErrInterrupted) {
		t.Fatalf("interrupt error=%v", err)
	}
	resumed, err := agent.NewBasicAgent(agent.BaseConfig{
		SystemMessage: "replacement policy", CheckpointStore: store,
		Model: llm.NewHandlerModel(func(_ context.Context, messages []schema.Message, _ llm.ChatOptions) ([]schema.Message, error) {
			if len(messages) != 2 || messages[0].Role != schema.RoleSystem || messages[0].Text() != "replacement policy\n\ncaller policy" || messages[1].Text() != "continue" {
				t.Fatalf("resume context=%#v", messages)
			}
			return []schema.Message{schema.AssistantMessage("resumed")}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agent.Resume(context.Background(), resumed, "caller-system-resume"); err != nil {
		t.Fatal(err)
	}
}

func TestInterruptedCheckpointResumesWithNewModelInstance(t *testing.T) {
	// The persistence contract is tested here without selecting a backend.
	// Redis support lives in the optional extensions/state/redis module.
	store := state.NewMemoryStore()
	var firstStore state.Store = store
	var secondStore state.Store = store
	var err error
	var oldCalls atomic.Int32
	interrupted, err := agent.NewBasicAgent(agent.BaseConfig{
		Name:          "first-device",
		SystemMessage: "old provider policy",
		Model: llm.NewHandlerModel(func(context.Context, []schema.Message, llm.ChatOptions) ([]schema.Message, error) {
			oldCalls.Add(1)
			return []schema.Message{schema.AssistantMessage("must not run")}, nil
		}),
		CheckpointStore: firstStore,
		Breakpointers: []agent.Breakpointer{agent.BreakpointFunc(func(context.Context, agent.Breakpoint) (agent.InterruptDecision, error) {
			return agent.InterruptDecision{Interrupt: true, Reason: "switch device and provider"}, nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = interrupted.Invoke(context.Background(), []schema.Message{schema.UserMessage("continue")}, agent.WithConversationID("cross-device"))
	if !errors.Is(err, agent.ErrInterrupted) {
		t.Fatalf("initial Invoke error = %v", err)
	}
	if oldCalls.Load() != 0 {
		t.Fatalf("old model calls = %d, want 0", oldCalls.Load())
	}
	checkpoint, found, err := secondStore.Load(context.Background(), "cross-device")
	if err != nil || !found || checkpoint.Status != state.CheckpointStatusInterrupted || checkpoint.Continuation == nil {
		t.Fatalf("checkpoint = %#v, found=%t, err=%v", checkpoint, found, err)
	}
	if len(checkpoint.Continuation.CursorStack) == 0 || checkpoint.Continuation.CursorStack[len(checkpoint.Continuation.CursorStack)-1].Node != "llm" {
		t.Fatalf("portable graph cursor stack = %#v", checkpoint.Continuation.CursorStack)
	}
	if _, err := checkpoint.JSON(); err != nil {
		t.Fatalf("checkpoint is not portable JSON: %v", err)
	}

	var newCalls atomic.Int32
	resumed, err := agent.NewBasicAgent(agent.BaseConfig{
		Name:          "second-device",
		SystemMessage: "replacement provider policy",
		Model: llm.NewHandlerModel(func(_ context.Context, messages []schema.Message, _ llm.ChatOptions) ([]schema.Message, error) {
			newCalls.Add(1)
			if len(messages) == 0 || messages[0].Role != schema.RoleSystem || messages[0].Text() != "replacement provider policy" {
				t.Fatalf("replacement model context = %#v", messages)
			}
			return []schema.Message{schema.AssistantMessage("continued by replacement model")}, nil
		}),
		CheckpointStore: secondStore,
	})
	if err != nil {
		t.Fatal(err)
	}
	messages, err := agent.Resume(context.Background(), resumed, "cross-device")
	if err != nil {
		t.Fatal(err)
	}
	if newCalls.Load() != 1 || len(messages) != 1 || messages[0].Text() != "continued by replacement model" {
		t.Fatalf("resume messages=%#v model calls=%d", messages, newCalls.Load())
	}
	checkpoint, found, err = secondStore.Load(context.Background(), "cross-device")
	if err != nil || !found || checkpoint.Status != state.CheckpointStatusReady || checkpoint.Continuation != nil {
		t.Fatalf("completed checkpoint = %#v, found=%t, err=%v", checkpoint, found, err)
	}
	if len(checkpoint.Messages) != 2 || checkpoint.Messages[0].Text() != "continue" || checkpoint.Messages[1].Text() != "continued by replacement model" {
		t.Fatalf("completed messages = %#v", checkpoint.Messages)
	}
}

func TestInterruptedCheckpointCapturesAndRestoresGraphState(t *testing.T) {
	store := state.NewMemoryStore()
	graphState := state.GraphStateCheckpoint{CodecID: "custom-state", Version: 2, Data: []byte{4, 5, 6}}
	first, err := agent.NewBasicAgent(agent.BaseConfig{
		Model:           llm.NewScriptedModel([]schema.Message{schema.AssistantMessage("unused")}),
		CheckpointStore: store,
		Breakpointers: []agent.Breakpointer{agent.BreakpointFunc(func(context.Context, agent.Breakpoint) (agent.InterruptDecision, error) {
			return agent.InterruptDecision{Interrupt: true, Reason: "persist state"}, nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := state.WithGraphStateCheckpoint(context.Background(), graphState)
	if _, err := first.Invoke(ctx, []schema.Message{schema.UserMessage("pause")}, agent.WithConversationID("graph-state-resume")); !errors.Is(err, agent.ErrInterrupted) {
		t.Fatalf("initial Invoke error = %v", err)
	}
	checkpoint, found, err := store.Load(context.Background(), "graph-state-resume")
	if err != nil || !found || checkpoint.Continuation == nil || checkpoint.Continuation.GraphState == nil {
		t.Fatalf("checkpoint = %#v, found=%t, err=%v", checkpoint, found, err)
	}
	if checkpoint.Continuation.GraphState.CodecID != graphState.CodecID || string(checkpoint.Continuation.GraphState.Data) != string(graphState.Data) {
		t.Fatalf("captured graph state = %#v", checkpoint.Continuation.GraphState)
	}

	resumed, err := agent.NewBasicAgent(agent.BaseConfig{
		Model: llm.NewHandlerModel(func(ctx context.Context, _ []schema.Message, _ llm.ChatOptions) ([]schema.Message, error) {
			restored, ok := state.GraphStateCheckpointFromContext(ctx)
			if !ok || restored.CodecID != graphState.CodecID || string(restored.Data) != string(graphState.Data) {
				t.Fatalf("restored graph state = %#v, ok=%t", restored, ok)
			}
			return []schema.Message{schema.AssistantMessage("resumed")}, nil
		}),
		CheckpointStore: store,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agent.Resume(context.Background(), resumed, "graph-state-resume"); err != nil {
		t.Fatal(err)
	}
}

func TestInterruptSignalPersistsFinalResponseWithoutRepeatingModelOnResume(t *testing.T) {
	store := state.NewMemoryStore()
	signal := agent.NewInterruptSignal()
	started := make(chan struct{})
	release := make(chan struct{})
	model := llm.NewHandlerModel(func(context.Context, []schema.Message, llm.ChatOptions) ([]schema.Message, error) {
		close(started)
		<-release
		return []schema.Message{schema.AssistantMessage("finished before reconnect")}, nil
	})
	value, err := agent.NewBasicAgent(agent.BaseConfig{Model: model, CheckpointStore: store})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := value.Invoke(
			context.Background(), []schema.Message{schema.UserMessage("long request")},
			agent.WithConversationID("final-response"), agent.WithInterruptSignal(signal),
		)
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("model did not start")
	}
	signal.Interrupt("client disconnected", map[string]any{"transport": "websocket"})
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, agent.ErrInterrupted) {
			t.Fatalf("interrupted run error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("interrupted run did not finish")
	}
	checkpoint, found, err := store.Load(context.Background(), "final-response")
	if err != nil || !found || checkpoint.Continuation == nil || checkpoint.Continuation.Point != string(agent.BreakpointBeforeCommit) {
		t.Fatalf("checkpoint = %#v, found=%t, err=%v", checkpoint, found, err)
	}
	if len(checkpoint.Continuation.Response) != 1 || checkpoint.Continuation.Response[0].Text() != "finished before reconnect" {
		t.Fatalf("continuation response = %#v", checkpoint.Continuation.Response)
	}
	var resumedCalls atomic.Int32
	resumed, err := agent.NewBasicAgent(agent.BaseConfig{
		Model: llm.NewHandlerModel(func(context.Context, []schema.Message, llm.ChatOptions) ([]schema.Message, error) {
			resumedCalls.Add(1)
			return nil, errors.New("resume should not invoke model")
		}),
		CheckpointStore: store,
	})
	if err != nil {
		t.Fatal(err)
	}
	messages, err := agent.Resume(context.Background(), resumed, "final-response")
	if err != nil {
		t.Fatal(err)
	}
	if resumedCalls.Load() != 0 || len(messages) != 1 || messages[0].Text() != "finished before reconnect" {
		t.Fatalf("resume messages=%#v model calls=%d", messages, resumedCalls.Load())
	}
}

func TestReactResumeDoesNotRepeatCompletedTool(t *testing.T) {
	store := state.NewMemoryStore()
	var toolCalls atomic.Int32
	echo := tool.NewFunctionTool(tool.Spec{Name: "echo", IsReadOnly: true}, func(ctx context.Context, input tool.CallInput) (tool.Result, error) {
		toolCalls.Add(1)
		return tool.TextResult("tool-result"), nil
	})
	firstModel := llm.NewScriptedModel([]schema.Message{functionCallMessage("echo", "call-1")})
	interrupted, err := agent.New(agent.BaseConfig{
		Model:           firstModel,
		Tools:           []tool.Tool{echo},
		CheckpointStore: store,
		Breakpointers: []agent.Breakpointer{agent.BreakpointFunc(func(_ context.Context, point agent.Breakpoint) (agent.InterruptDecision, error) {
			return agent.InterruptDecision{Interrupt: point.LLMCalls == 1, Reason: "change model after tools"}, nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = interrupted.Invoke(context.Background(), []schema.Message{schema.UserMessage("call echo")}, agent.WithConversationID("tool-resume"))
	if !errors.Is(err, agent.ErrInterrupted) || toolCalls.Load() != 1 {
		t.Fatalf("initial error=%v tool calls=%d", err, toolCalls.Load())
	}
	resumed, err := agent.New(agent.BaseConfig{
		Model:           llm.NewScriptedModel([]schema.Message{schema.AssistantMessage("final answer")}),
		Tools:           []tool.Tool{echo},
		CheckpointStore: store,
	})
	if err != nil {
		t.Fatal(err)
	}
	messages, err := agent.Resume(context.Background(), resumed, "tool-resume")
	if err != nil {
		t.Fatal(err)
	}
	if toolCalls.Load() != 1 || len(messages) != 3 || messages[len(messages)-1].Text() != "final answer" {
		t.Fatalf("resume messages=%#v tool calls=%d", messages, toolCalls.Load())
	}
}

func TestReactResumeRehydratesToolTurnsFromMessages(t *testing.T) {
	store := state.NewMemoryStore()
	echo := tool.NewFunctionTool(tool.Spec{Name: "echo", IsReadOnly: true}, func(context.Context, tool.CallInput) (tool.Result, error) {
		return tool.TextResult("tool-result"), nil
	})
	firstModel := llm.NewScriptedModel(
		[]schema.Message{schema.AssistantMessage("Thought: call echo\nAction: echo\nAction Input: {}")},
		[]schema.Message{schema.AssistantMessage("final answer")},
	)
	interrupted, err := agent.NewReactAgent(agent.ReactAgentConfig{BaseConfig: agent.BaseConfig{
		Model:           firstModel,
		Tools:           []tool.Tool{echo},
		CheckpointStore: store,
		Breakpointers: []agent.Breakpointer{agent.BreakpointFunc(func(_ context.Context, point agent.Breakpoint) (agent.InterruptDecision, error) {
			return agent.InterruptDecision{Interrupt: point.LLMCalls == 1, Reason: "rehydrate"}, nil
		})},
	}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = interrupted.Invoke(context.Background(), []schema.Message{schema.UserMessage("call echo")}, agent.WithConversationID("react-rehydrate"))
	if !errors.Is(err, agent.ErrInterrupted) {
		t.Fatalf("initial Invoke error = %v", err)
	}
	checkpoint, found, err := store.Load(context.Background(), "react-rehydrate")
	if err != nil || !found || checkpoint.Continuation == nil {
		t.Fatalf("checkpoint = %#v, found=%t, err=%v", checkpoint, found, err)
	}
	if checkpoint.Continuation.Engine != nil {
		t.Fatalf("ReAct unexpectedly persisted non-derivable engine state: %#v", checkpoint.Continuation.Engine)
	}
	checkpoint.Continuation.LLMCalls = 0
	if _, err := store.Save(context.Background(), checkpoint); err != nil {
		t.Fatalf("rewrite checkpoint = %v", err)
	}

	var resumedCalls atomic.Int32
	resumed, err := agent.NewReactAgent(agent.ReactAgentConfig{BaseConfig: agent.BaseConfig{
		Model: llm.NewHandlerModel(func(context.Context, []schema.Message, llm.ChatOptions) ([]schema.Message, error) {
			resumedCalls.Add(1)
			return []schema.Message{schema.AssistantMessage("must not run")}, nil
		}),
		Tools:           []tool.Tool{echo},
		CheckpointStore: store,
	}})
	if err != nil {
		t.Fatal(err)
	}
	messages, err := agent.Resume(context.Background(), resumed, "react-rehydrate", agent.WithRunMaxLLMCalls(1))
	if err != nil {
		t.Fatal(err)
	}
	if resumedCalls.Load() != 0 || len(messages) != 2 || !strings.Contains(messages[1].Text(), "Observation: tool-result") {
		t.Fatalf("rehydrated messages=%#v model calls=%d", messages, resumedCalls.Load())
	}
}

func TestInterruptedConversationRequiresResume(t *testing.T) {
	store := state.NewMemoryStore()
	value, err := agent.NewBasicAgent(agent.BaseConfig{
		Model:           llm.NewScriptedModel(),
		CheckpointStore: store,
		Breakpointers: []agent.Breakpointer{agent.BreakpointFunc(func(context.Context, agent.Breakpoint) (agent.InterruptDecision, error) {
			return agent.InterruptDecision{Interrupt: true}, nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = value.Invoke(context.Background(), []schema.Message{schema.UserMessage("pause")}, agent.WithConversationID("resume-required"))
	_, err = value.Invoke(context.Background(), []schema.Message{schema.UserMessage("new input")}, agent.WithConversationID("resume-required"))
	if !errors.Is(err, agent.ErrResumeRequired) {
		t.Fatalf("Invoke error = %v", err)
	}
}

func TestBreakpointerPanicBecomesRunError(t *testing.T) {
	store := state.NewMemoryStore()
	value, err := agent.NewBasicAgent(agent.BaseConfig{
		Model:           llm.NewScriptedModel(),
		CheckpointStore: store,
		Breakpointers: []agent.Breakpointer{agent.BreakpointFunc(func(context.Context, agent.Breakpoint) (agent.InterruptDecision, error) {
			panic("private extension failed")
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = value.Invoke(context.Background(), []schema.Message{schema.UserMessage("run")}, agent.WithConversationID("panic"))
	if err == nil || errors.Is(err, agent.ErrInterrupted) {
		t.Fatalf("Invoke error = %v", err)
	}
}
