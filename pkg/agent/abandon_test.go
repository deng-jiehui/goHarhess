package agent_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/tool"
)

// 确定性失败后，新输入可以显式放弃挂起的运行继续对话，失败的一轮不进入会话。
func TestNewInputCanAbandonFailedRun(t *testing.T) {
	ctx := context.Background()
	store := state.NewMemoryStore()
	var fail atomic.Bool
	fail.Store(true)
	a, err := agent.New(llm.NewHandlerModel(func(_ context.Context, request llm.Request, _ llm.Options) ([]schema.Message, error) {
		if fail.Load() {
			return nil, errors.New("provider rejected input")
		}
		return []schema.Message{schema.AssistantMessage("answer to " + request.Messages[len(request.Messages)-1].JoinedText(""))}, nil
	}), agent.WithCheckpointStore(store))
	if err != nil {
		t.Fatal(err)
	}
	option := agent.WithConversationID("recover")
	if _, err := a.Invoke(ctx, []schema.Message{schema.UserMessage("bad question")}, option); err == nil {
		t.Fatal("expected failure")
	}
	fail.Store(false)
	if _, err := a.Invoke(ctx, []schema.Message{schema.UserMessage("new question")}, option); !errors.Is(err, state.ErrUnfinishedRun) {
		t.Fatalf("new input without abandon err=%v", err)
	}
	output, err := a.Invoke(ctx, []schema.Message{schema.UserMessage("new question")}, option, agent.WithAbandonUnfinished())
	if err != nil || len(output) != 1 || output[0].JoinedText("") != "answer to new question" {
		t.Fatalf("output=%q err=%v", messageTexts(output), err)
	}
	latest, _, err := store.Load(ctx, "recover")
	if err != nil || latest.Status != state.StatusCompleted || fmt.Sprint(messageTexts(latest.Messages)) != "[new question answer to new question]" {
		t.Fatalf("conversation=%q status=%s err=%v", messageTexts(latest.Messages), latest.Status, err)
	}
}

// 等待人工输入时，新输入也可以放弃挂起的审批，之后旧中断无法再恢复。
func TestNewInputCanAbandonPendingInterrupt(t *testing.T) {
	ctx := context.Background()
	store := state.NewMemoryStore()
	var toolCalls atomic.Int32
	value, err := agent.New(llm.NewHandlerModel(func(_ context.Context, request llm.Request, _ llm.Options) ([]schema.Message, error) {
		if request.Messages[len(request.Messages)-1].JoinedText("") == "run tool" {
			return []schema.Message{functionCallMessage("echo", "call-1")}, nil
		}
		return []schema.Message{schema.AssistantMessage("plain answer")}, nil
	}), agent.WithCheckpointStore(store), agent.WithTools(tool.NewFunctionTool(tool.Spec{Name: "echo"}, func(context.Context, tool.CallInput) (tool.Result, error) {
		toolCalls.Add(1)
		return tool.TextResult("ok"), nil
	})), agent.WithBeforeToolBatch(tool.BeforeBatchFunc(func(ctx context.Context, batch tool.Batch) error {
		_, err := graph.Interrupt(ctx, map[string]any{"approve": len(batch.Calls)})
		return err
	})))
	if err != nil {
		t.Fatal(err)
	}
	option := agent.WithConversationID("pending")
	_, err = value.Invoke(ctx, []schema.Message{schema.UserMessage("run tool")}, option)
	var suspended *graph.InterruptedError
	if !errors.As(err, &suspended) {
		t.Fatalf("err=%v", err)
	}
	pending := suspended.PendingInterrupts()
	output, err := value.Invoke(ctx, []schema.Message{schema.UserMessage("never mind")}, option, agent.WithAbandonUnfinished())
	if err != nil || len(output) != 1 || output[0].JoinedText("") != "plain answer" {
		t.Fatalf("output=%q err=%v", messageTexts(output), err)
	}
	if _, err := agent.Resume(ctx, value, option, agent.WithResume(pending[0].ID, true)); err == nil {
		t.Fatal("abandoned interrupt was resumed")
	}
	if toolCalls.Load() != 0 {
		t.Fatalf("tool calls=%d", toolCalls.Load())
	}
}
