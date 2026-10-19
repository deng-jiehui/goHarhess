package subagent_test

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goharness/goharness/extensions/subagent"
	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/contextengine"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/stream"
	"github.com/goharness/goharness/pkg/tool"
)

// --- MA-1：AsTool ---

// captureModel 记录专家实际收到的消息并回显最后一条文本。
func captureModel(seen *[]schema.Message) llm.ChatModel {
	return llm.NewHandlerModel(func(_ context.Context, req llm.Request, _ llm.Options) ([]schema.Message, error) {
		for _, message := range req.Messages {
			if !contextengine.IsSystemReminder(message) {
				*seen = append(*seen, message)
			}
		}
		return []schema.Message{schema.AssistantMessage("echo:" + lastBusinessMessage(req.Messages).JoinedText(""))}, nil
	})
}

func expertAgent(t *testing.T, seen *[]schema.Message, tools ...tool.Tool) agent.Agent {
	t.Helper()
	opts := []agent.Option{agent.WithName("researcher")}
	if len(tools) > 0 {
		opts = append(opts, agent.WithTools(tools...))
	}
	a, err := agent.New(captureModel(seen), opts...)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func parentWithTool(t *testing.T, store state.Store, expert tool.Tool, steps ...step) (agent.Agent, *parentRun) {
	t.Helper()
	run := &parentRun{}
	model := llm.NewHandlerModel(func(_ context.Context, req llm.Request, _ llm.Options) ([]schema.Message, error) {
		results := toolOutputs(req)
		run.mu.Lock()
		run.last = results
		run.mu.Unlock()
		if len(results) < len(steps) {
			return []schema.Message{steps[len(results)](results)}, nil
		}
		return []schema.Message{schema.AssistantMessage("done")}, nil
	})
	a, err := agent.New(model, agent.WithName("parent"), agent.WithTools(expert))
	if err != nil {
		t.Fatal(err)
	}
	return a, run
}

// 默认专家只收到任务描述：父的系统提示、历史与其他工具输出都不泄露。
func TestAsToolPassesOnlyTask(t *testing.T) {
	store := state.NewMemoryStore()
	var seen []schema.Message
	expert := subagent.AsTool(expertAgent(t, &seen), "researcher", "调研专家")
	parent, run := parentWithTool(t, store, expert,
		func([]toolOut) schema.Message { return toolCall("c1", "researcher", map[string]any{"task": "调研"}) })
	out, err := parent.Invoke(context.Background(),
		[]schema.Message{schema.SystemMessage("父系统提示"), schema.UserMessage("go")},
		runOpts(store, "conv")...)
	if err != nil {
		t.Fatal(err)
	}
	if out[len(out)-1].JoinedText("") != "done" {
		t.Fatalf("out=%q", out[len(out)-1].JoinedText(""))
	}
	if len(seen) != 1 || seen[0].Role != schema.RoleUser || seen[0].JoinedText("") != "调研" {
		t.Fatalf("expert input=%+v", seen)
	}
	if got := run.seen()[0].Text; got != "echo:调研" {
		t.Fatalf("result=%q", got)
	}
}

// WithHistory 显式传入过滤后的历史：专家看到过滤结果加任务消息。
func TestAsToolHistoryFilter(t *testing.T) {
	store := state.NewMemoryStore()
	var seen []schema.Message
	expert := subagent.AsTool(expertAgent(t, &seen), "researcher", "调研专家",
		subagent.WithHistory(subagent.DropSystem()))
	parent, run := parentWithTool(t, store, expert,
		func([]toolOut) schema.Message { return toolCall("c1", "researcher", map[string]any{"task": "调研"}) })
	_, err := parent.Invoke(context.Background(),
		[]schema.Message{schema.SystemMessage("父系统提示"), schema.UserMessage("go")},
		runOpts(store, "conv")...)
	if err != nil {
		t.Fatal(err)
	}
	if len(seen) == 0 || seen[len(seen)-1].JoinedText("") != "调研" {
		t.Fatalf("expert input=%+v tool_results=%+v", seen, run.seen())
	}
	for _, m := range seen {
		if m.Role == schema.RoleSystem {
			t.Fatalf("system message leaked: %+v", seen)
		}
	}
	// 历史在任务之前，包含父的用户消息与工具调用消息。
	if len(seen) != 3 {
		t.Fatalf("expert input len=%d %+v", len(seen), seen)
	}
}

// WithInputSchema 用自定义参数结构组装专家输入。
func TestAsToolInputSchema(t *testing.T) {
	store := state.NewMemoryStore()
	var seen []schema.Message
	expert := subagent.AsTool(expertAgent(t, &seen), "reviewer", "审查专家",
		subagent.WithInputSchema(
			map[string]any{
				"type":       "object",
				"properties": map[string]any{"file": map[string]any{"type": "string"}},
				"required":   []string{"file"},
			},
			func(args map[string]any) ([]schema.Message, error) {
				file, _ := args["file"].(string)
				return []schema.Message{schema.UserMessage("review " + file)}, nil
			}))
	parent, run := parentWithTool(t, store, expert,
		func([]toolOut) schema.Message { return toolCall("c1", "reviewer", map[string]any{"file": "a.go"}) })
	_, err := parent.Invoke(context.Background(), []schema.Message{schema.UserMessage("go")}, runOpts(store, "conv")...)
	if err != nil {
		t.Fatal(err)
	}
	if got := run.seen()[0].Text; got != "echo:review a.go" {
		t.Fatalf("result=%q", got)
	}
}

// 专家内审批挂起上报到父运行，Resume 答复后续跑（嵌套执行语义不变）。
func TestAsToolApprovalSuspendsAndResumes(t *testing.T) {
	store := state.NewMemoryStore()
	var seen []schema.Message
	ask := tool.NewFunctionTool(tool.Spec{Name: "ask"}, func(ctx context.Context, _ tool.CallInput) (tool.Result, error) {
		answer, err := graph.Interrupt(ctx, "approve?")
		if err != nil {
			return tool.Result{}, err
		}
		return tool.TextResult("answered:" + answer.(string)), nil
	})
	// 专家模型：先调 ask，再把结果回显。
	var calls atomic.Int32
	model := llm.NewHandlerModel(func(_ context.Context, req llm.Request, _ llm.Options) ([]schema.Message, error) {
		if calls.Add(1) == 1 {
			return []schema.Message{toolCall("x1", "ask", map[string]any{})}, nil
		}
		for _, m := range req.Messages {
			for _, r := range m.FunctionToolResults() {
				for _, b := range r.ContentBlocks {
					if b.Text != nil {
						return []schema.Message{schema.AssistantMessage("final:" + b.Text.Text)}, nil
					}
				}
			}
		}
		return []schema.Message{schema.AssistantMessage("final")}, nil
	})
	expert, err := agent.New(model, agent.WithName("researcher"), agent.WithTools(ask))
	if err != nil {
		t.Fatal(err)
	}
	parent, run := parentWithTool(t, store, subagent.AsTool(expert, "researcher", "调研专家"),
		func([]toolOut) schema.Message { return toolCall("c1", "researcher", map[string]any{"task": "调研"}) })
	_, err = parent.Invoke(context.Background(), []schema.Message{schema.UserMessage("go")}, runOpts(store, "conv")...)
	var suspended *graph.InterruptedError
	if !errors.As(err, &suspended) || len(suspended.PendingInterrupts()) != 1 {
		t.Fatalf("err=%v", err)
	}
	out, err := agent.Resume(context.Background(), parent,
		append(runOpts(store, "conv"), agent.WithResume(suspended.PendingInterrupts()[0].ID, "yes"))...)
	if err != nil {
		t.Fatal(err)
	}
	if out[len(out)-1].JoinedText("") != "done" {
		t.Fatalf("out=%q", out[len(out)-1].JoinedText(""))
	}
	if got := run.seen()[0].Text; got != "final:answered:yes" {
		t.Fatalf("result=%q", got)
	}
	_ = seen
}

// --- MA-4：事件透传默认关闭 ---

// 收集流里的消息文本。
func streamTexts(t *testing.T, r *stream.Reader[[]schema.Message]) []string {
	t.Helper()
	var texts []string
	for {
		e, err := r.Recv()
		if errors.Is(err, io.EOF) {
			return texts
		}
		if err != nil {
			t.Fatal(err)
		}
		if e.Message != nil && e.Message.Snapshot != nil {
			texts = append(texts, e.Message.Snapshot.JoinedText(""))
		}
		if e.Message != nil && e.Message.Delta != nil && e.Message.Delta.Block != nil && e.Message.Delta.Block.Text != nil {
			texts = append(texts, e.Message.Delta.Block.Text.Text)
		}
	}
}

func hasText(texts []string, want string) bool {
	for _, text := range texts {
		if text == want {
			return true
		}
	}
	return false
}

// 默认专家事件不进父的公共流；WithEmitEvents 打开后可见。
func TestAsToolEventPassthrough(t *testing.T) {
	store := state.NewMemoryStore()
	var seen []schema.Message

	off := subagent.AsTool(expertAgent(t, &seen), "researcher_off", "调研专家")
	parentOff, _ := parentWithTool(t, store, off,
		func([]toolOut) schema.Message {
			return toolCall("c1", "researcher_off", map[string]any{"task": "调研"})
		})
	r, err := agent.StreamEvents(context.Background(), parentOff,
		[]schema.Message{schema.UserMessage("go")}, stream.Config{}, runOpts(store, "conv-off")...)
	if err != nil {
		t.Fatal(err)
	}
	if hasText(streamTexts(t, r), "echo:调研") {
		t.Fatal("默认透传了子 Agent 事件")
	}
	r.Close()

	on := subagent.AsTool(expertAgent(t, &seen), "researcher_on", "调研专家", subagent.WithEmitEvents())
	parentOn, _ := parentWithTool(t, store, on,
		func([]toolOut) schema.Message {
			return toolCall("c1", "researcher_on", map[string]any{"task": "调研"})
		})
	r, err = agent.StreamEvents(context.Background(), parentOn,
		[]schema.Message{schema.UserMessage("go")}, stream.Config{}, runOpts(store, "conv-on")...)
	if err != nil {
		t.Fatal(err)
	}
	if !hasText(streamTexts(t, r), "echo:调研") {
		t.Fatal("WithEmitEvents 后仍看不到子 Agent 事件")
	}
	r.Close()
}

// Manager 的通用工具同样默认不透传；Config.EmitEvents 打开。
func TestManagerEventPassthrough(t *testing.T) {
	store := state.NewMemoryStore()
	cfg := baseConfig(store)
	m := newManager(t, cfg)
	run := &parentRun{}
	parent := newParent(t, m, run, func([]toolOut) schema.Message {
		return toolCall("c1", "run_subagent", map[string]any{"task": "调研"})
	})
	r, err := agent.StreamEvents(context.Background(), parent,
		[]schema.Message{schema.UserMessage("go")}, stream.Config{}, runOpts(store, "conv-mgr-off")...)
	if err != nil {
		t.Fatal(err)
	}
	if hasText(streamTexts(t, r), "echo:调研") {
		t.Fatal("默认透传了子 Agent 事件")
	}
	r.Close()

	cfg.EmitEvents = true
	m = newManager(t, cfg)
	run = &parentRun{}
	parent = newParent(t, m, run, func([]toolOut) schema.Message {
		return toolCall("c1", "run_subagent", map[string]any{"task": "调研"})
	})
	r, err = agent.StreamEvents(context.Background(), parent,
		[]schema.Message{schema.UserMessage("go")}, stream.Config{}, runOpts(store, "conv-mgr-on")...)
	if err != nil {
		t.Fatal(err)
	}
	if !hasText(streamTexts(t, r), "echo:调研") {
		t.Fatal("EmitEvents 后仍看不到子 Agent 事件")
	}
	r.Close()
}

// --- MA-3：单任务限制 ---

func runOnceTool(t *testing.T, cfg subagent.Config) tool.Tool {
	t.Helper()
	m := newManager(t, cfg)
	for _, tt := range m.Tools() {
		if tt.Spec().Name == "run_subagent" {
			return tt
		}
	}
	t.Fatal("run_subagent tool missing")
	return nil
}

// 任务超时返回模型可修正的错误结果，不是硬错误。
func TestTaskTimeout(t *testing.T) {
	cfg := subagent.Config{
		Models: map[string]llm.ChatModel{"main": llm.NewHandlerModel(func(ctx context.Context, _ llm.Request, _ llm.Options) ([]schema.Message, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(500 * time.Millisecond):
				return []schema.Message{schema.AssistantMessage("late")}, nil
			}
		})},
		DefaultModel: "main",
		TaskTimeout:  50 * time.Millisecond,
	}
	tt := runOnceTool(t, cfg)
	result, err := tt.Invoke(context.Background(), tool.CallInput{Args: map[string]any{"task": "x"}})
	if err != nil {
		t.Fatal(err)
	}
	if !result.IsError {
		t.Fatalf("result=%+v", result)
	}
	blocks, _ := result.ToContentBlocks()
	if got := blocks[0].Text.Text; got == "" || !contains(got, "timed out") {
		t.Fatalf("result text=%q", got)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}

// 模型调用预算超限返回模型可修正的错误结果，而不是静默截断的输出。
func TestMaxLLMCallsLimit(t *testing.T) {
	cfg := subagent.Config{
		Models: map[string]llm.ChatModel{"main": llm.NewHandlerModel(func(context.Context, llm.Request, llm.Options) ([]schema.Message, error) {
			return []schema.Message{toolCall("x1", "noop", map[string]any{})}, nil
		})},
		DefaultModel: "main",
		Tools: []tool.Tool{tool.NewFunctionTool(tool.Spec{Name: "noop"},
			func(context.Context, tool.CallInput) (tool.Result, error) { return tool.TextResult("ok"), nil })},
		DefaultTools: []string{"noop"},
		MaxLLMCalls:  1,
	}
	tt := runOnceTool(t, cfg)
	result, err := tt.Invoke(context.Background(), tool.CallInput{Args: map[string]any{"task": "x"}})
	if err != nil {
		t.Fatal(err)
	}
	if !result.IsError {
		t.Fatalf("result=%+v", result)
	}
	blocks, _ := result.ToContentBlocks()
	if got := blocks[0].Text.Text; !contains(got, "model-call limit") {
		t.Fatalf("result text=%q", got)
	}
}

// 正常任务不受限制影响。
func TestLimitsLeaveNormalTasks(t *testing.T) {
	cfg := subagent.Config{
		Models:       map[string]llm.ChatModel{"main": echoModel()},
		DefaultModel: "main",
		TaskTimeout:  5 * time.Second,
		MaxLLMCalls:  5,
	}
	tt := runOnceTool(t, cfg)
	result, err := tt.Invoke(context.Background(), tool.CallInput{Args: map[string]any{"task": "hi"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError {
		blocks, _ := result.ToContentBlocks()
		t.Fatalf("unexpected error result: %s", blocks[0].Text.Text)
	}
}
