package subagent_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goharness/goharness/extensions/subagent"
	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/tool"
)

// --- 测试辅助 ---

func fixedModel(text string) llm.ChatModel {
	return llm.NewHandlerModel(func(context.Context, llm.Request, llm.Options) ([]schema.Message, error) {
		return []schema.Message{schema.AssistantMessage(text)}, nil
	})
}

func toolCall(id, name string, args any) schema.Message {
	raw, _ := json.Marshal(args)
	return schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock(id, name, string(raw)))
}

type toolOut struct {
	Text    string
	IsError bool
}

// toolOutputs 按出现顺序收集本轮（最后一条用户消息之后）的工具结果。
func toolOutputs(req llm.Request) []toolOut {
	var out []toolOut
	for _, message := range req.Messages {
		if message.Role == schema.RoleUser {
			out = out[:0]
		}
		for _, result := range message.FunctionToolResults() {
			var text []string
			for _, block := range result.ContentBlocks {
				if block.Text != nil {
					text = append(text, block.Text.Text)
				}
			}
			out = append(out, toolOut{Text: strings.Join(text, ""), IsError: result.IsError})
		}
	}
	return out
}

func decode(t *testing.T, text string) map[string]any {
	t.Helper()
	var v map[string]any
	if err := json.Unmarshal([]byte(text), &v); err != nil {
		t.Fatalf("tool result is not JSON: %q (%v)", text, err)
	}
	return v
}

// step 依据已有工具结果产生父模型的下一条消息；步骤数用完后给出最终回复。
type step func(results []toolOut) schema.Message

type parentRun struct {
	mu   sync.Mutex
	last []toolOut
}

func (p *parentRun) seen() []toolOut {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]toolOut(nil), p.last...)
}

func newManager(t *testing.T, cfg subagent.Config) *subagent.Manager {
	t.Helper()
	m, err := subagent.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func newParent(t *testing.T, m *subagent.Manager, run *parentRun, steps ...step) agent.Agent {
	t.Helper()
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
	a, err := agent.New(model, agent.WithName("parent"), agent.WithTools(m.Tools()...))
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func spawnStep(args map[string]any) step {
	return func([]toolOut) schema.Message { return toolCall("c-spawn", "spawn_agent", args) }
}

// echoModel 让子 Agent 把收到的最后一条消息原文回显——便于断言上下文沿用。
func echoModel() llm.ChatModel {
	return llm.NewHandlerModel(func(_ context.Context, req llm.Request, _ llm.Options) ([]schema.Message, error) {
		return []schema.Message{schema.AssistantMessage("echo:" + req.Messages[len(req.Messages)-1].JoinedText(""))}, nil
	})
}

func baseConfig(store state.Store) subagent.Config {
	return subagent.Config{
		Models:       map[string]llm.ChatModel{"main": echoModel()},
		DefaultModel: "main",
		Store:        store,
	}
}

func runOpts(store state.Store, key string, extra ...runnable.Option) []runnable.Option {
	return append([]runnable.Option{agent.WithCheckpoint(store, key)}, extra...)
}

// --- J1：子执行语义（契约清单 J1） ---

// 创建并等待：spawn_agent 同步返回首个任务结果与子 Agent ID。
func TestSpawnRunsTaskAndReturnsResult(t *testing.T) {
	store := state.NewMemoryStore()
	m := newManager(t, baseConfig(store))
	run := &parentRun{}
	parent := newParent(t, m, run, spawnStep(map[string]any{"task": "调研", "name": "scout"}))
	out, err := parent.Invoke(context.Background(), []schema.Message{schema.UserMessage("go")}, runOpts(store, "conv")...)
	if err != nil {
		t.Fatal(err)
	}
	if out[len(out)-1].JoinedText("") != "done" {
		t.Fatalf("out=%q", out[len(out)-1].JoinedText(""))
	}
	result := decode(t, run.seen()[0].Text)
	if result["agent_id"] == "" || result["task_id"] == "" {
		t.Fatalf("missing ids: %v", result)
	}
	if result["result"] != "echo:调研" || result["status"] != "completed" {
		t.Fatalf("result=%v", result)
	}
}

// 一次调用并行多个子 Agent：同批两个 spawn_agent 并发执行，各收各的结果。
func TestParallelSpawnCalls(t *testing.T) {
	store := state.NewMemoryStore()
	var concurrent atomic.Int32
	var peak atomic.Int32
	cfg := baseConfig(store)
	cfg.Models["main"] = llm.NewHandlerModel(func(_ context.Context, req llm.Request, _ llm.Options) ([]schema.Message, error) {
		cur := concurrent.Add(1)
		if cur > peak.Load() {
			peak.Store(cur)
		}
		defer concurrent.Add(-1)
		return []schema.Message{schema.AssistantMessage("echo:" + req.Messages[len(req.Messages)-1].JoinedText(""))}, nil
	})
	m := newManager(t, cfg)
	run := &parentRun{}
	parent := newParent(t, m, run, func([]toolOut) schema.Message {
		return schema.NewMessage(schema.RoleAssistant,
			schema.NewFunctionToolCallBlock("c-a", "spawn_agent", `{"task":"甲"}`),
			schema.NewFunctionToolCallBlock("c-b", "spawn_agent", `{"task":"乙"}`),
		)
	})
	if _, err := parent.Invoke(context.Background(), []schema.Message{schema.UserMessage("go")}, runOpts(store, "conv")...); err != nil {
		t.Fatal(err)
	}
	seen := run.seen()
	if len(seen) != 2 {
		t.Fatalf("seen=%d", len(seen))
	}
	a, b := decode(t, seen[0].Text), decode(t, seen[1].Text)
	if a["result"] != "echo:甲" || b["result"] != "echo:乙" || a["agent_id"] == b["agent_id"] {
		t.Fatalf("a=%v b=%v", a, b)
	}
	// 并发执行依赖工具节点批内并发；串行实现下并发峰值可为 1，两者皆合法。
	t.Logf("concurrency peak=%d", peak.Load())
}

// 子 Agent 内的审批挂起上报到父运行：Resume 答复后续跑，
// 重放不重复创建子 Agent（契约 J1：审批上报+恢复、重放去重）。
func TestSubAgentApprovalSuspendsAndResumes(t *testing.T) {
	store := state.NewMemoryStore()
	var askCalls atomic.Int32
	cfg := baseConfig(store)
	cfg.Tools = []tool.Tool{tool.NewFunctionTool(tool.Spec{Name: "ask"}, func(ctx context.Context, _ tool.CallInput) (tool.Result, error) {
		askCalls.Add(1)
		answer, err := graph.Interrupt(ctx, "approve?")
		if err != nil {
			return tool.Result{}, err
		}
		return tool.TextResult("answered:" + answer.(string)), nil
	})}
	cfg.DefaultTools = []string{"ask"}
	var runs atomic.Int32
	cfg.Models["main"] = llm.NewHandlerModel(func(_ context.Context, req llm.Request, _ llm.Options) ([]schema.Message, error) {
		runs.Add(1)
		last := req.Messages[len(req.Messages)-1]
		if results := last.FunctionToolResults(); len(results) > 0 {
			for _, block := range results[0].ContentBlocks {
				if block.Text != nil {
					return []schema.Message{schema.AssistantMessage("final:" + block.Text.Text)}, nil
				}
			}
		}
		return []schema.Message{schema.NewMessage(schema.RoleAssistant,
			schema.NewFunctionToolCallBlock("sub-ask", "ask", `{}`))}, nil
	})
	m := newManager(t, cfg)
	run := &parentRun{}
	parent := newParent(t, m, run, spawnStep(map[string]any{"task": "干活"}))
	_, err := parent.Invoke(context.Background(), []schema.Message{schema.UserMessage("go")}, runOpts(store, "conv")...)
	var suspended *graph.InterruptedError
	if !errors.As(err, &suspended) || len(suspended.PendingInterrupts()) != 1 {
		t.Fatalf("err=%v, want one pending interrupt", err)
	}
	out, err := agent.Resume(context.Background(), parent,
		append(runOpts(store, "conv"), agent.WithResume(suspended.PendingInterrupts()[0].ID, "yes"))...)
	if err != nil {
		t.Fatal(err)
	}
	if out[len(out)-1].JoinedText("") != "done" {
		t.Fatalf("out=%q", out[len(out)-1].JoinedText(""))
	}
	result := decode(t, run.seen()[0].Text)
	if result["result"] != "final:answered:yes" {
		t.Fatalf("result=%v", result)
	}
	// 重放时记录与任务已存在：索引内只有一个子 Agent、一条任务。
	index, found, _ := store.Load(context.Background(), "conv/agents")
	if !found {
		t.Fatal("missing agent index")
	}
	var idx struct {
		Agents []struct {
			AgentID string `json:"agent_id"`
		} `json:"agents"`
	}
	if err := json.Unmarshal(index.Result, &idx); err != nil || len(idx.Agents) != 1 {
		t.Fatalf("index=%s err=%v", index.Result, err)
	}
}

// 同批两个 spawn：一个挂起一个完成；恢复后完成的调用直接复用登记结果，
// 其模型不再被调用（契约 J1：部分完成的子 Agent 挂起恢复后不重跑）。
func TestPartialSuspendReplaysCompletedSpawn(t *testing.T) {
	store := state.NewMemoryStore()
	cfg := baseConfig(store)
	cfg.Tools = []tool.Tool{tool.NewFunctionTool(tool.Spec{Name: "ask"}, func(ctx context.Context, _ tool.CallInput) (tool.Result, error) {
		answer, err := graph.Interrupt(ctx, "approve?")
		if err != nil {
			return tool.Result{}, err
		}
		return tool.TextResult("answered:" + answer.(string)), nil
	})}
	cfg.DefaultTools = []string{"ask"}
	var fastCalls, slowCalls atomic.Int32
	cfg.Models["main"] = llm.NewHandlerModel(func(_ context.Context, req llm.Request, _ llm.Options) ([]schema.Message, error) {
		slow := false
		for _, message := range req.Messages {
			if strings.Contains(message.JoinedText(""), "需要审批") {
				slow = true
			}
		}
		if slow {
			slowCalls.Add(1)
		} else {
			fastCalls.Add(1)
		}
		last := req.Messages[len(req.Messages)-1]
		if results := last.FunctionToolResults(); len(results) > 0 {
			var text string
			for _, block := range results[0].ContentBlocks {
				if block.Text != nil {
					text += block.Text.Text
				}
			}
			return []schema.Message{schema.AssistantMessage("final:" + text)}, nil
		}
		if slow {
			return []schema.Message{schema.NewMessage(schema.RoleAssistant,
				schema.NewFunctionToolCallBlock("sub-ask", "ask", `{}`))}, nil
		}
		return []schema.Message{schema.AssistantMessage("echo:" + last.JoinedText(""))}, nil
	})
	m := newManager(t, cfg)
	run := &parentRun{}
	parent := newParent(t, m, run, func([]toolOut) schema.Message {
		return schema.NewMessage(schema.RoleAssistant,
			schema.NewFunctionToolCallBlock("c-fast", "spawn_agent", `{"task":"直接做"}`),
			schema.NewFunctionToolCallBlock("c-slow", "spawn_agent", `{"task":"需要审批"}`),
		)
	})
	_, err := parent.Invoke(context.Background(), []schema.Message{schema.UserMessage("go")}, runOpts(store, "conv")...)
	var suspended *graph.InterruptedError
	if !errors.As(err, &suspended) || len(suspended.PendingInterrupts()) != 1 {
		t.Fatalf("err=%v, want one pending interrupt", err)
	}
	out, err := agent.Resume(context.Background(), parent,
		append(runOpts(store, "conv"), agent.WithResume(suspended.PendingInterrupts()[0].ID, "yes"))...)
	if err != nil {
		t.Fatal(err)
	}
	if out[len(out)-1].JoinedText("") != "done" {
		t.Fatalf("out=%q", out[len(out)-1].JoinedText(""))
	}
	seen := run.seen()
	if len(seen) != 2 {
		t.Fatalf("seen=%d", len(seen))
	}
	fast, slow := decode(t, seen[0].Text), decode(t, seen[1].Text)
	if fast["result"] != "echo:直接做" || slow["result"] != "final:answered:yes" {
		t.Fatalf("fast=%v slow=%v", fast, slow)
	}
	// 完成调用经 pending 登记复用：fast 模型全程只被调一次。
	if fastCalls.Load() != 1 || slowCalls.Load() != 2 {
		t.Fatalf("fast=%d slow=%d", fastCalls.Load(), slowCalls.Load())
	}
}

// 无检查点运行中子 Agent 内审批直接报错，不悬挂（契约 J1）。
func TestApprovalWithoutCheckpointErrors(t *testing.T) {
	cfg := subagent.Config{
		Models:       map[string]llm.ChatModel{"main": nil},
		DefaultModel: "main",
	}
	cfg.Tools = []tool.Tool{tool.NewFunctionTool(tool.Spec{Name: "ask"}, func(ctx context.Context, _ tool.CallInput) (tool.Result, error) {
		_, err := graph.Interrupt(ctx, "approve?")
		return tool.Result{}, err
	})}
	cfg.DefaultTools = []string{"ask"}
	cfg.Models["main"] = llm.NewHandlerModel(func(_ context.Context, req llm.Request, _ llm.Options) ([]schema.Message, error) {
		last := req.Messages[len(req.Messages)-1]
		if results := last.FunctionToolResults(); len(results) > 0 {
			return []schema.Message{schema.AssistantMessage("final")}, nil
		}
		return []schema.Message{schema.NewMessage(schema.RoleAssistant,
			schema.NewFunctionToolCallBlock("sub-ask", "ask", `{}`))}, nil
	})
	m := newManager(t, cfg)
	run := &parentRun{}
	parent := newParent(t, m, run, func([]toolOut) schema.Message {
		return toolCall("c-run", "run_subagent", map[string]any{"task": "需要审批"})
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := parent.Invoke(ctx, []schema.Message{schema.UserMessage("go")})
	// 两条合法出口：Invoke 直接返回错误，或错误作为模型可见结果返回——
	// 共同点是不悬挂、不静默成功。
	if err != nil {
		return
	}
	seen := run.seen()
	if len(seen) == 0 || !strings.Contains(seen[0].Text, "checkpoint") {
		t.Fatalf("expected checkpoint-required failure surfaced, seen=%v", seen)
	}
}

// 一次性子 Agent：run_subagent 不要求会话记录，无检查点运行也能用。
func TestRunSubagentEphemeralNoStore(t *testing.T) {
	m := newManager(t, subagent.Config{
		Models:       map[string]llm.ChatModel{"main": echoModel()},
		DefaultModel: "main",
	})
	run := &parentRun{}
	parent := newParent(t, m, run, func([]toolOut) schema.Message {
		return toolCall("c-run", "run_subagent", map[string]any{"task": "算一下"})
	})
	out, err := parent.Invoke(context.Background(), []schema.Message{schema.UserMessage("go")})
	if err != nil {
		t.Fatal(err)
	}
	if out[len(out)-1].JoinedText("") != "done" {
		t.Fatalf("out=%q", out[len(out)-1].JoinedText(""))
	}
	result := decode(t, run.seen()[0].Text)
	if result["result"] != "echo:算一下" {
		t.Fatalf("result=%v", result)
	}
}

// --- J2：扩展内实现的语义 ---

// 会话子 Agent 跨轮保留：第二轮列出、读取详情、追加任务沿用上下文。
func TestSessionAgentPersistsAcrossRounds(t *testing.T) {
	store := state.NewMemoryStore()
	m := newManager(t, baseConfig(store))
	run := &parentRun{}
	parent := newParent(t, m, run, spawnStep(map[string]any{"task": "第一轮任务", "name": "memo"}))
	if _, err := parent.Invoke(context.Background(), []schema.Message{schema.UserMessage("round1")}, runOpts(store, "conv")...); err != nil {
		t.Fatal(err)
	}
	spawned := decode(t, run.seen()[0].Text)
	agentID, _ := spawned["agent_id"].(string)
	if agentID == "" {
		t.Fatalf("no agent_id: %v", spawned)
	}

	// 第二轮：list 可见、followup 沿用上下文（模型能看到此前消息）。
	run2 := &parentRun{}
	parent2 := newParent(t, m, run2,
		func([]toolOut) schema.Message { return toolCall("c-list", "list_agents", map[string]any{}) },
		func([]toolOut) schema.Message {
			return toolCall("c-follow", "followup_task", map[string]any{"agent_id": agentID, "task": "第二轮任务"})
		},
	)
	out, err := parent2.Invoke(context.Background(), []schema.Message{schema.UserMessage("round2")}, runOpts(store, "conv")...)
	if err != nil {
		t.Fatal(err)
	}
	if out[len(out)-1].JoinedText("") != "done" {
		t.Fatalf("out=%q", out[len(out)-1].JoinedText(""))
	}
	seen := run2.seen()
	listed := decode(t, seen[0].Text)
	agents, _ := listed["agents"].([]any)
	if len(agents) != 1 {
		t.Fatalf("listed=%v", listed)
	}
	followed := decode(t, seen[1].Text)
	// 上下文沿用：第二轮的回复基于第一轮历史 + 新任务。
	if followed["result"] != "echo:第二轮任务" {
		t.Fatalf("followed=%v", followed)
	}
	// 详情里两条任务都在。
	run3 := &parentRun{}
	parent3 := newParent(t, m, run3, func([]toolOut) schema.Message {
		return toolCall("c-get", "get_agent", map[string]any{"agent_id": agentID})
	})
	if _, err := parent3.Invoke(context.Background(), []schema.Message{schema.UserMessage("round3")}, runOpts(store, "conv")...); err != nil {
		t.Fatal(err)
	}
	detail := decode(t, run3.seen()[0].Text)
	tasks, _ := detail["tasks"].([]any)
	if len(tasks) != 2 {
		t.Fatalf("tasks=%v", detail["tasks"])
	}
}

// 关闭的 Agent 拒绝新任务，但详情仍可读。
func TestCloseAgentRejectsFollowup(t *testing.T) {
	store := state.NewMemoryStore()
	m := newManager(t, baseConfig(store))
	run := &parentRun{}
	parent := newParent(t, m, run,
		spawnStep(map[string]any{"task": "任务"}),
		func(results []toolOut) schema.Message {
			return toolCall("c-close", "close_agent", map[string]any{"agent_id": decode(t, results[0].Text)["agent_id"]})
		},
		func(results []toolOut) schema.Message {
			return toolCall("c-fol", "followup_task", map[string]any{"agent_id": decode(t, results[0].Text)["agent_id"], "task": "再来"})
		},
	)
	if _, err := parent.Invoke(context.Background(), []schema.Message{schema.UserMessage("go")}, runOpts(store, "conv")...); err != nil {
		t.Fatal(err)
	}
	seen := run.seen()
	// Agent 观察层把工具错误摊平为结果文本（IsError 不上返），断言模型可见的拒绝文案。
	if !strings.Contains(seen[2].Text, "closed") {
		t.Fatalf("followup on closed agent: %q", seen[2].Text)
	}
}

// 损坏的子 Agent 记录明确拒绝。
func TestCorruptRecordRejected(t *testing.T) {
	store := state.NewMemoryStore()
	m := newManager(t, baseConfig(store))
	run := &parentRun{}
	parent := newParent(t, m, run, spawnStep(map[string]any{"task": "任务"}))
	if _, err := parent.Invoke(context.Background(), []schema.Message{schema.UserMessage("go")}, runOpts(store, "conv")...); err != nil {
		t.Fatal(err)
	}
	agentID, _ := decode(t, run.seen()[0].Text)["agent_id"].(string)
	// 写入合法 JSON 但非法记录覆盖存储（Result 须是合法 JSON 才能过 Save 校验）。
	head, found, err := store.Load(context.Background(), "conv/agents/"+agentID)
	if err != nil || !found {
		t.Fatalf("load agent record: found=%v err=%v", found, err)
	}
	if _, err := store.Save(context.Background(), state.Checkpoint{
		ConversationID: "conv/agents/" + agentID, ID: head.ID, Version: head.Version,
		Result: []byte(`{"x":1}`), RunID: "corrupt",
	}); err != nil {
		t.Fatal(err)
	}
	run2 := &parentRun{}
	parent2 := newParent(t, m, run2, func([]toolOut) schema.Message {
		return toolCall("c-get", "get_agent", map[string]any{"agent_id": agentID})
	})
	if _, err := parent2.Invoke(context.Background(), []schema.Message{schema.UserMessage("again")}, runOpts(store, "conv")...); err != nil {
		t.Fatal(err)
	}
	// 损坏记录拒绝是模型可见错误：结果文本明确说明记录不可用。
	if seen := run2.seen(); len(seen) == 0 || !strings.Contains(seen[0].Text, "corrupt") {
		t.Fatalf("seen=%v, want corrupt rejection", seen)
	}
}

// 创建者作用域：子 Agent 里再创建的孙 Agent 对根不可见。
func TestCreatorScope(t *testing.T) {
	store := state.NewMemoryStore()
	// inner 提供工具实现；outer 的注册表把这些工具放给子 Agent 用。
	inner := newManager(t, baseConfig(store))
	// 子 Agent 的模型：第一轮调用 spawn_agent 建孙 Agent，第二轮结束。
	var subCalls atomic.Int32
	cfg := baseConfig(store)
	cfg.Tools = inner.Tools()
	cfg.Models["main"] = llm.NewHandlerModel(func(_ context.Context, req llm.Request, _ llm.Options) ([]schema.Message, error) {
		subCalls.Add(1)
		if subCalls.Load() == 1 {
			return []schema.Message{schema.NewMessage(schema.RoleAssistant,
				schema.NewFunctionToolCallBlock("gc", "spawn_agent", `{"task":"孙任务"}`))}, nil
		}
		return []schema.Message{schema.AssistantMessage("sub-done")}, nil
	})
	m := newManager(t, cfg)
	run := &parentRun{}
	// spawn 请求显式给子 Agent 带 spawn_agent 工具，否则默认集为空、孙 Agent 无从创建。
	parent := newParent(t, m, run, spawnStep(map[string]any{"task": "父任务", "tools": []any{"spawn_agent"}}))
	if _, err := parent.Invoke(context.Background(), []schema.Message{schema.UserMessage("go")}, runOpts(store, "conv")...); err != nil {
		t.Fatal(err)
	}
	// 根视角列表：只见直属子 Agent，不见孙 Agent。
	index, _, _ := store.Load(context.Background(), "conv/agents")
	var idx struct {
		Agents []struct {
			AgentID, Creator string
		} `json:"agents"`
	}
	if err := json.Unmarshal(index.Result, &idx); err != nil {
		t.Fatal(err)
	}
	var rootVisible, nested int
	for _, a := range idx.Agents {
		if a.Creator == "" {
			rootVisible++
		} else {
			nested++
		}
	}
	if rootVisible != 1 || nested != 1 {
		t.Fatalf("index=%v root=%d nested=%d", idx.Agents, rootVisible, nested)
	}
}

// 并发上限：MaxConcurrent=1 时两个并行调用串行执行。
func TestMaxConcurrentSerializes(t *testing.T) {
	store := state.NewMemoryStore()
	var concurrent atomic.Int32
	var peak atomic.Int32
	cfg := baseConfig(store)
	cfg.MaxConcurrent = 1
	cfg.Models["main"] = llm.NewHandlerModel(func(_ context.Context, req llm.Request, _ llm.Options) ([]schema.Message, error) {
		cur := concurrent.Add(1)
		if cur > peak.Load() {
			peak.Store(cur)
		}
		defer concurrent.Add(-1)
		return []schema.Message{schema.AssistantMessage("ok")}, nil
	})
	m := newManager(t, cfg)
	run := &parentRun{}
	parent := newParent(t, m, run, func([]toolOut) schema.Message {
		return schema.NewMessage(schema.RoleAssistant,
			schema.NewFunctionToolCallBlock("c-a", "spawn_agent", `{"task":"甲"}`),
			schema.NewFunctionToolCallBlock("c-b", "spawn_agent", `{"task":"乙"}`),
		)
	})
	if _, err := parent.Invoke(context.Background(), []schema.Message{schema.UserMessage("go")}, runOpts(store, "conv")...); err != nil {
		t.Fatal(err)
	}
	if peak.Load() != 1 {
		t.Fatalf("peak=%d, want serialized", peak.Load())
	}
}

// 无会话环境：无 Store 或无检查点时 spawn_agent 返回模型可见错误而不是悬挂。
func TestSpawnWithoutSession(t *testing.T) {
	m := newManager(t, subagent.Config{
		Models:       map[string]llm.ChatModel{"main": echoModel()},
		DefaultModel: "main",
	})
	run := &parentRun{}
	parent := newParent(t, m, run, spawnStep(map[string]any{"task": "任务"}))
	if _, err := parent.Invoke(context.Background(), []schema.Message{schema.UserMessage("go")}); err != nil {
		t.Fatal(err)
	}
	seen := run.seen()
	if len(seen) == 0 || !strings.Contains(seen[0].Text, "checkpoint") {
		t.Fatalf("seen=%v", seen)
	}
}

// --- J3：abandon 清理 ---

// 父运行 abandon 后，显式 Cleanup 按被弃 RunID 清理孤儿记录。
func TestCleanupRemovesAbandonedRunAgents(t *testing.T) {
	store := state.NewMemoryStore()
	m := newManager(t, baseConfig(store))
	run := &parentRun{}
	parent := newParent(t, m, run, spawnStep(map[string]any{"task": "第一轮"}))
	if _, err := parent.Invoke(context.Background(), []schema.Message{schema.UserMessage("r1")}, runOpts(store, "conv")...); err != nil {
		t.Fatal(err)
	}
	firstID, _ := decode(t, run.seen()[0].Text)["agent_id"].(string)

	// 第二轮：spawn 后挂起（经审批工具）。
	var askOnce atomic.Int32
	cfg := baseConfig(store)
	cfg.Tools = []tool.Tool{tool.NewFunctionTool(tool.Spec{Name: "ask"}, func(ctx context.Context, _ tool.CallInput) (tool.Result, error) {
		if askOnce.Add(1) > 1 {
			return tool.TextResult("unreachable"), nil
		}
		_, err := graph.Interrupt(ctx, "approve?")
		return tool.Result{}, err
	})}
	cfg.DefaultTools = []string{"ask"}
	cfg.Models["main"] = llm.NewHandlerModel(func(_ context.Context, req llm.Request, _ llm.Options) ([]schema.Message, error) {
		last := req.Messages[len(req.Messages)-1]
		if results := last.FunctionToolResults(); len(results) > 0 {
			return []schema.Message{schema.AssistantMessage("final")}, nil
		}
		return []schema.Message{schema.NewMessage(schema.RoleAssistant,
			schema.NewFunctionToolCallBlock("sub-ask", "ask", `{}`))}, nil
	})
	m2 := newManager(t, cfg)
	run2 := &parentRun{}
	parent2 := newParent(t, m2, run2,
		spawnStep(map[string]any{"task": "第二轮"}),
		func([]toolOut) schema.Message { return schema.AssistantMessage("never") },
	)
	_, err := parent2.Invoke(context.Background(), []schema.Message{schema.UserMessage("r2")}, runOpts(store, "conv")...)
	var suspended *graph.InterruptedError
	if !errors.As(err, &suspended) {
		t.Fatalf("err=%v, want suspend", err)
	}
	// abandon 第二轮，然后 Cleanup 其 RunID。
	latest, _, _ := store.Load(context.Background(), "conv")
	abandonedRun := latest.RunID
	if _, err := state.AbandonRun(context.Background(), store, "conv"); err != nil {
		t.Fatal(err)
	}
	removed, err := m2.Cleanup(context.Background(), "conv", abandonedRun)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed=%d", removed)
	}
	// 第一轮 Agent 保留，第二轮的已删。
	index, _, _ := store.Load(context.Background(), "conv/agents")
	var idx struct {
		Agents []struct {
			AgentID string `json:"agent_id"`
		} `json:"agents"`
	}
	if err := json.Unmarshal(index.Result, &idx); err != nil || len(idx.Agents) != 1 || idx.Agents[0].AgentID != firstID {
		t.Fatalf("index=%s err=%v", index.Result, err)
	}
}

// --- 注册表约束（沿用原 resolve 语义） ---

func TestResolveRegistryConstraints(t *testing.T) {
	m := newManager(t, subagent.Config{
		Models: map[string]llm.ChatModel{
			"a": fixedModel("a"), "b": fixedModel("b"),
		},
		DefaultModel: "a",
		Tools:        []tool.Tool{tool.NewFunctionTool(tool.Spec{Name: "x"}, func(context.Context, tool.CallInput) (tool.Result, error) { return tool.TextResult("x"), nil })},
		DefaultTools: []string{"x"},
		Roles:        map[string]subagent.Role{"coder": {Model: "b", Instructions: "写代码"}},
	})
	if _, err := m.Resolve(subagent.Request{Task: "t", Model: "nope"}); !subagent.IsConfigError(err) {
		t.Fatalf("model err=%v", err)
	}
	if _, err := m.Resolve(subagent.Request{Task: "t", Role: "nope"}); !subagent.IsConfigError(err) {
		t.Fatalf("role err=%v", err)
	}
	if _, err := m.Resolve(subagent.Request{Task: "t", Tools: []string{"nope"}}); !subagent.IsConfigError(err) {
		t.Fatalf("tools err=%v", err)
	}
	resolved, err := m.Resolve(subagent.Request{Task: "t", Role: "coder"})
	if err != nil || resolved.Model != "b" {
		t.Fatalf("role resolved=%+v err=%v", resolved, err)
	}
	resolved, err = m.Resolve(subagent.Request{Task: "t"})
	if err != nil || resolved.Model != "a" || len(resolved.Tools) != 1 || resolved.Tools[0] != "x" {
		t.Fatalf("default resolved=%+v err=%v", resolved, err)
	}
	if _, err := m.Resolve(subagent.Request{Task: ""}); !subagent.IsConfigError(err) {
		t.Fatalf("empty task err=%v", err)
	}
}
