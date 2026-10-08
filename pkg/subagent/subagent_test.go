package subagent_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/subagent"
	"github.com/goharness/goharness/pkg/tool"
)

// --- 测试辅助 ---

func echoTool(name string) tool.Tool {
	return tool.NewFunctionTool(tool.Spec{Name: name, Description: name, IsReadOnly: true}, func(_ context.Context, _ tool.CallInput) (tool.Result, error) {
		return tool.TextResult(name + " ok"), nil
	})
}

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

// toolOutputs 按出现顺序收集本轮（最后一条用户消息之后）的工具结果：
// 会话沿用此前各轮的历史，更早轮次的工具结果不属于当前轮的脚本进度。
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
	a, err := agent.New(agent.BaseConfig{Name: "parent", Model: model, Tools: m.Tools()})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func runOpts(m *subagent.Manager, store state.Store, key string, extra ...runnable.Option) []runnable.Option {
	opts := append(m.RunOptions(), agent.WithCheckpoint(store, key))
	return append(opts, extra...)
}

func spawnStep(args map[string]any) step {
	return func([]toolOut) schema.Message { return toolCall("spawn", "spawn_agent", args) }
}

func waitStep(from int) step {
	return func(results []toolOut) schema.Message {
		var spawned map[string]any
		_ = json.Unmarshal([]byte(results[from].Text), &spawned)
		id, _ := spawned["task_id"].(string)
		return toolCall("wait", "wait_agents", map[string]any{"task_ids": []string{id}})
	}
}

func agentRecords(t *testing.T, store state.Store, key string) []graph.AgentInfo {
	t.Helper()
	record, found, err := store.Load(context.Background(), key)
	if err != nil || !found {
		t.Fatalf("load %s: %v %v", key, found, err)
	}
	agents, err := graph.SessionAgentsOf(record)
	if err != nil {
		t.Fatal(err)
	}
	return agents
}

// --- 配置解析 ---

func TestResolveRulesAndPrecedence(t *testing.T) {
	read, search := echoTool("read_file"), echoTool("search_code")
	m := newManager(t, subagent.Config{
		Models:       map[string]llm.ChatModel{"main": fixedModel("m"), "review": fixedModel("r"), "parent_model": fixedModel("p")},
		DefaultModel: "main",
		Tools:        []tool.Tool{read, search},
		DefaultTools: []string{"read_file"},
		Instructions: "Be concise.",
		Roles: map[string]subagent.Role{
			"auditor": {Instructions: "Audit for security flaws.", Model: "review", Tools: []string{"read_file", "search_code"}},
			"scribe":  {Instructions: "Write notes.", Tools: []string{}},
			"plain":   {Instructions: "Plain."},
		},
	})
	cases := []struct {
		name    string
		req     subagent.Request
		model   string
		tools   []string
		instr   string
		wantErr string
	}{
		{"defaults", subagent.Request{Task: "t"}, "main", []string{"read_file"}, "Be concise.", ""},
		{"role overrides model and tools", subagent.Request{Task: "t", Role: "auditor"}, "review", []string{"read_file", "search_code"}, "Be concise.\n\nAudit for security flaws.", ""},
		{"request overrides role", subagent.Request{Task: "t", Role: "auditor", Model: "main", Tools: []string{"search_code"}}, "main", []string{"search_code"}, "Be concise.\n\nAudit for security flaws.", ""},
		{"empty tools means none", subagent.Request{Task: "t", Tools: []string{}}, "main", []string{}, "Be concise.", ""},
		{"role with empty tools", subagent.Request{Task: "t", Role: "scribe"}, "main", []string{}, "Be concise.\n\nWrite notes.", ""},
		{"role without tools keeps defaults", subagent.Request{Task: "t", Role: "plain"}, "main", []string{"read_file"}, "Be concise.\n\nPlain.", ""},
		{"duplicates collapse in order", subagent.Request{Task: "t", Tools: []string{"search_code", "read_file", "search_code"}}, "main", []string{"search_code", "read_file"}, "Be concise.", ""},
		{"extra instructions appended", subagent.Request{Task: "t", Instructions: "Only review."}, "main", []string{"read_file"}, "Be concise.\n\nOnly review.", ""},
		{"empty task", subagent.Request{Task: "  "}, "", nil, "", "task is required"},
		{"unknown model", subagent.Request{Task: "t", Model: "ghost"}, "", nil, "", `unknown model "ghost"; available models: main, parent_model, review`},
		{"unknown tool", subagent.Request{Task: "t", Tools: []string{"rm_rf"}}, "", nil, "", `unknown tool "rm_rf"; available tools: read_file, search_code`},
		{"unknown role", subagent.Request{Task: "t", Role: "boss"}, "", nil, "", `unknown role "boss"; available roles: auditor, plain, scribe`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := m.Resolve(tc.req)
			if tc.wantErr != "" {
				if err == nil || !subagent.IsConfigError(err) || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err=%v, want config error containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.Model != tc.model || strings.Join(got.Tools, ",") != strings.Join(tc.tools, ",") || got.Instructions != tc.instr || got.Tools == nil {
				t.Fatalf("resolved=%+v", got)
			}
			if got.BuilderID != subagent.DefaultBuilderID || got.BuilderVersion != subagent.DefaultBuilderVersion {
				t.Fatalf("builder identity %q/%q", got.BuilderID, got.BuilderVersion)
			}
		})
	}
}

func TestParentModelDefaultResolvesToRegisteredName(t *testing.T) {
	m := newManager(t, subagent.Config{
		Models:       map[string]llm.ChatModel{"gpt": fixedModel("g"), "small": fixedModel("s")},
		DefaultModel: subagent.ParentModel,
		ParentModel:  "gpt",
	})
	got, err := m.Resolve(subagent.Request{Task: "t"})
	if err != nil || got.Model != "gpt" {
		t.Fatalf("resolved=%+v err=%v", got, err)
	}
}

func TestConfigValidation(t *testing.T) {
	model := map[string]llm.ChatModel{"m": fixedModel("m")}
	bad := map[string]subagent.Config{
		"no models":              {},
		"unknown default model":  {Models: model, DefaultModel: "x"},
		"parent without name":    {Models: model, DefaultModel: subagent.ParentModel, ParentModel: "x"},
		"unknown default tool":   {Models: model, DefaultTools: []string{"x"}},
		"duplicate tool":         {Models: model, Tools: []tool.Tool{echoTool("a"), echoTool("a")}},
		"role unknown model":     {Models: model, Roles: map[string]subagent.Role{"r": {Model: "x"}}},
		"role unknown tool":      {Models: model, Roles: map[string]subagent.Role{"r": {Tools: []string{"x"}}}},
		"reserved model name":    {Models: map[string]llm.ChatModel{subagent.ParentModel: fixedModel("m")}},
		"custom builder default": {Models: model, Builder: func(context.Context, subagent.ResolvedSpec) (subagent.Runner, error) { return nil, nil }},
	}
	for name, cfg := range bad {
		if _, err := subagent.New(cfg); err == nil {
			t.Errorf("%s: config accepted", name)
		}
	}
	if _, err := subagent.New(subagent.Config{Models: model}); err != nil {
		t.Fatalf("single model should become the default: %v", err)
	}
}

// --- 端到端：预置 Agent 配方 ---

func TestSpawnAndWaitWithPresetAgent(t *testing.T) {
	store := state.NewMemoryStore()
	var childCalls atomic.Int32
	child := llm.NewHandlerModel(func(_ context.Context, req llm.Request, _ llm.Options) ([]schema.Message, error) {
		childCalls.Add(1)
		return []schema.Message{schema.AssistantMessage("child-result:" + schema.LastUserText(req.Messages))}, nil
	})
	m := newManager(t, subagent.Config{Models: map[string]llm.ChatModel{"child": child}, Instructions: "You are a reviewer."})
	run := &parentRun{}
	parent := newParent(t, m, run,
		spawnStep(map[string]any{"task": "audit login", "name": "login auditor"}),
		waitStep(0),
	)
	out, err := parent.Invoke(context.Background(), []schema.Message{schema.UserMessage("go")}, runOpts(m, store, "conv")...)
	if err != nil || len(out) == 0 || out[len(out)-1].JoinedText("") != "done" {
		t.Fatalf("out=%v err=%v", out, err)
	}
	spawned := decode(t, run.seen()[0].Text)
	if spawned["agent_id"] != "agent-1" || spawned["status"] != "running" {
		t.Fatalf("spawn result=%v", spawned)
	}
	waited := decode(t, run.seen()[1].Text)["results"].([]any)[0].(map[string]any)
	if waited["status"] != "completed" || waited["result"] != "child-result:audit login" || waited["agent_id"] != "agent-1" {
		t.Fatalf("wait result=%v", waited)
	}
	if childCalls.Load() != 1 {
		t.Fatalf("child model called %d times", childCalls.Load())
	}
	agents := agentRecords(t, store, "conv")
	if len(agents) != 1 || agents[0].Description != "login auditor" || agents[0].Status != graph.AgentStatusIdle ||
		string(agents[0].Tasks[0].Result) != `"child-result:audit login"` {
		t.Fatalf("agents=%+v", agents)
	}
	var saved map[string]any
	if err := json.Unmarshal(agents[0].Config, &saved); err != nil || saved["model"] != "child" || saved["instructions"] != "You are a reviewer." {
		t.Fatalf("saved config=%s", agents[0].Config)
	}
}

// 第二轮：新的管理器与父 Agent 实例，列出并追加任务，子 Agent 沿用此前的上下文。
func TestFollowupAcrossRoundsKeepsContext(t *testing.T) {
	store := state.NewMemoryStore()
	var mu sync.Mutex
	var requests [][]string
	child := llm.NewHandlerModel(func(_ context.Context, req llm.Request, _ llm.Options) ([]schema.Message, error) {
		var texts []string
		for _, message := range req.Messages {
			texts = append(texts, message.JoinedText(""))
		}
		mu.Lock()
		requests = append(requests, texts)
		mu.Unlock()
		return []schema.Message{schema.AssistantMessage("answer:" + schema.LastUserText(req.Messages))}, nil
	})
	cfg := subagent.Config{Models: map[string]llm.ChatModel{"child": child}}

	m1 := newManager(t, cfg)
	if _, err := newParent(t, m1, &parentRun{}, spawnStep(map[string]any{"task": "audit login"}), waitStep(0)).
		Invoke(context.Background(), []schema.Message{schema.UserMessage("round1")}, runOpts(m1, store, "conv")...); err != nil {
		t.Fatalf("round1: %v", err)
	}

	m2 := newManager(t, cfg)
	run := &parentRun{}
	parent := newParent(t, m2, run,
		func([]toolOut) schema.Message { return toolCall("list", "list_agents", map[string]any{}) },
		func(results []toolOut) schema.Message {
			id := decode(t, results[0].Text)["agents"].([]any)[0].(map[string]any)["agent_id"].(string)
			return toolCall("follow", "followup_task", map[string]any{"agent_id": id, "task": "verify the fix"})
		},
		waitStep(1),
	)
	if _, err := parent.Invoke(context.Background(), []schema.Message{schema.UserMessage("round2")}, runOpts(m2, store, "conv")...); err != nil {
		t.Fatalf("round2: %v", err)
	}
	seen := run.seen()
	listed := decode(t, seen[0].Text)["agents"].([]any)[0].(map[string]any)
	if listed["status"] != "idle" || listed["summary"] != "answer:audit login" {
		t.Fatalf("list result=%v", listed)
	}
	if len(requests) != 2 {
		t.Fatalf("child model calls=%d, want 2 (first task must not rerun)", len(requests))
	}
	joined := strings.Join(requests[1], "|")
	for _, want := range []string{"audit login", "answer:audit login", "verify the fix"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("follow-up request lacks %q: %v", want, requests[1])
		}
	}
	agents := agentRecords(t, store, "conv")
	if len(agents) != 1 || len(agents[0].Tasks) != 2 || string(agents[0].Tasks[1].Result) != `"answer:verify the fix"` {
		t.Fatalf("agents=%+v", agents)
	}
	got := decode(t, run.seen()[2].Text)["results"].([]any)[0].(map[string]any)
	if got["agent_id"] != "agent-1" {
		t.Fatalf("followup result=%v", got)
	}

	// get_agent 读取完整历史。
	m3 := newManager(t, cfg)
	run3 := &parentRun{}
	parent3 := newParent(t, m3, run3,
		func([]toolOut) schema.Message {
			return toolCall("get", "get_agent", map[string]any{"agent_id": "agent-1"})
		},
	)
	if _, err := parent3.Invoke(context.Background(), []schema.Message{schema.UserMessage("round3")}, runOpts(m3, store, "conv")...); err != nil {
		t.Fatal(err)
	}
	history := decode(t, run3.seen()[0].Text)
	tasks := history["tasks"].([]any)
	if len(tasks) != 2 || tasks[0].(map[string]any)["task"] != "audit login" || tasks[1].(map[string]any)["result"] != "answer:verify the fix" {
		t.Fatalf("history=%v", history)
	}
}

// 模型可修正的错误作为错误结果返回，父 Agent 能继续运行，且没有留下半成品子 Agent。
func TestModelCorrectableErrorsAreToolResults(t *testing.T) {
	store := state.NewMemoryStore()
	var childCalls atomic.Int32
	child := llm.NewHandlerModel(func(context.Context, llm.Request, llm.Options) ([]schema.Message, error) {
		childCalls.Add(1)
		return []schema.Message{schema.AssistantMessage("x")}, nil
	})
	m := newManager(t, subagent.Config{Models: map[string]llm.ChatModel{"child": child}})
	run := &parentRun{}
	parent := newParent(t, m, run,
		spawnStep(map[string]any{"task": "t", "model": "ghost"}),
		spawnStep(map[string]any{"task": "t", "tools": []string{"rm_rf"}}),
		func([]toolOut) schema.Message {
			return toolCall("f", "followup_task", map[string]any{"agent_id": "agent-9", "task": "t"})
		},
		func([]toolOut) schema.Message {
			return toolCall("w", "wait_agents", map[string]any{"task_ids": []string{"at/agent-1/1"}})
		},
		func([]toolOut) schema.Message {
			return toolCall("c", "close_agent", map[string]any{"agent_id": "agent-9"})
		},
	)
	if _, err := parent.Invoke(context.Background(), []schema.Message{schema.UserMessage("go")}, runOpts(m, store, "conv")...); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	seen := run.seen()
	wants := []string{`unknown model "ghost"`, `unknown tool "rm_rf"`, `"agent-9"`, "at/agent-1/1", `"agent-9"`}
	if len(seen) != len(wants) {
		t.Fatalf("results=%+v", seen)
	}
	for i, want := range wants {
		// 错误结果由工具节点统一改写为"调用工具出错"的模型可见文本。
		if !strings.Contains(seen[i].Text, "An error occurred") || !strings.Contains(seen[i].Text, want) {
			t.Fatalf("result[%d]=%+v, want error text containing %q", i, seen[i], want)
		}
	}
	if childCalls.Load() != 0 || len(agentRecords(t, store, "conv")) != 0 {
		t.Fatalf("rejected requests left residue: calls=%d", childCalls.Load())
	}
}

// send_message 的消息在子 Agent 的下一次模型调用时并入请求。
func TestSendMessageReachesNextModelCall(t *testing.T) {
	store := state.NewMemoryStore()
	gate := make(chan struct{})
	var once sync.Once
	slow := tool.NewFunctionTool(tool.Spec{Name: "slow", Description: "blocks until released"}, func(ctx context.Context, _ tool.CallInput) (tool.Result, error) {
		select {
		case <-gate:
			return tool.TextResult("released"), nil
		case <-ctx.Done():
			return tool.Result{}, ctx.Err()
		}
	})
	var calls atomic.Int32
	child := llm.NewHandlerModel(func(_ context.Context, req llm.Request, _ llm.Options) ([]schema.Message, error) {
		if calls.Add(1) == 1 {
			return []schema.Message{toolCall("s", "slow", map[string]any{})}, nil
		}
		for _, message := range req.Messages {
			if strings.Contains(message.JoinedText(""), "Message from the parent agent: hurry") {
				return []schema.Message{schema.AssistantMessage("got-message")}, nil
			}
		}
		return []schema.Message{schema.AssistantMessage("no-message")}, nil
	})
	m := newManager(t, subagent.Config{
		Models: map[string]llm.ChatModel{"child": child}, Tools: []tool.Tool{slow}, DefaultTools: []string{"slow"},
	})
	run := &parentRun{}
	parent := newParent(t, m, run,
		spawnStep(map[string]any{"task": "work"}),
		func(results []toolOut) schema.Message {
			return toolCall("m", "send_message", map[string]any{"task_id": decode(t, results[0].Text)["task_id"], "message": "hurry"})
		},
		func(results []toolOut) schema.Message {
			once.Do(func() { close(gate) })
			return waitStep(0)(results)
		},
	)
	if _, err := parent.Invoke(context.Background(), []schema.Message{schema.UserMessage("go")}, runOpts(m, store, "conv")...); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	seen := run.seen()
	if sent := decode(t, seen[1].Text); sent["status"] != "queued" || sent["message_id"] == "" {
		t.Fatalf("send result=%v", sent)
	}
	got := decode(t, seen[2].Text)["results"].([]any)[0].(map[string]any)
	if got["result"] != "got-message" {
		t.Fatalf("wait result=%v", got)
	}
}

// 子 Agent 的工具请求审批：根运行挂起，重建管理器与父 Agent 后回答并恢复，任务成功结束。
func TestSubAgentApprovalSurvivesRebuild(t *testing.T) {
	store := state.NewMemoryStore()
	var deployed atomic.Int32
	deploy := tool.NewFunctionTool(tool.Spec{Name: "deploy", Description: "deploys after approval"}, func(ctx context.Context, _ tool.CallInput) (tool.Result, error) {
		answer, err := graph.Interrupt(ctx, "approve deploy?")
		if err != nil {
			return tool.Result{}, err
		}
		deployed.Add(1)
		return tool.TextResult("deployed:" + toString(answer)), nil
	})
	var calls atomic.Int32
	child := llm.NewHandlerModel(func(_ context.Context, req llm.Request, _ llm.Options) ([]schema.Message, error) {
		if len(toolOutputs(req)) == 0 {
			calls.Add(1)
			return []schema.Message{toolCall("d", "deploy", map[string]any{})}, nil
		}
		return []schema.Message{schema.AssistantMessage("after-approval")}, nil
	})
	cfg := subagent.Config{Models: map[string]llm.ChatModel{"child": child}, Tools: []tool.Tool{deploy}, DefaultTools: []string{"deploy"}}
	steps := []step{spawnStep(map[string]any{"task": "release"}), waitStep(0)}

	m1 := newManager(t, cfg)
	_, err := newParent(t, m1, &parentRun{}, steps...).Invoke(context.Background(), []schema.Message{schema.UserMessage("go")}, runOpts(m1, store, "conv")...)
	var interrupted *graph.InterruptedError
	if !errors.As(err, &interrupted) || len(interrupted.PendingInterrupts()) != 1 {
		t.Fatalf("want one pending approval, got %v", err)
	}
	questionID := interrupted.PendingInterrupts()[0].ID

	m2 := newManager(t, cfg)
	out, err := agent.Resume(context.Background(), newParent(t, m2, &parentRun{}, steps...),
		runOpts(m2, store, "conv", agent.WithResume(questionID, "yes"))...)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if len(out) == 0 || out[len(out)-1].JoinedText("") != "done" {
		t.Fatalf("out=%v", out)
	}
	if deployed.Load() != 1 {
		t.Fatalf("deploy ran %d times after approval, want 1", deployed.Load())
	}
	agents := agentRecords(t, store, "conv")
	if len(agents) != 1 || agents[0].Status != graph.AgentStatusIdle || string(agents[0].Tasks[0].Result) != `"after-approval"` {
		t.Fatalf("agents=%+v", agents)
	}
}

func toString(v any) string {
	s, _ := v.(string)
	return s
}

// 追加任务沿用登记的构建身份：版本不符明确拒绝，不换实现继续运行。
func TestFollowupRejectsChangedBuilderVersion(t *testing.T) {
	store := state.NewMemoryStore()
	cfg := subagent.Config{Models: map[string]llm.ChatModel{"child": fixedModel("ok")}}
	m1 := newManager(t, cfg)
	if _, err := newParent(t, m1, &parentRun{}, spawnStep(map[string]any{"task": "t"}), waitStep(0)).
		Invoke(context.Background(), []schema.Message{schema.UserMessage("r1")}, runOpts(m1, store, "conv")...); err != nil {
		t.Fatal(err)
	}
	cfg.BuilderVersion = "v2"
	m2 := newManager(t, cfg)
	_, err := newParent(t, m2, &parentRun{}, func([]toolOut) schema.Message {
		return toolCall("f", "followup_task", map[string]any{"agent_id": "agent-1", "task": "again"})
	}).Invoke(context.Background(), []schema.Message{schema.UserMessage("r2")}, runOpts(m2, store, "conv")...)
	if !errors.Is(err, graph.ErrManagedBuilder) {
		t.Fatalf("err=%v, want builder mismatch", err)
	}
	if agents := agentRecords(t, store, "conv"); len(agents[0].Tasks) != 1 {
		t.Fatalf("rejected followup left a task: %+v", agents[0].Tasks)
	}
}

// --- 自建图 ---

type customState struct{ Steps int }

// 使用者自建的图按 Runner 契约接入：无需修改父图拓扑，结果与追加任务同样可用。
func TestCustomGraphRunner(t *testing.T) {
	store := state.NewMemoryStore()
	var built atomic.Int32
	builder := func(_ context.Context, spec subagent.ResolvedSpec) (subagent.Runner, error) {
		built.Add(1)
		g := graph.NewGraph[[]schema.Message, []schema.Message]("custom-sub")
		// 自建图需要具备图恢复所需的 State：开启持久化的受管任务由根检查点保存进度。
		if err := g.SetStateFactory(func(context.Context, []schema.Message) (*customState, error) { return &customState{}, nil }); err != nil {
			return nil, err
		}
		node := runnable.Func[[]schema.Message, []schema.Message](func(_ context.Context, in []schema.Message, _ ...runnable.Option) ([]schema.Message, error) {
			return []schema.Message{schema.AssistantMessage("custom[" + spec.Model + "]:" + schema.LastUserText(in))}, nil
		})
		if err := g.AddNode("work", node); err != nil {
			return nil, err
		}
		if err := g.AddEdge(graph.Start, "work"); err != nil {
			return nil, err
		}
		if err := g.AddEdge("work", graph.End); err != nil {
			return nil, err
		}
		return g.Compile()
	}
	cfg := subagent.Config{
		Models: map[string]llm.ChatModel{"child": fixedModel("unused")}, Builder: builder,
		BuilderID: "custom-graph", BuilderVersion: "1",
	}
	m := newManager(t, cfg)
	run := &parentRun{}
	parent := newParent(t, m, run, spawnStep(map[string]any{"task": "audit"}), waitStep(0))
	if _, err := parent.Invoke(context.Background(), []schema.Message{schema.UserMessage("go")}, runOpts(m, store, "conv")...); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	got := decode(t, run.seen()[1].Text)["results"].([]any)[0].(map[string]any)
	if got["result"] != "custom[child]:audit" || built.Load() != 1 {
		t.Fatalf("result=%v built=%d", got, built.Load())
	}
	agents := agentRecords(t, store, "conv")
	if agents[0].Builder != "custom-graph" || agents[0].Version != "1" {
		t.Fatalf("builder identity not saved: %+v", agents[0])
	}
}
