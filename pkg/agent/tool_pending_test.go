package agent_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/tool"
)

// during_node 恢复时整节点重跑：已完成的工具调用经 pending writes 跳过，
// 挂起的调用重放 Interrupt 消费答案后继续。
func TestResumeSkipsCompletedToolCalls(t *testing.T) {
	var echoRuns, askRuns atomic.Int32
	var echoKey atomic.Value
	value, err := agent.New(agent.BaseConfig{
		Name: "pending-agent",
		Model: llm.NewScriptedModel(
			[]schema.Message{schema.NewMessage(schema.RoleAssistant,
				schema.NewFunctionToolCallBlock("call-echo", "echo", `{}`),
				schema.NewFunctionToolCallBlock("call-ask", "ask", `{}`))},
			[]schema.Message{schema.AssistantMessage("finished")}),
		Tools: []tool.Tool{
			tool.NewFunctionTool(tool.Spec{Name: "echo"}, func(_ context.Context, input tool.CallInput) (tool.Result, error) {
				echoRuns.Add(1)
				echoKey.Store(input.IdempotencyKey)
				return tool.TextResult("echoed"), nil
			}),
			tool.NewFunctionTool(tool.Spec{Name: "ask"}, func(ctx context.Context, _ tool.CallInput) (tool.Result, error) {
				askRuns.Add(1)
				answer, err := graph.Interrupt(ctx, "approve?")
				if err != nil {
					return tool.Result{}, err
				}
				return tool.TextResult(fmt.Sprintf("answer:%v", answer)), nil
			}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	option := agent.WithCheckpoint(&agentExecutionStore{}, "pending-run")
	_, err = value.Invoke(context.Background(), []schema.Message{schema.UserMessage("run")}, option)
	var suspended *graph.InterruptedError
	if !errors.As(err, &suspended) {
		t.Fatalf("err=%v", err)
	}
	if echoRuns.Load() != 1 || askRuns.Load() != 1 {
		t.Fatalf("echo=%d ask=%d", echoRuns.Load(), askRuns.Load())
	}
	pending := suspended.PendingInterrupts()
	if len(pending) != 1 {
		t.Fatalf("pending=%v", pending)
	}
	output, err := agent.Resume(context.Background(), value, option, agent.WithResume(pending[0].ID, true))
	if err != nil {
		t.Fatal(err)
	}
	if echoRuns.Load() != 1 {
		t.Fatalf("echo tool re-executed: runs=%d", echoRuns.Load())
	}
	if askRuns.Load() != 2 {
		t.Fatalf("ask tool should replay once: runs=%d", askRuns.Load())
	}
	messages := output
	if messages[len(messages)-1].JoinedText("") != "finished" {
		t.Fatalf("output=%v", messages)
	}
	var echoResult, askResult string
	for _, m := range messages {
		for _, r := range m.FunctionToolResults() {
			switch r.Name {
			case "echo":
				echoResult = r.ContentBlocks[0].Text.Text
			case "ask":
				askResult = r.ContentBlocks[0].Text.Text
			}
		}
	}
	if echoResult != "echoed" || askResult != "answer:true" {
		t.Fatalf("echo=%q ask=%q", echoResult, askResult)
	}
	if key, _ := echoKey.Load().(string); key == "" || key == "call-echo" {
		t.Fatalf("idempotency key=%q must carry run scope", key)
	}
}

// 不同运行的幂等键必须不同：外部服务据此防重时不会误伤独立请求
// （复核问题 2）。
func TestIdempotencyKeyScopesToRun(t *testing.T) {
	var mu sync.Mutex
	var keys []string
	value, err := agent.New(agent.BaseConfig{
		Name: "scope-agent",
		// 两个运行共用同一脚本序列：各自一轮工具调用 + 一轮终结。
		Model: llm.NewScriptedModel(
			[]schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("call-1", "echo", `{}`))},
			[]schema.Message{schema.AssistantMessage("done")},
			[]schema.Message{schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock("call-1", "echo", `{}`))},
			[]schema.Message{schema.AssistantMessage("done")}),
		Tools: []tool.Tool{tool.NewFunctionTool(tool.Spec{Name: "echo"}, func(ctx context.Context, input tool.CallInput) (tool.Result, error) {
			mu.Lock()
			defer mu.Unlock()
			keys = append(keys, input.IdempotencyKey)
			return tool.TextResult("ok"), nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &agentExecutionStore{}
	for _, thread := range []string{"run-a", "run-b"} {
		if _, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("go")}, agent.WithCheckpoint(store, thread)); err != nil {
			t.Fatalf("%s: %v", thread, err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	// 两次运行的调用 ID 同为 call-1，但幂等键含运行范围，必须不同。
	if len(keys) != 2 || keys[0] == "" || keys[0] == keys[1] {
		t.Fatalf("idempotency keys=%v", keys)
	}
}

// 同批两个审批调用：先挂起的获批并完成、登记结果；恢复时其结果被复用，
// 且它消费过的审批日志同步标记——另一个调用消费自己的记录，运行正常
// 完成，不再报日志错位（复核问题 3）。
func TestResumeReplaysCallJournalsInOrder(t *testing.T) {
	var aRuns, bRuns atomic.Int32
	approving := func(name string, runs *atomic.Int32) tool.Handler {
		return func(ctx context.Context, _ tool.CallInput) (tool.Result, error) {
			runs.Add(1)
			answer, err := graph.Interrupt(ctx, "approve "+name+"?")
			if err != nil {
				return tool.Result{}, err
			}
			return tool.TextResult(fmt.Sprintf("%s:%v", name, answer)), nil
		}
	}
	value, err := agent.New(agent.BaseConfig{
		Name: "journal-agent",
		Model: llm.NewScriptedModel(
			[]schema.Message{schema.NewMessage(schema.RoleAssistant,
				schema.NewFunctionToolCallBlock("call-a", "callA", `{}`),
				schema.NewFunctionToolCallBlock("call-b", "callB", `{}`))},
			[]schema.Message{schema.AssistantMessage("finished")}),
		Tools: []tool.Tool{
			tool.NewFunctionTool(tool.Spec{Name: "callA"}, approving("A", &aRuns)),
			tool.NewFunctionTool(tool.Spec{Name: "callB"}, approving("B", &bRuns)),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	option := agent.WithCheckpoint(&agentExecutionStore{}, "journal-run")
	ctx := context.Background()
	// 首轮挂起一个问题（并发批次中先获锁者入日志）。
	_, err = value.Invoke(ctx, []schema.Message{schema.UserMessage("run")}, option)
	var suspended *graph.InterruptedError
	if !errors.As(err, &suspended) || len(suspended.PendingInterrupts()) != 1 {
		t.Fatalf("err=%v pending=%v", err, suspended)
	}
	// 回答第一个问题：获批的调用完成并登记结果，另一调用的审批随即挂起。
	_, err = agent.Resume(ctx, value, option, agent.WithResume(suspended.PendingInterrupts()[0].ID, true))
	if !errors.As(err, &suspended) || len(suspended.PendingInterrupts()) != 1 {
		t.Fatalf("second suspend err=%v", err)
	}
	before := aRuns.Load() + bRuns.Load()
	// 回答第二个问题：已完成的调用若已登记则复用结果（不重跑、不再
	// 消费日志）；并发时序下两个调用都可能在上一轮错过各自窗口，此时
	// 双方重跑并在本轮消费各自记录——两种路径都必须正常完成，不得报
	// 日志错位。
	output, err := agent.Resume(ctx, value, option, agent.WithResume(suspended.PendingInterrupts()[0].ID, true))
	if err != nil {
		t.Fatalf("final resume: %v", err)
	}
	if total := aRuns.Load() + bRuns.Load(); total-before < 1 || total-before > 2 {
		t.Fatalf("unexpected re-executions: before=%d total=%d", before, total)
	}
	if output[len(output)-1].JoinedText("") != "finished" {
		t.Fatalf("output=%v", output)
	}
	var sawA, sawB bool
	for _, m := range output {
		for _, r := range m.FunctionToolResults() {
			if r.IsError {
				continue
			}
			switch r.Name {
			case "callA":
				sawA = r.ContentBlocks[0].Text.Text == "A:true"
			case "callB":
				sawB = r.ContentBlocks[0].Text.Text == "B:true"
			}
		}
	}
	if !sawA || !sawB {
		t.Fatalf("results=%v", output)
	}
}

type childToolState struct {
	Text string
}

// newInterruptingChildGraph 构造一张带单点审批节点的子图：节点 Interrupt 请求
// 批准，获批后以 "input:answer" 返回。
func newInterruptingChildGraph(t *testing.T, name, question string) *graph.CompiledGraph[string, string] {
	t.Helper()
	inner := graph.NewGraph[string, string](name)
	if err := inner.SetStateFactory(func(_ context.Context, input string) (*childToolState, error) {
		return &childToolState{Text: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	ask := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		answer, err := graph.Interrupt(ctx, question)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s:%v", input, answer), nil
	})
	if err := inner.AddNode("ask", ask, graph.WithPreHandler(func(_ context.Context, s *childToolState) (string, error) {
		return s.Text, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := inner.AddEdge(graph.Start, "ask"); err != nil {
		t.Fatal(err)
	}
	if err := inner.AddEdge("ask", graph.End); err != nil {
		t.Fatal(err)
	}
	compiled, err := inner.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

// 工具内发起子执行（嵌套 Graph）时，子调用序号按调用键分派：恢复重放
// 该工具调用会找到同一子执行继续推进，而不是开新子调用（复核问题 3
// 的验收项：带子执行的工具恢复）。
func TestResumeContinuesChildExecutionUnderToolCall(t *testing.T) {
	inner := graph.NewGraph[string, string]("tool-child")
	if err := inner.SetStateFactory(func(_ context.Context, input string) (*childToolState, error) {
		return &childToolState{Text: input}, nil
	}); err != nil {
		t.Fatal(err)
	}
	var innerRuns atomic.Int32
	ask := runnable.Func[string, string](func(ctx context.Context, input string, _ ...runnable.Option) (string, error) {
		innerRuns.Add(1)
		answer, err := graph.Interrupt(ctx, "child approve?")
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("child:%v", answer), nil
	})
	if err := inner.AddNode("ask", ask, graph.WithPreHandler(func(_ context.Context, s *childToolState) (string, error) {
		return s.Text, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := inner.AddEdge(graph.Start, "ask"); err != nil {
		t.Fatal(err)
	}
	if err := inner.AddEdge("ask", graph.End); err != nil {
		t.Fatal(err)
	}
	child, err := inner.Compile()
	if err != nil {
		t.Fatal(err)
	}
	var toolRuns atomic.Int32
	value, err := agent.New(agent.BaseConfig{
		Name: "child-tool-agent",
		Model: llm.NewScriptedModel(
			[]schema.Message{schema.NewMessage(schema.RoleAssistant,
				schema.NewFunctionToolCallBlock("call-c", "spawn", `{}`))},
			[]schema.Message{schema.AssistantMessage("finished")}),
		Tools: []tool.Tool{tool.NewFunctionTool(tool.Spec{Name: "spawn"}, func(ctx context.Context, _ tool.CallInput) (tool.Result, error) {
			toolRuns.Add(1)
			out, runErr := child.Invoke(ctx, "seed")
			if runErr != nil {
				return tool.Result{}, runErr
			}
			return tool.TextResult(out), nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	option := agent.WithCheckpoint(&agentExecutionStore{}, "child-tool-run")
	ctx := context.Background()
	out, err := value.Invoke(ctx, []schema.Message{schema.UserMessage("run")}, option)
	var suspended *graph.InterruptedError
	if !errors.As(err, &suspended) {
		t.Fatalf("err=%v out=%v toolRuns=%d innerRuns=%d", err, out, toolRuns.Load(), innerRuns.Load())
	}
	pending := suspended.PendingInterrupts()
	if len(pending) != 1 {
		t.Fatalf("pending=%v", pending)
	}
	output, err := agent.Resume(ctx, value, option, agent.WithResume(pending[0].ID, true))
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if toolRuns.Load() != 2 || innerRuns.Load() != 2 {
		t.Fatalf("tool=%d inner=%d", toolRuns.Load(), innerRuns.Load())
	}
	var sawChild bool
	for _, m := range output {
		for _, r := range m.FunctionToolResults() {
			if r.Name == "spawn" && !r.IsError && r.ContentBlocks[0].Text.Text == "child:true" {
				sawChild = true
			}
		}
	}
	if !sawChild || output[len(output)-1].JoinedText("") != "finished" {
		t.Fatalf("output=%v", output)
	}
}

// 同批两个工具各自发起子调用：子执行身份含调用归属，唯一性校验不得
// 把两个序号 0 的子调用误判为重复（复核问题 1）。
func TestParallelToolCallsSpawnDistinctChildren(t *testing.T) {
	child := newInterruptingChildGraph(t, "inner-approve", "approve?")
	var runs atomic.Int32
	spawn := func(name string) tool.Handler {
		return func(ctx context.Context, _ tool.CallInput) (tool.Result, error) {
			runs.Add(1)
			out, err := child.Invoke(ctx, name)
			if err != nil {
				return tool.Result{}, err
			}
			return tool.TextResult(out), nil
		}
	}
	value, err := agent.New(agent.BaseConfig{
		Name: "dup-child-agent",
		Model: llm.NewScriptedModel(
			[]schema.Message{schema.NewMessage(schema.RoleAssistant,
				schema.NewFunctionToolCallBlock("call-a", "spawnA", `{}`),
				schema.NewFunctionToolCallBlock("call-b", "spawnB", `{}`))},
			[]schema.Message{schema.AssistantMessage("finished")}),
		Tools: []tool.Tool{
			tool.NewFunctionTool(tool.Spec{Name: "spawnA"}, spawn("A")),
			tool.NewFunctionTool(tool.Spec{Name: "spawnB"}, spawn("B")),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	option := agent.WithCheckpoint(&agentExecutionStore{}, "dup-child-run")
	ctx := context.Background()
	var output []schema.Message
	_, err = value.Invoke(ctx, []schema.Message{schema.UserMessage("run")}, option)
	var suspended *graph.InterruptedError
	for step := 0; err != nil && errors.As(err, &suspended) && step < 6; step++ {
		pending := suspended.PendingInterrupts()
		if len(pending) == 0 {
			t.Fatalf("suspended without pending: %v", suspended)
		}
		output, err = agent.Resume(ctx, value, option, agent.WithResume(pending[0].ID, true))
	}
	if err != nil {
		t.Fatalf("resume chain: %v", err)
	}
	if output[len(output)-1].JoinedText("") != "finished" {
		t.Fatalf("output=%v", output)
	}
	var sawA, sawB bool
	for _, m := range output {
		for _, r := range m.FunctionToolResults() {
			if r.IsError {
				continue
			}
			text := r.ContentBlocks[0].Text.Text
			if r.Name == "spawnA" && text == "A:true" {
				sawA = true
			}
			if r.Name == "spawnB" && text == "B:true" {
				sawB = true
			}
		}
	}
	if !sawA || !sawB {
		t.Fatalf("results=%v", output)
	}
}

// 未启用检查点时两个不同工具进入同一子图：子图内的工具幂等键仍须
// 按调用区分，不能把两笔独立操作合并（复核问题 2）。
func TestUncoordinatedNestedRunsScopeIdempotencyKeys(t *testing.T) {
	var mu sync.Mutex
	var keys []string
	orderNode, err := tool.NewToolsNode(tool.ToolNodeConfig{
		Tools: []tool.Tool{tool.NewFunctionTool(tool.Spec{Name: "order"}, func(_ context.Context, input tool.CallInput) (tool.Result, error) {
			mu.Lock()
			keys = append(keys, input.IdempotencyKey)
			mu.Unlock()
			return tool.TextResult("ordered"), nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	inner := graph.NewGraph[[]schema.Message, []schema.Message]("inner-order")
	if err := inner.SetStateFactory(func(_ context.Context, input []schema.Message) (*childToolState, error) {
		return &childToolState{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := inner.AddNode("order", orderNode, graph.WithPreHandler(func(_ context.Context, s *childToolState) ([]schema.Message, error) {
		return []schema.Message{schema.NewMessage(schema.RoleAssistant,
			schema.NewFunctionToolCallBlock("inner-1", "order", `{}`))}, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := inner.AddEdge(graph.Start, "order"); err != nil {
		t.Fatal(err)
	}
	if err := inner.AddEdge("order", graph.End); err != nil {
		t.Fatal(err)
	}
	child, err := inner.Compile()
	if err != nil {
		t.Fatal(err)
	}
	enter := func() tool.Handler {
		return func(ctx context.Context, _ tool.CallInput) (tool.Result, error) {
			out, runErr := child.Invoke(ctx, []schema.Message{schema.UserMessage("go")})
			if runErr != nil {
				return tool.Result{}, runErr
			}
			for _, m := range out {
				for _, r := range m.FunctionToolResults() {
					if r.IsError {
						return tool.Result{}, fmt.Errorf("inner order failed: %s", r.ContentBlocks[0].Text.Text)
					}
				}
			}
			return tool.TextResult(fmt.Sprintf("done:%d", len(out))), nil
		}
	}
	value, err := agent.New(agent.BaseConfig{
		Name: "uncoord-agent",
		Model: llm.NewScriptedModel(
			[]schema.Message{schema.NewMessage(schema.RoleAssistant,
				schema.NewFunctionToolCallBlock("call-1", "enterOne", `{}`),
				schema.NewFunctionToolCallBlock("call-2", "enterTwo", `{}`))},
			[]schema.Message{schema.AssistantMessage("done")}),
		Tools: []tool.Tool{
			tool.NewFunctionTool(tool.Spec{Name: "enterOne"}, enter()),
			tool.NewFunctionTool(tool.Spec{Name: "enterTwo"}, enter()),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// 不配置检查点：纯普通嵌套调用。
	out, err := value.Invoke(context.Background(), []schema.Message{schema.UserMessage("go")})
	if err != nil {
		t.Fatal(err)
	}
	var texts []string
	for _, m := range out {
		for _, r := range m.FunctionToolResults() {
			texts = append(texts, r.ContentBlocks[0].Text.Text)
		}
	}
	if len(texts) != 2 {
		t.Fatalf("tool results=%v", texts)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(keys) != 2 || keys[0] == keys[1] {
		t.Fatalf("idempotency keys must differ across sibling calls: keys=%v results=%v", keys, texts)
	}
}

// 子图挂起期间被替换为另一张图：恢复身份不兼容是框架控制错误，必须
// 终止根运行，不能吞成普通工具结果让运行「成功」结束（复核问题 3）。
func TestResumePropagatesChildIdentityMismatch(t *testing.T) {
	child := newInterruptingChildGraph(t, "inner-approve", "approve?")
	var mu sync.Mutex
	current := child
	value, err := agent.New(agent.BaseConfig{
		Name: "swap-child-agent",
		Model: llm.NewScriptedModel(
			[]schema.Message{schema.NewMessage(schema.RoleAssistant,
				schema.NewFunctionToolCallBlock("call-1", "spawn", `{}`))},
			[]schema.Message{schema.AssistantMessage("done")}),
		Tools: []tool.Tool{tool.NewFunctionTool(tool.Spec{Name: "spawn"}, func(ctx context.Context, _ tool.CallInput) (tool.Result, error) {
			mu.Lock()
			target := current
			mu.Unlock()
			out, runErr := target.Invoke(ctx, "seed")
			if runErr != nil {
				return tool.Result{}, runErr
			}
			return tool.TextResult(out), nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	option := agent.WithCheckpoint(&agentExecutionStore{}, "swap-child-run")
	ctx := context.Background()
	_, err = value.Invoke(ctx, []schema.Message{schema.UserMessage("run")}, option)
	var suspended *graph.InterruptedError
	if !errors.As(err, &suspended) || len(suspended.PendingInterrupts()) != 1 {
		t.Fatalf("err=%v", err)
	}
	// 挂起期间把子图替换为另一张图——恢复时必须识别身份不兼容。
	mu.Lock()
	current = newInterruptingChildGraph(t, "inner-other", "approve?")
	mu.Unlock()
	_, err = agent.Resume(ctx, value, option, agent.WithResume(suspended.PendingInterrupts()[0].ID, true))
	if !errors.Is(err, graph.ErrResumeIncompatible) {
		t.Fatalf("expected ErrResumeIncompatible, got %v", err)
	}
}
