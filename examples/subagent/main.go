package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/subagent"
	"github.com/goharness/goharness/pkg/tool"
)

// 父 Agent 通过 subagent.Manager 的管理工具演示子 Agent 全生命周期：
// 创建（审批）→ 运行中 send_message → 取消 → wait_agents 汇总。
// 示例用本地脚本模型；await_gate/open_gate 两个门闩工具让通信时机确定：
// 子 Agent A 在门闩内阻塞期间收到补充消息，放行后的下一次模型调用读到它。
func main() {
	ctx := context.Background()
	gate := make(chan struct{})
	var openOnce sync.Once

	// 子 Agent A 的模型：第一次调用先挂门闩；第二次调用时请求里已并入
	// 父 Agent 发来的补充消息（用户消息 "Message from the parent agent: ..."）。
	workerModel := llm.NewHandlerModel(func(_ context.Context, req llm.Request, _ llm.Options) ([]schema.Message, error) {
		ran := false
		var supplemental string
		for _, msg := range req.Messages {
			for range msg.FunctionToolResults() {
				ran = true
			}
			if msg.Role == schema.RoleUser {
				if text := msg.JoinedText(""); strings.Contains(text, "Message from the parent agent:") {
					supplemental = text
				}
			}
		}
		if !ran {
			return []schema.Message{schema.NewMessage(schema.RoleAssistant,
				schema.NewFunctionToolCallBlock("w1", "await_gate", `{}`))}, nil
		}
		out := "checklist: build, test, docs"
		if supplemental != "" {
			out += "；" + supplemental
		}
		return []schema.Message{schema.AssistantMessage(out)}, nil
	})

	// 子 Agent B 的模型一直阻塞，直到被 cancel_agent 取消。
	stallModel := llm.NewHandlerModel(func(ctx context.Context, _ llm.Request, _ llm.Options) ([]schema.Message, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})

	// await_gate 给子 Agent：阻塞到父 Agent 放行，保证 send_message 落在存活期。
	awaitGate := tool.NewFunctionTool(tool.Spec{
		Name:        "await_gate",
		Description: "Block until the parent agent releases the gate.",
	}, func(ctx context.Context, _ tool.CallInput) (tool.Result, error) {
		select {
		case <-gate:
			return tool.TextResult("gate released"), nil
		case <-ctx.Done():
			return tool.Result{}, ctx.Err()
		}
	})
	// open_gate 给父 Agent：放行子 Agent 的门闩。
	openGate := tool.NewFunctionTool(tool.Spec{
		Name:        "open_gate",
		Description: "Release the child agent's gate.",
	}, func(_ context.Context, _ tool.CallInput) (tool.Result, error) {
		openOnce.Do(func() { close(gate) })
		return tool.TextResult("gate opened"), nil
	})

	// 父模型按已收到的工具结果推进：创建 A → 发消息 → 放行 → 创建 B → 取消 B → 等待。
	parentModel := llm.NewHandlerModel(func(_ context.Context, req llm.Request, _ llm.Options) ([]schema.Message, error) {
		var taskIDs []string
		var msgSent, gateOpened, cancelled bool
		var waitResult string
		for _, msg := range req.Messages {
			for _, res := range msg.FunctionToolResults() {
				text := resultText(res)
				switch res.Name {
				case subagent.ToolSpawn:
					var out struct {
						TaskID string `json:"task_id"`
					}
					if json.Unmarshal([]byte(text), &out) == nil {
						taskIDs = append(taskIDs, out.TaskID)
					}
				case subagent.ToolMessage:
					msgSent = true
				case "open_gate":
					gateOpened = true
				case subagent.ToolCancel:
					cancelled = true
				case subagent.ToolWait:
					waitResult = text
				}
			}
		}
		call := func(id, name, args string) []schema.Message {
			return []schema.Message{schema.NewMessage(schema.RoleAssistant,
				schema.NewFunctionToolCallBlock(id, name, args))}
		}
		switch {
		case len(taskIDs) == 0:
			args, _ := json.Marshal(map[string]any{"task": "整理发布检查单，三条以内", "role": "auditor"})
			return call("c1", subagent.ToolSpawn, string(args)), nil
		case !msgSent:
			args, _ := json.Marshal(map[string]any{"task_id": taskIDs[0], "message": "补充：附上测试清单"})
			return call("c2", subagent.ToolMessage, string(args)), nil
		case !gateOpened:
			return call("c3", "open_gate", `{}`), nil
		case len(taskIDs) == 1:
			args, _ := json.Marshal(map[string]any{"task": "评审次要事项", "model": "stall"})
			return call("c4", subagent.ToolSpawn, string(args)), nil
		case !cancelled:
			args, _ := json.Marshal(map[string]any{"task_id": taskIDs[1]})
			return call("c5", subagent.ToolCancel, string(args)), nil
		case waitResult == "":
			args, _ := json.Marshal(map[string]any{"task_ids": taskIDs})
			return call("c6", subagent.ToolWait, string(args)), nil
		default:
			return []schema.Message{schema.AssistantMessage("sub-agent results: " + waitResult)}, nil
		}
	})

	mgr, err := subagent.New(subagent.Config{
		Models: map[string]llm.ChatModel{"worker": workerModel, "stall": stallModel},
		Tools:  []tool.Tool{awaitGate},
		Roles: map[string]subagent.Role{
			"auditor": {Instructions: "只做检查单审查，回答精炼。", Model: "worker", Tools: []string{"await_gate"}},
		},
		Instructions: "回答简洁，给出可复现的证据。",
	})
	if err != nil {
		log.Fatal(err)
	}

	// 每次创建批次都需要人工审批：审批挂起沿执行树传播到根，回答后 Resume 继续。
	approval := tool.BeforeBatchFunc(func(ctx context.Context, batch tool.Batch) error {
		for _, call := range batch.Calls {
			if call.Spec.Name == subagent.ToolSpawn {
				answer, err := graph.Interrupt(ctx, map[string]any{"action": "approve_spawn", "calls": batch.Calls})
				if err != nil {
					return err
				}
				if approved, ok := answer.(bool); !ok || !approved {
					return errors.New("创建子 Agent 未获批准")
				}
			}
		}
		return nil
	})

	parent, err := agent.New(agent.BaseConfig{
		Name:  "lead",
		Model: parentModel,
		Tools: append(mgr.Tools(), openGate),
	}, agent.WithBeforeToolBatch(approval))
	if err != nil {
		log.Fatal(err)
	}

	// 管理工具经运行选项绑定；审批与恢复共用同一检查点存储。
	opts := append(mgr.RunOptions(), agent.WithCheckpoint(state.NewMemoryStore(), "subagent-demo"))
	messages, err := parent.Invoke(ctx, []schema.Message{schema.UserMessage("请生成发布检查单")}, opts...)
	for {
		var interrupted *graph.InterruptedError
		if !errors.As(err, &interrupted) {
			break
		}
		fmt.Println("approval requested, resuming…")
		resume := append([]runnable.Option{}, opts...)
		for _, p := range interrupted.PendingInterrupts() {
			resume = append(resume, agent.WithResume(p.ID, true))
		}
		messages, err = agent.Resume(ctx, parent, resume...)
	}
	if err != nil {
		log.Fatal(err)
	}
	if len(messages) > 0 {
		fmt.Println(messages[len(messages)-1].JoinedText(""))
	}
}

// resultText 把工具结果块的内容拼成可解析文本。
func resultText(res schema.FunctionToolResult) string {
	var parts []string
	for _, block := range res.ContentBlocks {
		if block.Text != nil {
			parts = append(parts, block.Text.Text)
		}
	}
	return strings.Join(parts, "")
}
