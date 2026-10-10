package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/goharness/goharness/extensions/subagent"
	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/state"
	"github.com/goharness/goharness/pkg/tool"
)

// 父 Agent 通过 extensions/subagent 的管理工具演示子 Agent 生命周期：
// spawn_agent 创建并同步执行首个任务 → followup_task 沿用上下文追加 →
// close_agent 收尾。创建批次需要人工审批：审批挂起沿命名空间执行树传播
// 到根，回答后 Resume 继续；示例用本地脚本模型，无需外部模型服务。
func main() {
	ctx := context.Background()

	// 子 Agent 的模型：第一次任务输出检查单，追加任务时请求里已带此前的
	// 全部消息（会话上下文沿用），据此输出修订版。
	workerModel := llm.NewHandlerModel(func(_ context.Context, req llm.Request, _ llm.Options) ([]schema.Message, error) {
		tasks := 0
		for _, msg := range req.Messages {
			if msg.Role == schema.RoleUser {
				tasks++
			}
		}
		if tasks <= 1 {
			return []schema.Message{schema.AssistantMessage("checklist: build, test, docs")}, nil
		}
		return []schema.Message{schema.AssistantMessage("checklist(revised): build, test, docs, changelog")}, nil
	})

	mgr, err := subagent.New(subagent.Config{
		Models: map[string]llm.ChatModel{"worker": workerModel},
		Roles: map[string]subagent.Role{
			"auditor": {Instructions: "只做检查单审查，回答精炼。", Model: "worker"},
		},
		Instructions: "回答简洁，给出可复现的证据。",
		Store:        state.NewMemoryStore(),
	})
	if err != nil {
		log.Fatal(err)
	}

	// 父模型按已收到的工具结果推进：创建 → 追加 → 关闭 → 汇总。
	parentModel := llm.NewHandlerModel(func(_ context.Context, req llm.Request, _ llm.Options) ([]schema.Message, error) {
		var agentID, spawnResult, followupResult string
		closed := false
		for _, msg := range req.Messages {
			for _, res := range msg.FunctionToolResults() {
				text := resultText(res)
				switch res.Name {
				case subagent.ToolSpawn:
					var out struct {
						AgentID string `json:"agent_id"`
						Result  string `json:"result"`
					}
					if json.Unmarshal([]byte(text), &out) == nil {
						agentID, spawnResult = out.AgentID, out.Result
					}
				case subagent.ToolFollowup:
					var out struct {
						Result string `json:"result"`
					}
					if json.Unmarshal([]byte(text), &out) == nil {
						followupResult = out.Result
					}
				case subagent.ToolClose:
					closed = true
				}
			}
		}
		call := func(id, name, args string) []schema.Message {
			return []schema.Message{schema.NewMessage(schema.RoleAssistant,
				schema.NewFunctionToolCallBlock(id, name, args))}
		}
		switch {
		case agentID == "":
			args, _ := json.Marshal(map[string]any{"task": "整理发布检查单，三条以内", "role": "auditor"})
			return call("c1", subagent.ToolSpawn, string(args)), nil
		case followupResult == "":
			args, _ := json.Marshal(map[string]any{"agent_id": agentID, "task": "补充：附上变更日志"})
			return call("c2", subagent.ToolFollowup, string(args)), nil
		case !closed:
			args, _ := json.Marshal(map[string]any{"agent_id": agentID})
			return call("c3", subagent.ToolClose, string(args)), nil
		default:
			return []schema.Message{schema.AssistantMessage("sub-agent results: " + spawnResult + " | " + followupResult)}, nil
		}
	})

	// 创建批次需要人工审批：审批挂起沿执行树传播到根，回答后 Resume 继续。
	approval := tool.BeforeBatchFunc(func(ctx context.Context, batch tool.Batch) error {
		for _, call := range batch.Calls {
			if call.Spec.Name != subagent.ToolSpawn {
				continue
			}
			answer, err := graph.Interrupt(ctx, map[string]any{"action": "approve_spawn", "calls": batch.Calls})
			if err != nil {
				return err
			}
			if approved, ok := answer.(bool); !ok || !approved {
				return errors.New("创建子 Agent 未获批准")
			}
		}
		return nil
	})

	parent, err := agent.New(parentModel,
		agent.WithName("lead"),
		agent.WithTools(mgr.Tools()...),
		agent.WithBeforeToolBatch(approval))
	if err != nil {
		log.Fatal(err)
	}

	// 父运行开检查点：审批恢复与子 Agent 的命名空间记录共用同一存储。
	opts := []runnable.Option{agent.WithCheckpoint(state.NewMemoryStore(), "subagent-demo")}
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
