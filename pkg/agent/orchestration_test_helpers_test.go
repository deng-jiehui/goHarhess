package agent_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
)

// supervisorState 是编排测试 harness 的图状态：决策模型产生的函数调用进入
// Pending，由路由逐一分发给注册的子 AgentNode。
type supervisorState struct {
	Messages []schema.Message
	Pending  []schema.FunctionToolCall
	Dispatch []schema.Message
	Steps    int
	Final    []schema.Message
}

// routingCall 生成一条调用 name 的函数调用消息，模拟主管模型的路由决策。
func routingCall(name string) schema.Message {
	return schema.NewMessage(schema.RoleAssistant,
		schema.NewFunctionToolCallBlock("call-"+name, name, `{"task":"go"}`))
}

// newSupervisorApp 用图门面 + AddNode(AgentNode) 组一个最小主管编排图，
// 替代已下线的 RoutingAgent 作为嵌套执行语义的测试壳：
// decide →(路由)→ agent:<name> → decide（Supervisor）或 finish（Router）。
// router=true 时首个子结果直接作为图输出。maxSteps 限制决策次数。
func newSupervisorApp(t *testing.T, name string, model llm.ChatModel, router bool, maxSteps int, members ...agent.Agent) *graph.CompiledGraph[supervisorState, *supervisorState] {
	t.Helper()
	if maxSteps <= 0 {
		maxSteps = 8
	}
	functions := make([]schema.FunctionDefinition, 0, len(members))
	for _, m := range members {
		functions = append(functions, schema.FunctionDefinition{
			Name:        m.Name(),
			Description: m.Description(),
			Parameters:  map[string]any{"type": "object", "additionalProperties": true},
		})
	}
	modelNode, err := llm.NewChatModelNode(model)
	if err != nil {
		t.Fatal(err)
	}
	builder := graph.NewState[supervisorState](name)
	if err := builder.Graph().AddNode("decide", modelNode,
		graph.WithPreHandler(func(_ context.Context, s *supervisorState) (llm.Request, error) {
			return llm.Request{Messages: schema.CloneMessages(s.Messages), Functions: functions}, nil
		}),
		graph.WithPostHandler(func(_ context.Context, s *supervisorState, resp llm.Response) error {
			// Dispatch 是派发前的只读视图：不含主管自己的调用消息。
			s.Dispatch = schema.CloneMessages(s.Messages)
			s.Messages = append(s.Messages, resp.Messages...)
			for _, m := range resp.Messages {
				s.Pending = append(s.Pending, m.FunctionToolCalls()...)
			}
			s.Steps++
			return nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	targets := make([]string, 0, len(members)+1)
	for _, member := range members {
		member := member
		nodeID := "agent:" + member.Name()
		agentNode, err := agent.NewAgentNode(member)
		if err != nil {
			t.Fatal(err)
		}
		if err := builder.Graph().AddNode(nodeID, agentNode,
			graph.WithPreHandler(func(_ context.Context, s *supervisorState) ([]schema.Message, error) {
				return schema.CloneMessages(s.Dispatch), nil
			}),
			graph.WithPostHandler(func(_ context.Context, s *supervisorState, oc runnable.Outcome[[]schema.Message]) error {
				var call schema.FunctionToolCall
				if len(s.Pending) > 0 {
					call = s.Pending[0]
					s.Pending = s.Pending[1:]
				}
				failed := oc.Err != nil
				text := joinMemberOutput(oc.Value)
				if failed {
					text = fmt.Sprintf("Sub-agent %q failed: %v", member.Name(), oc.Err)
				}
				s.Messages = append(s.Messages, schema.NewMessage(schema.RoleTool,
					schema.NewFunctionToolResultBlock(call.ID, call.Name, failed, schema.NewTextBlock(text))))
				if router {
					s.Final = oc.Value
				}
				return nil
			}),
		); err != nil {
			t.Fatal(err)
		}
		targets = append(targets, nodeID)
	}
	builder.Node("finish", func(_ context.Context, s *supervisorState) error {
		if router && s.Final != nil {
			s.Messages = s.Final
		}
		return nil
	})
	builder.Start("decide")
	targets = append(targets, "finish")
	builder.Branch("decide", func(_ context.Context, s *supervisorState) (string, error) {
		if len(s.Pending) == 0 || s.Steps >= maxSteps {
			return "finish", nil
		}
		return "agent:" + s.Pending[0].Name, nil
	}, targets...)
	for _, member := range members {
		if router {
			builder.Edge("agent:"+member.Name(), "finish")
		} else {
			builder.Edge("agent:"+member.Name(), "decide")
		}
	}
	builder.Edge("finish", graph.End)
	app, err := builder.Compile()
	if err != nil {
		t.Fatal(err)
	}
	return app
}

func joinMemberOutput(messages []schema.Message) string {
	parts := make([]string, 0, len(messages))
	for _, m := range messages {
		if text := strings.TrimSpace(m.JoinedText("")); text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, "\n")
}
