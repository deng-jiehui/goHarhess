// examples/routing 用 graph.NewState 门面手写主管路由循环：
// 主管模型每次决定是否调用成员 Agent（以函数调用形式），成员结果作为
// 工具观察写回会话，直到没有待处理调用或达到步数上限。
package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/schema"
)

const maxSteps = 8

type routeState struct {
	Messages []schema.Message
	Pending  []schema.FunctionToolCall
	Steps    int
}

func main() {
	ctx := context.Background()
	worker := mustAgent("worker", "worker completed: evidence attached")
	members := map[string]agent.Agent{worker.Name(): worker}
	functions := []schema.FunctionDefinition{{
		Name:        worker.Name(),
		Description: worker.Description(),
		Parameters:  map[string]any{"type": "object", "additionalProperties": true},
	}}
	// 主管模型：第一轮决定调用 worker，第二轮总结后不再调用。
	supervisor := llm.NewScriptedModel(
		[]schema.Message{schema.NewMessage(schema.RoleAssistant,
			schema.NewFunctionToolCallBlock("1", "worker", `{"task":"checklist"}`))},
		[]schema.Message{schema.AssistantMessage("routing done")},
	)

	call := func(model llm.ChatModel, functions []schema.FunctionDefinition) func(context.Context, *routeState) error {
		return func(ctx context.Context, s *routeState) error {
			resp, err := model.Invoke(ctx, llm.Request{Messages: s.Messages, Functions: functions})
			if err != nil {
				return err
			}
			s.Messages = append(s.Messages, resp.Messages...)
			for _, m := range resp.Messages {
				s.Pending = append(s.Pending, m.FunctionToolCalls()...)
			}
			s.Steps++
			return nil
		}
	}
	dispatch := func(ctx context.Context, s *routeState) error {
		for _, c := range s.Pending {
			member, ok := members[c.Name]
			if !ok {
				return fmt.Errorf("routed agent %q is not registered", c.Name)
			}
			out, err := member.Invoke(ctx, s.Messages)
			content := joinText(out)
			if err != nil {
				content = fmt.Sprintf("Sub-agent %q failed", c.Name)
			}
			s.Messages = append(s.Messages, schema.NewMessage(schema.RoleTool,
				schema.NewFunctionToolResultBlock(c.ID, c.Name, err != nil, schema.NewTextBlock(content))))
		}
		s.Pending = nil
		return nil
	}

	app, err := graph.NewState[routeState]("routing").
		Node("supervisor", call(supervisor, functions)).
		Node("members", dispatch).
		Start("supervisor").
		Branch("supervisor", func(_ context.Context, s *routeState) (string, error) {
			if len(s.Pending) == 0 || s.Steps >= maxSteps {
				return graph.End, nil
			}
			return "members", nil
		}, "members", graph.End).
		Edge("members", "supervisor").
		Compile()
	must(err)

	out, err := app.Invoke(ctx, routeState{
		Messages: []schema.Message{schema.UserMessage("please review the checklist")},
	})
	must(err)
	for _, m := range out.Messages {
		if text := m.JoinedText(""); text != "" {
			fmt.Println(text)
		}
	}
}

func joinText(messages []schema.Message) string {
	parts := make([]string, 0, len(messages))
	for _, m := range messages {
		if text := strings.TrimSpace(m.JoinedText("")); text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, "\n")
}

func mustAgent(name, response string) agent.Agent {
	value, err := agent.New(
		llm.NewScriptedModel([]schema.Message{schema.AssistantMessage(response)}),
		agent.WithName(name),
		agent.WithDescription(name+" member agent"),
	)
	must(err)
	return value
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
