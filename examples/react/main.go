// examples/react 用 graph.NewState 门面手写一个 ReAct 循环：模型节点
// 产出工具调用，工具节点执行并回写观察，Branch 路由按是否还有待处理
// 调用决定循环或结束。生产路径直接用 agent.New 的预置配方。
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/tool"
)

type reactState struct {
	Messages []schema.Message
	Pending  []schema.FunctionToolCall
}

func main() {
	weather := tool.New("weather", "查询城市天气", func(_ context.Context, a struct {
		City string `json:"city"`
	}) (string, error) {
		return a.City + ": sunny, 24C", nil
	}, tool.ReadOnly())
	model := llm.NewScriptedModel(
		[]schema.Message{schema.NewMessage(schema.RoleAssistant,
			schema.NewFunctionToolCallBlock("1", "weather", `{"city":"shanghai"}`))},
		[]schema.Message{schema.AssistantMessage("shanghai is sunny, 24C")},
	)
	spec := weather.Spec()
	defs := []schema.FunctionDefinition{{Name: spec.Name, Description: spec.Description, Parameters: spec.Parameters}}

	app, err := graph.NewState[reactState]("react").
		Node("think", func(ctx context.Context, s *reactState) error {
			resp, err := model.Invoke(ctx, llm.Request{Messages: s.Messages, Functions: defs})
			if err != nil {
				return err
			}
			s.Messages = append(s.Messages, resp.Messages...)
			for _, m := range resp.Messages {
				s.Pending = append(s.Pending, m.FunctionToolCalls()...)
			}
			return nil
		}).
		Node("act", func(ctx context.Context, s *reactState) error {
			for _, call := range s.Pending {
				result, err := weather.Invoke(ctx, tool.CallInput{RawArgs: call.Arguments})
				if err != nil {
					return err
				}
				blocks, err := result.ToContentBlocks()
				if err != nil {
					return err
				}
				s.Messages = append(s.Messages, schema.NewMessage(schema.RoleTool,
					schema.NewFunctionToolResultBlock(call.ID, call.Name, false, blocks...)))
			}
			s.Pending = nil
			return nil
		}).
		Start("think").
		Branch("think", func(_ context.Context, s *reactState) (string, error) {
			if len(s.Pending) == 0 {
				return graph.End, nil
			}
			return "act", nil
		}, "act", graph.End).
		Edge("act", "think").
		Compile()
	must(err)

	out, err := app.Invoke(context.Background(), reactState{
		Messages: []schema.Message{schema.UserMessage("what is the weather in shanghai?")},
	})
	must(err)
	fmt.Println(out.Messages[len(out.Messages)-1].JoinedText(""))
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
