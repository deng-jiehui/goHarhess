// examples/groupchat 用 graph.NewState 门面手写群聊循环：
// 每轮由选择器挑一个成员 Agent 发言，消息追加到共享会话，
// 达到轮数上限后结束。
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/schema"
)

const maxRounds = 3

type chatState struct {
	Messages []schema.Message
	Round    int
	Next     string
}

func main() {
	ctx := context.Background()
	names := []string{"analyst", "critic"}
	members := []agent.Agent{
		mustAgent(names[0], "analyst: finding recorded", "analyst: summary posted"),
		mustAgent(names[1], "critic: risk noted", "critic: follow-up"),
	}
	pick := func(_ context.Context, s *chatState) error {
		// 轮询选择器：接替上一个发言者。
		for i, name := range names {
			if name == s.Next {
				s.Next = names[(i+1)%len(names)]
				return nil
			}
		}
		s.Next = names[0]
		return nil
	}
	speak := func(ctx context.Context, s *chatState) error {
		for i, m := range members {
			if names[i] == s.Next {
				out, err := m.Invoke(ctx, s.Messages)
				s.Messages = append(s.Messages, out...)
				s.Round++
				return err
			}
		}
		return fmt.Errorf("selected agent %q is not registered", s.Next)
	}

	app, err := graph.NewState[chatState]("group-chat").
		Node("pick", pick).
		Node("speak", speak).
		Start("pick").
		Edge("pick", "speak").
		Branch("speak", func(_ context.Context, s *chatState) (string, error) {
			if s.Round >= maxRounds {
				return graph.End, nil
			}
			return "pick", nil
		}, "pick", graph.End).
		Compile()
	must(err)

	out, err := app.Invoke(ctx, chatState{
		Messages: []schema.Message{schema.UserMessage("discuss the release plan")},
	})
	must(err)
	for _, m := range out.Messages {
		if text := m.JoinedText(""); text != "" {
			fmt.Println(text)
		}
	}
}

func mustAgent(name string, responses ...string) agent.Agent {
	turns := make([][]schema.Message, 0, len(responses))
	for _, text := range responses {
		turns = append(turns, []schema.Message{schema.AssistantMessage(text)})
	}
	value, err := agent.New(llm.NewScriptedModel(turns...), agent.WithName(name))
	must(err)
	return value
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
