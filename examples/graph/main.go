package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
)

func main() {
	researcher := mustAgent("researcher", "research completed")
	reviewer := mustAgent("reviewer", "review completed")

	type teamState struct {
		Messages []schema.Message
		Output   []schema.Message
	}
	builder := graph.NewGraph[[]schema.Message, []schema.Message]("research-team")
	builder.SetDescription("Research, then review")
	err := builder.SetStateFactory(func(_ context.Context, input []schema.Message) (*teamState, error) {
		return &teamState{Messages: schema.CloneMessages(input)}, nil
	})
	addAgent := func(name string, value agent.Agent) error {
		return builder.AddNode(name, value,
			graph.WithPreHandler(func(_ context.Context, state *teamState) ([]schema.Message, error) {
				return schema.CloneMessages(state.Messages), nil
			}),
			graph.WithPostHandler(func(_ context.Context, state *teamState, output []schema.Message) error {
				state.Messages = append(state.Messages, schema.CloneMessages(output)...)
				state.Output = append(state.Output, schema.CloneMessages(output)...)
				return nil
			}),
		)
	}
	if err == nil {
		err = addAgent("research", researcher)
	}
	if err == nil {
		err = addAgent("review", reviewer)
	}
	finish := runnable.Func[[]schema.Message, []schema.Message](func(_ context.Context, input []schema.Message, _ ...runnable.Option) ([]schema.Message, error) {
		return schema.CloneMessages(input), nil
	})
	if err == nil {
		err = builder.AddNode("finish", finish, graph.WithPreHandler(func(_ context.Context, state *teamState) ([]schema.Message, error) {
			return schema.CloneMessages(state.Output), nil
		}))
	}
	if err == nil {
		err = builder.AddEdge(graph.Start, "research")
	}
	if err == nil {
		err = builder.AddEdge("research", "review")
	}
	if err == nil {
		err = builder.AddEdge("review", "finish")
	}
	if err == nil {
		err = builder.AddEdge("finish", graph.End)
	}
	must(err)
	team, err := builder.Compile()
	must(err)

	messages, err := team.Invoke(context.Background(), []schema.Message{schema.UserMessage("analyze the topic")})
	must(err)

	data, err := json.MarshalIndent(messages, "", "  ")
	must(err)
	fmt.Println(string(data))
}

func mustAgent(name, response string) *agent.BasicAgent {
	value, err := agent.NewBasicAgent(agent.BaseConfig{
		Name:  name,
		Model: llm.NewScriptedModel([]schema.Message{schema.AssistantMessage(response)}),
	})
	must(err)
	return value
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
