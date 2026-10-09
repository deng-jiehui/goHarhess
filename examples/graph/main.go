package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/schema"
)

type teamState struct {
	Messages []schema.Message
	Output   []schema.Message
}

func main() {
	researcher := mustAgent("researcher", "research completed")
	reviewer := mustAgent("reviewer", "review completed")

	run := func(a agent.Agent) func(context.Context, *teamState) error {
		return func(ctx context.Context, s *teamState) error {
			out, err := a.Invoke(ctx, s.Messages)
			s.Messages = append(s.Messages, out...)
			s.Output = out
			return err
		}
	}
	app, err := graph.NewState[teamState]("research-team").
		Node("research", run(researcher)).
		Node("review", run(reviewer)).
		Node("finish", func(_ context.Context, s *teamState) error { return nil }).
		Start("research").
		Edge("research", "review").
		Edge("review", "finish").
		Edge("finish", graph.End).
		Compile()
	must(err)

	out, err := app.Invoke(context.Background(), teamState{
		Messages: []schema.Message{schema.UserMessage("analyze the topic")},
	})
	must(err)

	data, err := json.MarshalIndent(out.Messages, "", "  ")
	must(err)
	fmt.Println(string(data))
}

func mustAgent(name, response string) agent.Agent {
	value, err := agent.New(
		llm.NewScriptedModel([]schema.Message{schema.AssistantMessage(response)}),
		agent.WithName(name),
	)
	must(err)
	return value
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
