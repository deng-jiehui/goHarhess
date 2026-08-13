package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"goharness/pkg/agent"
	"goharness/pkg/compose"
	"goharness/pkg/graph"
	"goharness/pkg/llm"
	"goharness/pkg/schema"
)

func main() {
	researcher := mustAgent("researcher", "research completed")
	reviewer := mustAgent("reviewer", "review completed")

	team, err := compose.NewGraph("research-team").
		Description("Research, then review").
		Agent("research", researcher).
		Agent("review", reviewer).
		Edge(graph.Start, "research").
		Edge("research", "review").
		Edge("review", graph.End).
		Build()
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
