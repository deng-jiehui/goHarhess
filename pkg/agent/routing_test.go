package agent_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"goharness/pkg/agent"
	"goharness/pkg/llm"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

func routingCall(name string) schema.Message {
	message := schema.AssistantMessage("")
	message.FunctionCall = &schema.FunctionCall{Name: name, Arguments: `{}`}
	message.Extra = map[string]any{"function_id": name + "-1"}
	return message
}

func TestRoutingAgentSupervisorReturnsControlAfterSubAgent(t *testing.T) {
	worker, err := agent.NewBasicAgent(agent.BaseConfig{
		Name:  "researcher",
		Model: llm.NewScriptedModel([]schema.Message{schema.AssistantMessage("research result")}),
	})
	if err != nil {
		t.Fatal(err)
	}
	supervisor, err := agent.NewRoutingAgent(agent.SupervisorConfig{
		BaseConfig: agent.BaseConfig{
			Name:  "supervisor",
			Model: llm.NewScriptedModel([][]schema.Message{{routingCall("researcher")}, {schema.AssistantMessage("final answer")}}...),
		},
		SubAgents:     []agent.Agent{worker},
		ReturnControl: true,
		MaxSteps:      2,
	})
	if err != nil {
		t.Fatal(err)
	}
	output, err := supervisor.Invoke(context.Background(), []schema.Message{schema.UserMessage("investigate")})
	if err != nil {
		t.Fatal(err)
	}
	if len(output) != 2 || output[0].Role != schema.RoleFunction || output[0].Text() != "research result" || output[1].Text() != "final answer" {
		t.Fatalf("supervisor output = %#v", output)
	}
}

func TestRoutingAgentRejectsModelStreamWithoutDone(t *testing.T) {
	worker, err := agent.NewBasicAgent(agent.BaseConfig{
		Name:  "worker",
		Model: llm.NewScriptedModel([]schema.Message{schema.AssistantMessage("worker")}),
	})
	if err != nil {
		t.Fatal(err)
	}
	router, err := agent.NewRoutingAgent(agent.SupervisorConfig{
		BaseConfig: agent.BaseConfig{Name: "router", Model: incompleteModel{}},
		SubAgents:  []agent.Agent{worker},
		Mode:       agent.RoutingModeRouter,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := router.Invoke(context.Background(), []schema.Message{schema.UserMessage("route")}); err == nil || !strings.Contains(err.Error(), "without done event") {
		t.Fatalf("routing error = %v", err)
	}
}

func TestRoutingAgentRouterReturnsSubAgentOutputDirectly(t *testing.T) {
	worker, err := agent.NewBasicAgent(agent.BaseConfig{
		Name:  "writer",
		Model: llm.NewScriptedModel([]schema.Message{schema.AssistantMessage("direct result")}),
	})
	if err != nil {
		t.Fatal(err)
	}
	router, err := agent.NewRoutingAgent(agent.SupervisorConfig{
		BaseConfig: agent.BaseConfig{
			Name:  "router",
			Model: llm.NewScriptedModel([][]schema.Message{{routingCall("writer")}}...),
		},
		SubAgents:     []agent.Agent{worker},
		ReturnControl: false,
		MaxSteps:      7,
	})
	if err != nil {
		t.Fatal(err)
	}
	output, err := router.Invoke(context.Background(), []schema.Message{schema.UserMessage("write")})
	if err != nil {
		t.Fatal(err)
	}
	if len(output) != 1 || output[0].Text() != "direct result" || output[0].Name != "writer" {
		t.Fatalf("router output = %#v", output)
	}
}

func TestRoutingAgentDoesNotSendSupervisorToolCallToChild(t *testing.T) {
	var childInput []schema.Message
	worker, err := agent.NewBasicAgent(agent.BaseConfig{
		Name: "worker",
		Model: llm.NewHandlerModel(func(_ context.Context, messages []schema.Message, _ llm.ChatOptions) ([]schema.Message, error) {
			childInput = schema.CloneMessages(messages)
			return []schema.Message{schema.AssistantMessage("child result")}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	router, err := agent.NewRoutingAgent(agent.SupervisorConfig{
		BaseConfig: agent.BaseConfig{Name: "router", Model: llm.NewScriptedModel([]schema.Message{routingCall("worker")})},
		SubAgents:  []agent.Agent{worker}, Mode: agent.RoutingModeRouter,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := router.Invoke(context.Background(), []schema.Message{schema.UserMessage("delegate")}); err != nil {
		t.Fatal(err)
	}
	if len(childInput) != 1 || childInput[0].Role != schema.RoleUser || childInput[0].Text() != "delegate" {
		t.Fatalf("child input=%#v", childInput)
	}
	for _, message := range childInput {
		if message.FunctionCall != nil {
			t.Fatalf("unresolved supervisor call leaked: %#v", message)
		}
	}
}

func TestRoutingAgentExplicitRouterModeAllowsZeroMaxSteps(t *testing.T) {
	worker, err := agent.NewBasicAgent(agent.BaseConfig{
		Name:  "worker",
		Model: llm.NewScriptedModel([]schema.Message{schema.AssistantMessage("router result")}),
	})
	if err != nil {
		t.Fatal(err)
	}
	router, err := agent.NewRoutingAgent(agent.SupervisorConfig{
		BaseConfig: agent.BaseConfig{
			Name:  "router",
			Model: llm.NewScriptedModel([]schema.Message{routingCall("worker")}),
		},
		SubAgents: []agent.Agent{worker},
		Mode:      agent.RoutingModeRouter,
	})
	if err != nil {
		t.Fatal(err)
	}
	output, err := router.Invoke(context.Background(), []schema.Message{schema.UserMessage("route")})
	if err != nil || len(output) != 1 || output[0].Text() != "router result" {
		t.Fatalf("router output=%#v err=%v", output, err)
	}
}

func TestRoutingAgentRejectsUnknownMode(t *testing.T) {
	worker, err := agent.NewBasicAgent(agent.BaseConfig{
		Name:  "worker",
		Model: llm.NewScriptedModel([]schema.Message{schema.AssistantMessage("ok")}),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = agent.NewRoutingAgent(agent.SupervisorConfig{
		BaseConfig: agent.BaseConfig{Name: "router"},
		SubAgents:  []agent.Agent{worker},
		Mode:       agent.RoutingMode("invalid"),
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported routing mode") {
		t.Fatalf("invalid mode error = %v", err)
	}
}

func TestRoutingAgentSupervisorHandlesUnknownSubAgentAndChildError(t *testing.T) {
	failing, err := agent.NewBasicAgent(agent.BaseConfig{
		Name: "failing",
		Model: llm.NewHandlerModel(func(context.Context, []schema.Message, llm.ChatOptions) ([]schema.Message, error) {
			return nil, errors.New("provider unavailable")
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	supervisor, err := agent.NewRoutingAgent(agent.SupervisorConfig{
		BaseConfig: agent.BaseConfig{
			Name: "supervisor",
			Model: llm.NewScriptedModel(
				[]schema.Message{routingCall("missing")},
				[]schema.Message{routingCall("failing")},
				[]schema.Message{schema.AssistantMessage("recovered")},
			),
		},
		SubAgents:     []agent.Agent{failing},
		Mode:          agent.RoutingModeSupervisor,
		ReturnControl: true,
		MaxSteps:      3,
	})
	if err != nil {
		t.Fatal(err)
	}
	output, err := supervisor.Invoke(context.Background(), []schema.Message{schema.UserMessage("route safely")})
	if err != nil || len(output) == 0 || output[len(output)-1].Text() != "recovered" {
		t.Fatalf("supervisor output=%#v err=%v", output, err)
	}
	joined := ""
	for _, message := range output {
		joined += message.Text() + "\n"
	}
	if !strings.Contains(joined, "unregistered") || !strings.Contains(joined, "provider unavailable") {
		t.Fatalf("supervisor observations=%q", joined)
	}
}

func TestRoutingAgentRouterPropagatesUnknownAndChildErrors(t *testing.T) {
	worker, err := agent.NewBasicAgent(agent.BaseConfig{
		Name: "worker",
		Model: llm.NewHandlerModel(func(context.Context, []schema.Message, llm.ChatOptions) ([]schema.Message, error) {
			return nil, errors.New("child failed")
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"missing", "worker"} {
		router, buildErr := agent.NewRoutingAgent(agent.SupervisorConfig{
			BaseConfig: agent.BaseConfig{
				Name:  "router",
				Model: llm.NewScriptedModel([]schema.Message{routingCall(name)}),
			},
			SubAgents: []agent.Agent{worker},
			Mode:      agent.RoutingModeRouter,
		})
		if buildErr != nil {
			t.Fatal(buildErr)
		}
		if _, invokeErr := router.Invoke(context.Background(), []schema.Message{schema.UserMessage("route")}); invokeErr == nil {
			t.Fatalf("router call %q unexpectedly succeeded", name)
		}
	}
}

func TestRoutingAgentStopsAtMaxStepsWithoutLooping(t *testing.T) {
	var calls atomic.Int32
	worker, err := agent.NewBasicAgent(agent.BaseConfig{
		Name:  "worker",
		Model: llm.NewScriptedModel([]schema.Message{schema.AssistantMessage("ok")}),
	})
	if err != nil {
		t.Fatal(err)
	}
	supervisor, err := agent.NewRoutingAgent(agent.SupervisorConfig{
		BaseConfig: agent.BaseConfig{
			Name: "supervisor",
			Model: llm.NewHandlerModel(func(context.Context, []schema.Message, llm.ChatOptions) ([]schema.Message, error) {
				calls.Add(1)
				return []schema.Message{routingCall("worker")}, nil
			}),
		},
		SubAgents:     []agent.Agent{worker},
		Mode:          agent.RoutingModeSupervisor,
		ReturnControl: true,
		MaxSteps:      2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := supervisor.Invoke(context.Background(), []schema.Message{schema.UserMessage("do not loop")}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("model calls=%d, want max steps 2", calls.Load())
	}
}

type closeOnlyAgent struct{}

func (closeOnlyAgent) Name() string        { return "closed" }
func (closeOnlyAgent) Description() string { return "malformed test agent" }
func (closeOnlyAgent) Invoke(context.Context, []schema.Message, ...runnable.Option) ([]schema.Message, error) {
	return nil, nil
}
func (closeOnlyAgent) Stream(context.Context, []schema.Message, ...runnable.Option) (runnable.Stream[schema.MessageEvent], error) {
	stream := make(chan runnable.Item[schema.MessageEvent])
	close(stream)
	return stream, nil
}

func TestRoutingAgentRejectsChildStreamWithoutDone(t *testing.T) {
	router, err := agent.NewRoutingAgent(agent.SupervisorConfig{
		BaseConfig: agent.BaseConfig{
			Name:  "router",
			Model: llm.NewScriptedModel([]schema.Message{routingCall("closed")}),
		},
		SubAgents: []agent.Agent{closeOnlyAgent{}},
		Mode:      agent.RoutingModeRouter,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := router.Invoke(context.Background(), []schema.Message{schema.UserMessage("route")}); err == nil || !strings.Contains(err.Error(), "without done event") {
		t.Fatalf("malformed child error=%v", err)
	}
}
