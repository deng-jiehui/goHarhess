package agent_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/goharness/goharness/pkg/agent"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
)

func routingCall(name string) schema.Message {
	return schema.NewMessage(schema.RoleAssistant, schema.NewFunctionToolCallBlock(name+"-1", name, `{}`))
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
	if len(output) != 2 || output[0].Role != schema.RoleTool || toolResultText(output[0]) != "research result" || output[1].JoinedText("") != "final answer" {
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
	if _, err := router.Invoke(context.Background(), []schema.Message{schema.UserMessage("route")}); err == nil || !strings.Contains(err.Error(), "without a result") {
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
	if len(output) != 1 || output[0].JoinedText("") != "direct result" || output[0].Extra["agent_name"] != "writer" {
		t.Fatalf("router output = %#v", output)
	}
}

func TestRoutingAgentDoesNotSendSupervisorToolCallToChild(t *testing.T) {
	var childInput []schema.Message
	worker, err := agent.NewBasicAgent(agent.BaseConfig{
		Name: "worker",
		Model: llm.NewHandlerModel(func(_ context.Context, request llm.Request, _ llm.Options) ([]schema.Message, error) {
			childInput = schema.CloneMessages(request.Messages)
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
	if len(childInput) != 1 || childInput[0].Role != schema.RoleUser || childInput[0].JoinedText("") != "delegate" {
		t.Fatalf("child input=%#v", childInput)
	}
	for _, message := range childInput {
		if len(message.FunctionToolCalls()) != 0 {
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
	if err != nil || len(output) != 1 || output[0].JoinedText("") != "router result" {
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
		Model: llm.NewHandlerModel(func(context.Context, llm.Request, llm.Options) ([]schema.Message, error) {
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
	if err != nil || len(output) == 0 || output[len(output)-1].JoinedText("") != "recovered" {
		t.Fatalf("supervisor output=%#v err=%v", output, err)
	}
	joined := ""
	for _, message := range output {
		joined += toolResultText(message) + message.JoinedText("") + "\n"
	}
	// 未注册的子代理名对模型可见（可自纠）；子 Agent 内部错误细节已脱敏。
	if !strings.Contains(joined, "unregistered sub-agent") || !strings.Contains(joined, `Sub-agent "failing" failed`) {
		t.Fatalf("supervisor observations=%q", joined)
	}
	if strings.Contains(joined, "provider unavailable") {
		t.Fatalf("internal child error leaked to model: %q", joined)
	}
}

func TestRoutingAgentRouterPropagatesUnknownAndChildErrors(t *testing.T) {
	worker, err := agent.NewBasicAgent(agent.BaseConfig{
		Name: "worker",
		Model: llm.NewHandlerModel(func(context.Context, llm.Request, llm.Options) ([]schema.Message, error) {
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
			Model: llm.NewHandlerModel(func(context.Context, llm.Request, llm.Options) ([]schema.Message, error) {
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
func (closeOnlyAgent) Stream(ctx context.Context, _ []schema.Message, _ ...runnable.Option) (*runnable.StreamReader[[]schema.Message], error) {
	reader, writer := runnable.Pipe[[]schema.Message](ctx)
	go func() { _ = writer.CloseWithError(errors.New("child stream failed")) }()
	return reader, nil
}
func (a closeOnlyAgent) Collect(ctx context.Context, in *runnable.StreamReader[[]schema.Message], opts ...runnable.Option) ([]schema.Message, error) {
	return runnable.CollectFromInvoke(ctx, in, a.Invoke, opts...)
}
func (a closeOnlyAgent) Transform(ctx context.Context, in *runnable.StreamReader[[]schema.Message], opts ...runnable.Option) (*runnable.StreamReader[[]schema.Message], error) {
	return runnable.TransformFromInvoke(ctx, in, a.Invoke, opts...)
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
	if _, err := router.Invoke(context.Background(), []schema.Message{schema.UserMessage("route")}); err == nil || !strings.Contains(err.Error(), "child stream failed") {
		t.Fatalf("malformed child error=%v", err)
	}
}
