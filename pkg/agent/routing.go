package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"goharness/internal/agentrecipe"
	"goharness/pkg/llm"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

const DefaultRoutingMaxSteps = 8

// RoutingMode makes the supervisor/router choice explicit when a config is
// assembled from zero values or external data. ReturnControl remains the
// convenient boolean shorthand for callers that do not need a named mode.
type RoutingMode string

const (
	RoutingModeSupervisor RoutingMode = "supervisor"
	RoutingModeRouter     RoutingMode = "router"
)

// SupervisorConfig configures a RoutingAgent. Model overrides BaseConfig.Model
// when provided. Use DefaultSupervisorConfig for the zero-ambiguity default of
// returning control to the supervisor after each routed Agent call.
type SupervisorConfig struct {
	BaseConfig

	SubAgents []Agent
	Model     llm.ChatModel
	// Mode is optional for source-level convenience. Set it when the config is
	// decoded from data where false cannot distinguish omitted from explicit
	// Router mode.
	Mode RoutingMode

	// ReturnControl selects supervisor behavior when Mode is empty. When false,
	// Router mode runs exactly one dispatch and returns that Agent's result
	// directly. Prefer Mode when configs are decoded from external data because
	// Go cannot distinguish an omitted bool from false.
	ReturnControl bool
	MaxSteps      int
}

func DefaultSupervisorConfig() SupervisorConfig {
	return SupervisorConfig{Mode: RoutingModeSupervisor, ReturnControl: true, MaxSteps: DefaultRoutingMaxSteps}
}

// RoutingAgent uses a model to select sub-Agents through standard function
// calls. It never owns a bespoke execution loop; its workflow is an internal
// Graph recipe like the other built-in Agents.
type RoutingAgent struct {
	*base
	returnControl bool
	maxSteps      int
	functions     []schema.FunctionDefinition
	recipe        *agentrecipe.Recipe
}

func NewRoutingAgent(cfg SupervisorConfig, opts ...Option) (*RoutingAgent, error) {
	baseConfig := cfg.BaseConfig
	if cfg.Model != nil {
		baseConfig.Model = cfg.Model
	}
	base, err := newBase(baseConfig, opts...)
	if err != nil {
		return nil, err
	}
	if len(base.toolOrder) > 0 {
		return nil, errors.New("RoutingAgent routes SubAgents; use ReactAgent for Tool execution")
	}
	hub, err := NewHub(cfg.SubAgents)
	if err != nil {
		return nil, fmt.Errorf("configure routing sub-agents: %w", err)
	}
	returnControl, err := resolveRoutingMode(cfg)
	if err != nil {
		return nil, err
	}
	maxSteps := cfg.MaxSteps
	if returnControl {
		if maxSteps <= 0 {
			maxSteps = DefaultRoutingMaxSteps
		}
	} else {
		maxSteps = 1
	}
	functions, err := routingFunctions(hub.Agents())
	if err != nil {
		return nil, err
	}
	a := &RoutingAgent{
		base:          base,
		returnControl: returnControl,
		maxSteps:      maxSteps,
		functions:     functions,
	}
	agentNodes := make([]agentrecipe.RoutedAgent, 0, len(hub.Agents()))
	for _, child := range hub.Agents() {
		node, nodeErr := NewAgentNode(child)
		if nodeErr != nil {
			return nil, nodeErr
		}
		agentNodes = append(agentNodes, agentrecipe.RoutedAgent{Name: child.Name(), Node: node})
	}
	a.recipe, err = agentrecipe.NewRoutingRecipe(recipeName(base, string(AgentKindRouting)), agentrecipe.RoutingHooks{
		MaxSteps: maxSteps,
		Model:    a.modelNode,
		Agents:   agentNodes,
		PrepareModel: func(ctx context.Context, messages []schema.Message) (llm.Request, error) {
			runOpts, _ := resolvedRunOptionsFromContext(ctx)
			return a.prepareLLMRequest(ctx, messages, a.functions, runOpts)
		},
		DetectCalls:  detectRoutingCalls,
		Observation:  routingObservation,
		NameMessages: a.withAgentName,
	})
	if err != nil {
		return nil, err
	}
	return a, nil
}

func resolveRoutingMode(cfg SupervisorConfig) (bool, error) {
	switch cfg.Mode {
	case "":
		// Keep the zero-value config useful as a Supervisor. Router mode is
		// unambiguous with Mode: RoutingModeRouter.
		return cfg.ReturnControl || cfg.MaxSteps <= 0, nil
	case RoutingModeSupervisor:
		return true, nil
	case RoutingModeRouter:
		return false, nil
	default:
		return false, fmt.Errorf("unsupported routing mode %q", cfg.Mode)
	}
}

func (a *RoutingAgent) stream(ctx context.Context, messages []schema.Message, opts ...RunOption) (*runnable.StreamReader[[]schema.Message], error) {
	runOpts := resolveRunOptions(a.limits, opts...)
	runOpts.agentKind = AgentKindRouting
	loaded, session, err := a.loadConversation(ctx, messages, &runOpts)
	if err != nil {
		return nil, err
	}
	ctx = bindRunContext(ctx, session)
	prepared := a.prepareMessages(loaded, &runOpts)
	runCtx, lifecycle, err := a.beginRun(ctx, prepared, runOpts)
	if err != nil {
		return nil, err
	}
	lifecycle.session = session
	runOpts.session = session
	runCtx = withResolvedRunOptions(runCtx, runOpts)
	runCtx = a.bindModelStream(runCtx)
	runCtx = withToolExecutionLimits(runCtx, runOpts)
	runCtx = withRecipeCompletion(runCtx, session)
	runCtx = agentrecipe.WithRoutingRunConfig(runCtx, agentrecipe.RoutingRunConfig{
		MaxSteps:      a.maxSteps,
		ReturnControl: a.returnControl,
		Input:         conversationInput(session, messages),
	})
	var stream *runnable.StreamReader[[]schema.Message]
	if runOpts.resume {
		stream, err = a.recipe.ResumeStream(runCtx, graphNodeOptions(runOpts)...)
	} else {
		stream, err = a.recipe.Stream(runCtx, prepared, graphNodeOptions(runOpts)...)
	}
	if err != nil {
		lifecycle.fail(err)
		return nil, err
	}
	return lifecycle.wrap(stream)
}

func routingFunctions(agents []Agent) ([]schema.FunctionDefinition, error) {
	functions := make([]schema.FunctionDefinition, 0, len(agents))
	seen := map[string]struct{}{}
	for _, value := range agents {
		if value == nil {
			return nil, errors.New("routing sub-agent cannot be nil")
		}
		name := strings.TrimSpace(value.Name())
		if name == "" {
			return nil, errors.New("routing sub-agent requires a name")
		}
		if _, exists := seen[name]; exists {
			return nil, fmt.Errorf("duplicate routing sub-agent %q", name)
		}
		seen[name] = struct{}{}
		functions = append(functions, schema.FunctionDefinition{
			Name:        name,
			Description: value.Description(),
			Parameters:  map[string]any{"type": "object", "additionalProperties": true},
		})
	}
	return functions, nil
}

func detectRoutingCalls(messages []schema.Message) []agentrecipe.Call {
	calls := make([]agentrecipe.Call, 0)
	for _, message := range messages {
		for _, call := range message.FunctionToolCalls() {
			calls = append(calls, agentrecipe.Call{
				Name:       call.Name,
				Arguments:  call.Arguments,
				FunctionID: call.ID,
				Structured: true,
			})
		}
	}
	return calls
}

func routingObservation(result agentrecipe.RoutedResult) schema.Message {
	content := ""
	if result.Err != nil {
		content = fmt.Sprintf("Sub-agent %q failed: %v", result.Call.Name, result.Err)
	} else {
		parts := make([]string, 0, len(result.Messages))
		for _, message := range result.Messages {
			if text := strings.TrimSpace(message.JoinedText("")); text != "" {
				parts = append(parts, text)
			}
		}
		content = strings.Join(parts, "\n")
	}
	message := schema.NewMessage(schema.RoleTool, schema.NewFunctionToolResultBlock(result.Call.FunctionID, result.Call.Name, result.Err != nil, schema.NewTextBlock(content)))
	message.Extra = map[string]any{
		"function_id":       result.Call.FunctionID,
		"agent_observation": true,
		"routed_agent":      result.Call.Name,
	}
	return message
}
