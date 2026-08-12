package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"goharness/internal/agentrecipe"
	"goharness/pkg/graph"
	"goharness/pkg/llm"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
	"goharness/pkg/state"
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
	*Base
	hub           *Hub
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
	base, err := NewBase(baseConfig, opts...)
	if err != nil {
		return nil, err
	}
	if len(base.toolOrder) > 0 {
		return nil, errors.New("RoutingAgent routes SubAgents; use ReactAgent for BaseTool execution")
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
		Base:          base,
		hub:           hub,
		returnControl: returnControl,
		maxSteps:      maxSteps,
		functions:     functions,
	}
	a.recipe, err = agentrecipe.NewRoutingRecipe(recipeName(base, string(AgentKindRouting)), agentrecipe.RoutingHooks{
		MaxSteps:      maxSteps,
		ReturnControl: returnControl,
		CallModel: func(ctx context.Context, messages []schema.Message) (runnable.Stream[llm.Event], error) {
			runOpts, _ := resolvedRunOptionsFromContext(ctx)
			return a.callLLM(ctx, messages, a.functions, runOpts)
		},
		CheckBreakpoint: func(ctx context.Context, messages, response []schema.Message, steps int) error {
			runOpts, _ := resolvedRunOptionsFromContext(ctx)
			return a.checkBreakpoint(ctx, runOpts.session, runOpts, AgentKindRouting, BreakpointBeforeLLM, messages, response, steps, nil, &state.EngineCheckpoint{LLMCalls: steps})
		},
		DetectCalls:  detectRoutingCalls,
		Dispatch:     a.dispatch,
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

func (a *RoutingAgent) stream(ctx context.Context, messages []schema.Message, opts ...RunOption) (runnable.Stream[schema.MessageEvent], error) {
	runOpts := resolveRunOptions(a.limits, opts...)
	runOpts.agentKind = AgentKindRouting
	loaded, session, err := a.loadConversation(ctx, messages, &runOpts)
	if err != nil {
		return nil, err
	}
	commitResponse, resumeCommit, err := resumeAtCommit(&runOpts, AgentKindRouting)
	if err != nil {
		return nil, err
	}
	ctx = bindRunContext(ctx, session, &runOpts)
	prepared := a.prepareMessages(loaded, &runOpts)
	runCtx, lifecycle, err := a.beginRun(ctx, prepared, runOpts)
	if err != nil {
		return nil, err
	}
	lifecycle.session = session
	runOpts.session = session
	if resumeCommit {
		return lifecycle.wrap(completedResumeEvents(commitResponse), runOpts.StreamBufferSize), nil
	}
	continuation, resumed, err := continuationFor(runOpts, AgentKindRouting, BreakpointBeforeLLM)
	if err != nil {
		lifecycle.fail(err)
		return nil, err
	}
	response := []schema.Message{}
	steps := 0
	if resumed {
		response = schema.CloneMessages(continuation.Response)
		steps = continuation.LLMCalls
		if continuation.Engine != nil {
			steps = continuation.Engine.LLMCalls
		}
	}
	runCtx = withResolvedRunOptions(runCtx, runOpts)
	runCtx = agentrecipe.WithRoutingRunConfig(runCtx, agentrecipe.RoutingRunConfig{
		MaxSteps:      a.maxSteps,
		ReturnControl: a.returnControl,
		Steps:         steps,
		Response:      response,
	})
	stream, err := a.recipe.Stream(runCtx, prepared)
	if err != nil {
		lifecycle.fail(err)
		return nil, err
	}
	stream, err = prefetchMessageEvents(runCtx, stream)
	if err != nil {
		lifecycle.fail(err)
		return nil, err
	}
	return lifecycle.wrap(stream, runOpts.StreamBufferSize), nil
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
		if message.FunctionCall == nil {
			continue
		}
		calls = append(calls, agentrecipe.Call{
			Name:       message.FunctionCall.Name,
			Arguments:  message.FunctionCall.Arguments,
			FunctionID: message.FunctionID(fmt.Sprint(len(calls) + 1)),
			Structured: true,
		})
	}
	return calls
}

func (a *RoutingAgent) dispatch(ctx context.Context, calls []agentrecipe.Call, messages []schema.Message) ([]agentrecipe.RoutedResult, error) {
	runOpts, _ := resolvedRunOptionsFromContext(ctx)
	results := make([]agentrecipe.RoutedResult, len(calls))
	for index, call := range calls {
		results[index].Call = call
		selected, err := a.selectSubAgent(call.Name)
		if err != nil {
			results[index].Err = err
			continue
		}
		childOptions := []RunOption{WithCheckpointBypass(), withInterruptBypass()}
		if runOpts.DeltaStream {
			childOptions = append(childOptions, WithDeltaStream(true))
		}
		runnableOptions := runOptionsAsRunnable(childOptions)
		runnableOptions = append(runnableOptions, runOpts.GlobalOptions...)
		runnableOptions = append(runnableOptions, runOpts.NodeOptions...)
		childCtx := graph.WithDataSinkIsolated(ctx, nil)
		childCtx = graph.WithEventSinkIsolated(childCtx, nil)
		stream, err := selected.Stream(childCtx, schema.CloneMessages(messages), runnableOptions...)
		if err != nil {
			results[index].Err = err
			continue
		}
		output, err := consumeRoutedStream(ctx, stream)
		if err != nil {
			results[index].Err = err
			continue
		}
		results[index].Messages = withSpeakerName(output, selected.Name())
	}
	return results, nil
}

func (a *RoutingAgent) selectSubAgent(name string) (Agent, error) {
	for _, candidate := range a.hub.Agents() {
		if candidate.Name() == name {
			return candidate, nil
		}
	}
	return nil, fmt.Errorf("routing model selected unregistered sub-agent %q", name)
}

func consumeRoutedStream(ctx context.Context, stream runnable.Stream[schema.MessageEvent]) ([]schema.Message, error) {
	if stream == nil {
		return nil, errors.New("routed agent returned a nil stream")
	}
	accumulator := llm.NewDeltaAccumulator(true)
	var output []schema.Message
	done := false
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case item, open := <-stream:
			if !open {
				if !done {
					return nil, errors.New("routed agent stream closed without done event")
				}
				return schema.CloneMessages(output), nil
			}
			if item.Err != nil {
				return nil, item.Err
			}
			event := item.Value
			if event.Kind != schema.MessageEventDone && !graph.EmitData(ctx, event) {
				return nil, ctx.Err()
			}
			switch event.Kind {
			case schema.MessageEventDelta:
				merged, err := accumulator.Apply(llm.Event{Kind: llm.EventDelta, Messages: event.Messages})
				if err != nil {
					return nil, err
				}
				output = merged
			case schema.MessageEventSnapshot:
				output = schema.CloneMessages(event.Messages)
				accumulator = llm.NewDeltaAccumulator(true)
			case schema.MessageEventDone:
				done = true
				return schema.CloneMessages(output), nil
			default:
				return nil, fmt.Errorf("routed agent returned unsupported event kind %q", event.Kind)
			}
		}
	}
}

func routingObservation(result agentrecipe.RoutedResult) schema.Message {
	content := ""
	if result.Err != nil {
		content = fmt.Sprintf("Sub-agent %q failed: %v", result.Call.Name, result.Err)
	} else {
		parts := make([]string, 0, len(result.Messages))
		for _, message := range result.Messages {
			if text := strings.TrimSpace(message.Text()); text != "" {
				parts = append(parts, text)
			}
		}
		content = strings.Join(parts, "\n")
	}
	message := schema.FunctionMessage(result.Call.Name, schema.TextContent(content))
	message.Extra = map[string]any{
		"function_id":       result.Call.FunctionID,
		"agent_observation": true,
		"routed_agent":      result.Call.Name,
	}
	return message
}
