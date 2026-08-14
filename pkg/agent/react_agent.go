package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"goharness/internal/agentrecipe"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

const DefaultReactPrompt = `You are a ReAct agent. Solve the user's request by alternating between reasoning and actions.

Use this format when calling a tool:
Thought: explain the next step briefly
Action: <tool name>
Action Input: <valid JSON arguments>

After a tool call, the framework will provide an Observation. You may repeat the cycle as needed.
When no more tools are needed, provide the final answer directly.`

type ReactAgentConfig struct {
	BaseConfig
	Prompt string
}

type ReactAgent struct {
	*base
	prompt string
	recipe *agentrecipe.Recipe
}

func NewReactAgent(cfg ReactAgentConfig, opts ...Option) (*ReactAgent, error) {
	base, err := newBase(cfg.BaseConfig, opts...)
	if err != nil {
		return nil, err
	}
	prompt := cfg.Prompt
	if prompt == "" {
		prompt = DefaultReactPrompt
	}
	a := &ReactAgent{base: base, prompt: prompt}
	a.recipe, err = reactRecipe(base, a.prepareReactMessages)
	if err != nil {
		return nil, err
	}
	return a, nil
}

func (a *ReactAgent) stream(ctx context.Context, messages []schema.Message, opts ...RunOption) (*runnable.StreamReader[[]schema.Message], error) {
	runOpts := resolveRunOptions(a.limits, opts...)
	runOpts.agentKind = AgentKindReact
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
	runCtx = a.bindModelStream(runCtx)
	runCtx = withToolExecutionLimits(runCtx, runOpts)
	stream, err := a.runReact(runCtx, prepared, conversationInput(session, messages), runOpts)
	if err != nil {
		lifecycle.fail(err)
		return nil, err
	}
	return lifecycle.wrap(stream)
}

func (a *ReactAgent) prepareReactMessages(messages []schema.Message) []schema.Message {
	prepared := schema.CloneMessages(messages)
	prompt := a.prompt + "\n\n" + a.toolPrompt()
	if len(prepared) > 0 && prepared[0].Role == schema.RoleSystem {
		prepared[0].ContentBlocks = append(prepared[0].ContentBlocks, schema.NewTextBlock("\n\n"+prompt))
		return prepared
	}
	return append([]schema.Message{schema.SystemMessage(prompt)}, prepared...)
}

func (a *ReactAgent) toolPrompt() string {
	if len(a.toolOrder) == 0 {
		return "Available tools: none"
	}

	var builder strings.Builder
	builder.WriteString("Available tools:\n")
	for _, name := range a.toolOrder {
		definition := a.tools[name]
		parameters, err := json.Marshal(definition.Parameters)
		if err != nil {
			parameters = []byte("{}")
		}
		fmt.Fprintf(&builder, "- %s: %s Parameters: %s\n", definition.Name, definition.Description, parameters)
	}
	return strings.TrimRight(builder.String(), "\n")
}

func (a *ReactAgent) runReact(ctx context.Context, messages, input []schema.Message, opts RunOptions) (*runnable.StreamReader[[]schema.Message], error) {
	if a.modelNode == nil {
		return nil, ErrMissingModel
	}
	ctx = withResolvedRunOptions(ctx, opts)
	ctx = withRecipeCompletion(ctx, opts.session)
	ctx = agentrecipe.WithRunConfig(ctx, agentrecipe.RunConfig{
		MaxLLMCalls: opts.MaxLLMCalls, Input: input,
	})
	var stream *runnable.StreamReader[[]schema.Message]
	var err error
	if opts.resume {
		stream, err = a.recipe.ResumeStream(ctx, graphNodeOptions(opts)...)
	} else {
		stream, err = a.recipe.Stream(ctx, messages, graphNodeOptions(opts)...)
	}
	if err != nil {
		return nil, err
	}
	return stream, nil
}

type ReactAction struct {
	Thought    string
	Name       string
	Input      string
	FunctionID string
	Structured bool
}

func detectReactActions(messages []schema.Message) []ReactAction {
	actions := make([]ReactAction, 0)
	for _, message := range messages {
		for _, call := range message.FunctionToolCalls() {
			actions = append(actions, ReactAction{
				Thought:    message.JoinedText(""),
				Name:       call.Name,
				Input:      call.Arguments,
				FunctionID: call.ID,
				Structured: true,
			})
		}
		if action, ok := ParseReactAction(message.JoinedText("")); ok {
			actions = append(actions, action)
		}
	}
	return actions
}

func reactObservationMessage(action ReactAction, observation string) schema.Message {
	if action.Structured {
		return schema.NewMessage(schema.RoleTool,
			schema.NewFunctionToolResultBlock(action.FunctionID, action.Name, false, schema.NewTextBlock(observation)))
	}
	message := schema.AssistantMessage("Observation: " + observation + "\nThought: ")
	message.Extra = map[string]any{"agent_observation": true}
	return message
}

func ParseReactAction(text string) (ReactAction, bool) {
	actionIndex := lastLineMarker(text, "Action:")
	inputIndex := lastLineMarker(text, "Action Input:")
	if actionIndex < 0 || inputIndex <= actionIndex {
		return ReactAction{}, false
	}

	observationIndex := lastLineMarkerAfter(text, "Observation:", inputIndex)
	inputEnd := len(text)
	if observationIndex >= 0 {
		inputEnd = observationIndex
	}

	name := strings.TrimSpace(text[actionIndex+len("Action:") : inputIndex])
	input := strings.TrimSpace(text[inputIndex+len("Action Input:") : inputEnd])
	if name == "" {
		return ReactAction{}, false
	}
	if input == "" {
		input = "{}"
	}

	thought := strings.TrimSpace(text[:actionIndex])
	thought = strings.TrimSpace(strings.TrimPrefix(thought, "Thought:"))
	return ReactAction{
		Thought: thought,
		Name:    name,
		Input:   normalizeReactInput(input),
	}, true
}

func lastLineMarker(text, marker string) int {
	return lastLineMarkerAfter(text, marker, -1)
}

func lastLineMarkerAfter(text, marker string, after int) int {
	searchStart := len(text)
	for searchStart >= 0 {
		index := strings.LastIndex(text[:searchStart], marker)
		if index < 0 {
			return -1
		}
		if index > after && (index == 0 || text[index-1] == '\n' || text[index-1] == '\r') {
			return index
		}
		searchStart = index
	}
	return -1
}

func normalizeReactInput(input string) string {
	input = strings.TrimSpace(input)
	if strings.HasPrefix(input, "```") && strings.HasSuffix(input, "```") {
		input = strings.TrimSpace(strings.TrimPrefix(input, "```json"))
		input = strings.TrimSpace(strings.TrimSuffix(input, "```"))
	}
	return input
}
