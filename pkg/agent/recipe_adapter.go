package agent

import (
	"context"
	"fmt"
	"strings"

	"goharness/internal/agentrecipe"
	"goharness/pkg/llm"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

type recipeToolBatch struct {
	base  *Base
	batch *toolTaskBatch
}

func (b *recipeToolBatch) Resolve(calls []agentrecipe.Call, messages []schema.Message) ([]agentrecipe.ToolResult, error) {
	requests := fromRecipeCalls(calls)
	if err := b.batch.Admit(requests, messages); err != nil {
		return nil, err
	}
	results := b.batch.Join(requests)
	converted := make([]agentrecipe.ToolResult, len(results))
	for i, result := range results {
		converted[i] = agentrecipe.ToolResult{Call: calls[i], Content: result.Content, Err: result.Err}
	}
	return converted, nil
}

func fromRecipeCalls(calls []agentrecipe.Call) []toolCallRequest {
	result := make([]toolCallRequest, len(calls))
	for i, call := range calls {
		result[i] = toolCallRequest{Name: call.Name, Arguments: call.Arguments, FunctionID: call.FunctionID}
	}
	return result
}

func reactRecipe(base *Base) (*agentrecipe.Recipe, error) {
	return agentrecipe.NewToolRecipe(string(AgentKindReact), agentrecipe.Hooks{
		Name: base.Name(),
		CallModel: func(ctx context.Context, messages []schema.Message) (runnable.Stream[llm.Event], error) {
			opts, _ := resolvedRunOptionsFromContext(ctx)
			return base.callLLM(ctx, messages, base.toolDefinitions(), opts)
		},
		CheckBreakpoint: func(ctx context.Context, messages, response []schema.Message, calls int) error {
			opts, _ := resolvedRunOptionsFromContext(ctx)
			return base.checkBreakpoint(ctx, opts.session, opts, AgentKindReact, BreakpointBeforeLLM, messages, response, calls, nil, nil)
		},
		NewToolBatch: func(ctx context.Context) agentrecipe.ToolBatch {
			opts, _ := resolvedRunOptionsFromContext(ctx)
			return &recipeToolBatch{base: base, batch: base.newToolTaskBatch(ctx, opts)}
		},
		DetectCalls: func(messages []schema.Message) []agentrecipe.Call {
			actions := detectReactActions(messages)
			calls := make([]agentrecipe.Call, len(actions))
			for i, action := range actions {
				calls[i] = agentrecipe.Call{Name: action.Name, Arguments: action.Input, FunctionID: action.FunctionID, Thought: action.Thought, Structured: action.Structured}
			}
			return calls
		},
		RehydrateLLMCalls: countToolResponseTurns,
		Observation: func(result agentrecipe.ToolResult) schema.Message {
			content := result.Content
			if result.Err != nil {
				content = schema.TextContent(fmt.Sprintf("An error occurred when calling tool %q: %v", result.Call.Name, result.Err))
			}
			return reactObservationMessage(ReactAction{
				Thought: result.Call.Thought, Name: result.Call.Name, Input: result.Call.Arguments,
				FunctionID: result.Call.FunctionID, Structured: result.Call.Structured,
			}, content)
		},
		NameMessages: base.withAgentName,
	}, true)
}

// countToolResponseTurns derives the completed model-call count at the only
// ReAct checkpoint boundary: immediately before the next model call.
// Consecutive observations are one tool batch. The explicit marker is used
// for newly persisted messages; role/text detection remains as a migration
// fallback for checkpoints written by older versions.
func countToolResponseTurns(messages []schema.Message) int {
	turns := 0
	inBatch := false
	for _, message := range messages {
		marked, _ := message.Extra["agent_observation"].(bool)
		isToolResponse := marked || message.Role == schema.RoleFunction ||
			(message.Role == schema.RoleAssistant && strings.HasPrefix(strings.TrimSpace(message.Text()), "Observation:"))
		if isToolResponse {
			if !inBatch {
				turns++
			}
			inBatch = true
			continue
		}
		inBatch = false
	}
	return turns
}

func recipeName(base *Base, fallback string) string {
	if base != nil && base.Name() != "" {
		return base.Name()
	}
	return fallback
}
