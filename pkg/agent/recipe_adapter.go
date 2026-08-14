package agent

import (
	"context"
	"fmt"
	"strings"

	"goharness/internal/agentrecipe"
	"goharness/pkg/llm"
	"goharness/pkg/schema"
)

func reactRecipe(base *base, prepareModelMessages func([]schema.Message) []schema.Message) (*agentrecipe.Recipe, error) {
	return agentrecipe.NewToolRecipe(string(AgentKindReact), agentrecipe.Hooks{
		Model: base.modelNode,
		Tools: base.toolNode,
		PrepareModel: func(ctx context.Context, messages []schema.Message) (llm.Request, error) {
			opts, _ := resolvedRunOptionsFromContext(ctx)
			if prepareModelMessages != nil {
				messages = prepareModelMessages(messages)
			}
			return base.prepareLLMRequest(ctx, messages, base.toolDefinitions(), opts)
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
			content := joinAgentTextBlocks(result.ContentBlocks)
			if result.Err != nil {
				content = fmt.Sprintf("An error occurred when calling tool %q: %v", result.Call.Name, result.Err)
			}
			return reactObservationMessage(ReactAction{
				Thought: result.Call.Thought, Name: result.Call.Name, Input: result.Call.Arguments,
				FunctionID: result.Call.FunctionID, Structured: result.Call.Structured,
			}, content)
		},
		NameMessages: base.withAgentName,
	}, true)
}

func joinAgentTextBlocks(blocks []schema.ContentBlock) string {
	parts := make([]string, 0)
	for _, block := range blocks {
		if block.Type == schema.ContentBlockTypeText && block.Text != nil {
			parts = append(parts, block.Text.Text)
		}
	}
	return strings.Join(parts, "")
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
		isToolResponse := marked || message.Role == schema.RoleTool || len(message.FunctionToolResults()) > 0 ||
			(message.Role == schema.RoleAssistant && strings.HasPrefix(strings.TrimSpace(message.JoinedText("")), "Observation:"))
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

func recipeName(base *base, fallback string) string {
	if base != nil && base.Name() != "" {
		return base.Name()
	}
	return fallback
}
