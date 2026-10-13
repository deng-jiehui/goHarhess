package agent

import (
	"context"
	"fmt"
	"strings"

	"github.com/goharness/goharness/internal/agentrecipe"
	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/schema"
)

// textReAct 为 true 时使用文本 ReAct 兼容模式：请求不携带原生 Functions，
// 工具以文本协议注入提示，并从模型正文解析 Action/Action Input。
func reactRecipe(base *base, prepareModelMessages func([]schema.Message) []schema.Message, textReAct bool) (*agentrecipe.Recipe, error) {
	return agentrecipe.NewToolRecipe(string(AgentKindReact), agentrecipe.Hooks{
		Model: base.modelNode,
		Tools: base.toolNode,
		PrepareModel: func(ctx context.Context, messages []schema.Message) (llm.Request, error) {
			opts, _ := resolvedRunOptionsFromContext(ctx)
			if prepareModelMessages != nil {
				messages = prepareModelMessages(messages)
			}
			functions := base.toolDefinitions()
			if textReAct {
				functions = nil
			}
			return base.prepareLLMRequest(ctx, messages, functions, opts)
		},
		DetectCalls: func(messages []schema.Message) []agentrecipe.Call {
			actions := detectReactActions(messages, textReAct)
			calls := make([]agentrecipe.Call, len(actions))
			for i, action := range actions {
				calls[i] = agentrecipe.Call{Name: action.Name, Arguments: action.Input, FunctionID: action.FunctionID, Thought: action.Thought, Structured: action.Structured}
			}
			return calls
		},
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
	}, true, base.definitionVersion)
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
