package app

import (
	"fmt"
	"strings"

	"github.com/goharness/goharness/pkg/llm"
	"github.com/goharness/goharness/pkg/schema"
)

var goalDecisionOutput, goalDecisionOutputError = llm.NewStructuredOutput[GoalDecision]()

// decodeGoalDecision 只兼容包裹 JSON 的围栏，不从说明文字中猜测或提取业务路线。
func decodeGoalDecision(text string) (GoalDecision, error) {
	return decodeGoalDecisionResponse(llm.Response{Messages: []schema.Message{schema.AssistantMessage(text)}})
}

func decodeGoalDecisionResponse(response llm.Response) (GoalDecision, error) {
	if goalDecisionOutputError != nil {
		return GoalDecision{}, goalDecisionOutputError
	}
	if len(response.Messages) != 1 || response.Messages[0].Role != schema.RoleAssistant {
		return GoalDecision{}, fmt.Errorf("业务路由必须返回单条助手 JSON 消息")
	}
	messages := schema.CloneMessages(response.Messages)
	if len(messages[0].FunctionToolCalls()) > 0 {
		return GoalDecision{}, fmt.Errorf("业务路由必须返回 JSON，不能调用工具")
	}
	text := strings.TrimSpace(messages[0].JoinedText(""))
	if strings.HasPrefix(text, "```json\n") && strings.HasSuffix(text, "```") {
		text = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(text, "```json\n"), "```"))
	}
	messages[0].ContentBlocks = []schema.ContentBlock{schema.NewTextBlock(text)}
	decision, err := goalDecisionOutput.Decode(messages)
	if err != nil {
		return GoalDecision{}, fmt.Errorf("业务路由格式错误: %w", err)
	}
	switch decision.Route {
	case "focus", "notes", "answer", "workspace", "boundary":
		decision.Scope = strings.TrimSpace(decision.Scope)
		return decision, nil
	default:
		return GoalDecision{}, fmt.Errorf("不支持的业务路由 %q", decision.Route)
	}
}
