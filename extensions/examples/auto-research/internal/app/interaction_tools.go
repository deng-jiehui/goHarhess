package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/goharness/goharness/pkg/graph"
	"github.com/goharness/goharness/pkg/schema"
	"github.com/goharness/goharness/pkg/tool"
)

const maxClarificationRounds = 3
const confirmedAnswersKey = "autoresearch.confirmed_answers"

// AskUserArgs 由模型生成，前端沿用可编辑选项和自定义输入。
type AskUserArgs struct {
	Question string   `json:"question" jsonschema:"需要用户补充的具体问题，最多 1000 个字符"`
	Choices  []string `json:"choices,omitempty" jsonschema:"可选的 2 至 6 个建议答案，每项最多 120 个字符；用户可以编辑或自行输入"`
}

type AskUserResult struct {
	Answer string `json:"answer"`
}

type ConfirmedAnswer struct {
	Question string `json:"question"`
	Answer   string `json:"answer"`
}

// InteractionState 固定已接受的调用和返回节点，恢复时不重新请求模型生成问题。
type InteractionState struct {
	Pending    []schema.Message  `json:"pending,omitempty"`
	ReturnNode string            `json:"return_node,omitempty"`
	Rounds     int               `json:"rounds"`
	Answers    []ConfirmedAnswer `json:"answers,omitempty"`
}

func newAskUserTool() tool.Tool {
	return tool.New[AskUserArgs, AskUserResult]("ask_user",
		"仅在缺少的信息会改变下一步行动时向用户提问，可给出可编辑选项；已确认的信息不要重复询问。此调用必须独占一批。",
		func(ctx context.Context, args AskUserArgs) (AskUserResult, error) {
			if err := validateAskUser(args); err != nil {
				return AskUserResult{}, err
			}
			value, err := graph.Interrupt(ctx, map[string]any{
				"kind": "clarification", "question": args.Question, "choices": args.Choices,
				"allow_custom": true, "placeholder": "选择、编辑选项，或输入自己的回答",
			})
			if err != nil {
				return AskUserResult{}, err
			}
			answer, ok := value.(string)
			if !ok || strings.TrimSpace(answer) == "" {
				return AskUserResult{}, errors.New("用户回答必须是非空文本")
			}
			return AskUserResult{Answer: strings.TrimSpace(answer)}, nil
		})
}

func validateAskUser(args AskUserArgs) error {
	question := strings.TrimSpace(args.Question)
	if question == "" || utf8.RuneCountInString(question) > 1000 {
		return errors.New("ask_user 问题必须为 1 至 1000 个字符")
	}
	if len(args.Choices) != 0 && (len(args.Choices) < 2 || len(args.Choices) > 6) {
		return errors.New("ask_user 选项必须为空或包含 2 至 6 项")
	}
	seen := make(map[string]bool)
	for _, choice := range args.Choices {
		choice = strings.TrimSpace(choice)
		if choice == "" || utf8.RuneCountInString(choice) > 120 || seen[choice] {
			return errors.New("ask_user 选项必须非空、不重复且不超过 120 个字符")
		}
		seen[choice] = true
	}
	return nil
}

func decodeAskUser(raw string) (AskUserArgs, error) {
	var args AskUserArgs
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&args); err != nil {
		return args, fmt.Errorf("ask_user 参数错误: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return args, errors.New("ask_user 参数必须为单个 JSON 对象")
	}
	return args, validateAskUser(args)
}

// prepareInteraction 只校验和固定模型调用，不执行工具或中断。
func prepareInteraction(state *ResearchState, messages []schema.Message, returnNode string) (bool, error) {
	var calls []schema.FunctionToolCall
	for _, message := range messages {
		calls = append(calls, message.FunctionToolCalls()...)
	}
	var question *AskUserArgs
	for _, call := range calls {
		if call.Name != "ask_user" {
			continue
		}
		if len(calls) != 1 {
			return false, errors.New("ask_user 必须独占工具调用批次，不得与写入或其他调用混合")
		}
		if len(messages) != 1 || messages[0].Role != schema.RoleAssistant || strings.TrimSpace(call.ID) == "" {
			return false, errors.New("ask_user 必须有唯一的助手调用消息和调用 ID")
		}
		args, err := decodeAskUser(call.Arguments)
		if err != nil {
			return false, err
		}
		question = &args
	}
	if question == nil {
		return false, nil
	}
	if state.Interaction.Rounds >= maxClarificationRounds {
		return false, errors.New("本轮追问次数已达上限，请基于已有信息给出结果")
	}
	state.Question = question.Question
	state.Interaction.Pending = schema.CloneMessages(messages)
	state.Interaction.ReturnNode = returnNode
	return true, nil
}

func completeInteraction(state *ResearchState, results []schema.Message) error {
	if len(state.Interaction.Pending) != 1 || len(results) != 1 {
		return errors.New("用户交互缺少完整的调用与结果")
	}
	calls := state.Interaction.Pending[0].FunctionToolCalls()
	returned := results[0].FunctionToolResults()
	if len(calls) != 1 || len(returned) != 1 || returned[0].CallID != calls[0].ID || returned[0].Name != "ask_user" || returned[0].IsError {
		return errors.New("用户交互调用和结果不匹配或回答无效")
	}
	var answer AskUserResult
	text := schema.NewMessage(schema.RoleTool, returned[0].ContentBlocks...).JoinedText("")
	if err := json.Unmarshal([]byte(text), &answer); err != nil || strings.TrimSpace(answer.Answer) == "" {
		return errors.New("用户交互没有返回有效答案")
	}
	switch state.Interaction.ReturnNode {
	case "draft_focus", "draft_notes", "draft_answer":
		state.DraftMessages = append(state.DraftMessages, schema.CloneMessages(results)...)
	case "workspace_model":
		state.Workspace.Messages = append(state.Workspace.Messages, schema.CloneMessages(results)...)
	default:
		return errors.New("用户交互返回了未知业务阶段")
	}
	state.Answer = strings.TrimSpace(state.Answer + "\n" + state.Question + "：" + answer.Answer)
	state.Interaction.Answers = append(state.Interaction.Answers, ConfirmedAnswer{Question: state.Question, Answer: answer.Answer})
	state.Interaction.Rounds++
	state.Interaction.Pending = nil
	return nil
}

func preserveConfirmedAnswers(state *ResearchState) {
	if len(state.Interaction.Answers) == 0 || len(state.Output) == 0 {
		return
	}
	message := &state.Output[len(state.Output)-1]
	if message.Extra == nil {
		message.Extra = make(map[string]any)
	}
	message.Extra[confirmedAnswersKey] = append([]ConfirmedAnswer(nil), state.Interaction.Answers...)
}

func interactionInstructions() string {
	return "直接回答或完成当前研究任务；只有缺失信息会实质改变下一步行动时才调用 ask_user，并根据当前主题生成问题与可选答案。用户只要候选方向时先给具体选项，不必追问。每次 ask_user 独占一批，已回答的信息不重复询问，保持原任务和保存意图，不重新选择全局路线。"
}

func publishQuestion(ctx context.Context, turnID string, state *ResearchState) error {
	return publishProgress(ctx, ResearchProgress{
		ID: "turn:" + turnID + ":scope", Stage: "scope", Status: "waiting",
		Title: "等待你的补充", Summary: state.Question,
	})
}

func interactionResultText(message schema.Message) string {
	for _, result := range message.FunctionToolResults() {
		text := schema.NewMessage(schema.RoleTool, result.ContentBlocks...).JoinedText("")
		var answer AskUserResult
		if result.Name == "ask_user" && json.Unmarshal([]byte(text), &answer) == nil {
			return answer.Answer
		}
	}
	return ""
}

func validateWorkspaceBatch(messages []schema.Message, definitions []schema.FunctionDefinition) error {
	allowed := make(map[string]bool, len(definitions))
	for _, definition := range definitions {
		allowed[definition.Name] = true
	}
	var calls []schema.FunctionToolCall
	for _, message := range messages {
		calls = append(calls, message.FunctionToolCalls()...)
	}
	seen := make(map[string]bool)
	for _, call := range calls {
		if !allowed[call.Name] || call.ID == "" || seen[call.ID] {
			return errors.New("工作区模型返回了未知工具或无效调用 ID")
		}
		seen[call.ID] = true
		switch call.Name {
		case "ask_user", "write_file", "apply_patch", "run_shell":
			if len(calls) != 1 {
				return fmt.Errorf("%s 必须独占工具调用批次", call.Name)
			}
		}
	}
	if len(calls) > 4 || (len(calls) > 0 && (len(messages) != 1 || messages[0].Role != schema.RoleAssistant)) {
		return errors.New("工作区一次最多接受一条助手消息中的四个只读调用")
	}
	return nil
}
