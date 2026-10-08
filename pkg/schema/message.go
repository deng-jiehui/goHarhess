package schema

import (
	"fmt"
	"strings"
)

type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

type FunctionDefinition struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Parameters  map[string]any `json:"parameters,omitempty"`
}

type Message struct {
	// ID identifies this logical message across deltas, snapshots and final results.
	// It is independent of the provider response ID in ResponseMeta.
	ID            string         `json:"id,omitempty"`
	Role          Role           `json:"role"`
	ContentBlocks []ContentBlock `json:"content_blocks,omitempty"`
	ResponseMeta  *ResponseMeta  `json:"response_meta,omitempty"`
	Extra         map[string]any `json:"extra,omitempty"`
}

func NewMessage(role Role, blocks ...ContentBlock) Message {
	return Message{Role: role, ContentBlocks: append([]ContentBlock(nil), blocks...)}
}

func TextMessage(role Role, text string) Message {
	return NewMessage(role, NewTextBlock(text))
}

func SystemMessage(text string) Message {
	return TextMessage(RoleSystem, text)
}

func UserMessage(text string) Message {
	return TextMessage(RoleUser, text)
}

func AssistantMessage(text string) Message {
	return TextMessage(RoleAssistant, text)
}

func (m Message) Validate() error {
	switch m.Role {
	case RoleSystem, RoleUser, RoleAssistant, RoleTool:
	default:
		return fmt.Errorf("invalid role %q", m.Role)
	}
	for i, block := range m.ContentBlocks {
		if err := block.Validate(); err != nil {
			return fmt.Errorf("content_blocks[%d]: %w", i, err)
		}
	}
	if err := m.ResponseMeta.validate(); err != nil {
		return err
	}
	return nil
}

func (m Message) TextBlocks() []Text {
	texts := make([]Text, 0)
	for i, block := range m.ContentBlocks {
		if block.Type != ContentBlockTypeText || block.Text == nil {
			continue
		}
		cloned, _ := cloneContentBlock(block, false, fmt.Sprintf("content_blocks[%d]", i), nil)
		texts = append(texts, *cloned.Text)
	}
	return texts
}

func (m Message) JoinedText(separator string) string {
	texts := m.TextBlocks()
	parts := make([]string, len(texts))
	for i := range texts {
		parts[i] = texts[i].Text
	}
	return strings.Join(parts, separator)
}

func (m Message) ReasoningBlocks() []Reasoning {
	reasoning := make([]Reasoning, 0)
	for i, block := range m.ContentBlocks {
		if block.Type != ContentBlockTypeReasoning || block.Reasoning == nil {
			continue
		}
		cloned, _ := cloneContentBlock(block, false, fmt.Sprintf("content_blocks[%d]", i), nil)
		reasoning = append(reasoning, *cloned.Reasoning)
	}
	return reasoning
}

func (m Message) FunctionToolCalls() []FunctionToolCall {
	calls := make([]FunctionToolCall, 0)
	for i, block := range m.ContentBlocks {
		if block.Type != ContentBlockTypeFunctionToolCall || block.FunctionToolCall == nil {
			continue
		}
		cloned, _ := cloneContentBlock(block, false, fmt.Sprintf("content_blocks[%d]", i), nil)
		calls = append(calls, *cloned.FunctionToolCall)
	}
	return calls
}

func (m Message) FunctionToolResults() []FunctionToolResult {
	results := make([]FunctionToolResult, 0)
	for i, block := range m.ContentBlocks {
		if block.Type != ContentBlockTypeFunctionToolResult || block.FunctionToolResult == nil {
			continue
		}
		cloned, _ := cloneContentBlock(block, false, fmt.Sprintf("content_blocks[%d]", i), nil)
		results = append(results, *cloned.FunctionToolResult)
	}
	return results
}

func CloneMessages(messages []Message) []Message {
	cloned, _ := cloneMessages(messages, false, nil)
	return cloned
}

// CloneMessagesStrict returns fully isolated messages or rejects an unknown
// mutable value in Extra or multimodal content.
func CloneMessagesStrict(messages []Message) ([]Message, error) {
	return cloneMessages(messages, true, nil)
}

// CloneMessagesFallback 严格克隆消息；仅当某个值无法被结构克隆安全隔离
// （CloneUnsupportedError）时才调用 fallback 生成替代副本——通用 map/slice
// 就地容纳替代值并保留旁边的普通值与 Clone 类型，只有强类型容器装不下
// 替代结果时才扩大到该容器整体。Clone() panic、返回类型错误及 fallback
// 本身的错误都直接上抛，不被保底掩盖。
func CloneMessagesFallback(messages []Message, fallback CloneFallbackFunc) ([]Message, error) {
	return cloneMessages(messages, true, fallback)
}

func cloneMessages(messages []Message, strict bool, fallback CloneFallbackFunc) ([]Message, error) {
	copied := make([]Message, len(messages))
	for i, msg := range messages {
		copied[i] = msg
		blocks, err := cloneContentBlocks(msg.ContentBlocks, strict, fmt.Sprintf("messages[%d].content_blocks", i), fallback)
		if err != nil {
			return nil, err
		}
		copied[i].ContentBlocks = blocks
		meta, err := cloneResponseMeta(msg.ResponseMeta, strict, fmt.Sprintf("messages[%d].response_meta", i), fallback)
		if err != nil {
			return nil, err
		}
		copied[i].ResponseMeta = meta
		if msg.Extra != nil {
			copied[i].Extra = make(map[string]any, len(msg.Extra))
			for k, v := range msg.Extra {
				cloned, err := cloneAnyValue(v, strict, fmt.Sprintf("messages[%d].extra[%q]", i, k), 0, fallback)
				if err != nil {
					return nil, err
				}
				copied[i].Extra[k] = cloned
			}
		}
	}
	return copied, nil
}

func HasChinese(messages []Message) bool {
	for _, msg := range messages {
		for _, r := range msg.JoinedText("") {
			if r >= '\u4e00' && r <= '\u9fff' {
				return true
			}
		}
	}
	return false
}

func LastUserText(messages []Message) string {
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role == RoleUser {
			return messages[i].JoinedText("")
		}
	}
	return ""
}

func ExtractFiles(messages []Message, includeImages bool) []string {
	seen := map[string]struct{}{}
	files := make([]string, 0)
	add := func(path string) {
		if strings.TrimSpace(path) == "" {
			return
		}
		if _, ok := seen[path]; ok {
			return
		}
		seen[path] = struct{}{}
		files = append(files, path)
	}

	for _, msg := range messages {
		for _, block := range msg.ContentBlocks {
			if block.Type == ContentBlockTypeFile && block.File != nil {
				add(block.File.URI)
			}
			if includeImages && block.Type == ContentBlockTypeImage && block.Image != nil {
				add(block.Image.URI)
			}
		}
	}
	return files
}
