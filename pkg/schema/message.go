package schema

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleFunction  Role = "function"
)

type FunctionCall struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type FunctionDefinition struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Parameters  map[string]any `json:"parameters,omitempty"`
}

type Message struct {
	Role             Role           `json:"role"`
	Content          Content        `json:"content"`
	ReasoningContent Content        `json:"reasoning_content,omitempty"`
	Name             string         `json:"name,omitempty"`
	FunctionCall     *FunctionCall  `json:"function_call,omitempty"`
	Extra            map[string]any `json:"extra,omitempty"`
}

func (m Message) MarshalJSON() ([]byte, error) {
	type messageJSON struct {
		Role             Role           `json:"role"`
		Content          Content        `json:"content"`
		ReasoningContent *Content       `json:"reasoning_content,omitempty"`
		Name             string         `json:"name,omitempty"`
		FunctionCall     *FunctionCall  `json:"function_call,omitempty"`
		Extra            map[string]any `json:"extra,omitempty"`
	}

	var reasoning *Content
	if !m.ReasoningContent.IsZero() {
		rc := m.ReasoningContent
		reasoning = &rc
	}

	var extra map[string]any
	if len(m.Extra) > 0 {
		extra = m.Extra
	}

	return json.Marshal(messageJSON{
		Role:             m.Role,
		Content:          m.Content,
		ReasoningContent: reasoning,
		Name:             m.Name,
		FunctionCall:     m.FunctionCall,
		Extra:            extra,
	})
}

func NewMessage(role Role, content Content) Message {
	return Message{Role: role, Content: content}
}

func TextMessage(role Role, text string) Message {
	return NewMessage(role, TextContent(text))
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

func FunctionMessage(name string, content Content) Message {
	return Message{Role: RoleFunction, Name: name, Content: content}
}

func (m Message) Validate() error {
	switch m.Role {
	case RoleSystem, RoleUser, RoleAssistant, RoleFunction:
	default:
		return fmt.Errorf("invalid role %q", m.Role)
	}
	if err := m.Content.Validate(); err != nil {
		return err
	}
	if !m.ReasoningContent.IsZero() {
		if err := m.ReasoningContent.Validate(); err != nil {
			return err
		}
	}
	if m.Role == RoleFunction && strings.TrimSpace(m.Name) == "" {
		return errors.New("function message requires a tool name")
	}
	if m.FunctionCall != nil && strings.TrimSpace(m.FunctionCall.Name) == "" {
		return errors.New("function call requires a name")
	}
	return nil
}

func (m Message) Text() string {
	return m.Content.String()
}

func (m Message) FunctionID(defaultID string) string {
	if m.Extra == nil {
		return defaultID
	}
	if value, ok := m.Extra["function_id"]; ok {
		return fmt.Sprint(value)
	}
	return defaultID
}

func CloneMessages(messages []Message) []Message {
	cloned, _ := cloneMessages(messages, false)
	return cloned
}

// CloneMessagesStrict returns fully isolated messages or rejects an unknown
// mutable value in Extra or multimodal content.
func CloneMessagesStrict(messages []Message) ([]Message, error) {
	return cloneMessages(messages, true)
}

func cloneMessages(messages []Message, strict bool) ([]Message, error) {
	copied := make([]Message, len(messages))
	for i, msg := range messages {
		copied[i] = msg
		content, err := cloneMessageContent(msg.Content, strict, fmt.Sprintf("messages[%d].content", i))
		if err != nil {
			return nil, err
		}
		copied[i].Content = content
		reasoning, err := cloneMessageContent(msg.ReasoningContent, strict, fmt.Sprintf("messages[%d].reasoning_content", i))
		if err != nil {
			return nil, err
		}
		copied[i].ReasoningContent = reasoning
		if msg.FunctionCall != nil {
			fc := *msg.FunctionCall
			copied[i].FunctionCall = &fc
		}
		if msg.Extra != nil {
			copied[i].Extra = make(map[string]any, len(msg.Extra))
			for k, v := range msg.Extra {
				cloned, err := cloneAnyValue(v, strict, fmt.Sprintf("messages[%d].extra[%q]", i, k), 0)
				if err != nil {
					return nil, err
				}
				copied[i].Extra[k] = cloned
			}
		}
	}
	return copied, nil
}

func cloneMessageContent(content Content, strict bool, path string) (Content, error) {
	if !content.IsMultimodal() {
		return cloneContent(content), nil
	}
	items := content.Items()
	for index := range items {
		if items[index].Audio != nil {
			value, err := cloneAnyValue(items[index].Audio, strict, fmt.Sprintf("%s[%d].audio", path, index), 0)
			if err != nil {
				return Content{}, err
			}
			items[index].Audio = value
		}
		if items[index].Video != nil {
			value, err := cloneAnyValue(items[index].Video, strict, fmt.Sprintf("%s[%d].video", path, index), 0)
			if err != nil {
				return Content{}, err
			}
			items[index].Video = value
		}
	}
	return MultiContent(items...), nil
}

func HasChinese(messages []Message) bool {
	for _, msg := range messages {
		for _, r := range msg.Text() {
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
			return messages[i].Text()
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
		for _, item := range msg.Content.Items() {
			if item.File != "" {
				add(item.File)
			}
			if includeImages && item.Image != "" {
				add(item.Image)
			}
		}
	}
	return files
}
