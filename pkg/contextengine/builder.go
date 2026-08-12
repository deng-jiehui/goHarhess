package contextengine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

// ConversationBuilder constructs and validates provider-neutral messages.
type ConversationBuilder struct {
	messages []schema.Message
	err      error
}

func NewConversationBuilder() *ConversationBuilder { return &ConversationBuilder{} }

func (b *ConversationBuilder) Message(message schema.Message) *ConversationBuilder {
	if b == nil || b.err != nil {
		return b
	}
	b.messages = append(b.messages, schema.CloneMessages([]schema.Message{message})[0])
	return b
}

func (b *ConversationBuilder) System(text string) *ConversationBuilder {
	return b.Message(schema.SystemMessage(text))
}

func (b *ConversationBuilder) User(text string) *ConversationBuilder {
	return b.Message(schema.UserMessage(text))
}

func (b *ConversationBuilder) UserContent(content schema.Content) *ConversationBuilder {
	return b.Message(schema.NewMessage(schema.RoleUser, content))
}

func (b *ConversationBuilder) Assistant(text string) *ConversationBuilder {
	return b.Message(schema.AssistantMessage(text))
}

func (b *ConversationBuilder) AssistantContent(content schema.Content) *ConversationBuilder {
	return b.Message(schema.NewMessage(schema.RoleAssistant, content))
}

func (b *ConversationBuilder) AssistantCall(name, arguments, functionID string) *ConversationBuilder {
	if b == nil || b.err != nil {
		return b
	}
	if strings.TrimSpace(name) == "" {
		b.err = errors.New("assistant function call requires a name")
		return b
	}
	if strings.TrimSpace(arguments) == "" {
		arguments = "{}"
	}
	if !json.Valid([]byte(arguments)) {
		b.err = fmt.Errorf("assistant function call %q arguments are not valid JSON", name)
		return b
	}
	message := schema.Message{
		Role:         schema.RoleAssistant,
		FunctionCall: &schema.FunctionCall{Name: name, Arguments: arguments},
	}
	if functionID != "" {
		message.Extra = map[string]any{"function_id": functionID}
	}
	return b.Message(message)
}

func (b *ConversationBuilder) Function(name string, content schema.Content, functionID string) *ConversationBuilder {
	message := schema.FunctionMessage(name, content)
	if functionID != "" {
		message.Extra = map[string]any{"function_id": functionID}
	}
	return b.Message(message)
}

func (b *ConversationBuilder) Reasoning(text string) *ConversationBuilder {
	if b == nil || b.err != nil {
		return b
	}
	if len(b.messages) == 0 || b.messages[len(b.messages)-1].Role != schema.RoleAssistant {
		b.err = errors.New("reasoning requires a preceding assistant message")
		return b
	}
	b.messages[len(b.messages)-1].ReasoningContent = schema.TextContent(text)
	return b
}

func (b *ConversationBuilder) Metadata(key string, value any) *ConversationBuilder {
	if b == nil || b.err != nil {
		return b
	}
	if strings.TrimSpace(key) == "" {
		b.err = errors.New("message metadata key cannot be empty")
		return b
	}
	if len(b.messages) == 0 {
		b.err = errors.New("message metadata requires a preceding message")
		return b
	}
	last := len(b.messages) - 1
	b.messages[last] = cloneMessageExtra(b.messages[last])
	b.messages[last].Extra[key] = value
	return b
}

func (b *ConversationBuilder) Build() ([]schema.Message, error) {
	if b == nil {
		return nil, errors.New("conversation builder cannot be nil")
	}
	if b.err != nil {
		return nil, b.err
	}
	if err := validateMessages(b.messages, "conversation"); err != nil {
		return nil, err
	}
	return schema.CloneMessages(b.messages), nil
}

// ContextBuilder collects the three assembly inputs and delegates policy to an
// Engine at Build time.
type ContextBuilder struct {
	engine  runnable.Runnable[Request, Result]
	request Request
	err     error
}

func NewContextBuilder(engine runnable.Runnable[Request, Result]) *ContextBuilder {
	if engine == nil {
		engine = NewDefault()
	}
	return &ContextBuilder{engine: engine}
}

func (b *ContextBuilder) Pinned(messages ...schema.Message) *ContextBuilder {
	if b != nil && b.err == nil {
		b.request.Pinned = append(b.request.Pinned, schema.CloneMessages(messages)...)
	}
	return b
}

func (b *ContextBuilder) History(messages ...schema.Message) *ContextBuilder {
	if b != nil && b.err == nil {
		b.request.History = append(b.request.History, schema.CloneMessages(messages)...)
	}
	return b
}

func (b *ContextBuilder) Input(messages ...schema.Message) *ContextBuilder {
	if b != nil && b.err == nil {
		b.request.Input = append(b.request.Input, schema.CloneMessages(messages)...)
	}
	return b
}

func (b *ContextBuilder) TokenBudget(maxContextTokens, reservedOutputTokens int) *ContextBuilder {
	if b == nil || b.err != nil {
		return b
	}
	if maxContextTokens < 1 || reservedOutputTokens < 0 || reservedOutputTokens >= maxContextTokens {
		b.err = fmt.Errorf("invalid context builder token budget: max=%d reserved=%d", maxContextTokens, reservedOutputTokens)
		return b
	}
	b.request.MaxContextTokens = maxContextTokens
	b.request.ReservedOutputTokens = reservedOutputTokens
	return b
}

func (b *ContextBuilder) Build(ctx context.Context) (Result, error) {
	if b == nil || b.engine == nil {
		return Result{}, errors.New("context builder is not configured")
	}
	if b.err != nil {
		return Result{}, b.err
	}
	return b.engine.Invoke(ctx, b.request)
}
