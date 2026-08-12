// Package contextengine assembles durable conversation state into a validated,
// token-bounded provider-neutral []schema.Message payload.
package contextengine

import (
	"context"
	"errors"
	"fmt"

	"goharness/pkg/runnable"
	"goharness/pkg/schema"
)

const (
	DefaultMaxContextTokens     = 128 * 1024
	DefaultReservedOutputTokens = 4096
	PromptCacheMetadataKey      = "prompt_cache"
)

type RolePolicy string

const (
	RolePolicyRepair RolePolicy = "repair"
	RolePolicyStrict RolePolicy = "strict"
)

type PromptCacheConfig struct {
	Enabled         bool
	MinPrefixTokens int
	MetadataKey     string
}

type Config struct {
	MaxContextTokens     int
	ReservedOutputTokens int
	Counter              TokenCounter
	RolePolicy           RolePolicy
	MergeAdjacentRoles   bool
	PromptCache          PromptCacheConfig
}

type Request struct {
	// Pinned is an immutable stable prefix. It is never removed by trimming.
	Pinned               []schema.Message
	History              []schema.Message
	Input                []schema.Message
	MaxContextTokens     int
	ReservedOutputTokens int
}

type Stats struct {
	BudgetTokens         int
	InputTokens          int
	OutputTokens         int
	StablePrefixMessages int
	StablePrefixTokens   int
	DroppedTurns         int
	DroppedMessages      int
	TruncatedMessages    int
	RoleRepairs          int
	PromptCacheAligned   bool
}

type Result struct {
	Messages []schema.Message
	Stats    Stats
}

type BudgetError struct {
	Required  int
	Available int
	Reason    string
}

func (e *BudgetError) Error() string {
	return fmt.Sprintf("context token budget exceeded: required %d, available %d (%s)", e.Required, e.Available, e.Reason)
}

type Assembler struct {
	config Config
}

var _ runnable.Runnable[Request, Result] = (*Assembler)(nil)

// DefaultContextBuilder is the framework-provided context Runnable. It
// normalizes role order, appends stable/history/input messages, aligns prompt
// cache prefixes and enforces the configured token budget.
type DefaultContextBuilder = Assembler

// ContextBuilderFunc adapts a plain function into the standard context
// Runnable node. Invocation options are available at the composition boundary
// even when a custom function does not need them.
type ContextBuilderFunc func(context.Context, Request) (Result, error)

func (f ContextBuilderFunc) Invoke(ctx context.Context, request Request, _ ...runnable.Option) (Result, error) {
	if f == nil {
		return Result{}, errors.New("context builder function is nil")
	}
	return f(ctx, request)
}

var _ runnable.Runnable[Request, Result] = ContextBuilderFunc(nil)

func New(config Config) (*Assembler, error) {
	if config.MaxContextTokens == 0 {
		config.MaxContextTokens = DefaultMaxContextTokens
	}
	if config.ReservedOutputTokens == 0 {
		config.ReservedOutputTokens = DefaultReservedOutputTokens
	}
	if config.Counter == nil {
		config.Counter = ApproxTokenCounter{}
	}
	if config.RolePolicy == "" {
		config.RolePolicy = RolePolicyRepair
	}
	if config.MaxContextTokens < 1 {
		return nil, errors.New("context max tokens must be greater than zero")
	}
	if config.ReservedOutputTokens < 0 || config.ReservedOutputTokens >= config.MaxContextTokens {
		return nil, errors.New("context reserved output tokens must be non-negative and smaller than max context tokens")
	}
	if config.RolePolicy != RolePolicyRepair && config.RolePolicy != RolePolicyStrict {
		return nil, fmt.Errorf("unsupported context role policy %q", config.RolePolicy)
	}
	if config.PromptCache.MinPrefixTokens < 0 {
		return nil, errors.New("prompt cache minimum prefix tokens cannot be negative")
	}
	if config.PromptCache.MetadataKey == "" {
		config.PromptCache.MetadataKey = PromptCacheMetadataKey
	}
	return &Assembler{config: config}, nil
}

func NewDefault() *Assembler {
	assembler, _ := New(Config{})
	return assembler
}

// NewDefaultContextBuilder creates the standard history-appending context
// node. It is equivalent to NewDefault and makes the node role explicit in
// application composition code.
func NewDefaultContextBuilder() *DefaultContextBuilder {
	return NewDefault()
}

func (a *Assembler) Invoke(ctx context.Context, request Request, _ ...runnable.Option) (Result, error) {
	if a == nil {
		return Result{}, errors.New("context assembler is not configured")
	}
	if ctx == nil {
		return Result{}, errors.New("context assembly context cannot be nil")
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	maxTokens := request.MaxContextTokens
	if maxTokens == 0 {
		maxTokens = a.config.MaxContextTokens
	}
	reserved := request.ReservedOutputTokens
	if reserved == 0 && request.MaxContextTokens == 0 {
		reserved = a.config.ReservedOutputTokens
	}
	if maxTokens < 1 || reserved < 0 || reserved >= maxTokens {
		return Result{}, errors.New("invalid per-request context token budget")
	}
	budget := maxTokens - reserved
	prefix := schema.CloneMessages(request.Pinned)
	body := append(schema.CloneMessages(request.History), schema.CloneMessages(request.Input)...)

	var systemMessages []schema.Message
	conversation := make([]schema.Message, 0, len(body))
	for _, message := range body {
		if message.Role == schema.RoleSystem {
			systemMessages = append(systemMessages, message)
		} else {
			conversation = append(conversation, message)
		}
	}
	prefix = append(prefix, systemMessages...)
	if err := validateMessages(prefix, "stable prefix"); err != nil {
		return Result{}, err
	}
	normalized, repairs, err := a.normalizeRoles(conversation)
	if err != nil {
		return Result{}, err
	}

	stats := Stats{
		BudgetTokens:         budget,
		InputTokens:          countMessages(append(schema.CloneMessages(prefix), normalized...), a.config.Counter),
		StablePrefixMessages: len(prefix),
		StablePrefixTokens:   countMessages(prefix, a.config.Counter),
		RoleRepairs:          repairs,
	}
	if stats.StablePrefixTokens > budget {
		return Result{}, &BudgetError{Required: stats.StablePrefixTokens, Available: budget, Reason: "stable prefix cannot be trimmed"}
	}

	turns := splitTurns(normalized)
	currentTokens := stats.StablePrefixTokens + countMessages(flattenTurns(turns), a.config.Counter)
	for currentTokens > budget && len(turns) > 1 {
		stats.DroppedTurns++
		stats.DroppedMessages += len(turns[0])
		turns = turns[1:]
		currentTokens = stats.StablePrefixTokens + countMessages(flattenTurns(turns), a.config.Counter)
	}
	normalized = flattenTurns(turns)
	for currentTokens > budget {
		changed := false
		for index := range normalized {
			before := countMessage(normalized[index], a.config.Counter)
			if normalized[index].ReasoningContent.IsZero() && normalized[index].Content.String() == "" {
				continue
			}
			excess := currentTokens - budget
			if !normalized[index].ReasoningContent.IsZero() {
				normalized[index].ReasoningContent = schema.TextContent("")
			} else {
				target := before - excess
				if target < 1 {
					target = 1
				}
				fixed := before - a.config.Counter.Count(normalized[index].Content.String())
				contentBudget := target - fixed
				normalized[index].Content = truncateContent(normalized[index].Content, contentBudget, a.config.Counter, true)
			}
			after := countMessage(normalized[index], a.config.Counter)
			if after < before {
				stats.TruncatedMessages++
				currentTokens -= before - after
				changed = true
			}
			if currentTokens <= budget {
				break
			}
		}
		if !changed {
			return Result{}, &BudgetError{Required: currentTokens, Available: budget, Reason: "non-text message overhead cannot be trimmed"}
		}
	}

	if a.config.PromptCache.Enabled && len(prefix) > 0 && stats.StablePrefixTokens >= a.config.PromptCache.MinPrefixTokens {
		last := len(prefix) - 1
		prefix[last] = cloneMessageExtra(prefix[last])
		prefix[last].Extra[a.config.PromptCache.MetadataKey] = map[string]any{
			"type":       "ephemeral",
			"breakpoint": true,
			"scope":      "stable_prefix",
		}
		stats.PromptCacheAligned = true
	}
	messages := append(schema.CloneMessages(prefix), normalized...)
	if err := validateMessages(messages, "assembled context"); err != nil {
		return Result{}, err
	}
	stats.OutputTokens = countMessages(messages, a.config.Counter)
	return Result{Messages: schema.CloneMessages(messages), Stats: stats}, nil
}

func (a *Assembler) normalizeRoles(messages []schema.Message) ([]schema.Message, int, error) {
	result := make([]schema.Message, 0, len(messages))
	repairs := 0
	seenUser := false
	pendingFunctions := map[string]int{}
	for index, message := range messages {
		if err := message.Validate(); err != nil {
			return nil, repairs, fmt.Errorf("context message %d: %w", index, err)
		}
		switch message.Role {
		case schema.RoleUser:
			seenUser = true
			pendingFunctions = map[string]int{}
		case schema.RoleAssistant:
			if !seenUser {
				if a.config.RolePolicy == RolePolicyStrict {
					return nil, repairs, fmt.Errorf("assistant message %d appears before a user message", index)
				}
				repairs++
				continue
			}
			if message.FunctionCall != nil {
				pendingFunctions[message.FunctionCall.Name]++
			} else {
				pendingFunctions = map[string]int{}
			}
		case schema.RoleFunction:
			if pendingFunctions[message.Name] == 0 {
				if a.config.RolePolicy == RolePolicyStrict {
					return nil, repairs, fmt.Errorf("function message %d for %q has no preceding assistant call", index, message.Name)
				}
				repairs++
				continue
			}
			pendingFunctions[message.Name]--
		default:
			return nil, repairs, fmt.Errorf("context message %d has unsupported role %q", index, message.Role)
		}
		if a.config.MergeAdjacentRoles && len(result) > 0 && canMerge(result[len(result)-1], message) {
			result[len(result)-1] = mergeAdjacent(result[len(result)-1], message)
			repairs++
			continue
		}
		result = append(result, schema.CloneMessages([]schema.Message{message})[0])
	}
	return result, repairs, nil
}

func validateMessages(messages []schema.Message, label string) error {
	for index, message := range messages {
		if err := message.Validate(); err != nil {
			return fmt.Errorf("%s message %d: %w", label, index, err)
		}
	}
	return nil
}

func splitTurns(messages []schema.Message) [][]schema.Message {
	turns := make([][]schema.Message, 0)
	for _, message := range messages {
		if message.Role == schema.RoleUser || len(turns) == 0 {
			turns = append(turns, nil)
		}
		last := len(turns) - 1
		turns[last] = append(turns[last], message)
	}
	return turns
}

func flattenTurns(turns [][]schema.Message) []schema.Message {
	count := 0
	for _, turn := range turns {
		count += len(turn)
	}
	messages := make([]schema.Message, 0, count)
	for _, turn := range turns {
		messages = append(messages, schema.CloneMessages(turn)...)
	}
	return messages
}

func canMerge(left, right schema.Message) bool {
	if left.Role != right.Role || left.Name != right.Name || left.FunctionCall != nil || right.FunctionCall != nil {
		return false
	}
	return left.Role == schema.RoleUser || left.Role == schema.RoleAssistant
}

func mergeAdjacent(left, right schema.Message) schema.Message {
	merged := cloneMessageExtra(left)
	if !merged.Content.IsZero() && !right.Content.IsZero() {
		merged.Content = merged.Content.Append(schema.TextContent("\n\n"))
	}
	merged.Content = merged.Content.Append(right.Content)
	if !merged.ReasoningContent.IsZero() && !right.ReasoningContent.IsZero() {
		merged.ReasoningContent = merged.ReasoningContent.Append(schema.TextContent("\n\n"))
	}
	merged.ReasoningContent = merged.ReasoningContent.Append(right.ReasoningContent)
	for key, value := range right.Extra {
		merged.Extra[key] = value
	}
	return merged
}

func cloneMessageExtra(message schema.Message) schema.Message {
	cloned := schema.CloneMessages([]schema.Message{message})[0]
	extra := make(map[string]any, len(cloned.Extra)+1)
	for key, value := range cloned.Extra {
		extra[key] = value
	}
	cloned.Extra = extra
	return cloned
}
