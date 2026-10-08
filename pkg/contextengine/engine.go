// Package contextengine assembles durable conversation state into a validated,
// token-bounded provider-neutral []schema.Message payload.
package contextengine

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
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
	// MediaEstimator 估算图片/文件/音频/视频块的 token 成本；nil 时使用
	// 保守固定值的 ApproxMediaEstimator。媒体内嵌 Data 不参与文本计数。
	MediaEstimator     MediaTokenEstimator
	RolePolicy         RolePolicy
	MergeAdjacentRoles bool
	PromptCache        PromptCacheConfig
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

// ContextBuilder is the standard provider-neutral context assembly Runnable.
type ContextBuilder struct {
	config Config
}

var _ runnable.Runnable[Request, Result] = (*ContextBuilder)(nil)

// NewContextBuilder creates a context assembly Runnable with the supplied policy.
func NewContextBuilder(config Config) (*ContextBuilder, error) {
	if config.MaxContextTokens == 0 {
		config.MaxContextTokens = DefaultMaxContextTokens
	}
	if config.ReservedOutputTokens == 0 {
		config.ReservedOutputTokens = DefaultReservedOutputTokens
	}
	if config.Counter == nil {
		config.Counter = ApproxTokenCounter{}
	}
	if config.MediaEstimator == nil {
		config.MediaEstimator = ApproxMediaEstimator{}
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
	return &ContextBuilder{config: config}, nil
}

// Options 是 ContextBuilder 单次调用的类型化运行配置，覆盖 Request 中的
// Token 预算字段；未设置时回落到 Request 字段，再回落到构造配置。
type Options struct {
	MaxContextTokens     int
	ReservedOutputTokens int
}

// OptionTypes 声明 ContextBuilder 接收 contextengine.Options 类型的配置选项。
func (*ContextBuilder) OptionTypes() []reflect.Type {
	return []reflect.Type{runnable.OptionType[Options]()}
}

// WithMaxContextTokens 覆盖本次调用的上下文窗口大小。
func WithMaxContextTokens(max int) runnable.Option {
	return runnable.WrapOption(func(o *Options) { o.MaxContextTokens = max })
}

// WithReservedOutputTokens 覆盖本次调用的输出预留 Token 数。
func WithReservedOutputTokens(reserved int) runnable.Option {
	return runnable.WrapOption(func(o *Options) { o.ReservedOutputTokens = reserved })
}

func (a *ContextBuilder) Invoke(ctx context.Context, request Request, options ...runnable.Option) (Result, error) {
	if a == nil {
		return Result{}, errors.New("context assembler is not configured")
	}
	if ctx == nil {
		return Result{}, errors.New("context assembly context cannot be nil")
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	resolved, _, err := runnable.ResolveComponentOptions(Options{}, options...)
	if err != nil {
		return Result{}, fmt.Errorf("resolve context builder options: %w", err)
	}
	maxTokens := resolved.MaxContextTokens
	if maxTokens == 0 {
		maxTokens = request.MaxContextTokens
	}
	if maxTokens == 0 {
		maxTokens = a.config.MaxContextTokens
	}
	reserved := resolved.ReservedOutputTokens
	if reserved == 0 && resolved.MaxContextTokens == 0 {
		reserved = request.ReservedOutputTokens
	}
	if reserved == 0 && maxTokens == a.config.MaxContextTokens {
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

	counter := usageCounter{text: a.config.Counter, media: a.config.MediaEstimator}
	stats := Stats{
		BudgetTokens:         budget,
		StablePrefixMessages: len(prefix),
		StablePrefixTokens:   countMessages(prefix, counter),
		RoleRepairs:          repairs,
	}
	if stats.StablePrefixTokens > budget {
		return Result{}, &BudgetError{Required: stats.StablePrefixTokens, Available: budget, Reason: "stable prefix cannot be trimmed"}
	}

	turns := splitTurns(normalized)
	// 每轮只计一次数：token 计数随丢弃单调下降，逐轮扣减即可定位
	// 裁剪点，不再每丢一轮就 flatten+全量重计（原 O(T²)）。
	turnTokens := make([]int, len(turns))
	currentTokens := stats.StablePrefixTokens
	for i, turn := range turns {
		turnTokens[i] = countMessages(turn, counter)
		currentTokens += turnTokens[i]
	}
	// 全量输入 token 等于前缀与全部轮次之和，直接复用累计值，
	// 不再拼接克隆后重算。
	stats.InputTokens = currentTokens
	dropped := 0
	for currentTokens > budget && dropped < len(turns)-1 {
		stats.DroppedTurns++
		stats.DroppedMessages += len(turns[dropped])
		currentTokens -= turnTokens[dropped]
		dropped++
	}
	// turn 中的消息已是 normalizeRoles 产出的独有副本，直接拼接，
	// flattenTurns 不再做第二轮深拷贝。
	normalized = flattenTurns(turns[dropped:])
	for currentTokens > budget {
		changed := false
		for index := range normalized {
			before := countMessage(normalized[index], counter)
			if len(normalized[index].ContentBlocks) == 0 {
				continue
			}
			excess := currentTokens - budget
			if removeReasoningBlock(&normalized[index]) {
			} else {
				target := before - excess
				if target < 1 {
					target = 1
				}
				fixed := before - countTopLevelTextBlocks(normalized[index].ContentBlocks, counter.text)
				contentBudget := target - fixed
				normalized[index].ContentBlocks = truncateBlocks(normalized[index].ContentBlocks, contentBudget, counter.text, true)
			}
			after := countMessage(normalized[index], counter)
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
	// prefix/normalized 都是引擎自有的独立副本，拼接结果直接移交调用方；
	// prefix 容量等于长度，append 必然重新分配，不回写原切片。
	messages := append(prefix, normalized...)
	if err := validateMessages(messages, "assembled context"); err != nil {
		return Result{}, err
	}
	stats.OutputTokens = countMessages(messages, counter)
	return Result{Messages: messages, Stats: stats}, nil
}

func (a *ContextBuilder) Stream(ctx context.Context, input Request, options ...runnable.Option) (*runnable.StreamReader[Result], error) {
	return runnable.StreamFromInvoke(ctx, input, a.Invoke, options...)
}
func (a *ContextBuilder) Collect(ctx context.Context, input *runnable.StreamReader[Request], options ...runnable.Option) (Result, error) {
	return runnable.CollectFromInvoke(ctx, input, a.Invoke, options...)
}
func (a *ContextBuilder) Transform(ctx context.Context, input *runnable.StreamReader[Request], options ...runnable.Option) (*runnable.StreamReader[Result], error) {
	return runnable.TransformFromInvoke(ctx, input, a.Invoke, options...)
}

func (a *ContextBuilder) normalizeRoles(messages []schema.Message) ([]schema.Message, int, error) {
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
			calls := message.FunctionToolCalls()
			for _, call := range calls {
				pendingFunctions[call.ID]++
			}
			if len(calls) == 0 {
				pendingFunctions = map[string]int{}
			}
		case schema.RoleTool:
			candidate := make(map[string]int, len(pendingFunctions))
			for id, count := range pendingFunctions {
				candidate[id] = count
			}
			valid := true
			for _, result := range message.FunctionToolResults() {
				if candidate[result.CallID] <= 0 {
					valid = false
					break
				}
				candidate[result.CallID]--
			}
			if !valid {
				if a.config.RolePolicy == RolePolicyStrict {
					return nil, repairs, fmt.Errorf("tool message %d has no preceding assistant call", index)
				}
				repairs++
				continue
			}
			pendingFunctions = candidate
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
		messages = append(messages, turn...)
	}
	return messages
}

func canMerge(left, right schema.Message) bool {
	if left.Role != right.Role || len(left.FunctionToolCalls()) > 0 || len(right.FunctionToolCalls()) > 0 {
		return false
	}
	return left.Role == schema.RoleUser || left.Role == schema.RoleAssistant
}

func mergeAdjacent(left, right schema.Message) schema.Message {
	merged := cloneMessageExtra(left)
	merged.ContentBlocks = append(merged.ContentBlocks, schema.CloneMessages([]schema.Message{right})[0].ContentBlocks...)
	for key, value := range right.Extra {
		merged.Extra[key] = value
	}
	return merged
}

func removeReasoningBlock(message *schema.Message) bool {
	for i, block := range message.ContentBlocks {
		if block.Type == schema.ContentBlockTypeReasoning {
			message.ContentBlocks = append(message.ContentBlocks[:i], message.ContentBlocks[i+1:]...)
			return true
		}
	}
	return false
}

func countTopLevelTextBlocks(blocks []schema.ContentBlock, counter TokenCounter) int {
	total := 0
	for _, block := range blocks {
		if block.Type == schema.ContentBlockTypeText && block.Text != nil {
			total += counter.Count(block.Text.Text)
		}
	}
	return total
}

func truncateBlocks(blocks []schema.ContentBlock, limit int, counter TokenCounter, keepEnd bool) []schema.ContentBlock {
	cloned := schema.CloneMessages([]schema.Message{{ContentBlocks: blocks}})[0].ContentBlocks
	budget := limit
	indices := make([]int, len(cloned))
	for i := range indices {
		indices[i] = i
	}
	if keepEnd {
		for i, j := 0, len(indices)-1; i < j; i, j = i+1, j-1 {
			indices[i], indices[j] = indices[j], indices[i]
		}
	}
	for _, i := range indices {
		if cloned[i].Type == schema.ContentBlockTypeText && cloned[i].Text != nil {
			cloned[i].Text.Text = truncateText(cloned[i].Text.Text, budget, counter, keepEnd)
			budget -= counter.Count(cloned[i].Text.Text)
		}
	}
	return cloned
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
