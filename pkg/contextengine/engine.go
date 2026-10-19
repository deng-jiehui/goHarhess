// Package contextengine assembles durable conversation state into a validated,
// token-bounded provider-neutral []schema.Message payload.
package contextengine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

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

// HistoryCompressor 只负责把传入的历史消息转换为本次模型调用使用的历史视图。
// Pinned、Input、Functions 和 SystemReminder 由 ContextBuilder 继续管理，
// 压缩结果也不会写回 Checkpoint。
type HistoryCompressor interface {
	Compress(context.Context, []schema.Message) ([]schema.Message, error)
}

// HistoryCompressorFunc 将普通函数适配为 HistoryCompressor。
type HistoryCompressorFunc func(context.Context, []schema.Message) ([]schema.Message, error)

func (f HistoryCompressorFunc) Compress(ctx context.Context, history []schema.Message) ([]schema.Message, error) {
	if f == nil {
		return schema.CloneMessages(history), nil
	}
	return f(ctx, history)
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
	HistoryCompressor  HistoryCompressor
}

type Request struct {
	// Pinned is an immutable stable prefix. It is never removed by trimming.
	Pinned  []schema.Message
	History []schema.Message
	Input   []schema.Message
	// Functions 是本次模型调用可用的工具定义；它们属于请求契约，不能
	// 被上下文引擎静默裁掉，但必须计入同一份窗口预算。
	Functions []schema.FunctionDefinition
	// SystemReminder 是本次调用临时生成的尾部提示，不属于会话历史。
	SystemReminder       *Reminder
	MaxContextTokens     int
	ReservedOutputTokens int
}

// Reminder 是一次模型调用的动态提示快照。Fields 使用稳定的键值格式，
// 引擎会排序键名并加上边界标记，避免把外部文本伪装成框架指令。
type Reminder struct {
	Text   string
	Fields map[string]string
}

const (
	systemReminderStart = "<system-reminder>"
	systemReminderEnd   = "</system-reminder>"
)

func (r Reminder) message() schema.Message {
	var builder strings.Builder
	builder.WriteString(systemReminderStart)
	if strings.TrimSpace(r.Text) != "" {
		builder.WriteByte('\n')
		builder.WriteString(sanitizeReminderValue(r.Text))
	}
	keys := make([]string, 0, len(r.Fields))
	for key := range r.Fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if strings.TrimSpace(key) == "" {
			continue
		}
		builder.WriteByte('\n')
		builder.WriteString(key)
		builder.WriteString(": ")
		builder.WriteString(sanitizeReminderValue(r.Fields[key]))
	}
	builder.WriteByte('\n')
	builder.WriteString(systemReminderEnd)
	message := schema.SystemMessage(builder.String())
	message.Extra = map[string]any{
		"context_kind": "system_reminder",
		"ephemeral":    true,
	}
	return message
}

func sanitizeReminderValue(value string) string {
	value = strings.TrimSpace(value)
	value = strings.ReplaceAll(value, "\r", " ")
	value = strings.ReplaceAll(value, "\n", " ")
	value = strings.ReplaceAll(value, systemReminderStart, "[system-reminder-start]")
	value = strings.ReplaceAll(value, systemReminderEnd, "[system-reminder-end]")
	return value
}

// IsSystemReminder 判断消息是否为 Context Engine 生成的临时提醒。
// 调试器、测试模型和 Provider 适配器可用它跳过提醒读取业务历史。
func IsSystemReminder(message schema.Message) bool {
	return message.Role == schema.RoleSystem && message.Extra != nil && message.Extra["context_kind"] == "system_reminder"
}

type Stats struct {
	BudgetTokens             int
	InputTokens              int
	OutputTokens             int
	RequestTokens            int
	FunctionDefinitionTokens int
	SystemReminderTokens     int
	StablePrefixMessages     int
	StablePrefixTokens       int
	DroppedTurns             int
	DroppedMessages          int
	TruncatedMessages        int
	TruncatedToolResults     int
	DroppedReasoningBlocks   int
	DroppedMediaBlocks       int
	RoleRepairs              int
	PromptCacheAligned       bool
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
	history := schema.CloneMessages(request.History)
	if a.config.HistoryCompressor != nil {
		compressed, err := a.config.HistoryCompressor.Compress(ctx, history)
		if err != nil {
			return Result{}, fmt.Errorf("compress context history: %w", err)
		}
		// 压缩器返回值可能复用其输入或内部缓冲区；交回标准引擎前
		// 再复制一次，避免后续裁剪修改压缩器持有的数据。
		history = schema.CloneMessages(compressed)
	}
	body := append(history, schema.CloneMessages(request.Input)...)
	reminderMessage := schema.Message{}
	reminderTokens := 0
	if request.SystemReminder != nil {
		reminderMessage = request.SystemReminder.message()
		reminderTokens = countMessage(reminderMessage, usageCounter{text: a.config.Counter, media: a.config.MediaEstimator})
	}
	functionTokens, err := countFunctionDefinitions(request.Functions, a.config.Counter)
	if err != nil {
		return Result{}, fmt.Errorf("count function definitions: %w", err)
	}

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
		BudgetTokens:             budget,
		StablePrefixMessages:     len(prefix),
		StablePrefixTokens:       countMessages(prefix, counter),
		FunctionDefinitionTokens: functionTokens,
		SystemReminderTokens:     reminderTokens,
		RoleRepairs:              repairs,
	}
	fixedTokens := stats.StablePrefixTokens + functionTokens + reminderTokens
	if fixedTokens > budget {
		reason := "stable prefix cannot be trimmed"
		switch {
		case functionTokens > 0 && stats.StablePrefixTokens+functionTokens > budget:
			reason = "function definitions and stable prefix cannot be trimmed"
		case reminderTokens > 0:
			reason = "system reminder and stable prefix cannot be trimmed"
		}
		return Result{}, &BudgetError{Required: fixedTokens, Available: budget, Reason: reason}
	}

	turns := splitTurns(normalized)
	// 每轮只计一次数：token 计数随丢弃单调下降，逐轮扣减即可定位
	// 裁剪点，不再每丢一轮就 flatten+全量重计（原 O(T²)）。
	turnTokens := make([]int, len(turns))
	messageTokens := stats.StablePrefixTokens
	for i, turn := range turns {
		turnTokens[i] = countMessages(turn, counter)
		messageTokens += turnTokens[i]
	}
	// 全量输入 token 等于前缀与全部轮次之和，直接复用累计值，
	// 不再拼接克隆后重算。
	stats.InputTokens = messageTokens
	currentTokens := messageTokens + functionTokens + reminderTokens
	stats.RequestTokens = currentTokens
	// 先从仍有机会保留的旧轮次移除推理和媒体块，再决定是否整轮
	// 删除；这样短的工具交换可以留下调用关系和有用文本摘要。
	for currentTokens > budget {
		changed := false
		for turnIndex := 0; turnIndex < len(turns)-1 && !changed; turnIndex++ {
			for messageIndex := range turns[turnIndex] {
				before := countMessage(turns[turnIndex][messageIndex], counter)
				removedReasoning := removeReasoningBlock(&turns[turnIndex][messageIndex])
				removedMedia := false
				if !removedReasoning {
					removedMedia = removeMediaBlock(&turns[turnIndex][messageIndex]) > 0
				}
				if !removedReasoning && !removedMedia {
					continue
				}
				after := countMessage(turns[turnIndex][messageIndex], counter)
				currentTokens -= before - after
				turnTokens[turnIndex] -= before - after
				if removedReasoning {
					stats.DroppedReasoningBlocks++
				} else {
					stats.DroppedMediaBlocks++
				}
				stats.TruncatedMessages++
				changed = true
				break
			}
		}
		if !changed {
			break
		}
	}
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
	currentTurnStart := len(normalized)
	if len(turns) > dropped {
		currentTurnStart -= len(turns[len(turns)-1])
	}
	for currentTokens > budget {
		changed := false
		for index := range normalized {
			before := countMessage(normalized[index], counter)
			if len(normalized[index].ContentBlocks) == 0 {
				continue
			}
			excess := currentTokens - budget
			containsToolResult := hasFunctionToolResult(normalized[index].ContentBlocks)
			if removed := removeReasoningBlock(&normalized[index]); removed {
				stats.DroppedReasoningBlocks++
			} else if index < currentTurnStart {
				if removed := removeMediaBlock(&normalized[index]); removed > 0 {
					stats.DroppedMediaBlocks += removed
				} else {
					normalized[index] = trimMessageTexts(normalized[index], before-(currentTokens-budget), counter, true)
				}
			} else {
				target := before - excess
				if target < 1 {
					target = 1
				}
				normalized[index] = trimMessageTexts(normalized[index], target, counter, true)
			}
			after := countMessage(normalized[index], counter)
			if after < before {
				stats.TruncatedMessages++
				if containsToolResult {
					stats.TruncatedToolResults++
				}
				currentTokens -= before - after
				stats.RequestTokens = currentTokens
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
	if request.SystemReminder != nil {
		messages = append(messages, reminderMessage)
	}
	if err := validateMessages(messages, "assembled context"); err != nil {
		return Result{}, err
	}
	stats.OutputTokens = countMessages(messages, counter)
	stats.RequestTokens = stats.OutputTokens + functionTokens
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
	return removeBlockRecursive(&message.ContentBlocks, func(block schema.ContentBlock) bool {
		return block.Type == schema.ContentBlockTypeReasoning
	})
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
	budget := limit
	return trimTextBlocks(blocks, &budget, counter, keepEnd)
}

// countFunctionDefinitions 使用稳定 JSON 表示估算工具定义成本。工具定义
// 属于模型调用契约，因此序列化失败直接返回错误，不伪造一个较小的成本。
func countFunctionDefinitions(functions []schema.FunctionDefinition, counter TokenCounter) (int, error) {
	total := 0
	for index, function := range functions {
		data, err := json.Marshal(function)
		if err != nil {
			return 0, fmt.Errorf("function definition %d: %w", index, err)
		}
		total += counter.Count(string(data))
	}
	return total, nil
}

func hasFunctionToolResult(blocks []schema.ContentBlock) bool {
	for _, block := range blocks {
		if block.Type == schema.ContentBlockTypeFunctionToolResult && block.FunctionToolResult != nil {
			return true
		}
		if block.FunctionToolResult != nil && hasFunctionToolResult(block.FunctionToolResult.ContentBlocks) {
			return true
		}
	}
	return false
}

func removeMediaBlock(message *schema.Message) int {
	if removeBlockRecursive(&message.ContentBlocks, func(block schema.ContentBlock) bool {
		switch block.Type {
		case schema.ContentBlockTypeImage, schema.ContentBlockTypeFile, schema.ContentBlockTypeAudio, schema.ContentBlockTypeVideo:
			return true
		default:
			return false
		}
	}) {
		return 1
	}
	return 0
}

func removeBlockRecursive(blocks *[]schema.ContentBlock, predicate func(schema.ContentBlock) bool) bool {
	for index, block := range *blocks {
		if predicate(block) {
			*blocks = append((*blocks)[:index], (*blocks)[index+1:]...)
			return true
		}
		if block.FunctionToolResult == nil {
			continue
		}
		nested := block.FunctionToolResult.ContentBlocks
		if removeBlockRecursive(&nested, predicate) {
			block.FunctionToolResult.ContentBlocks = nested
			(*blocks)[index] = block
			return true
		}
	}
	return false
}

func trimMessageTexts(message schema.Message, target int, counter usageCounter, keepEnd bool) schema.Message {
	cloned := cloneMessageExtra(message)
	fixed := 4 + counter.text.Count(string(cloned.Role)) + countNonTextBlocks(cloned.ContentBlocks, counter)
	contentBudget := target - fixed
	if contentBudget < 0 {
		contentBudget = 0
	}
	cloned.ContentBlocks = trimTextBlocks(cloned.ContentBlocks, &contentBudget, counter.text, keepEnd)
	return cloned
}

func countNonTextBlocks(blocks []schema.ContentBlock, counter usageCounter) int {
	total := 0
	for _, block := range blocks {
		switch block.Type {
		case schema.ContentBlockTypeText:
			continue
		case schema.ContentBlockTypeFunctionToolResult:
			if block.FunctionToolResult != nil {
				total += counter.text.Count(block.FunctionToolResult.CallID) + counter.text.Count(block.FunctionToolResult.Name)
				total += countNonTextBlocks(block.FunctionToolResult.ContentBlocks, counter)
			}
		case schema.ContentBlockTypeReasoning:
			if block.Reasoning != nil {
				total += counter.text.Count(block.Reasoning.Text) + counter.text.Count(block.Reasoning.Signature)
			}
		case schema.ContentBlockTypeFunctionToolCall:
			if block.FunctionToolCall != nil {
				total += counter.text.Count(block.FunctionToolCall.ID) + counter.text.Count(block.FunctionToolCall.Name) + counter.text.Count(block.FunctionToolCall.Arguments)
			}
		case schema.ContentBlockTypeOpaque:
			if block.Opaque != nil {
				total += counter.text.Count(block.Opaque.Kind) + counter.text.Count(string(block.Opaque.Raw))
			}
		case schema.ContentBlockTypeImage, schema.ContentBlockTypeFile, schema.ContentBlockTypeAudio, schema.ContentBlockTypeVideo:
			total += counter.media.EstimateMedia(block)
		}
	}
	return total
}

func trimTextBlocks(blocks []schema.ContentBlock, budget *int, counter TokenCounter, keepEnd bool) []schema.ContentBlock {
	cloned := schema.CloneMessages([]schema.Message{{ContentBlocks: blocks}})[0].ContentBlocks
	for index := len(cloned) - 1; index >= 0; index-- {
		block := &cloned[index]
		if block.Type == schema.ContentBlockTypeText && block.Text != nil {
			block.Text.Text = truncateText(block.Text.Text, *budget, counter, keepEnd)
			*budget -= counter.Count(block.Text.Text)
			continue
		}
		if block.FunctionToolResult != nil {
			nestedBudget := *budget
			block.FunctionToolResult.ContentBlocks = trimTextBlocks(block.FunctionToolResult.ContentBlocks, &nestedBudget, counter, keepEnd)
			*budget = nestedBudget
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
