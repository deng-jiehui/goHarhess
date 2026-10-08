package tool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/goharness/goharness/pkg/control"
	"github.com/goharness/goharness/pkg/observe"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
)

const (
	DefaultToolsNodeConcurrency = 8
	MaxToolsNodeConcurrency     = 64
)

// ToolCall is the provider-neutral representation of one trailing assistant
// tool call. FunctionID is preserved so OpenAI/Claude adapters can correlate
// the appended result with the original request.
type ToolCall struct {
	Name       string
	Arguments  string
	FunctionID string
	Metadata   map[string]any
}

// ToolNodeConfig configures the runnable ToolsNode.
type ToolNodeConfig struct {
	Name           string
	Description    string
	Tools          []Tool
	BeforeBatch    []BeforeBatchHook
	Middlewares    []Middleware
	MaxConcurrency int
	MaxDepth       int
	Timeout        time.Duration
	MaxRetries     int
	// RetryBackoff 是重试间隔；RetryBackoffSet 为真或间隔非零时覆盖默认值
	// （可显式覆盖为零表示无等待），未标记的结构体零值沿用原默认间隔。
	RetryBackoff    time.Duration
	RetryBackoffSet bool
	// MaxAbandoned 是单个工具未归还执行的熔断阈值，0 使用默认值（8）；
	// 达到阈值后新调用拒绝执行，防止不响应取消的工具耗尽 worker 池。
	MaxAbandoned int
}

// ToolsNode executes only the assistant tool calls at the message tail. It
// returns append-only tool result messages; callers append those results to
// the existing conversation before the next model loop.
type ToolsNode struct {
	name        string
	description string
	tools       map[string]*Binding
	order       []string
	scope       *Scope
	maxParallel int
	beforeBatch []BeforeBatchHook
}

func NewToolsNode(config ToolNodeConfig) (*ToolsNode, error) {
	name := config.Name
	if name == "" {
		name = "tools"
	}
	if config.MaxConcurrency == 0 {
		config.MaxConcurrency = DefaultToolsNodeConcurrency
	}
	if config.MaxConcurrency < 1 || config.MaxConcurrency > MaxToolsNodeConcurrency {
		return nil, fmt.Errorf("tools node concurrency must be between 1 and %d", MaxToolsNodeConcurrency)
	}
	runtimeConfig := defaultRuntimeConfig()
	runtimeConfig.MaxConcurrency = config.MaxConcurrency
	if config.MaxDepth != 0 {
		runtimeConfig.MaxDepth = config.MaxDepth
	}
	if config.Timeout != 0 {
		runtimeConfig.Timeout = config.Timeout
	}
	runtimeConfig.MaxRetries = config.MaxRetries
	if config.RetryBackoffSet || config.RetryBackoff != 0 {
		runtimeConfig.RetryBackoff = config.RetryBackoff
	}
	if config.MaxAbandoned != 0 {
		runtimeConfig.MaxAbandoned = config.MaxAbandoned
	}
	runtime, err := NewRuntime(config.Middlewares, runtimeConfig)
	if err != nil {
		return nil, fmt.Errorf("configure tools node runtime: %w", err)
	}
	node := &ToolsNode{
		name:        name,
		description: config.Description,
		tools:       map[string]*Binding{},
		scope:       runtime.NewScope(config.MaxConcurrency),
		maxParallel: config.MaxConcurrency,
		beforeBatch: append([]BeforeBatchHook(nil), config.BeforeBatch...),
	}
	if node.description == "" {
		node.description = "append-only tool execution node"
	}
	for index, value := range config.Tools {
		if value == nil {
			return nil, fmt.Errorf("tools node tool %d is nil", index)
		}
		binding, err := runtime.Register(value)
		if err != nil {
			return nil, fmt.Errorf("tools node tool %d: %w", index, err)
		}
		spec := binding.Spec()
		if _, exists := node.tools[spec.Name]; exists {
			return nil, fmt.Errorf("tools node duplicate tool %q", spec.Name)
		}
		node.tools[spec.Name] = binding
		node.order = append(node.order, spec.Name)
	}
	return node, nil
}

func (n *ToolsNode) Name() string {
	if n == nil {
		return ""
	}
	return n.name
}

func (n *ToolsNode) Description() string {
	if n == nil {
		return ""
	}
	return n.description
}

func (*ToolsNode) NodeKind() runnable.NodeKind { return runnable.NodeKindTools }

// Tools returns a defensive copy of the registered tool map.
func (n *ToolsNode) Tools() []Tool {
	if n == nil {
		return nil
	}
	values := make([]Tool, 0, len(n.order))
	for _, name := range n.order {
		values = append(values, n.tools[name].value)
	}
	return values
}

// Definitions exposes the model-facing schemas discovered from all tools.
func (n *ToolsNode) Definitions() []schema.FunctionDefinition {
	if n == nil {
		return nil
	}
	definitions := make([]schema.FunctionDefinition, 0, len(n.order))
	for _, name := range n.order {
		definitions = append(definitions, n.tools[name].Spec().Definition())
	}
	return definitions
}

// TailToolCalls extracts only a contiguous suffix of assistant messages containing call blocks
// messages. Historical calls are intentionally ignored, preventing a resumed
// graph from executing an already completed tool a second time.
func TailToolCalls(messages []schema.Message) []ToolCall {
	groups := make([][]ToolCall, 0)
	for index := len(messages) - 1; index >= 0; index-- {
		message := messages[index]
		if message.Role != schema.RoleAssistant {
			break
		}
		messageCalls := message.FunctionToolCalls()
		if len(messageCalls) == 0 {
			break
		}
		group := make([]ToolCall, len(messageCalls))
		for callIndex, call := range messageCalls {
			group[callIndex] = ToolCall{
				Name:       call.Name,
				Arguments:  call.Arguments,
				FunctionID: call.ID,
				Metadata:   cloneAnyMap(call.Extra),
			}
		}
		groups = append(groups, group)
	}
	calls := make([]ToolCall, 0)
	for index := len(groups) - 1; index >= 0; index-- {
		calls = append(calls, groups[index]...)
	}
	return calls
}

// Invoke executes trailing calls concurrently while preserving their source
// order in the returned append-only result messages.
func (n *ToolsNode) Invoke(ctx context.Context, messages []schema.Message, options ...runnable.Option) ([]schema.Message, error) {
	if n == nil || len(n.tools) == 0 {
		return nil, errors.New("tools node is not configured")
	}
	if ctx == nil {
		return nil, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// 解析本节点声明的执行限制；其余选项原样透传给工具调用。
	limits, remaining, err := n.resolveExecutionLimits(ctx, options)
	if err != nil {
		return nil, err
	}
	options = remaining
	// 解析结果经 ctx 带给逐调用执行路径，同一次调用不再重复执行设置函数。
	ctx = WithExecutionLimits(ctx, limits)
	maxParallel := n.maxParallel
	if limits.MaxConcurrency > 0 && limits.MaxConcurrency < maxParallel {
		maxParallel = limits.MaxConcurrency
	}
	invocationSlots := make(chan struct{}, maxParallel)
	isolatedMessages, err := schema.CloneMessagesStrict(messages)
	if err != nil {
		return nil, fmt.Errorf("clone tools node messages: %w", err)
	}
	messages = isolatedMessages
	calls := TailToolCalls(messages)
	if len(calls) == 0 {
		return nil, nil
	}
	batch := Batch{Calls: make([]BatchCall, 0, len(calls))}
	batch.Subject, _ = SubjectFromContext(ctx)
	for _, call := range calls {
		binding, exists := n.tools[call.Name]
		if !exists {
			continue
		}
		args, parseErr := ParseArgsStrict(call.Arguments)
		if parseErr != nil {
			continue
		}
		batch.Calls = append(batch.Calls, BatchCall{
			ID:       call.FunctionID,
			Spec:     cloneSpec(binding.Spec()),
			RawArgs:  call.Arguments,
			Args:     cloneAnyMap(args),
			Metadata: cloneAnyMap(call.Metadata),
		})
	}
	if len(batch.Calls) > 0 {
		if err := RunBeforeBatchHooks(ctx, n.beforeBatch, batch); err != nil {
			return nil, err
		}
	}
	// 每个调用分配批次作用域内的去重键：同一批次恢复重放时键不变，命中
	// 已登记结果的调用不再执行；主动发起的新批次获得新键，正常执行。
	// 同一批次出现重复基准键时追加序号，保持唯一。
	callBatch := control.BeginCallBatch(ctx)
	dedupeKeys := make([]string, len(calls))
	seenKeys := make(map[string]int, len(calls))
	for index, call := range calls {
		base := callDedupeBase(call)
		if ordinal := seenKeys[base]; ordinal > 0 {
			dedupeKeys[index] = base + "#" + strconv.Itoa(ordinal+1)
		} else {
			dedupeKeys[index] = base
		}
		seenKeys[base]++
	}
	type indexedResult struct {
		index   int
		message schema.Message
		suspend error
	}
	results := make(chan indexedResult, len(calls))
	var workers sync.WaitGroup
	workers.Add(len(calls))
	for index, call := range calls {
		go func(index int, call ToolCall, dedupeKey string) {
			defer workers.Done()
			if err := acquire(ctx, invocationSlots); err != nil {
				results <- indexedResult{index: index, message: toolErrorMessage(call, err)}
				return
			}
			defer release(invocationSlots)
			message, suspend := n.invokeCall(ctx, n.scope, call, callBatch, dedupeKey, messages, options...)
			results <- indexedResult{index: index, message: message, suspend: suspend}
		}(index, call, dedupeKeys[index])
	}
	go func() {
		workers.Wait()
		close(results)
	}()
	output := make([]schema.Message, len(calls))
	var framework []error
	var suspended error
	for result := range results {
		output[result.index] = result.message
		if result.suspend == nil {
			continue
		}
		// 框架控制错误与挂起信号分开汇总：挂起可以等待恢复，校验与
		// 持久化错误必须让调用方立即看到，不得被先到的挂起掩盖。
		if control.AsFramework(result.suspend) {
			framework = append(framework, result.suspend)
			continue
		}
		if suspended == nil {
			suspended = result.suspend
		}
	}
	if len(framework) > 0 {
		return nil, errors.Join(framework...)
	}
	// 任一调用携带挂起信号（会话内 Interrupt、子调用等待）时整节点挂起：
	// 错误上交调度器登记等待位置，其余结果丢弃、恢复后整批重放。
	if suspended != nil {
		return nil, suspended
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return output, nil
}

func (n *ToolsNode) Stream(ctx context.Context, input []schema.Message, options ...runnable.Option) (*runnable.StreamReader[[]schema.Message], error) {
	return runnable.StreamFromInvoke(ctx, input, n.Invoke, options...)
}
func (n *ToolsNode) Collect(ctx context.Context, input *runnable.StreamReader[[]schema.Message], options ...runnable.Option) ([]schema.Message, error) {
	return runnable.CollectFromInvoke(ctx, input, n.Invoke, options...)
}
func (n *ToolsNode) Transform(ctx context.Context, input *runnable.StreamReader[[]schema.Message], options ...runnable.Option) (*runnable.StreamReader[[]schema.Message], error) {
	return runnable.TransformFromInvoke(ctx, input, n.Invoke, options...)
}

// callDedupeBase 推导调用的基准去重键：优先使用提供商调用 ID，缺失时
// 用工具名与参数的稳定摘要代替，保证同一输入重放得到同一键。
func callDedupeBase(call ToolCall) string {
	if call.FunctionID != "" {
		return call.FunctionID
	}
	sum := sha256.Sum256([]byte(call.Name + "\x00" + call.Arguments))
	return call.Name + ":" + hex.EncodeToString(sum[:8])
}

func (n *ToolsNode) invokeCall(ctx context.Context, scope *Scope, call ToolCall, batch control.CallBatch, dedupeKey string, messages []schema.Message, options ...runnable.Option) (schema.Message, error) {
	callKey := dedupeKey
	if batch != nil {
		callKey = batch.CallKey(dedupeKey)
		if stored, ok := lookupPendingCall(batch, callKey); ok {
			return toolResultMessage(call, stored.ContentBlocks, stored.IsError), nil
		}
		// 调用身份进上下文：工具内的 Interrupt 与子调用据此归属本调用，
		// 结果复用时由登记器同步消费其日志位置。
		ctx = control.WithCallKey(ctx, callKey)
	}
	// done 产出一条结果消息并登记完成结果：挂起恢复后该调用直接复用，
	// 不再执行工具本体。挂起型错误（Interrupt/子调用挂起）不是完成结果，
	// 恢复时调用须重新执行以重放交互，因此不登记。
	done := func(blocks []schema.ContentBlock, isError bool) schema.Message {
		if batch != nil {
			recordPendingCall(batch, callKey, Result{ContentBlocks: blocks, IsError: isError})
		}
		return toolResultMessage(call, blocks, isError)
	}
	failed := func(err error) (schema.Message, error) {
		if suspendCaused(err) || control.AsFramework(err) {
			// 挂起与框架控制错误（恢复校验、检查点保存等）作为节点级
			// 错误上传：调度器据此挂起或终止运行。吞成普通错误结果会让
			// 子调用永远等待，或把校验失败伪装成业务失败继续推进。
			return toolErrorMessage(call, err), err
		}
		return done([]schema.ContentBlock{schema.NewTextBlock(modelFacingToolError(call, err))}, true), nil
	}
	binding, exists := n.tools[call.Name]
	if !exists {
		return failed(fmt.Errorf("tool %q: %w", call.Name, ErrUnknownTool))
	}
	args, err := ParseArgsStrict(call.Arguments)
	if err != nil {
		return failed(err)
	}
	input := CallInput{
		RawArgs:  call.Arguments,
		Args:     args,
		Metadata: observe.MetadataFromContext(ctx),
	}
	// 幂等键含运行级前缀：同一运行的恢复中保持不变，不同运行互不冲突。
	if batch != nil {
		input.IdempotencyKey = batch.Namespace() + "/" + callKey
	} else {
		input.IdempotencyKey = dedupeKey
	}
	for key, value := range cloneAnyMap(call.Metadata) {
		input.Metadata[key] = value
	}
	if binding.Spec().NeedsMessages {
		// 浅拷贝头部共享底层消息与内容块：同批声明历史需求的调用看到
		// 同一快照视图，避免逐调用深拷贝整段历史；视图按只读约定使用。
		input.Messages = append([]schema.Message(nil), messages...)
	}
	if binding.Spec().FileAccess {
		input.Files = schema.ExtractFiles(messages, true)
	}
	result, err := scope.Execute(ctx, binding, input, options...)
	if err != nil {
		return failed(err)
	}
	blocks, err := result.ToContentBlocks()
	if err != nil {
		return failed(err)
	}
	return done(blocks, result.IsError), nil
}

func toolResultMessage(call ToolCall, blocks []schema.ContentBlock, isError bool) schema.Message {
	return schema.NewMessage(schema.RoleTool,
		schema.NewFunctionToolResultBlock(call.FunctionID, call.Name, isError, blocks...),
	)
}

func toolErrorMessage(call ToolCall, err error) schema.Message {
	return toolResultMessage(call, []schema.ContentBlock{schema.NewTextBlock(modelFacingToolError(call, err))}, true)
}

// modelFacingToolError 决定模型看到的错误文本。默认脱敏为通用信息，内部
// 细节只进日志与 callback；工具/中间件可用 ModelVisible 显式暴露反馈，
// 参数语法与 schema 违规是框架为模型自纠设计的可见错误。
func modelFacingToolError(call ToolCall, err error) string {
	if err == nil {
		return fmt.Sprintf("tool %q failed", call.Name)
	}
	var visible *ModelVisibleError
	hasVisible := errors.As(err, &visible)
	switch {
	case errors.Is(err, ErrPermissionDenied):
		text := fmt.Sprintf("tool %q permission denied", call.Name)
		if hasVisible {
			return text + ": " + visible.Err.Error()
		}
		return text
	case errors.Is(err, ErrUnknownTool):
		return fmt.Sprintf("tool %q does not exist", call.Name)
	case errors.Is(err, context.DeadlineExceeded):
		return fmt.Sprintf("tool %q timed out", call.Name)
	case errors.Is(err, ErrToolUnavailable):
		return fmt.Sprintf("tool %q is temporarily unavailable", call.Name)
	case hasVisible:
		return fmt.Sprintf("tool %q failed: %v", call.Name, visible.Err)
	case errors.Is(err, ErrInvalidToolArgs):
		// 只输出类型化错误自身的提示；包装层或 errors.Join 合并的内部
		// 诊断不外发（错误链里可能混有未标记的内部信息）。
		var invalid *InvalidArgsError
		if errors.As(err, &invalid) {
			return invalid.Error()
		}
		return fmt.Sprintf("tool %q arguments violate the declared schema", call.Name)
	case errors.Is(err, ErrMalformedArgs):
		// JSON 语法错误同理：能取到具体的语法位置则给出，否则固定提示。
		var syntaxErr *json.SyntaxError
		if errors.As(err, &syntaxErr) {
			return fmt.Sprintf("tool %q failed: arguments must be a valid JSON object (syntax error at byte %d)", call.Name, syntaxErr.Offset)
		}
		return fmt.Sprintf("tool %q failed: arguments must be a valid JSON object", call.Name)
	default:
		return fmt.Sprintf("tool %q failed", call.Name)
	}
}

var _ runnable.NamedRunnable[[]schema.Message, []schema.Message] = (*ToolsNode)(nil)
var _ runnable.NodeDescriptor = (*ToolsNode)(nil)
