package tool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"goharness/pkg/callback"
	"goharness/pkg/runnable"
	"goharness/pkg/schema"
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
	RetryBackoff   time.Duration
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
	if config.RetryBackoff != 0 {
		runtimeConfig.RetryBackoff = config.RetryBackoff
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
	plan, err := runnable.NewOptionPlan(options...)
	if err != nil {
		return nil, fmt.Errorf("resolve tools node options: %w", err)
	}
	nodeOptions := plan.ForwardOptionsFor(runnable.OptionScopeNode)
	nodePlan, err := runnable.NewOptionPlan(nodeOptions...)
	if err != nil {
		return nil, fmt.Errorf("isolate tools node options: %w", err)
	}
	ctx = runnable.WithOptionPlan(ctx, nodePlan)
	options = nodePlan.ForwardOptionsFor(runnable.OptionScopeNode)
	executionOptions := nodeExecutionOptionsFromPlan(nodePlan)
	contextLimits := executionLimitsFromContext(ctx)
	maxParallel := n.maxParallel
	if contextLimits.MaxConcurrency > 0 && contextLimits.MaxConcurrency < maxParallel {
		maxParallel = contextLimits.MaxConcurrency
	}
	if executionOptions.MaxConcurrency > 0 && executionOptions.MaxConcurrency < maxParallel {
		maxParallel = executionOptions.MaxConcurrency
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
	type indexedResult struct {
		index   int
		message schema.Message
	}
	results := make(chan indexedResult, len(calls))
	var workers sync.WaitGroup
	workers.Add(len(calls))
	for index, call := range calls {
		go func(index int, call ToolCall) {
			defer workers.Done()
			if err := acquire(ctx, invocationSlots); err != nil {
				results <- indexedResult{index: index, message: toolErrorMessage(call, err)}
				return
			}
			defer release(invocationSlots)
			message := n.invokeCall(ctx, n.scope, call, messages, options...)
			results <- indexedResult{index: index, message: message}
		}(index, call)
	}
	go func() {
		workers.Wait()
		close(results)
	}()
	output := make([]schema.Message, len(calls))
	for result := range results {
		output[result.index] = result.message
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

func (n *ToolsNode) invokeCall(ctx context.Context, scope *Scope, call ToolCall, messages []schema.Message, options ...runnable.Option) schema.Message {
	binding, exists := n.tools[call.Name]
	if !exists {
		return toolErrorMessage(call, fmt.Errorf("tool %q does not exist", call.Name))
	}
	args, err := ParseArgsStrict(call.Arguments)
	if err != nil {
		return toolErrorMessage(call, err)
	}
	isolatedMessages, err := schema.CloneMessagesStrict(messages)
	if err != nil {
		return toolErrorMessage(call, fmt.Errorf("clone tool input messages: %w", err))
	}
	input := CallInput{
		RawArgs:    call.Arguments,
		Args:       args,
		HTTPClient: HTTPClient(ctx),
		Messages:   isolatedMessages,
		Metadata:   callback.MetadataFromContext(ctx),
	}
	for key, value := range cloneAnyMap(call.Metadata) {
		input.Metadata[key] = value
	}
	if binding.Spec().FileAccess {
		input.Files = schema.ExtractFiles(messages, true)
	}
	result, err := scope.Execute(ctx, binding, input, options...)
	if err != nil {
		return toolErrorMessage(call, err)
	}
	blocks, err := result.ToContentBlocks()
	if err != nil {
		return toolErrorMessage(call, err)
	}
	return toolResultMessage(call, blocks, result.IsError)
}

func toolResultMessage(call ToolCall, blocks []schema.ContentBlock, isError bool) schema.Message {
	return schema.NewMessage(schema.RoleTool,
		schema.NewFunctionToolResultBlock(call.FunctionID, call.Name, isError, blocks...),
	)
}

func toolErrorMessage(call ToolCall, err error) schema.Message {
	if err == nil {
		err = errors.New("unknown tool error")
	}
	return toolResultMessage(call, []schema.ContentBlock{schema.NewTextBlock(fmt.Sprintf("tool %q failed: %v", call.Name, err))}, true)
}

var _ runnable.NamedRunnable[[]schema.Message, []schema.Message] = (*ToolsNode)(nil)
var _ runnable.NodeDescriptor = (*ToolsNode)(nil)
