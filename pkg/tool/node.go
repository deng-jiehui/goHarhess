package tool

import (
	"context"
	"errors"
	"fmt"
	"sync"

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
	Tools          []BaseTool
	BeforeBatch    []BeforeBatchHook
	MaxConcurrency int
}

// ToolsNode executes only the assistant tool calls at the message tail. It
// returns append-only tool result messages; callers append those results to
// the existing conversation before the next model loop.
type ToolsNode struct {
	name           string
	description    string
	tools          map[string]BaseTool
	policies       map[string]ConcurrencyPolicy
	order          []string
	maxConcurrency int
	semaphore      chan struct{}
	scheduler      *toolNodeScheduler
	beforeBatch    []BeforeBatchHook
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
	node := &ToolsNode{
		name:           name,
		description:    config.Description,
		tools:          map[string]BaseTool{},
		policies:       map[string]ConcurrencyPolicy{},
		maxConcurrency: config.MaxConcurrency,
		semaphore:      make(chan struct{}, config.MaxConcurrency),
		scheduler:      newToolNodeScheduler(),
		beforeBatch:    append([]BeforeBatchHook(nil), config.BeforeBatch...),
	}
	if node.description == "" {
		node.description = "append-only tool execution node"
	}
	for index, value := range config.Tools {
		if value == nil {
			return nil, fmt.Errorf("tools node tool %d is nil", index)
		}
		spec := value.Spec()
		if err := spec.Validate(); err != nil {
			return nil, fmt.Errorf("tools node tool %d: %w", index, err)
		}
		policy := spec.Concurrency.Normalize()
		if err := node.scheduler.register(policy); err != nil {
			return nil, fmt.Errorf("tools node tool %d: %w", index, err)
		}
		if _, exists := node.tools[spec.Name]; exists {
			return nil, fmt.Errorf("tools node duplicate tool %q", spec.Name)
		}
		node.tools[spec.Name] = value
		node.policies[spec.Name] = policy
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

// Tools returns a defensive copy of the registered tool map.
func (n *ToolsNode) Tools() []BaseTool {
	if n == nil {
		return nil
	}
	values := make([]BaseTool, 0, len(n.order))
	for _, name := range n.order {
		values = append(values, n.tools[name])
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

// TailToolCalls extracts only a contiguous suffix of assistant function-call
// messages. Historical calls are intentionally ignored, preventing a resumed
// graph from executing an already completed tool a second time.
func TailToolCalls(messages []schema.Message) []ToolCall {
	calls := make([]ToolCall, 0)
	for index := len(messages) - 1; index >= 0; index-- {
		message := messages[index]
		if message.Role != schema.RoleAssistant || message.FunctionCall == nil {
			break
		}
		calls = append(calls, ToolCall{
			Name:       message.FunctionCall.Name,
			Arguments:  message.FunctionCall.Arguments,
			FunctionID: message.FunctionID(fmt.Sprintf("call-%d", index+1)),
			Metadata:   cloneAnyMap(message.Extra),
		})
	}
	for left, right := 0, len(calls)-1; left < right; left, right = left+1, right-1 {
		calls[left], calls[right] = calls[right], calls[left]
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
		value, exists := n.tools[call.Name]
		if !exists {
			continue
		}
		args, parseErr := ParseArgsStrict(call.Arguments)
		if parseErr != nil {
			continue
		}
		batch.Calls = append(batch.Calls, BatchCall{
			ID:       call.FunctionID,
			Spec:     cloneSpec(value.Spec()),
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
			select {
			case <-ctx.Done():
				results <- indexedResult{index: index, message: toolErrorMessage(call, ctx.Err())}
			case n.semaphore <- struct{}{}:
				message := n.invokeCall(ctx, call, messages, options...)
				<-n.semaphore
				results <- indexedResult{index: index, message: message}
			}
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

func (n *ToolsNode) invokeCall(ctx context.Context, call ToolCall, messages []schema.Message, options ...runnable.Option) schema.Message {
	value, exists := n.tools[call.Name]
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
	}
	callbackCtx, invocation := BeginCallback(ctx, value.Spec(), input)
	policy := n.policies[call.Name]
	result, err := n.scheduler.execute(ctx, policy, func() (Result, error) {
		return invokeBaseTool(callbackCtx, call.Name, value, input, options...)
	})
	EndCallback(callbackCtx, invocation, value.Spec(), input, result, err)
	if err != nil {
		return toolErrorMessage(call, err)
	}
	content, err := result.ToContent()
	if err != nil {
		return toolErrorMessage(call, err)
	}
	return toolResultMessage(call, content, result.IsError)
}

func invokeBaseTool(ctx context.Context, name string, value BaseTool, input CallInput, options ...runnable.Option) (result Result, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("tool %q panicked: %v", name, recovered)
		}
	}()
	return value.Invoke(ctx, input, options...)
}

func toolResultMessage(call ToolCall, content schema.Content, isError bool) schema.Message {
	message := schema.FunctionMessage(call.Name, content)
	message.Extra = map[string]any{
		"function_id":       call.FunctionID,
		"tool_call_id":      call.FunctionID,
		"agent_observation": true,
		"tool_error":        isError,
	}
	return message
}

func toolErrorMessage(call ToolCall, err error) schema.Message {
	if err == nil {
		err = errors.New("unknown tool error")
	}
	return toolResultMessage(call, schema.TextContent(fmt.Sprintf("tool %q failed: %v", call.Name, err)), true)
}

var _ runnable.NamedRunnable[[]schema.Message, []schema.Message] = (*ToolsNode)(nil)
