package tool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
)

// Spec describes a tool to models, authorization policies and the scheduler.
type Spec struct {
	Name        string
	Description string
	Parameters  map[string]any
	FileAccess  bool
	// NeedsMessages 声明工具需要读取会话历史：为 true 时 CallInput.Messages
	// 注入调用时的历史视图，否则为 nil——历史包含 system prompt 与其他
	// 工具输出，默认不暴露给工具，也省去逐调用拷贝历史的开销。
	NeedsMessages bool
	// IsReadOnly defaults to false so undeclared tools are treated as mutating.
	IsReadOnly bool
	// Concurrency's zero value serializes the tool with other undeclared tools.
	Concurrency ConcurrencyPolicy
}

// Validate checks required fields and concurrency settings.
func (s Spec) Validate() error {
	if s.Name == "" {
		return errors.New("tool name cannot be empty")
	}
	if err := s.Concurrency.Validate(); err != nil {
		return fmt.Errorf("tool %q concurrency: %w", s.Name, err)
	}
	return nil
}

// Definition converts the specification to the model-facing function schema.
func (s Spec) Definition() schema.FunctionDefinition {
	params := s.Parameters
	if params == nil {
		params = EmptyParameters()
	}
	return schema.FunctionDefinition{
		Name:        s.Name,
		Description: s.Description,
		Parameters:  params,
	}
}

// EmptyParameters returns a JSON schema for a function with no arguments.
func EmptyParameters() map[string]any {
	return map[string]any{
		"type":       "object",
		"properties": map[string]any{},
		"required":   []string{},
	}
}

// CallInput contains parsed arguments and non-identity invocation data.
// Authentication identity belongs in context.Context via WithSubject.
type CallInput struct {
	RawArgs string
	Args    map[string]any
	// Messages 仅当 Spec.NeedsMessages 为 true 时注入：会话历史的只读
	// 视图，与同批其他声明了历史需求的调用共享底层内容。工具不得修改
	// 其中的消息或内容块，否则会影响同批调用与调用方的历史视图。
	Messages []schema.Message
	Files    []string
	Metadata map[string]any
	// IdempotencyKey 是本次调用的稳定去重键：恢复重放同一调用时取值不变，
	// 有副作用的工具可把它传给下游幂等接口防止重复执行。
	IdempotencyKey string
}

// Result contains either model-facing content blocks or structured Data.
type Result struct {
	ContentBlocks []schema.ContentBlock
	Data          any
	// IsError preserves a protocol-level tool result that is intentionally
	// visible to the next model turn. It is distinct from an execution error.
	IsError bool
}

// TextResult creates a text tool result.
func TextResult(text string) Result {
	return BlockResult(schema.NewTextBlock(text))
}

// BlockResult creates a tool result from ordered content blocks.
func BlockResult(blocks ...schema.ContentBlock) Result {
	return Result{ContentBlocks: cloneResultBlocks(blocks)}
}

// DataResult creates a structured tool result serialized by ToContentBlocks.
func DataResult(data any) Result {
	return Result{Data: data}
}

// ErrorResult creates a model-visible tool error without converting it into a
// transport/runtime failure. This matches MCP's isError result semantics.
func ErrorResult(blocks ...schema.ContentBlock) Result {
	result := BlockResult(blocks...)
	result.IsError = true
	return result
}

// ToContentBlocks converts structured result data to model-facing blocks.
func (r Result) ToContentBlocks() ([]schema.ContentBlock, error) {
	if r.ContentBlocks != nil {
		return cloneResultBlocksStrict(r.ContentBlocks)
	}
	if r.Data == nil {
		return []schema.ContentBlock{schema.NewTextBlock("")}, nil
	}
	if text, ok := r.Data.(string); ok {
		return []schema.ContentBlock{schema.NewTextBlock(text)}, nil
	}
	data, err := json.MarshalIndent(r.Data, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal tool result: %w", err)
	}
	return []schema.ContentBlock{schema.NewTextBlock(string(data))}, nil
}

func cloneResultBlocks(blocks []schema.ContentBlock) []schema.ContentBlock {
	if blocks == nil {
		return nil
	}
	message := schema.NewMessage(schema.RoleTool, blocks...)
	return schema.CloneMessages([]schema.Message{message})[0].ContentBlocks
}

func cloneResultBlocksStrict(blocks []schema.ContentBlock) ([]schema.ContentBlock, error) {
	if blocks == nil {
		return nil, nil
	}
	message := schema.NewMessage(schema.RoleTool, blocks...)
	cloned, err := schema.CloneMessagesStrict([]schema.Message{message})
	if err != nil {
		return nil, fmt.Errorf("clone tool result blocks: %w", err)
	}
	return cloned[0].ContentBlocks, nil
}

// Tool combines model-facing metadata with the canonical execution
// contract. Providers, MCP adapters and application tools all implement this
// small interface and can be placed in a ToolsNode.
type Tool interface {
	Spec() Spec
	runnable.Runnable[CallInput, Result]
}

// Handler implements a FunctionTool invocation.
type Handler func(ctx context.Context, input CallInput) (Result, error)

// FunctionTool adapts a Handler to Tool.
type FunctionTool struct {
	spec    Spec
	handler Handler
}

// NewFunctionTool creates a tool and fills an omitted parameter schema.
func NewFunctionTool(spec Spec, handler Handler) *FunctionTool {
	if spec.Parameters == nil {
		spec.Parameters = EmptyParameters()
	}
	return &FunctionTool{spec: spec, handler: handler}
}

// Spec returns the tool specification.
func (t *FunctionTool) Spec() Spec {
	return t.spec
}

func (t *FunctionTool) Invoke(ctx context.Context, input CallInput, _ ...runnable.Option) (Result, error) {
	if t.handler == nil {
		return Result{}, errors.New("tool handler is nil")
	}
	return t.handler(ctx, input)
}

func (t *FunctionTool) Stream(ctx context.Context, input CallInput, options ...runnable.Option) (*runnable.StreamReader[Result], error) {
	return runnable.StreamFromInvoke(ctx, input, t.Invoke, options...)
}
func (t *FunctionTool) Collect(ctx context.Context, input *runnable.StreamReader[CallInput], options ...runnable.Option) (Result, error) {
	return runnable.CollectFromInvoke(ctx, input, t.Invoke, options...)
}
func (t *FunctionTool) Transform(ctx context.Context, input *runnable.StreamReader[CallInput], options ...runnable.Option) (*runnable.StreamReader[Result], error) {
	return runnable.TransformFromInvoke(ctx, input, t.Invoke, options...)
}

var _ Tool = (*FunctionTool)(nil)

// ParseArgsStrict decodes finalized tool arguments as a JSON object.
func ParseArgsStrict(raw string) (map[string]any, error) {
	if strings.TrimSpace(raw) == "" {
		return map[string]any{}, nil
	}
	var args map[string]any
	if err := json.Unmarshal([]byte(raw), &args); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedArgs, err)
	}
	if args == nil {
		return nil, fmt.Errorf("%w: arguments must be a JSON object", ErrMalformedArgs)
	}
	return args, nil
}
