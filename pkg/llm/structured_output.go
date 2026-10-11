package llm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"strings"

	"github.com/goharness/goharness/internal/jsonvalidation"
	"github.com/goharness/goharness/pkg/runnable"
	"github.com/goharness/goharness/pkg/schema"
)

// ErrInvalidStructuredOutput 标记结构化输出校验解码失败；错误链保留底层
// 原因，errors.Is/As 可按此分类，消息不携带完整模型输出。
var ErrInvalidStructuredOutput = errors.New("invalid structured output")

// structuredOutputConfig 是组件构造选项的私有配置。
type structuredOutputConfig struct {
	name        string
	nameSet     bool
	description string
}

// StructuredOutputOption 是 NewStructuredOutput 的构造选项，与模型单次运行
// 的 runnable.Option 无关。
type StructuredOutputOption func(*structuredOutputConfig)

// WithOutputName 指定输出格式名称：字母、数字、下划线或连字符，长度 1~64。
func WithOutputName(name string) StructuredOutputOption {
	return func(c *structuredOutputConfig) {
		c.name = name
		c.nameSet = true
	}
}

// WithOutputDescription 指定随格式发送的描述。
func WithOutputDescription(description string) StructuredOutputOption {
	return func(c *structuredOutputConfig) { c.description = description }
}

var outputNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// StructuredOutput[T] 把模型最终回答按目标类型 T 的严格 JSON Schema 校验并
// 解码为 T。构造期完成类型检查、Schema 规范化与校验器编译；构造完成后
// 组件只读，可并发复用。组件不持有模型引用，不发起模型调用。
type StructuredOutput[T any] struct {
	format ResponseFormat
	schema *jsonvalidation.Compiled
}

// NewStructuredOutput 构造校验解码组件：T 必须是结构体，推导出的严格模式
// Schema 同时用于 Format（发送给模型）与本地校验。
func NewStructuredOutput[T any](options ...StructuredOutputOption) (*StructuredOutput[T], error) {
	var cfg structuredOutputConfig
	for _, option := range options {
		if option != nil {
			option(&cfg)
		}
	}
	name := defaultOutputName(reflect.TypeFor[T]())
	if cfg.nameSet {
		name = strings.TrimSpace(cfg.name)
		if !outputNamePattern.MatchString(name) {
			return nil, fmt.Errorf("structured output name %q must match %s", cfg.name, outputNamePattern)
		}
	}
	compiled, err := compileStructuredSchema(reflect.TypeFor[T]())
	if err != nil {
		return nil, err
	}
	return &StructuredOutput[T]{
		format: ResponseFormat{
			Name:        name,
			Description: cfg.description,
			Schema:      compiled.JSONSchema(),
			Strict:      true,
		},
		schema: compiled,
	}, nil
}

// Format 返回发送给模型的类型化输出格式副本；默认 Strict=true。修改返回值
// 不影响组件内部状态。
func (o *StructuredOutput[T]) Format() *ResponseFormat {
	if o == nil {
		return nil
	}
	format := o.format.Clone()
	return &format
}

// Decode 按固定流程解码：最后一条 assistant 消息 → 检查完整性 → 提取文本 →
// 解析唯一 JSON 值 → Schema 校验 → 严格解码进 T。失败返回 T 零值。
func (o *StructuredOutput[T]) Decode(messages []schema.Message) (T, error) {
	var zero T
	if o == nil {
		return zero, errors.New("structured output component is nil")
	}
	message, err := finalAssistantMessage(messages)
	if err != nil {
		return zero, err
	}
	text := message.JoinedText("")
	if text == "" {
		return zero, fmt.Errorf("%w: last assistant message has no text", ErrInvalidStructuredOutput)
	}
	raw := []byte(text)
	instance, err := jsonvalidation.ParseJSON(raw)
	if err != nil {
		return zero, fmt.Errorf("%w: %w", ErrInvalidStructuredOutput, err)
	}
	if err := o.schema.Validate(instance); err != nil {
		return zero, fmt.Errorf("%w: schema validation: %w", ErrInvalidStructuredOutput, err)
	}
	value, err := jsonvalidation.DecodeTyped[T](raw, true)
	if err != nil {
		return zero, fmt.Errorf("%w: decode into %T: %w", ErrInvalidStructuredOutput, value, err)
	}
	return value, nil
}

// finalAssistantMessage 选择最后一条 assistant 消息并校验其为完整最终回答：
// 拒绝回答、被截断或仍带工具调用的消息直接报错，不回退到更早的回答。
func finalAssistantMessage(messages []schema.Message) (schema.Message, error) {
	for index := len(messages) - 1; index >= 0; index-- {
		message := messages[index]
		if message.Role != schema.RoleAssistant {
			continue
		}
		if refusal, _ := message.Extra["llm.refusal"].(string); refusal != "" {
			return message, fmt.Errorf("%w: assistant refused: %s", ErrInvalidStructuredOutput, refusal)
		}
		if meta := message.ResponseMeta; meta != nil {
			switch meta.FinishReason {
			case "length", "content_filter":
				return message, fmt.Errorf("%w: assistant response incomplete: finish_reason=%s",
					ErrInvalidStructuredOutput, meta.FinishReason)
			}
		}
		if len(message.FunctionToolCalls()) > 0 {
			return message, fmt.Errorf("%w: last assistant message still contains tool calls",
				ErrInvalidStructuredOutput)
		}
		return message, nil
	}
	return schema.Message{}, fmt.Errorf("%w: no assistant message", ErrInvalidStructuredOutput)
}

// OptionTypes 声明组件不接受任何模型配置类型：图广播按声明类型匹配，
// 模型 Option 不会进入解码节点。
func (o *StructuredOutput[T]) OptionTypes() []reflect.Type {
	return nil
}

// checkOptions 校验组件调用入口的选项：非空配置或定向选项都返回错误。
func checkStructuredOptions(options []runnable.Option) error {
	specs, err := runnable.FlattenOptions(options)
	if err != nil {
		return err
	}
	if len(specs) > 0 {
		return fmt.Errorf("structured output accepts no run options, got %d", len(specs))
	}
	return nil
}

func (o *StructuredOutput[T]) Invoke(_ context.Context, messages []schema.Message, options ...runnable.Option) (T, error) {
	var zero T
	if err := checkStructuredOptions(options); err != nil {
		return zero, err
	}
	return o.Decode(messages)
}

// Stream 成功时交付一次 T 后结束，失败时以错误终止。
func (o *StructuredOutput[T]) Stream(ctx context.Context, messages []schema.Message, options ...runnable.Option) (*runnable.StreamReader[T], error) {
	if err := checkStructuredOptions(options); err != nil {
		return nil, err
	}
	reader, writer := runnable.Pipe[T](ctx)
	go func() {
		value, err := o.Decode(messages)
		if err != nil {
			_ = writer.CloseWithError(err)
			return
		}
		if err := writer.Send(value); err != nil {
			return
		}
		_ = writer.Close()
	}()
	return reader, nil
}

// Collect 等待输入流正常结束后取最后一份完整消息列表解码；尾部错误不吞。
func (o *StructuredOutput[T]) Collect(ctx context.Context, input *runnable.StreamReader[[]schema.Message], options ...runnable.Option) (T, error) {
	var zero T
	if err := checkStructuredOptions(options); err != nil {
		return zero, err
	}
	if input == nil {
		return zero, errors.New("structured output input stream is nil")
	}
	messages, err := drainMessages(ctx, input)
	if err != nil {
		return zero, err
	}
	return o.Decode(messages)
}

// Transform 在输入流结束后输出一次解码结果；关闭输出读者会取消内部工作并
// 关闭上游 Reader。
func (o *StructuredOutput[T]) Transform(ctx context.Context, input *runnable.StreamReader[[]schema.Message], options ...runnable.Option) (*runnable.StreamReader[T], error) {
	if err := checkStructuredOptions(options); err != nil {
		return nil, err
	}
	if input == nil {
		return nil, errors.New("structured output input stream is nil")
	}
	reader, writer := runnable.Pipe[T](ctx)
	finished := make(chan struct{})
	go func() {
		// 输出读者关闭（或其 Context 取消）时释放上游输入。
		select {
		case <-reader.Context().Done():
			_ = input.Close()
		case <-finished:
		}
	}()
	go func() {
		defer close(finished)
		defer func() { _ = input.Close() }()
		messages, err := drainMessages(ctx, input)
		if err != nil {
			_ = writer.CloseWithError(err)
			return
		}
		value, err := o.Decode(messages)
		if err != nil {
			_ = writer.CloseWithError(err)
			return
		}
		if err := writer.Send(value); err != nil {
			return
		}
		_ = writer.Close()
	}()
	return reader, nil
}

// drainMessages 读尽输入流并返回最后一份完整消息列表；输入尾部错误原样
// 返回，不交付部分成功。读取在独立 goroutine 中执行：上游 Reader 的 Context
// 由创建方持有，本次调用的 ctx 取消时通过关闭上游使阻塞的 Recv 退出，
// 函数返回前内部 goroutine 一定已经结束。
func drainMessages(ctx context.Context, input *runnable.StreamReader[[]schema.Message]) ([]schema.Message, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	type recvResult struct {
		messages []schema.Message
		err      error
	}
	results := make(chan recvResult)
	stop := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for {
			messages, err := input.Recv()
			select {
			case results <- recvResult{messages: messages, err: err}:
				if err != nil {
					return
				}
			case <-stop:
				return
			}
		}
	}()
	waitPump := func() {
		close(stop)
		<-finished
	}
	var last []schema.Message
	have := false
	for {
		select {
		case result := <-results:
			switch {
			case errors.Is(result.err, io.EOF):
				waitPump()
				if !have {
					return nil, fmt.Errorf("%w: input stream delivered no message list", ErrInvalidStructuredOutput)
				}
				return last, nil
			case result.err != nil:
				_ = input.Close()
				waitPump()
				return nil, result.err
			default:
				last, have = result.messages, true
			}
		case <-ctx.Done():
			_ = input.Close()
			waitPump()
			return nil, ctx.Err()
		}
	}
}

// defaultOutputName 从命名类型推导默认格式名（驼峰转下划线、非限定字符
// 转下划线，超过 64 截断）；匿名类型使用 structured_output。
func defaultOutputName(t reflect.Type) string {
	if t == nil || t.Name() == "" {
		return "structured_output"
	}
	var out strings.Builder
	previousLower := false
	for index, r := range t.Name() {
		switch {
		case r >= 'A' && r <= 'Z':
			if index > 0 && previousLower {
				out.WriteByte('_')
			}
			out.WriteRune(r - 'A' + 'a')
			previousLower = false
		case r >= 'a' && r <= 'z' || r >= '0' && r <= '9':
			out.WriteRune(r)
			previousLower = true
		case r == '_':
			out.WriteByte('_')
			previousLower = false
		default:
			out.WriteByte('_')
			previousLower = false
		}
	}
	name := out.String()
	name = strings.Trim(name, "_")
	if name == "" {
		return "structured_output"
	}
	if len(name) > 64 {
		name = name[:64]
	}
	return name
}
