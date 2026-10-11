package tool

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/goharness/goharness/internal/jsonvalidation"
	"github.com/goharness/goharness/pkg/schema"
)

// Option 调整 New 生成的工具 Spec。
type Option func(*Spec)

// ReadOnly 声明工具只读，调度器可据此放开只读工具间的并发。
func ReadOnly() Option {
	return func(s *Spec) { s.IsReadOnly = true }
}

// NeedsMessages 声明工具需要读取会话历史；见 Spec.NeedsMessages。
func NeedsMessages() Option {
	return func(s *Spec) { s.NeedsMessages = true }
}

// WithFileAccess 声明工具会读写文件。
func WithFileAccess() Option {
	return func(s *Spec) { s.FileAccess = true }
}

// WithConcurrency 声明工具的并发分组与上限。
func WithConcurrency(policy ConcurrencyPolicy) Option {
	return func(s *Spec) { s.Concurrency = policy }
}

// New 用类型化参数构造工具：参数 JSON Schema 由 A 推导（json 标签定字段名、
// omitempty 定可选、jsonschema 标签定描述），调用参数经严格 JSON 解码进 A，
// 未知字段按畸形参数处理。返回值 R 映射为 Result：
// Result 原样返回，string 转文本，[]schema.ContentBlock 转块，其余转 Data。
// NewFunctionTool 仍是自定义 Spec 与底层 Handler 的入口。
func New[A any, R any](name, description string, fn func(context.Context, A) (R, error), options ...Option) *FunctionTool {
	spec := Spec{Name: name, Description: description}
	for _, option := range options {
		if option != nil {
			option(&spec)
		}
	}
	var schemaErr error
	if generated, err := jsonvalidation.Infer(reflect.TypeFor[A]()); err != nil {
		schemaErr = fmt.Errorf("tool %q args schema: %w", name, err)
	} else {
		data, err := generated.MarshalJSON()
		if err != nil {
			schemaErr = fmt.Errorf("tool %q args schema: %w", name, err)
		} else if err := json.Unmarshal(data, &spec.Parameters); err != nil {
			schemaErr = fmt.Errorf("tool %q args schema: %w", name, err)
		}
	}
	handler := func(ctx context.Context, input CallInput) (Result, error) {
		if schemaErr != nil {
			return Result{}, schemaErr
		}
		var args A
		raw := strings.TrimSpace(input.RawArgs)
		if raw == "" && len(input.Args) > 0 {
			data, err := json.Marshal(input.Args)
			if err != nil {
				return Result{}, fmt.Errorf("%w: serialize args: %v", ErrMalformedArgs, err)
			}
			raw = string(data)
		}
		if raw != "" && raw != "null" {
			decoded, err := jsonvalidation.DecodeTyped[A]([]byte(raw), false)
			if err != nil {
				return Result{}, fmt.Errorf("%w: %v", ErrMalformedArgs, err)
			}
			args = decoded
		}
		output, err := fn(ctx, args)
		if err != nil {
			return Result{}, err
		}
		return toolResult(output), nil
	}
	return &FunctionTool{spec: spec, handler: handler}
}

// toolResult 把类型化返回值统一为 Result。
func toolResult[R any](value R) Result {
	switch v := any(value).(type) {
	case Result:
		return v
	case string:
		return TextResult(v)
	case []schema.ContentBlock:
		return BlockResult(v...)
	default:
		return DataResult(value)
	}
}
