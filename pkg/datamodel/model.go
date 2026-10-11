// Package datamodel 提供通用数据模型：把 Go 类型推导的 JSON Schema 冻结
// 编译，对外部 JSON 数据做 Schema 校验、严格解码与业务校验。它不调用
// LLM，不管理运行、会话或检查点。
package datamodel

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/goharness/goharness/internal/jsonvalidation"
	"github.com/google/jsonschema-go/jsonschema"
)

// Model 是 T 的已编译数据模型：持有一份冻结 Schema 与对应校验器，可
// 并发复用。通过 New 构造。
type Model[T any] struct {
	schema     *jsonvalidation.Compiled
	validators []func(T) error
}

// New 推导 T 的 JSON Schema、应用可选编辑并在构造期编译校验器。T 中
// 无法被 Schema 准确表达的写法（自定义编解码、json.Number、,string
// 标签、具名内嵌字段）会在构造期报错而不是运行期才暴露。
func New[T any](options ...Option[T]) (*Model[T], error) {
	var cfg config[T]
	for _, option := range options {
		if option != nil {
			option(&cfg)
		}
	}
	t := reflect.TypeFor[T]()
	if err := checkExpressible(t); err != nil {
		return nil, err
	}
	schema, err := jsonvalidation.Infer(t)
	if err != nil {
		return nil, err
	}
	for _, edit := range cfg.schemaEdits {
		if edit == nil {
			continue
		}
		if err := edit(schema); err != nil {
			return nil, fmt.Errorf("datamodel schema edit for %s: %w", t, err)
		}
	}
	compiled, err := jsonvalidation.Compile(schema)
	if err != nil {
		return nil, fmt.Errorf("datamodel schema for %s: %w", t, err)
	}
	return &Model[T]{
		schema:     compiled,
		validators: append([]func(T) error(nil), cfg.validators...),
	}, nil
}

// JSONSchema 返回冻结 Schema 字节的独立副本；调用方的修改不会写回
// Model 持有的校验器。
func (m *Model[T]) JSONSchema() json.RawMessage {
	if m == nil {
		return nil
	}
	return m.schema.JSONSchema()
}

// ValidateJSON 只校验原始字节是否满足 Schema，不解码。输入必须是恰好
// 一个 JSON 值；尾部第二个值或额外文本同样拒绝。
func (m *Model[T]) ValidateJSON(data []byte) error {
	if m == nil || m.schema == nil {
		return fmt.Errorf("%w: model is nil", ErrInvalidData)
	}
	instance, err := jsonvalidation.ParseJSON(data)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidData, err)
	}
	if err := m.schema.Validate(instance); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidData, err)
	}
	return nil
}

// DecodeJSON 校验并解码原始字节：先确认输入是单个 JSON 值并通过
// Schema，再以 unknown-fields-strict 方式解码进 T，最后执行业务校验。
// 任一环节失败返回 T 的零值与包裹 ErrInvalidData 的错误；成功时返回
// 独立的 T 值，不共享输入字节。
func (m *Model[T]) DecodeJSON(data []byte) (T, error) {
	var zero T
	if m == nil || m.schema == nil {
		return zero, fmt.Errorf("%w: model is nil", ErrInvalidData)
	}
	instance, err := jsonvalidation.ParseJSON(data)
	if err != nil {
		return zero, fmt.Errorf("%w: %w", ErrInvalidData, err)
	}
	if err := m.schema.Validate(instance); err != nil {
		return zero, fmt.Errorf("%w: %w", ErrInvalidData, err)
	}
	value, err := jsonvalidation.DecodeTyped[T](data, true)
	if err != nil {
		return zero, fmt.Errorf("%w: %w", ErrInvalidData, err)
	}
	for _, check := range m.validators {
		if err := check(value); err != nil {
			return zero, fmt.Errorf("%w: %w", ErrInvalidData, err)
		}
	}
	return value, nil
}

// WithSchema 返回派生的新 Model：在一份 Schema 副本上应用编辑后重新
// 编译，原 Model 不受影响。编辑返回错误或结果无法编译时返回错误。
func (m *Model[T]) WithSchema(edit func(*jsonschema.Schema) error) (*Model[T], error) {
	if m == nil || m.schema == nil {
		return nil, errors.New("cannot derive schema from a nil model")
	}
	if edit == nil {
		return nil, errors.New("schema edit is nil")
	}
	var parsed jsonschema.Schema
	if err := parsed.UnmarshalJSON(m.schema.JSONSchema()); err != nil {
		return nil, fmt.Errorf("reload schema: %w", err)
	}
	if err := edit(&parsed); err != nil {
		return nil, err
	}
	compiled, err := jsonvalidation.Compile(&parsed)
	if err != nil {
		return nil, fmt.Errorf("compile derived schema: %w", err)
	}
	return &Model[T]{schema: compiled, validators: m.validators}, nil
}
