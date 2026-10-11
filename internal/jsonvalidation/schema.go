// Package jsonvalidation 提供内部共享的 Schema 推导、冻结编译与原始 JSON
// 解析能力，供数据模型、LLM 和工具使用。它是实现细节，不面向普通用户。
package jsonvalidation

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/google/jsonschema-go/jsonschema"
)

// Compiled 是冻结后的 Schema 产物：一份序列化字节与编译好的校验器。
// 发送方和本地校验复用同一字节与同一校验器，保证两侧规则一致。
type Compiled struct {
	raw      json.RawMessage
	resolved *jsonschema.Resolved
}

// 当前校验实现支持的 $schema 版本；空字符串表示未声明，沿用默认行为。
var supportedSchemaVersions = map[string]bool{
	"": true,
	"http://json-schema.org/draft-07/schema#":      true,
	"https://json-schema.org/draft-07/schema#":     true,
	"https://json-schema.org/draft/2020-12/schema": true,
}

// Infer 只调用依赖库的类型推导，不施加调用方的额外政策。
func Infer(t reflect.Type) (*jsonschema.Schema, error) {
	if t == nil {
		return nil, fmt.Errorf("cannot infer schema for nil type")
	}
	schema, err := jsonschema.ForType(t, nil)
	if err != nil {
		return nil, fmt.Errorf("infer schema for %s: %w", t, err)
	}
	return schema, nil
}

// Compile 先序列化再经 CompileJSON 重新解析为独立 Schema：调用方之后修改
// 原对象或其属性指针，不影响已编译的结果。
func Compile(s *jsonschema.Schema) (*Compiled, error) {
	if s == nil {
		return nil, fmt.Errorf("cannot compile nil schema")
	}
	raw, err := s.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("marshal schema: %w", err)
	}
	return CompileJSON(raw)
}

// CompileJSON 解析原始 Schema 字节，检查版本支持并编译校验器。版本不受
// 支持的 schema 即使能解析，运行期每次校验也必然失败——在构造期拒绝。
func CompileJSON(raw json.RawMessage) (*Compiled, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("schema is empty")
	}
	var parsed jsonschema.Schema
	if err := parsed.UnmarshalJSON(raw); err != nil {
		return nil, fmt.Errorf("invalid JSON schema: %w", err)
	}
	if !supportedSchemaVersions[parsed.Schema] {
		return nil, fmt.Errorf("unsupported $schema version %q (supported: draft-07, draft 2020-12)", parsed.Schema)
	}
	resolved, err := parsed.Resolve(nil)
	if err != nil {
		return nil, fmt.Errorf("resolve schema: %w", err)
	}
	return &Compiled{raw: append(json.RawMessage(nil), raw...), resolved: resolved}, nil
}

// JSONSchema 返回冻结字节的独立副本。
func (c *Compiled) JSONSchema() json.RawMessage {
	if c == nil {
		return nil
	}
	return append(json.RawMessage(nil), c.raw...)
}

// Validate 按编译好的 Schema 校验解析后的 JSON 值。
func (c *Compiled) Validate(instance any) error {
	if c == nil || c.resolved == nil {
		return fmt.Errorf("schema validator is nil")
	}
	return c.resolved.Validate(instance)
}
