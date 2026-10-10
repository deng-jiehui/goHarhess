package llm

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/goharness/goharness/pkg/runnable"
)

// ErrResponseFormatUnsupported 表示模型声明的能力不支持类型化输出格式。
var ErrResponseFormatUnsupported = errors.New("model does not support structured output format")

// ErrResponseFormatConflict 表示类型化输出格式与原始 response_format 扩展
// 参数同时配置。
var ErrResponseFormatConflict = errors.New("response format configured both as typed format and raw generate config")

// ResponseFormat 是第一版类型化输出格式：仅指 JSON Schema 输出，Provider 负责
// 映射成自己的线路字段。Schema 保存 JSON 字节，不保存校验器或 Go 类型对象。
type ResponseFormat struct {
	Name        string
	Description string
	Schema      json.RawMessage
	Strict      bool
}

// Clone 返回独立副本，Schema 字节不与原值共享。
func (f ResponseFormat) Clone() ResponseFormat {
	f.Schema = append(json.RawMessage(nil), f.Schema...)
	return f
}

// WithResponseFormat 设置本次调用的类型化输出格式；传入 nil 清除已设置的格式。
// 格式在创建与应用选项时都复制，调用方之后修改原值不影响已冻结的配置。
func WithResponseFormat(format *ResponseFormat) runnable.Option {
	var frozen *ResponseFormat
	if format != nil {
		cloned := format.Clone()
		frozen = &cloned
	}
	return runnable.WrapOption(func(o *Options) {
		if frozen == nil {
			o.ResponseFormat = nil
			return
		}
		cloned := frozen.Clone()
		o.ResponseFormat = &cloned
	})
}

// validateResponseFormat 在类型化格式进入 Provider 请求前统一核对：名称符合
// 线路要求、Schema 是 JSON 对象且根类型为 object；Strict 时递归核对每个对象
// 节点声明 additionalProperties=false 且 required 覆盖全部属性。手写的公开
// ResponseFormat 与组件生成的格式走同一条校验路径。
func validateResponseFormat(format *ResponseFormat) error {
	if format == nil {
		return errors.New("response format is nil")
	}
	if !outputNamePattern.MatchString(format.Name) {
		return fmt.Errorf("response format name %q must match %s", format.Name, outputNamePattern)
	}
	var root map[string]any
	if err := json.Unmarshal(format.Schema, &root); err != nil {
		return fmt.Errorf("response format schema is not valid JSON: %w", err)
	}
	if t, _ := root["type"].(string); t != "object" {
		return fmt.Errorf("response format schema root type must be %q, got %q", "object", t)
	}
	if format.Strict {
		if err := validateStrictSchemaNode(root, "schema"); err != nil {
			return err
		}
	}
	return nil
}

// validateStrictSchemaNode 递归核对 strict 约束：每个对象节点必须关闭额外
// 字段并把全部属性列入 required，与原生 strict 格式的服务端要求一致。
func validateStrictSchemaNode(node map[string]any, path string) error {
	properties, _ := node["properties"].(map[string]any)
	if t, _ := node["type"].(string); t == "object" || properties != nil {
		if additional, ok := node["additionalProperties"]; !ok || additional != false {
			return fmt.Errorf("strict response format %s must set additionalProperties=false", path)
		}
		required := map[string]bool{}
		if list, ok := node["required"].([]any); ok {
			for _, item := range list {
				if name, ok := item.(string); ok {
					required[name] = true
				}
			}
		}
		for name := range properties {
			if !required[name] {
				return fmt.Errorf("strict response format %s property %q is missing from required", path, name)
			}
		}
	}
	for name, child := range properties {
		if object, ok := child.(map[string]any); ok {
			if err := validateStrictSchemaNode(object, path+".properties."+name); err != nil {
				return err
			}
		}
	}
	for _, key := range []string{"items", "additionalProperties"} {
		if child, ok := node[key].(map[string]any); ok {
			if err := validateStrictSchemaNode(child, path+"."+key); err != nil {
				return err
			}
		}
	}
	for _, key := range []string{"$defs", "definitions"} {
		if defs, ok := node[key].(map[string]any); ok {
			for name, child := range defs {
				if object, ok := child.(map[string]any); ok {
					if err := validateStrictSchemaNode(object, path+"."+key+"."+name); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}
