package llm

import (
	"encoding/json"
	"errors"

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
