package tool

import (
	"encoding/json"
	"fmt"

	"github.com/goharness/goharness/internal/jsonvalidation"
)

// compileParamsSchema 把 Spec.Parameters（JSON Schema 的 map 形式）编译为
// 可重复校验的产物：注册期编译一次，运行期校验只做值匹配。$schema 版本
// 不受支持的声明即使能解析，运行期每次 Validate 也必然失败，模型改参数
// 无法自愈——随公共编译一起在注册期拒绝。nil 参数表回落到与 Definition
// 一致的 EmptyParameters。
func compileParamsSchema(spec Spec) (*jsonvalidation.Compiled, error) {
	params := spec.Parameters
	if params == nil {
		params = EmptyParameters()
	}
	raw, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("tool %q parameters are not JSON-marshalable: %w", spec.Name, err)
	}
	compiled, err := jsonvalidation.CompileJSON(raw)
	if err != nil {
		return nil, fmt.Errorf("tool %q parameters: %w", spec.Name, err)
	}
	return compiled, nil
}

// validateArgs 按注册期编译的 schema 校验调用参数。Args 已由
// ParseArgsStrict 保证是 JSON 对象。
func (b *Binding) validateArgs(args map[string]any) error {
	if b == nil || b.validator == nil {
		return nil
	}
	if err := b.validator.Validate(args); err != nil {
		return &InvalidArgsError{ToolName: b.spec.Name, Err: err}
	}
	return nil
}
