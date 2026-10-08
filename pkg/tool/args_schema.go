package tool

import (
	"encoding/json"
	"fmt"

	"github.com/google/jsonschema-go/jsonschema"
)

// compileParamsSchema 把 Spec.Parameters（JSON Schema 的 map 形式）编译为
// 可重复校验的 Resolved。注册期编译一次，运行期校验只做值匹配。
// nil 参数表回落到与 Definition 一致的 EmptyParameters。
func compileParamsSchema(spec Spec) (*jsonschema.Resolved, error) {
	params := spec.Parameters
	if params == nil {
		params = EmptyParameters()
	}
	raw, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("tool %q parameters are not JSON-marshalable: %w", spec.Name, err)
	}
	var parsed jsonschema.Schema
	if err := parsed.UnmarshalJSON(raw); err != nil {
		return nil, fmt.Errorf("tool %q parameters are not a valid JSON schema: %w", spec.Name, err)
	}
	// 依赖只支持 draft-07 与 2020-12：版本不受支持的 schema 即使能解析，
	// 运行期每次 Validate 也必然失败，模型改参数无法自愈——在注册期拒绝。
	if err := checkSchemaVersion(spec.Name, parsed.Schema); err != nil {
		return nil, err
	}
	resolved, err := parsed.Resolve(nil)
	if err != nil {
		return nil, fmt.Errorf("tool %q parameters cannot be resolved: %w", spec.Name, err)
	}
	return resolved, nil
}

// 当前校验实现支持的 $schema 版本；空字符串表示未声明，沿用默认行为。
var supportedSchemaVersions = map[string]bool{
	"": true,
	"http://json-schema.org/draft-07/schema#":      true,
	"https://json-schema.org/draft-07/schema#":     true,
	"https://json-schema.org/draft/2020-12/schema": true,
}

func checkSchemaVersion(toolName, version string) error {
	if !supportedSchemaVersions[version] {
		return fmt.Errorf("tool %q parameters declare unsupported $schema version %q (supported: draft-07, draft 2020-12)", toolName, version)
	}
	return nil
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
