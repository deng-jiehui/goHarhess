package jsonvalidation

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// ParseJSON 把原始字节解析为唯一 JSON 值：只允许前后空白，尾部出现第二个
// JSON 值或额外文本都拒绝。返回值中的 json.Number 已规范化为 int64 或
// float64，使 Schema 校验器能识别数值种类。
func ParseJSON(raw []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var instance any
	if err := decoder.Decode(&instance); err != nil {
		return nil, fmt.Errorf("malformed JSON: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("trailing JSON value after the first")
		}
		return nil, fmt.Errorf("trailing data after JSON value: %w", err)
	}
	return normalizeJSONNumbers(instance), nil
}

// DecodeTyped 从原始字节严格解码进 T：DisallowUnknownFields 且尾部必须
// 结束。useNumber 为 true 时数字按 json.Number 保留精度（数据模型），
// false 时按既有 float64 行为（工具参数）。
func DecodeTyped[T any](raw []byte, useNumber bool) (T, error) {
	var value T
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if useNumber {
		decoder.UseNumber()
	}
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return value, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return value, fmt.Errorf("trailing JSON value after the first")
		}
		return value, err
	}
	return value, nil
}

// normalizeJSONNumbers 把 UseNumber 解码结果中的 json.Number 转为校验器可
// 识别的数值种类：能进入 int64 的转 int64，否则转 float64。
func normalizeJSONNumbers(instance any) any {
	switch value := instance.(type) {
	case json.Number:
		if n, err := value.Int64(); err == nil {
			return n
		}
		if n, err := value.Float64(); err == nil {
			return n
		}
		return value.String()
	case []any:
		for index, item := range value {
			value[index] = normalizeJSONNumbers(item)
		}
	case map[string]any:
		for key, item := range value {
			value[key] = normalizeJSONNumbers(item)
		}
	}
	return instance
}
