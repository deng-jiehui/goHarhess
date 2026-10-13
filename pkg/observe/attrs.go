package observe

import (
	"context"
	"strings"
)

// redactedValue 是敏感键命中后的占位值。
const redactedValue = "[redacted]"

// 默认敏感词表：属性键按分隔符切词后整词命中即脱敏。整词匹配保证
// input_tokens、max_tokens、tool.read_only 不被误伤。
var defaultSensitiveWords = []string{
	"authorization", "password", "passwd", "secret",
	"token", "cookie", "credential", "credentials",
}

type sensitiveKeysContextKey struct{}

// WithSensitiveKeys 追加应用自定义的敏感键：键经与属性键相同的归一化
// （小写、'.' 与 '-' 视为 '_'）后整词精确匹配，随 ctx 传递、子调用继承。
func WithSensitiveKeys(ctx context.Context, keys ...string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(keys) == 0 {
		return ctx
	}
	existing, _ := ctx.Value(sensitiveKeysContextKey{}).(map[string]struct{})
	merged := make(map[string]struct{}, len(existing)+len(keys))
	for k := range existing {
		merged[k] = struct{}{}
	}
	for _, k := range keys {
		if normalized := normalizeKey(k); normalized != "" {
			merged[normalized] = struct{}{}
		}
	}
	return context.WithValue(ctx, sensitiveKeysContextKey{}, merged)
}

// sanitizeEventAttrs 在事件分发边界对 Event.Attrs 脱敏：命中敏感键的值
// 替换为 "[redacted]"。写时复制——无命中时原样返回、零额外分配；命中时
// 复制整表再替换，调用方与 ctx 中的元数据映射不受影响。
func sanitizeEventAttrs(ctx context.Context, attrs map[string]any) map[string]any {
	if len(attrs) == 0 {
		return attrs
	}
	extra, _ := ctx.Value(sensitiveKeysContextKey{}).(map[string]struct{})
	var out map[string]any
	for key := range attrs {
		if !isSensitiveKey(key, extra) {
			continue
		}
		if out == nil {
			out = make(map[string]any, len(attrs))
			for k, v := range attrs {
				out[k] = v
			}
		}
		out[key] = redactedValue
	}
	if out != nil {
		return out
	}
	return attrs
}

// isSensitiveKey 判定属性键是否敏感：自定义键做归一化整词精确匹配；
// 默认词表按分隔切词后逐词匹配，并检查 api+key、private+key 相邻对。
// 判定不分配内存：切词就地扫描，比较用 EqualFold。
func isSensitiveKey(key string, extra map[string]struct{}) bool {
	for custom := range extra {
		if equalFoldNormalized(key, custom) {
			return true
		}
	}
	prev := ""
	hit := false
	eachKeyWord(key, func(word string) bool {
		for _, sensitive := range defaultSensitiveWords {
			if strings.EqualFold(word, sensitive) {
				hit = true
				return false
			}
		}
		if strings.EqualFold(word, "key") &&
			(strings.EqualFold(prev, "api") || strings.EqualFold(prev, "private")) {
			hit = true
			return false
		}
		prev = word
		return true
	})
	return hit
}

// eachKeyWord 按 '.'、'-'、'_' 切分属性键并逐词回调；回调返回 false 时
// 停止遍历。就地扫描，无分配。
func eachKeyWord(key string, yield func(string) bool) {
	start := -1
	for i := 0; i < len(key); i++ {
		switch key[i] {
		case '.', '-', '_':
			if start >= 0 {
				if !yield(key[start:i]) {
					return
				}
				start = -1
			}
		default:
			if start < 0 {
				start = i
			}
		}
	}
	if start >= 0 {
		yield(key[start:])
	}
}

// equalFoldNormalized 按归一化口径比较两个键：大小写不敏感，'.' 与 '-'
// 视同 '_'。candidate 已归一化；key 逐字符归一后比较，无分配。
func equalFoldNormalized(key, candidate string) bool {
	if len(key) != len(candidate) {
		return false
	}
	for i := 0; i < len(key); i++ {
		c := key[i]
		if c == '.' || c == '-' {
			c = '_'
		} else if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != candidate[i] {
			return false
		}
	}
	return true
}

// normalizeKey 把自定义敏感键归一为小写、'.' 与 '-' 换成 '_'，供精确
// 匹配。只在 WithSensitiveKeys 注册时调用一次，不在事件热路径上。
func normalizeKey(key string) string {
	var b strings.Builder
	b.Grow(len(key))
	for i := 0; i < len(key); i++ {
		c := key[i]
		switch {
		case c == '.' || c == '-':
			b.WriteByte('_')
		case c >= 'A' && c <= 'Z':
			b.WriteByte(c + 'a' - 'A')
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}
