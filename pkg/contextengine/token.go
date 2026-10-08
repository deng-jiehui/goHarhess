package contextengine

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/goharness/goharness/pkg/schema"
)

// TokenCounter is intentionally provider-neutral. Applications can inject a
// model tokenizer while the default estimator remains dependency-free.
type TokenCounter interface {
	Count(text string) int
}

// ApproxTokenCounter estimates Latin text at roughly four runes per token and
// CJK/emoji at one token per rune.
type ApproxTokenCounter struct{}

func (ApproxTokenCounter) Count(text string) int {
	if text == "" {
		return 0
	}
	tokens := 0
	latinRunes := 0
	flushLatin := func() {
		if latinRunes > 0 {
			tokens += (latinRunes + 3) / 4
			latinRunes = 0
		}
	}
	for _, value := range text {
		switch {
		case unicode.Is(unicode.Han, value), unicode.Is(unicode.Hiragana, value), unicode.Is(unicode.Katakana, value), unicode.Is(unicode.Hangul, value), value > utf8.RuneSelf:
			flushLatin()
			tokens++
		case unicode.IsSpace(value):
			flushLatin()
		default:
			latinRunes++
		}
	}
	flushLatin()
	return tokens
}

// MediaTokenEstimator 估算图片、文件、音频、视频等非文本块的 token 成本。
// 媒体的真实计费由 Provider 按分辨率、时长等决定，内嵌的 base64 Data 是
// 二进制载荷，不按文本计数；框架只提供保守默认实现，应用可用 Provider
// 的实际规则替换。
type MediaTokenEstimator interface {
	EstimateMedia(block schema.ContentBlock) int
}

// 默认媒体估算量级：图片与视频按高分辨率档位的常见计量（约 85 加每块
// tile 170 token 的叠加上限）取保守值；文件与音频为固定占位。需要精确
// 计费时通过 Config.MediaEstimator 注入自定义实现。
const (
	defaultImageMediaTokens = 1105
	defaultFileMediaTokens  = 256
	defaultAudioMediaTokens = 300
	defaultVideoMediaTokens = 1105
)

// ApproxMediaEstimator 按媒体类型返回固定估算值；零值字段回落到默认量级，
// 负值表示按零计。内嵌 Data 与 URI 不参与文本计数。
type ApproxMediaEstimator struct {
	Image int
	File  int
	Audio int
	Video int
}

func (e ApproxMediaEstimator) EstimateMedia(block schema.ContentBlock) int {
	resolve := func(media *schema.MediaContent, value, fallback int) int {
		if media == nil {
			return 0
		}
		if value == 0 {
			return fallback
		}
		if value < 0 {
			return 0
		}
		return value
	}
	switch block.Type {
	case schema.ContentBlockTypeImage:
		return resolve(block.Image, e.Image, defaultImageMediaTokens)
	case schema.ContentBlockTypeFile:
		return resolve(block.File, e.File, defaultFileMediaTokens)
	case schema.ContentBlockTypeAudio:
		return resolve(block.Audio, e.Audio, defaultAudioMediaTokens)
	case schema.ContentBlockTypeVideo:
		return resolve(block.Video, e.Video, defaultVideoMediaTokens)
	}
	return 0
}

// usageCounter 是消息计费的两个入口：文本经 TokenCounter 估算，媒体块经
// MediaTokenEstimator 估算，互不混用。
type usageCounter struct {
	text  TokenCounter
	media MediaTokenEstimator
}

func countMessage(message schema.Message, counter usageCounter) int {
	total := 4 + counter.text.Count(string(message.Role))
	var count func([]schema.ContentBlock) int
	count = func(blocks []schema.ContentBlock) int {
		n := 0
		for _, b := range blocks {
			switch b.Type {
			case schema.ContentBlockTypeText:
				if b.Text != nil {
					n += counter.text.Count(b.Text.Text)
				}
			case schema.ContentBlockTypeReasoning:
				if b.Reasoning != nil {
					n += counter.text.Count(b.Reasoning.Text) + counter.text.Count(b.Reasoning.Signature)
				}
			case schema.ContentBlockTypeImage,
				schema.ContentBlockTypeFile,
				schema.ContentBlockTypeAudio,
				schema.ContentBlockTypeVideo:
				n += counter.media.EstimateMedia(b)
			case schema.ContentBlockTypeFunctionToolCall:
				if b.FunctionToolCall != nil {
					n += counter.text.Count(b.FunctionToolCall.ID) + counter.text.Count(b.FunctionToolCall.Name) + counter.text.Count(b.FunctionToolCall.Arguments)
				}
			case schema.ContentBlockTypeFunctionToolResult:
				if b.FunctionToolResult != nil {
					n += counter.text.Count(b.FunctionToolResult.CallID) + counter.text.Count(b.FunctionToolResult.Name) + count(b.FunctionToolResult.ContentBlocks)
				}
			case schema.ContentBlockTypeOpaque:
				if b.Opaque != nil {
					n += counter.text.Count(b.Opaque.Kind) + counter.text.Count(string(b.Opaque.Raw))
				}
			}
		}
		return n
	}
	total += count(message.ContentBlocks)
	return total
}

func countMessages(messages []schema.Message, counter usageCounter) int {
	total := 0
	for _, message := range messages {
		total += countMessage(message, counter)
	}
	return total
}

func truncateText(text string, limit int, counter TokenCounter, keepEnd bool) string {
	if limit <= 0 {
		return ""
	}
	if counter.Count(text) <= limit {
		return text
	}
	runes := []rune(text)
	low, high, best := 0, len(runes), 0
	for low <= high {
		middle := low + (high-low)/2
		var candidate string
		if keepEnd {
			candidate = string(runes[len(runes)-middle:])
		} else {
			candidate = string(runes[:middle])
		}
		if counter.Count(candidate) <= limit {
			best = middle
			low = middle + 1
		} else {
			high = middle - 1
		}
	}
	if keepEnd {
		return strings.TrimSpace(string(runes[len(runes)-best:]))
	}
	return strings.TrimSpace(string(runes[:best]))
}
