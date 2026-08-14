package contextengine

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"goharness/pkg/schema"
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

func countMessage(message schema.Message, counter TokenCounter) int {
	total := 4 + counter.Count(string(message.Role))
	var count func([]schema.ContentBlock) int
	count = func(blocks []schema.ContentBlock) int {
		n := 0
		for _, b := range blocks {
			switch b.Type {
			case schema.ContentBlockTypeText:
				if b.Text != nil {
					n += counter.Count(b.Text.Text)
				}
			case schema.ContentBlockTypeReasoning:
				if b.Reasoning != nil {
					n += counter.Count(b.Reasoning.Text) + counter.Count(b.Reasoning.Signature)
				}
			case schema.ContentBlockTypeImage:
				n += countMedia(b.Image, counter)
			case schema.ContentBlockTypeFile:
				n += countMedia(b.File, counter)
			case schema.ContentBlockTypeAudio:
				n += countMedia(b.Audio, counter)
			case schema.ContentBlockTypeVideo:
				n += countMedia(b.Video, counter)
			case schema.ContentBlockTypeFunctionToolCall:
				if b.FunctionToolCall != nil {
					n += counter.Count(b.FunctionToolCall.ID) + counter.Count(b.FunctionToolCall.Name) + counter.Count(b.FunctionToolCall.Arguments)
				}
			case schema.ContentBlockTypeFunctionToolResult:
				if b.FunctionToolResult != nil {
					n += counter.Count(b.FunctionToolResult.CallID) + counter.Count(b.FunctionToolResult.Name) + count(b.FunctionToolResult.ContentBlocks)
				}
			case schema.ContentBlockTypeOpaque:
				if b.Opaque != nil {
					n += counter.Count(b.Opaque.Kind) + counter.Count(string(b.Opaque.Raw))
				}
			}
		}
		return n
	}
	total += count(message.ContentBlocks)
	return total
}

func countMedia(media *schema.MediaContent, counter TokenCounter) int {
	if media == nil {
		return 0
	}
	return counter.Count(media.URI) + counter.Count(media.MIMEType) + counter.Count(string(media.Data))
}

func countMessages(messages []schema.Message, counter TokenCounter) int {
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
