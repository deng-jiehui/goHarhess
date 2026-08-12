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
	// Provider message framing is normally three to five tokens.
	total := 4 + counter.Count(string(message.Role)) + counter.Count(message.Name)
	total += counter.Count(message.Content.String())
	total += counter.Count(message.ReasoningContent.String())
	if message.FunctionCall != nil {
		total += counter.Count(message.FunctionCall.Name)
		total += counter.Count(message.FunctionCall.Arguments)
	}
	return total
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

func truncateContent(content schema.Content, limit int, counter TokenCounter, keepEnd bool) schema.Content {
	if !content.IsMultimodal() {
		return schema.TextContent(truncateText(content.String(), limit, counter, keepEnd))
	}
	items := content.Items()
	nonTextTokens := 0
	for _, item := range items {
		if item.Text == nil {
			nonTextTokens += counter.Count(item.ValueString())
		}
	}
	textBudget := limit - nonTextTokens
	if textBudget < 0 {
		textBudget = 0
	}
	remaining := textBudget
	if keepEnd {
		for index := len(items) - 1; index >= 0; index-- {
			if items[index].Text == nil {
				continue
			}
			trimmed := truncateText(*items[index].Text, remaining, counter, true)
			items[index] = schema.TextItem(trimmed)
			remaining -= counter.Count(trimmed)
		}
	} else {
		for index := range items {
			if items[index].Text == nil {
				continue
			}
			trimmed := truncateText(*items[index].Text, remaining, counter, false)
			items[index] = schema.TextItem(trimmed)
			remaining -= counter.Count(trimmed)
		}
	}
	return schema.MultiContent(items...)
}
