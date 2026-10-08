package schema

import "fmt"

type ResponseMeta struct {
	ID           string         `json:"id,omitempty"`
	Model        string         `json:"model,omitempty"`
	FinishReason string         `json:"finish_reason,omitempty"`
	Usage        *TokenUsage    `json:"usage,omitempty"`
	Extra        map[string]any `json:"extra,omitempty"`
}

type TokenUsage struct {
	InputTokens      int            `json:"input_tokens,omitempty"`
	OutputTokens     int            `json:"output_tokens,omitempty"`
	TotalTokens      int            `json:"total_tokens,omitempty"`
	ReasoningTokens  int            `json:"reasoning_tokens,omitempty"`
	CacheReadTokens  int            `json:"cache_read_tokens,omitempty"`
	CacheWriteTokens int            `json:"cache_write_tokens,omitempty"`
	Extra            map[string]any `json:"extra,omitempty"`
}

func (m *ResponseMeta) validate() error {
	if m == nil || m.Usage == nil {
		return nil
	}
	usage := m.Usage
	counts := []struct {
		name  string
		value int
	}{
		{"input_tokens", usage.InputTokens},
		{"output_tokens", usage.OutputTokens},
		{"total_tokens", usage.TotalTokens},
		{"reasoning_tokens", usage.ReasoningTokens},
		{"cache_read_tokens", usage.CacheReadTokens},
		{"cache_write_tokens", usage.CacheWriteTokens},
	}
	for _, count := range counts {
		if count.value < 0 {
			return fmt.Errorf("response_meta.usage.%s must not be negative", count.name)
		}
	}
	return nil
}

func cloneResponseMeta(meta *ResponseMeta, strict bool, path string, fallback CloneFallbackFunc) (*ResponseMeta, error) {
	if meta == nil {
		return nil, nil
	}
	cloned := *meta
	extra, err := cloneExtra(meta.Extra, strict, path+".extra", fallback)
	if err != nil {
		return nil, err
	}
	cloned.Extra = extra
	if meta.Usage != nil {
		usage := *meta.Usage
		usage.Extra, err = cloneExtra(meta.Usage.Extra, strict, path+".usage.extra", fallback)
		if err != nil {
			return nil, err
		}
		cloned.Usage = &usage
	}
	return &cloned, nil
}
