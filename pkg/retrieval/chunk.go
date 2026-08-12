package retrieval

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"unicode"

	"goharness/pkg/runnable"
)

const DefaultChunkMaxTokens = 512

// Document is the normalized input to the indexing pipeline.
type Document struct {
	ID       string
	Text     string
	Metadata Metadata
}

// Chunk preserves source offsets in runes so citations can be mapped back to
// the original document without breaking UTF-8 text.
type Chunk struct {
	ID         string
	DocumentID string
	Index      int
	Text       string
	Start      int
	End        int
	Tokens     int
	Metadata   Metadata
}

type ChunkConfig struct {
	MaxTokens      int
	OverlapTokens  int
	MinTokens      int
	DisableOverlap bool
}

// TokenCounter lets applications inject the exact tokenizer used by their
// embedding model. ApproxTokenCounter is used when it is nil.
type TokenCounter interface {
	Count(string) int
}

// ApproxTokenCounter treats CJK characters and punctuation as individual
// tokens and groups other letters/digits into words. It is deterministic but
// should be replaced when exact model token limits matter.
type ApproxTokenCounter struct{}

func (ApproxTokenCounter) Count(text string) int {
	count := 0
	inWord := false
	flushWord := func() {
		if inWord {
			count++
			inWord = false
		}
	}
	for _, current := range text {
		switch {
		case unicode.IsSpace(current):
			flushWord()
		case isCJK(current):
			flushWord()
			count++
		case unicode.IsLetter(current) || unicode.IsDigit(current) || current == '_' || current == '-':
			inWord = true
		default:
			flushWord()
			count++
		}
	}
	flushWord()
	return count
}

// TextChunker uses a sliding token window and prefers paragraph, sentence,
// line and word boundaries in that order.
type TextChunker struct {
	config  ChunkConfig
	counter TokenCounter
}

func NewTextChunker(cfg ChunkConfig, counter TokenCounter) (*TextChunker, error) {
	if cfg.MaxTokens == 0 {
		cfg.MaxTokens = DefaultChunkMaxTokens
	}
	if cfg.MaxTokens < 1 {
		return nil, errors.New("chunk max tokens must be greater than zero")
	}
	if cfg.DisableOverlap {
		cfg.OverlapTokens = 0
	} else if cfg.OverlapTokens == 0 {
		cfg.OverlapTokens = max(1, cfg.MaxTokens/8)
	}
	if cfg.OverlapTokens < 0 || cfg.OverlapTokens >= cfg.MaxTokens {
		return nil, errors.New("chunk overlap tokens must be non-negative and smaller than max tokens")
	}
	if cfg.MinTokens == 0 {
		cfg.MinTokens = max(1, cfg.MaxTokens/8)
	}
	if cfg.MinTokens < 0 || cfg.MinTokens > cfg.MaxTokens {
		return nil, errors.New("chunk min tokens must be between zero and max tokens")
	}
	if counter == nil {
		counter = ApproxTokenCounter{}
	}
	return &TextChunker{config: cfg, counter: counter}, nil
}

func (c *TextChunker) Invoke(ctx context.Context, document Document, _ ...runnable.Option) ([]Chunk, error) {
	if ctx == nil {
		return nil, errors.New("chunk context cannot be nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	runes := []rune(document.Text)
	if len(runes) == 0 || strings.TrimSpace(document.Text) == "" {
		return nil, nil
	}
	documentID := strings.TrimSpace(document.ID)
	if documentID == "" {
		sum := sha256.Sum256([]byte(document.Text))
		documentID = "document-" + hex.EncodeToString(sum[:8])
	}

	chunks := make([]Chunk, 0)
	occurrences := map[string]int{}
	for start := 0; start < len(runes); {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		end := c.maxFittingEnd(runes, start)
		if end < len(runes) {
			end = c.preferredBoundary(runes, start, end)
		}
		if end <= start {
			end = min(len(runes), start+1)
		}
		contentStart, contentEnd := trimRuneRange(runes, start, end)
		if contentEnd > contentStart {
			text := string(runes[contentStart:contentEnd])
			hash := sha256.Sum256([]byte(text))
			fingerprint := hex.EncodeToString(hash[:12])
			occurrence := occurrences[fingerprint]
			occurrences[fingerprint]++
			chunks = append(chunks, Chunk{
				ID:         fmt.Sprintf("%s:%s:%d", documentID, fingerprint, occurrence),
				DocumentID: documentID,
				Index:      len(chunks),
				Text:       text,
				Start:      contentStart,
				End:        contentEnd,
				Tokens:     c.counter.Count(text),
				Metadata:   cloneMetadata(document.Metadata),
			})
		}
		if end >= len(runes) {
			break
		}
		next := end
		if c.config.OverlapTokens > 0 {
			next = c.overlapStart(runes, start, end)
		}
		if next <= start {
			next = start + 1
		}
		start = next
	}
	return chunks, nil
}

func (c *TextChunker) maxFittingEnd(runes []rune, start int) int {
	low, high := start+1, len(runes)
	best := start
	for low <= high {
		middle := low + (high-low)/2
		if c.counter.Count(string(runes[start:middle])) <= c.config.MaxTokens {
			best = middle
			low = middle + 1
		} else {
			high = middle - 1
		}
	}
	return best
}

func (c *TextChunker) preferredBoundary(runes []rune, start, end int) int {
	minimum := start + 1
	for candidate := start + 1; candidate <= end; candidate++ {
		if c.counter.Count(string(runes[start:candidate])) >= c.config.MinTokens {
			minimum = candidate
			break
		}
	}
	for _, boundary := range []func([]rune, int) bool{
		isParagraphBoundary,
		isSentenceBoundary,
		isLineBoundary,
		isWordBoundary,
	} {
		for candidate := end; candidate >= minimum; candidate-- {
			if boundary(runes, candidate) {
				return candidate
			}
		}
	}
	return end
}

func (c *TextChunker) overlapStart(runes []rune, start, end int) int {
	if c.counter.Count(string(runes[start:end])) <= c.config.OverlapTokens {
		return end
	}
	low, high := start+1, end
	best := end
	for low <= high {
		middle := low + (high-low)/2
		if c.counter.Count(string(runes[middle:end])) <= c.config.OverlapTokens {
			best = middle
			high = middle - 1
		} else {
			low = middle + 1
		}
	}
	return best
}

func trimRuneRange(runes []rune, start, end int) (int, int) {
	for start < end && unicode.IsSpace(runes[start]) {
		start++
	}
	for end > start && unicode.IsSpace(runes[end-1]) {
		end--
	}
	return start, end
}

func isParagraphBoundary(runes []rune, index int) bool {
	return index >= 2 && runes[index-1] == '\n' && runes[index-2] == '\n'
}

func isSentenceBoundary(runes []rune, index int) bool {
	if index == 0 {
		return false
	}
	switch runes[index-1] {
	case '.', '!', '?', ';', '。', '！', '？', '；':
		return true
	default:
		return false
	}
}

func isLineBoundary(runes []rune, index int) bool {
	return index > 0 && runes[index-1] == '\n'
}

func isWordBoundary(runes []rune, index int) bool {
	return index > 0 && unicode.IsSpace(runes[index-1])
}

func isCJK(current rune) bool {
	return unicode.In(current, unicode.Han, unicode.Hiragana, unicode.Katakana, unicode.Hangul)
}
