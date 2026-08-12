package memory

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"goharness/pkg/retrieval"
	"goharness/pkg/runnable"
)

const DefaultMaxReferenceTokens = 4096

type FileFilterBuilder func([]string) retrieval.Metadata

type HybridRetrieverConfig struct {
	Searcher      runnable.Runnable[retrieval.HybridQuery, []retrieval.SearchHit]
	Index         string
	Namespace     string
	TopK          int
	TokenCounter  retrieval.TokenCounter
	FilterBuilder FileFilterBuilder
}

// HybridRetriever adapts retrieval.HybridSearcher to Memory's retrieval
// contract and produces citation-bearing context within the reference budget.
type HybridRetriever struct {
	searcher      runnable.Runnable[retrieval.HybridQuery, []retrieval.SearchHit]
	index         string
	namespace     string
	topK          int
	counter       retrieval.TokenCounter
	filterBuilder FileFilterBuilder
}

func NewHybridRetriever(cfg HybridRetrieverConfig) (*HybridRetriever, error) {
	if cfg.Searcher == nil {
		return nil, errors.New("hybrid retriever requires a searcher")
	}
	if cfg.TopK == 0 {
		cfg.TopK = 10
	}
	if cfg.TopK < 1 {
		return nil, errors.New("hybrid retriever TopK must be greater than zero")
	}
	if cfg.TokenCounter == nil {
		cfg.TokenCounter = retrieval.ApproxTokenCounter{}
	}
	if cfg.FilterBuilder == nil {
		cfg.FilterBuilder = defaultFileFilter
	}
	return &HybridRetriever{
		searcher: cfg.Searcher, index: cfg.Index, namespace: cfg.Namespace,
		topK: cfg.TopK, counter: cfg.TokenCounter, filterBuilder: cfg.FilterBuilder,
	}, nil
}

func (r *HybridRetriever) Invoke(ctx context.Context, request RetrieveRequest, options ...runnable.Option) (string, error) {
	if ctx == nil {
		return "", errors.New("hybrid retrieve context cannot be nil")
	}
	searchQuery := retrieval.HybridQuery{
		Text: request.Query, Index: r.index, Namespace: r.namespace,
		Filter: r.filterBuilder(append([]string(nil), request.Files...)), TopK: r.topK,
	}
	hits, err := r.searcher.Invoke(ctx, searchQuery, options...)
	if err != nil {
		return "", err
	}
	budget := request.Config.MaxRefToken
	if budget <= 0 {
		budget = DefaultMaxReferenceTokens
	}
	var builder strings.Builder
	used := 0
	for index, hit := range hits {
		header := fmt.Sprintf("[Reference %d id=%s source=%s score=%.6f]\n", index+1, hit.ID, hit.Source, hit.Score)
		headerTokens := r.counter.Count(header)
		if used+headerTokens >= budget {
			break
		}
		remaining := budget - used - headerTokens
		text := truncateToTokens(hit.Text, remaining, r.counter)
		if strings.TrimSpace(text) == "" {
			continue
		}
		if builder.Len() > 0 {
			builder.WriteString("\n\n")
		}
		builder.WriteString(header)
		builder.WriteString(text)
		used += headerTokens + r.counter.Count(text)
		if used >= budget {
			break
		}
	}
	return builder.String(), nil
}

var _ runnable.Runnable[RetrieveRequest, string] = (*HybridRetriever)(nil)

func defaultFileFilter(files []string) retrieval.Metadata {
	if len(files) == 0 {
		return nil
	}
	values := make([]any, len(files))
	for index, file := range files {
		values[index] = file
	}
	return retrieval.Metadata{"file": map[string]any{"$in": values}}
}

func truncateToTokens(text string, limit int, counter retrieval.TokenCounter) string {
	if limit <= 0 {
		return ""
	}
	if counter.Count(text) <= limit {
		return text
	}
	runes := []rune(text)
	low, high, best := 1, len(runes), 0
	for low <= high {
		middle := low + (high-low)/2
		if counter.Count(string(runes[:middle])) <= limit {
			best = middle
			low = middle + 1
		} else {
			high = middle - 1
		}
	}
	return strings.TrimSpace(string(runes[:best]))
}
