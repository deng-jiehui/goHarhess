package retrieval

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"goharness/pkg/runnable"
)

type FusionStrategy string

const (
	FusionRRF      FusionStrategy = "rrf"
	FusionWeighted FusionStrategy = "weighted"
	maxHybridTopK                 = 10_000
)

type HybridSearchConfig struct {
	Embedder      runnable.Runnable[EmbeddingRequest, EmbeddingResponse]
	VectorStore   runnable.Runnable[VectorRequest, VectorResponse]
	KeywordSearch runnable.Runnable[TextSearchRequest, []SearchHit]
	Reranker      runnable.Runnable[RerankRequest, []RerankResult]

	EmbeddingModel string
	RerankModel    string
	VectorTopK     int
	KeywordTopK    int
	FinalTopK      int
	RerankTopK     int
	Fusion         FusionStrategy
	RRFK           float64
	VectorWeight   float64
	KeywordWeight  float64
	MMRLambda      float64
	RequireBoth    bool
	StrictRerank   bool
}

type HybridQuery struct {
	Text           string
	Index          string
	Namespace      string
	Filter         Metadata
	TopK           int
	VectorMinScore float64
	EmbeddingRaw   map[string]any
	VectorRaw      map[string]any
	KeywordRaw     map[string]any
	RerankRaw      map[string]any
}

// HybridSearcher performs vector and keyword retrieval concurrently, fuses
// ranks, optionally applies MMR diversity and sends candidates to a Reranker.
type HybridSearcher struct {
	config HybridSearchConfig
}

func (h *HybridSearcher) Invoke(ctx context.Context, query HybridQuery, options ...runnable.Option) ([]SearchHit, error) {
	if ctx == nil {
		return nil, errors.New("hybrid search context cannot be nil")
	}
	if strings.TrimSpace(query.Text) == "" {
		return nil, errors.New("hybrid search query cannot be empty")
	}
	finalTopK := h.config.FinalTopK
	if query.TopK > 0 {
		if query.TopK > maxHybridTopK {
			return nil, errors.New("hybrid query TopK exceeds framework limit")
		}
		finalTopK = query.TopK
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type branchResult struct {
		name string
		hits []SearchHit
		err  error
	}
	results := make(chan branchResult, 2)
	go func() {
		hits, err := h.config.KeywordSearch.Invoke(ctx, TextSearchRequest{
			Index: query.Index, Query: query.Text, TopK: max(h.config.KeywordTopK, finalTopK),
			Filters: cloneMetadata(query.Filter), Raw: cloneStringAnyMap(query.KeywordRaw),
		}, options...)
		results <- branchResult{name: "keyword", hits: cloneSearchHits(hits), err: err}
	}()
	go func() {
		response, err := h.config.Embedder.Invoke(ctx, EmbeddingRequest{
			Model: h.config.EmbeddingModel, Inputs: []string{query.Text}, Raw: cloneStringAnyMap(query.EmbeddingRaw),
		}, options...)
		if err == nil && len(response.Vectors) != 1 {
			err = fmt.Errorf("query embedding returned %d vectors, want 1", len(response.Vectors))
		}
		if err == nil && len(response.Vectors[0]) == 0 {
			err = errors.New("query embedding returned an empty vector")
		}
		var hits []SearchHit
		if err == nil {
			queryRequest := VectorQuery{
				Vector: append([]float32(nil), response.Vectors[0]...), TopK: max(h.config.VectorTopK, finalTopK),
				MinScore: query.VectorMinScore, Filter: cloneMetadata(query.Filter), Namespace: query.Namespace,
				Include: IncludeOptions{Vectors: h.config.MMRLambda > 0, Text: true, Metadata: true},
				Raw:     cloneStringAnyMap(query.VectorRaw),
			}
			var vectorResponse VectorResponse
			vectorResponse, err = h.config.VectorStore.Invoke(ctx, VectorRequest{Operation: VectorQueryOperation, Query: &queryRequest}, options...)
			hits = vectorResponse.Hits
		}
		results <- branchResult{name: "vector", hits: cloneSearchHits(hits), err: err}
	}()

	var vectorHits, keywordHits []SearchHit
	var branchErrors []error
	for range 2 {
		result := <-results
		if result.err != nil {
			branchErrors = append(branchErrors, fmt.Errorf("%s retrieval: %w", result.name, result.err))
			continue
		}
		if result.name == "vector" {
			vectorHits = result.hits
		} else {
			keywordHits = result.hits
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(branchErrors) == 2 || (h.config.RequireBoth && len(branchErrors) > 0) {
		return nil, errors.Join(branchErrors...)
	}

	fused := fuseSearchHits(vectorHits, keywordHits, h.config)
	if h.config.MMRLambda > 0 {
		fused = diversifyMMR(fused, h.config.MMRLambda)
	}
	if h.config.Reranker != nil && len(fused) > 0 {
		candidateCount := min(len(fused), max(h.config.RerankTopK, finalTopK))
		reranked, err := h.rerank(ctx, query, fused[:candidateCount], finalTopK, options...)
		if err == nil {
			return reranked, nil
		}
		if h.config.StrictRerank {
			return nil, err
		}
	}
	return cloneSearchHits(fused[:min(len(fused), finalTopK)]), nil
}

var _ runnable.Runnable[HybridQuery, []SearchHit] = (*HybridSearcher)(nil)

func NewHybridSearcher(cfg HybridSearchConfig) (*HybridSearcher, error) {
	if cfg.Embedder == nil || cfg.VectorStore == nil || cfg.KeywordSearch == nil {
		return nil, errors.New("hybrid search requires embedder, vector store and keyword search")
	}
	if cfg.VectorTopK == 0 {
		cfg.VectorTopK = 20
	}
	if cfg.KeywordTopK == 0 {
		cfg.KeywordTopK = 20
	}
	if cfg.FinalTopK == 0 {
		cfg.FinalTopK = 10
	}
	if cfg.RerankTopK == 0 {
		cfg.RerankTopK = max(cfg.FinalTopK*2, cfg.FinalTopK)
	}
	if cfg.VectorTopK < 1 || cfg.KeywordTopK < 1 || cfg.FinalTopK < 1 || cfg.RerankTopK < 1 {
		return nil, errors.New("hybrid search TopK values must be greater than zero")
	}
	if cfg.VectorTopK > maxHybridTopK || cfg.KeywordTopK > maxHybridTopK || cfg.FinalTopK > maxHybridTopK || cfg.RerankTopK > maxHybridTopK {
		return nil, errors.New("hybrid search TopK exceeds framework limit")
	}
	if cfg.Fusion == "" {
		cfg.Fusion = FusionRRF
	}
	if cfg.Fusion != FusionRRF && cfg.Fusion != FusionWeighted {
		return nil, fmt.Errorf("unsupported fusion strategy %q", cfg.Fusion)
	}
	if cfg.RRFK == 0 {
		cfg.RRFK = 60
	}
	if cfg.RRFK < 0 || math.IsNaN(cfg.RRFK) || math.IsInf(cfg.RRFK, 0) {
		return nil, errors.New("RRF K cannot be negative")
	}
	if cfg.VectorWeight == 0 && cfg.KeywordWeight == 0 {
		cfg.VectorWeight, cfg.KeywordWeight = 1, 1
	}
	if cfg.VectorWeight < 0 || cfg.KeywordWeight < 0 || math.IsNaN(cfg.VectorWeight) || math.IsNaN(cfg.KeywordWeight) || math.IsInf(cfg.VectorWeight, 0) || math.IsInf(cfg.KeywordWeight, 0) {
		return nil, errors.New("hybrid search weights cannot be negative")
	}
	if cfg.MMRLambda < 0 || cfg.MMRLambda > 1 || math.IsNaN(cfg.MMRLambda) || math.IsInf(cfg.MMRLambda, 0) {
		return nil, errors.New("MMR lambda must be between zero and one")
	}
	return &HybridSearcher{config: cfg}, nil
}

func (h *HybridSearcher) rerank(ctx context.Context, query HybridQuery, candidates []SearchHit, topK int, options ...runnable.Option) ([]SearchHit, error) {
	documents := make([]RerankDocument, len(candidates))
	for index, hit := range candidates {
		documents[index] = RerankDocument{ID: hit.ID, Text: hit.Text, Metadata: cloneMetadata(hit.Metadata)}
	}
	results, err := h.config.Reranker.Invoke(ctx, RerankRequest{
		Model: h.config.RerankModel, Query: query.Text, Documents: documents,
		TopK: min(topK, len(documents)), Raw: cloneStringAnyMap(query.RerankRaw),
	}, options...)
	if err != nil {
		return nil, fmt.Errorf("rerank hybrid results: %w", err)
	}
	reranked := make([]SearchHit, 0, min(topK, len(results)))
	seen := map[int]struct{}{}
	for _, result := range results {
		index := result.Index
		if result.ID != "" {
			if matched := searchHitIndex(candidates, result.ID); matched >= 0 {
				index = matched
			}
		}
		if index < 0 || index >= len(candidates) {
			continue
		}
		if _, exists := seen[index]; exists {
			continue
		}
		seen[index] = struct{}{}
		hit := cloneSearchHits(candidates[index : index+1])[0]
		if hit.Raw == nil {
			hit.Raw = map[string]any{}
		}
		hit.Raw["fusion_score"] = hit.Score
		hit.Score = result.Score
		hit.Source = appendSource(hit.Source, "rerank")
		reranked = append(reranked, hit)
		if len(reranked) == topK {
			break
		}
	}
	if len(reranked) == 0 {
		return nil, errors.New("reranker returned no valid results")
	}
	return reranked, nil
}

type fusedSearchHit struct {
	hit     SearchHit
	key     string
	score   float64
	sources map[string]struct{}
}

func fuseSearchHits(vectorHits, keywordHits []SearchHit, cfg HybridSearchConfig) []SearchHit {
	combined := map[string]*fusedSearchHit{}
	add := func(source string, hits []SearchHit, weight float64) {
		normalized := normalizedScores(hits)
		for rank, original := range hits {
			key := searchHitKey(original)
			current := combined[key]
			if current == nil {
				current = &fusedSearchHit{hit: cloneSearchHits(hits[rank : rank+1])[0], key: key, sources: map[string]struct{}{}}
				combined[key] = current
			} else {
				mergeSearchHit(&current.hit, original)
			}
			current.sources[source] = struct{}{}
			if current.hit.Raw == nil {
				current.hit.Raw = map[string]any{}
			}
			current.hit.Raw[source+"_score"] = original.Score
			if cfg.Fusion == FusionWeighted {
				current.score += weight * normalized[rank]
			} else {
				current.score += weight / (cfg.RRFK + float64(rank+1))
			}
		}
	}
	add("vector", vectorHits, cfg.VectorWeight)
	add("keyword", keywordHits, cfg.KeywordWeight)
	values := make([]*fusedSearchHit, 0, len(combined))
	for _, value := range combined {
		value.hit.Score = value.score
		value.hit.Source = joinedSources(value.sources)
		value.hit.Raw["fusion_score"] = value.score
		values = append(values, value)
	}
	sort.SliceStable(values, func(left, right int) bool {
		if values[left].score == values[right].score {
			return values[left].key < values[right].key
		}
		return values[left].score > values[right].score
	})
	hits := make([]SearchHit, len(values))
	for index, value := range values {
		hits[index] = cloneSearchHits([]SearchHit{value.hit})[0]
	}
	return hits
}

func normalizedScores(hits []SearchHit) []float64 {
	values := make([]float64, len(hits))
	if len(hits) == 0 {
		return values
	}
	minimum, maximum := hits[0].Score, hits[0].Score
	for _, hit := range hits[1:] {
		minimum = math.Min(minimum, hit.Score)
		maximum = math.Max(maximum, hit.Score)
	}
	if maximum == minimum {
		for index := range values {
			values[index] = 1 / float64(index+1)
		}
		return values
	}
	for index, hit := range hits {
		values[index] = (hit.Score - minimum) / (maximum - minimum)
	}
	return values
}

func diversifyMMR(hits []SearchHit, lambda float64) []SearchHit {
	if len(hits) < 2 {
		return cloneSearchHits(hits)
	}
	relevance := normalizedScores(hits)
	remaining := make([]int, len(hits))
	for index := range remaining {
		remaining[index] = index
	}
	selected := make([]int, 0, len(hits))
	for len(remaining) > 0 {
		bestPosition, bestScore := 0, math.Inf(-1)
		for position, candidate := range remaining {
			maxSimilarity := 0.0
			for _, chosen := range selected {
				maxSimilarity = math.Max(maxSimilarity, cosineSimilarity(hits[candidate].Vector, hits[chosen].Vector))
			}
			score := lambda*relevance[candidate] - (1-lambda)*maxSimilarity
			if score > bestScore {
				bestPosition, bestScore = position, score
			}
		}
		selected = append(selected, remaining[bestPosition])
		remaining = append(remaining[:bestPosition], remaining[bestPosition+1:]...)
	}
	result := make([]SearchHit, len(selected))
	for index, selectedIndex := range selected {
		result[index] = cloneSearchHits(hits[selectedIndex : selectedIndex+1])[0]
		if result[index].Raw == nil {
			result[index].Raw = map[string]any{}
		}
		result[index].Raw["mmr_rank"] = index
	}
	return result
}

func cosineSimilarity(left, right []float32) float64 {
	if len(left) == 0 || len(left) != len(right) {
		return 0
	}
	var dot, leftNorm, rightNorm float64
	for index := range left {
		l, r := float64(left[index]), float64(right[index])
		dot += l * r
		leftNorm += l * l
		rightNorm += r * r
	}
	if leftNorm == 0 || rightNorm == 0 {
		return 0
	}
	return dot / math.Sqrt(leftNorm*rightNorm)
}

func searchHitKey(hit SearchHit) string {
	if strings.TrimSpace(hit.ID) != "" {
		return "id:" + hit.ID
	}
	sum := sha256.Sum256([]byte(strings.TrimSpace(hit.Text)))
	return "text:" + hex.EncodeToString(sum[:])
}

func mergeSearchHit(target *SearchHit, source SearchHit) {
	if target.ID == "" {
		target.ID = source.ID
	}
	if target.Text == "" {
		target.Text = source.Text
	}
	if len(target.Vector) == 0 {
		target.Vector = append([]float32(nil), source.Vector...)
	}
	if target.Metadata == nil {
		target.Metadata = cloneMetadata(source.Metadata)
	} else {
		for key, value := range source.Metadata {
			if _, exists := target.Metadata[key]; !exists {
				target.Metadata[key] = cloneMetadataValue(value)
			}
		}
	}
}

func joinedSources(sources map[string]struct{}) string {
	values := make([]string, 0, len(sources))
	for source := range sources {
		values = append(values, source)
	}
	sort.Strings(values)
	return strings.Join(values, "+")
}

func appendSource(current, source string) string {
	values := map[string]struct{}{}
	for _, value := range strings.Split(current, "+") {
		if value != "" {
			values[value] = struct{}{}
		}
	}
	values[source] = struct{}{}
	return joinedSources(values)
}

func searchHitIndex(hits []SearchHit, id string) int {
	for index, hit := range hits {
		if hit.ID == id {
			return index
		}
	}
	return -1
}
