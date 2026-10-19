package mcp

import (
	"context"
	"fmt"
	"strings"

	"github.com/goharness/autoresearch/internal/domain"
)

// FixtureSource 是阶段三离线验收用的学术来源。它只证明应用流程和归档
// 一致性，不代表真实 arXiv MCP 的搜索质量或协议兼容性。
type FixtureSource struct {
	items map[string]domain.Candidate
	docs  map[string]domain.SourceDocument
}

func NewFixtureSource() *FixtureSource {
	item := domain.Candidate{SourceKey: "arxiv:2401.00001", Title: "Fixture Research", Authors: []string{"A. Author"}, Year: 2024, Abstract: "A fixture paper for reproducible research workflows.", URL: "https://arxiv.org/abs/2401.00001", ArxivID: "2401.00001", Provider: "arxiv"}
	return &FixtureSource{
		items: map[string]domain.Candidate{item.SourceKey: item},
		docs:  map[string]domain.SourceDocument{item.SourceKey: {ID: "fixture-doc-1", Locator: "abstract", Text: "Fixture Research studies a reproducible workflow with ten samples."}},
	}
}

func (s *FixtureSource) Search(_ context.Context, query domain.PaperQuery) ([]domain.Candidate, error) {
	if s == nil {
		return nil, fmt.Errorf("fixture 来源未配置")
	}
	limit := query.Limit
	if limit <= 0 || limit > 30 {
		limit = 10
	}
	text := strings.ToLower(strings.TrimSpace(query.Text))
	out := make([]domain.Candidate, 0, limit)
	for _, item := range s.items {
		if text != "" && !strings.Contains(strings.ToLower(item.Title+" "+item.Abstract), text) {
			continue
		}
		if query.YearFrom > 0 && item.Year < query.YearFrom || query.YearTo > 0 && item.Year > query.YearTo {
			continue
		}
		out = append(out, item)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

func (s *FixtureSource) Resolve(_ context.Context, reference string) (domain.Candidate, error) {
	key, err := domain.NormalizePaperKey(reference)
	if err != nil {
		return domain.Candidate{}, err
	}
	item, ok := s.items[key]
	if !ok {
		return domain.Candidate{}, domain.ErrNotFound
	}
	return item, nil
}

func (s *FixtureSource) Read(_ context.Context, reference, _ string) (domain.SourceDocument, error) {
	key, err := domain.NormalizePaperKey(reference)
	if err != nil {
		return domain.SourceDocument{}, err
	}
	doc, ok := s.docs[key]
	if !ok {
		return domain.SourceDocument{}, domain.ErrNotFound
	}
	return doc, nil
}

var _ domain.PaperSource = (*FixtureSource)(nil)
