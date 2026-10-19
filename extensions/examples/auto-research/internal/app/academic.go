package app

import (
	"context"

	"github.com/goharness/autoresearch/internal/domain"
)

type AcademicService struct {
	source domain.PaperSource
}

func NewAcademicService(source domain.PaperSource) *AcademicService {
	return &AcademicService{source: source}
}

func (s *AcademicService) Search(ctx context.Context, query domain.PaperQuery) ([]domain.Candidate, error) {
	if s == nil || s.source == nil {
		return nil, domain.ErrUnavailable
	}
	return s.source.Search(ctx, query)
}
