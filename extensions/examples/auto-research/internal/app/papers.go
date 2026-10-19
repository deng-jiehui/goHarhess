package app

import (
	"context"
	"strings"
	"unicode/utf8"

	"github.com/goharness/autoresearch/internal/domain"
)

type PaperService struct {
	repo domain.PaperRepository
}

func NewPaperService(repo domain.PaperRepository) *PaperService {
	return &PaperService{repo: repo}
}

type PaperPatch struct {
	Title        *string  `json:"title"`
	Tags         []string `json:"tags"`
	Note         *string  `json:"note"`
	ReadingState *string  `json:"reading_state"`
	Revision     int64    `json:"revision"`
}

func (s *PaperService) Import(ctx context.Context, scope domain.Scope, candidate domain.Candidate) (domain.Paper, error) {
	if s == nil || s.repo == nil || scope.UserID == "" || scope.ProjectID == "" {
		return domain.Paper{}, domain.ErrInvalid
	}
	title := strings.TrimSpace(candidate.Title)
	if title == "" || utf8.RuneCountInString(title) > 500 {
		return domain.Paper{}, domain.ErrInvalid
	}
	input := strings.TrimSpace(candidate.SourceKey)
	if input == "" {
		switch {
		case strings.TrimSpace(candidate.DOI) != "":
			input = candidate.DOI
		case strings.TrimSpace(candidate.ArxivID) != "":
			input = candidate.ArxivID
		case strings.TrimSpace(candidate.URL) != "":
			input = candidate.URL
		default:
			return domain.Paper{}, domain.ErrInvalid
		}
	}
	key, err := domain.NormalizePaperKey(input)
	if err != nil {
		return domain.Paper{}, err
	}
	paper := domain.Paper{
		ProjectID: scope.ProjectID,
		SourceKey: key,
		Title:     title,
		Authors:   append([]string(nil), candidate.Authors...),
		Year:      candidate.Year,
		DOI:       candidate.DOI,
		ArxivID:   candidate.ArxivID,
		URL:       candidate.URL,
		Abstract:  candidate.Abstract,
		Tags:      []string{},
	}
	if strings.HasPrefix(key, "doi:") && paper.DOI == "" {
		paper.DOI = strings.TrimPrefix(key, "doi:")
	}
	if strings.HasPrefix(key, "arxiv:") && paper.ArxivID == "" {
		paper.ArxivID = strings.TrimPrefix(key, "arxiv:")
	}
	return s.repo.ImportPaper(ctx, scope, paper)
}

func (s *PaperService) Get(ctx context.Context, scope domain.Scope, paperID string) (domain.Paper, error) {
	if s == nil || s.repo == nil || paperID == "" {
		return domain.Paper{}, domain.ErrInvalid
	}
	return s.repo.GetPaper(ctx, scope, paperID)
}

func (s *PaperService) List(ctx context.Context, scope domain.Scope, query domain.PaperQuery) ([]domain.Paper, error) {
	if s == nil || s.repo == nil {
		return nil, domain.ErrInvalid
	}
	if query.Limit < 0 || query.Limit > 100 || query.YearFrom < 0 || query.YearTo < 0 || (query.YearFrom > 0 && query.YearTo > 0 && query.YearFrom > query.YearTo) {
		return nil, domain.ErrInvalid
	}
	return s.repo.ListPapers(ctx, scope, query)
}

func (s *PaperService) Update(ctx context.Context, scope domain.Scope, paperID string, patch PaperPatch) (domain.Paper, error) {
	if s == nil || s.repo == nil || paperID == "" || patch.Revision < 1 {
		return domain.Paper{}, domain.ErrInvalid
	}
	paper, err := s.repo.GetPaper(ctx, scope, paperID)
	if err != nil {
		return domain.Paper{}, err
	}
	if patch.Title != nil {
		paper.Title = strings.TrimSpace(*patch.Title)
	}
	if patch.Tags != nil {
		paper.Tags = append([]string(nil), patch.Tags...)
	}
	if patch.Note != nil {
		paper.Note = *patch.Note
	}
	if patch.ReadingState != nil {
		paper.ReadingState = strings.TrimSpace(*patch.ReadingState)
	}
	if paper.Title == "" || utf8.RuneCountInString(paper.Title) > 500 {
		return domain.Paper{}, domain.ErrInvalid
	}
	if err = s.repo.UpdatePaper(ctx, scope, paper, patch.Revision); err != nil {
		return domain.Paper{}, err
	}
	paper.Revision++
	return paper, nil
}
