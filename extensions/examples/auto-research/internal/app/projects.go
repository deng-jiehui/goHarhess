package app

import (
	"context"
	"strings"

	"github.com/goharness/autoresearch/internal/domain"
)

type ProjectService struct{ repo domain.ProjectRepository }

func NewProjectService(repo domain.ProjectRepository) *ProjectService {
	return &ProjectService{repo: repo}
}

type ProjectPatch struct {
	Title    *string                  `json:"title"`
	Question *string                  `json:"question"`
	Field    *string                  `json:"field"`
	Settings *domain.ResearchSettings `json:"settings"`
	Revision int64                    `json:"revision"`
}

func (s *ProjectService) Create(ctx context.Context, userID, title string) (domain.Project, error) {
	title, err := domain.NormalizeTitle(title)
	if err != nil {
		return domain.Project{}, err
	}
	if strings.TrimSpace(userID) == "" {
		return domain.Project{}, domain.ErrInvalid
	}
	return s.repo.CreateProject(ctx, userID, title)
}

func (s *ProjectService) Get(ctx context.Context, scope domain.Scope) (domain.Project, error) {
	return s.repo.GetProject(ctx, scope)
}

func (s *ProjectService) List(ctx context.Context, userID string, archived bool) ([]domain.Project, error) {
	all, err := s.repo.ListProjects(ctx, userID)
	if err != nil {
		return nil, err
	}
	items := make([]domain.Project, 0, len(all))
	for _, p := range all {
		if p.Archived == archived {
			items = append(items, p)
		}
	}
	return items, nil
}

func (s *ProjectService) Update(ctx context.Context, scope domain.Scope, in ProjectPatch) (domain.Project, error) {
	if in.Revision < 1 || (in.Title == nil && in.Question == nil && in.Field == nil && in.Settings == nil) {
		return domain.Project{}, domain.ErrInvalid
	}
	p, err := s.repo.GetProject(ctx, scope)
	if err != nil {
		return p, err
	}
	if in.Title != nil {
		p.Title = *in.Title
	}
	if in.Question != nil {
		p.Question = *in.Question
	}
	if in.Field != nil {
		p.Field = *in.Field
	}
	if in.Settings != nil {
		p.Settings = *in.Settings
	}
	p, err = domain.NormalizeProject(p)
	if err != nil {
		return p, err
	}
	if err = s.repo.UpdateProject(ctx, scope, p, in.Revision); err != nil {
		return domain.Project{}, err
	}
	p.Revision = in.Revision + 1
	return p, nil
}

func (s *ProjectService) Archive(ctx context.Context, scope domain.Scope, revision int64, archived bool) (domain.Project, error) {
	if revision < 1 {
		return domain.Project{}, domain.ErrInvalid
	}
	p, err := s.repo.GetProject(ctx, scope)
	if err != nil {
		return p, err
	}
	p.Archived = archived
	if err = s.repo.UpdateProject(ctx, scope, p, revision); err != nil {
		return domain.Project{}, err
	}
	p.Revision = revision + 1
	return p, nil
}

func (s *ProjectService) Delete(ctx context.Context, scope domain.Scope, confirmTitle string, revision int64) error {
	if revision < 1 || confirmTitle == "" {
		return domain.ErrInvalid
	}
	return s.repo.DeleteProject(ctx, scope, confirmTitle, revision)
}
