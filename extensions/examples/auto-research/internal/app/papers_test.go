package app

import (
	"context"
	"testing"

	"github.com/goharness/autoresearch/internal/domain"
)

type paperRepositoryStub struct {
	imported domain.Paper
}

func (s *paperRepositoryStub) ImportPaper(_ context.Context, scope domain.Scope, paper domain.Paper) (domain.Paper, error) {
	paper.ProjectID = scope.ProjectID
	paper.ID = "paper-1"
	paper.Revision = 1
	s.imported = paper
	return paper, nil
}
func (s *paperRepositoryStub) GetPaper(context.Context, domain.Scope, string) (domain.Paper, error) {
	return s.imported, nil
}
func (s *paperRepositoryStub) GetPaperBySourceKey(context.Context, domain.Scope, string) (domain.Paper, error) {
	return s.imported, nil
}
func (s *paperRepositoryStub) ListPapers(context.Context, domain.Scope, domain.PaperQuery) ([]domain.Paper, error) {
	return []domain.Paper{s.imported}, nil
}
func (s *paperRepositoryStub) UpdatePaper(context.Context, domain.Scope, domain.Paper, int64) error {
	return nil
}

func TestPaperServiceImportNormalizesCandidateIdentity(t *testing.T) {
	repo := &paperRepositoryStub{}
	service := NewPaperService(repo)
	got, err := service.Import(context.Background(), domain.Scope{UserID: "alice", ProjectID: "project-1"}, domain.Candidate{
		Title:   "一篇论文",
		DOI:     "https://doi.org/10.1234/ABC",
		Authors: []string{"作者"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.SourceKey != "doi:10.1234/abc" || repo.imported.SourceKey != got.SourceKey {
		t.Fatalf("论文身份未归一化: got=%+v imported=%+v", got, repo.imported)
	}
}
