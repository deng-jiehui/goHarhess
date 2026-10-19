package app

import (
	"context"
	"testing"

	"github.com/goharness/autoresearch/internal/domain"
)

type paperSourceStub struct{}

func (paperSourceStub) Search(context.Context, domain.PaperQuery) ([]domain.Candidate, error) {
	return []domain.Candidate{{SourceKey: "arxiv:2401.00001", Title: "结果"}}, nil
}
func (paperSourceStub) Resolve(context.Context, string) (domain.Candidate, error) {
	return domain.Candidate{}, nil
}
func (paperSourceStub) Read(context.Context, string, string) (domain.SourceDocument, error) {
	return domain.SourceDocument{}, nil
}

func TestAcademicServiceSearchDelegatesToSource(t *testing.T) {
	items, err := NewAcademicService(paperSourceStub{}).Search(context.Background(), domain.PaperQuery{Text: "结果"})
	if err != nil || len(items) != 1 || items[0].Title != "结果" {
		t.Fatalf("搜索结果=%+v err=%v", items, err)
	}
}
