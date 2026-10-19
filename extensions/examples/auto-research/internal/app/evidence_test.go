package app

import (
	"context"
	"errors"
	"testing"

	"github.com/goharness/autoresearch/internal/domain"
)

type evidenceRepositoryStub struct {
	document domain.SourceDocument
}

func (s evidenceRepositoryStub) SaveDocument(_ context.Context, _ domain.Scope, document domain.SourceDocument) (domain.SourceDocument, error) {
	return document, nil
}
func (s evidenceRepositoryStub) GetDocument(context.Context, domain.Scope, string) (domain.SourceDocument, error) {
	return s.document, nil
}
func (s evidenceRepositoryStub) ListDocuments(context.Context, domain.Scope, string) ([]domain.SourceDocument, error) {
	return []domain.SourceDocument{s.document}, nil
}
func (s evidenceRepositoryStub) SaveEvidence(_ context.Context, _ domain.Scope, evidence domain.Evidence) (domain.Evidence, error) {
	evidence.ID = "e1"
	return evidence, nil
}
func (s evidenceRepositoryStub) ListEvidence(context.Context, domain.Scope, string) ([]domain.Evidence, error) {
	return nil, nil
}

func TestEvidenceServiceRejectsQuoteThatIsNotInDocument(t *testing.T) {
	doc := domain.SourceDocument{ID: "d1", PaperID: "p1", Text: "原文片段", SHA256: domain.SourceTextSHA256("原文片段")}
	service := NewEvidenceService(evidenceRepositoryStub{document: doc})
	_, err := service.Save(context.Background(), domain.Scope{UserID: "u", ProjectID: "project"}, domain.Evidence{PaperID: "p1", DocumentID: "d1", StartRune: 0, EndRune: 3, Quote: "伪造片段"})
	if !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("伪造引用未拒绝: %v", err)
	}
}
