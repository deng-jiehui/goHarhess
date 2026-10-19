package app

import (
	"context"

	"github.com/goharness/autoresearch/internal/domain"
)

type EvidenceService struct {
	repo domain.EvidenceRepository
}

func NewEvidenceService(repo domain.EvidenceRepository) *EvidenceService {
	return &EvidenceService{repo: repo}
}

func (s *EvidenceService) SaveDocument(ctx context.Context, scope domain.Scope, document domain.SourceDocument) (domain.SourceDocument, error) {
	if s == nil || s.repo == nil || document.PaperID == "" || document.Text == "" {
		return domain.SourceDocument{}, domain.ErrInvalid
	}
	document.Text = domain.NormalizeSourceText(document.Text)
	document.SHA256 = domain.SourceTextSHA256(document.Text)
	return s.repo.SaveDocument(ctx, scope, document)
}

func (s *EvidenceService) GetDocument(ctx context.Context, scope domain.Scope, documentID string) (domain.SourceDocument, error) {
	if s == nil || s.repo == nil || documentID == "" {
		return domain.SourceDocument{}, domain.ErrInvalid
	}
	return s.repo.GetDocument(ctx, scope, documentID)
}

func (s *EvidenceService) ListDocuments(ctx context.Context, scope domain.Scope, paperID string) ([]domain.SourceDocument, error) {
	if s == nil || s.repo == nil || paperID == "" {
		return nil, domain.ErrInvalid
	}
	return s.repo.ListDocuments(ctx, scope, paperID)
}

func (s *EvidenceService) Save(ctx context.Context, scope domain.Scope, evidence domain.Evidence) (domain.Evidence, error) {
	if s == nil || s.repo == nil {
		return domain.Evidence{}, domain.ErrInvalid
	}
	document, err := s.repo.GetDocument(ctx, scope, evidence.DocumentID)
	if err != nil {
		return domain.Evidence{}, err
	}
	evidence.ProjectID = scope.ProjectID
	if err = domain.ValidateEvidence(document, evidence); err != nil {
		return domain.Evidence{}, err
	}
	evidence.Locator = document.Locator
	return s.repo.SaveEvidence(ctx, scope, evidence)
}

func (s *EvidenceService) List(ctx context.Context, scope domain.Scope, paperID string) ([]domain.Evidence, error) {
	if s == nil || s.repo == nil || paperID == "" {
		return nil, domain.ErrInvalid
	}
	return s.repo.ListEvidence(ctx, scope, paperID)
}
