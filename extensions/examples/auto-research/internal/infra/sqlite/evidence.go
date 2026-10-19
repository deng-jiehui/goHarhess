package sqlite

import (
	"context"
	"crypto/rand"

	"github.com/goharness/autoresearch/internal/domain"
)

const documentColumns = "id,owner_id,project_id,paper_id,artifact_id,text,sha256,locator"

func scanDocument(row scanner) (domain.SourceDocument, error) {
	var document domain.SourceDocument
	var ownerID, projectID string
	if err := row.Scan(&document.ID, &ownerID, &projectID, &document.PaperID, &document.ArtifactID, &document.Text, &document.SHA256, &document.Locator); err != nil {
		return document, dbError(err)
	}
	return document, nil
}

func (s *Store) SaveDocument(ctx context.Context, scope domain.Scope, document domain.SourceDocument) (domain.SourceDocument, error) {
	if scope.UserID == "" || scope.ProjectID == "" || document.PaperID == "" || document.Text == "" || document.SHA256 == "" {
		return domain.SourceDocument{}, domain.ErrInvalid
	}
	if document.ID == "" {
		document.ID = rand.Text()
	}
	document.Text = domain.NormalizeSourceText(document.Text)
	var paperExists int
	if err := s.db.QueryRowContext(ctx, "SELECT count(*) FROM papers WHERE id=? AND project_id=? AND owner_id=?", document.PaperID, scope.ProjectID, scope.UserID).Scan(&paperExists); err != nil {
		return domain.SourceDocument{}, dbError(err)
	}
	if paperExists == 0 {
		return domain.SourceDocument{}, domain.ErrNotFound
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO source_documents(id,owner_id,project_id,paper_id,artifact_id,text,sha256,locator)
VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(project_id,paper_id,sha256,locator) DO NOTHING`, document.ID, scope.UserID, scope.ProjectID, document.PaperID, document.ArtifactID, document.Text, document.SHA256, document.Locator)
	if err != nil {
		return domain.SourceDocument{}, dbError(err)
	}
	return s.GetDocumentByIdentity(ctx, scope, document.PaperID, document.SHA256, document.Locator)
}

func (s *Store) GetDocumentByIdentity(ctx context.Context, scope domain.Scope, paperID, sha256, locator string) (domain.SourceDocument, error) {
	return scanDocument(s.db.QueryRowContext(ctx, "SELECT "+documentColumns+" FROM source_documents WHERE project_id=? AND owner_id=? AND paper_id=? AND sha256=? AND locator=?", scope.ProjectID, scope.UserID, paperID, sha256, locator))
}

func (s *Store) GetDocument(ctx context.Context, scope domain.Scope, documentID string) (domain.SourceDocument, error) {
	return scanDocument(s.db.QueryRowContext(ctx, "SELECT "+documentColumns+" FROM source_documents WHERE id=? AND project_id=? AND owner_id=?", documentID, scope.ProjectID, scope.UserID))
}

func (s *Store) ListDocuments(ctx context.Context, scope domain.Scope, paperID string) ([]domain.SourceDocument, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT "+documentColumns+" FROM source_documents WHERE project_id=? AND owner_id=? AND paper_id=? ORDER BY created_at DESC", scope.ProjectID, scope.UserID, paperID)
	if err != nil {
		return nil, dbError(err)
	}
	defer rows.Close()
	items := make([]domain.SourceDocument, 0)
	for rows.Next() {
		item, scanErr := scanDocument(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, item)
	}
	return items, dbError(rows.Err())
}

const evidenceColumns = "id,owner_id,project_id,paper_id,document_id,field,value,quote,locator,start_rune,end_rune"

func scanEvidence(row scanner) (domain.Evidence, error) {
	var evidence domain.Evidence
	var ownerID string
	if err := row.Scan(&evidence.ID, &ownerID, &evidence.ProjectID, &evidence.PaperID, &evidence.DocumentID, &evidence.Field, &evidence.Value, &evidence.Quote, &evidence.Locator, &evidence.StartRune, &evidence.EndRune); err != nil {
		return evidence, dbError(err)
	}
	return evidence, nil
}

func (s *Store) SaveEvidence(ctx context.Context, scope domain.Scope, evidence domain.Evidence) (domain.Evidence, error) {
	if scope.UserID == "" || scope.ProjectID == "" || evidence.ProjectID != scope.ProjectID || evidence.PaperID == "" || evidence.DocumentID == "" {
		return domain.Evidence{}, domain.ErrInvalid
	}
	if evidence.ID == "" {
		evidence.ID = rand.Text()
	}
	var paperExists int
	if err := s.db.QueryRowContext(ctx, "SELECT count(*) FROM papers WHERE id=? AND project_id=? AND owner_id=?", evidence.PaperID, scope.ProjectID, scope.UserID).Scan(&paperExists); err != nil {
		return domain.Evidence{}, dbError(err)
	}
	if paperExists == 0 {
		return domain.Evidence{}, domain.ErrNotFound
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO evidence(id,owner_id,project_id,paper_id,document_id,field,value,quote,locator,start_rune,end_rune)
VALUES(?,?,?,?,?,?,?,?,?,?,?)`, evidence.ID, scope.UserID, scope.ProjectID, evidence.PaperID, evidence.DocumentID, evidence.Field, evidence.Value, evidence.Quote, evidence.Locator, evidence.StartRune, evidence.EndRune)
	if err != nil {
		return domain.Evidence{}, dbError(err)
	}
	return s.GetEvidence(ctx, scope, evidence.ID)
}

func (s *Store) GetEvidence(ctx context.Context, scope domain.Scope, evidenceID string) (domain.Evidence, error) {
	return scanEvidence(s.db.QueryRowContext(ctx, "SELECT "+evidenceColumns+" FROM evidence WHERE id=? AND project_id=? AND owner_id=?", evidenceID, scope.ProjectID, scope.UserID))
}

func (s *Store) ListEvidence(ctx context.Context, scope domain.Scope, paperID string) ([]domain.Evidence, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT "+evidenceColumns+" FROM evidence WHERE project_id=? AND owner_id=? AND paper_id=? ORDER BY rowid DESC", scope.ProjectID, scope.UserID, paperID)
	if err != nil {
		return nil, dbError(err)
	}
	defer rows.Close()
	items := make([]domain.Evidence, 0)
	for rows.Next() {
		item, scanErr := scanEvidence(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, item)
	}
	return items, dbError(rows.Err())
}

var _ domain.EvidenceRepository = (*Store)(nil)
