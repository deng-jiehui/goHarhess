package sqlite

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"strings"

	"github.com/goharness/autoresearch/internal/domain"
)

const paperColumns = "id,owner_id,project_id,source_key,title,authors_json,year,doi,arxiv_id,url,abstract,tags_json,note,reading_state,revision"

func scanPaper(row scanner) (domain.Paper, error) {
	var paper domain.Paper
	var ownerID string
	var authors, tags string
	if err := row.Scan(&paper.ID, &ownerID, &paper.ProjectID, &paper.SourceKey, &paper.Title, &authors, &paper.Year, &paper.DOI, &paper.ArxivID, &paper.URL, &paper.Abstract, &tags, &paper.Note, &paper.ReadingState, &paper.Revision); err != nil {
		return paper, dbError(err)
	}
	if err := json.Unmarshal([]byte(authors), &paper.Authors); err != nil {
		return paper, dbError(err)
	}
	if err := json.Unmarshal([]byte(tags), &paper.Tags); err != nil {
		return paper, dbError(err)
	}
	return paper, nil
}

func (s *Store) ImportPaper(ctx context.Context, scope domain.Scope, paper domain.Paper) (domain.Paper, error) {
	if strings.TrimSpace(scope.UserID) == "" || strings.TrimSpace(scope.ProjectID) == "" || strings.TrimSpace(paper.SourceKey) == "" || strings.TrimSpace(paper.Title) == "" {
		return domain.Paper{}, domain.ErrInvalid
	}
	authors, err := json.Marshal(paper.Authors)
	if err != nil {
		return domain.Paper{}, err
	}
	tags, err := json.Marshal(paper.Tags)
	if err != nil {
		return domain.Paper{}, err
	}
	if paper.ReadingState == "" {
		paper.ReadingState = "unread"
	}
	paper.ProjectID = scope.ProjectID
	paper.ID = rand.Text()
	paper.Revision = 1
	_, err = s.db.ExecContext(ctx, `INSERT INTO papers(id,owner_id,project_id,source_key,title,authors_json,year,doi,arxiv_id,url,abstract,tags_json,note,reading_state)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(project_id,source_key) DO NOTHING`, paper.ID, scope.UserID, scope.ProjectID, paper.SourceKey, paper.Title, string(authors), paper.Year, paper.DOI, paper.ArxivID, paper.URL, paper.Abstract, string(tags), paper.Note, paper.ReadingState)
	if err != nil {
		return domain.Paper{}, dbError(err)
	}
	return s.GetPaperBySourceKey(ctx, scope, paper.SourceKey)
}

func (s *Store) GetPaperBySourceKey(ctx context.Context, scope domain.Scope, sourceKey string) (domain.Paper, error) {
	return scanPaper(s.db.QueryRowContext(ctx, "SELECT "+paperColumns+" FROM papers WHERE project_id=? AND owner_id=? AND source_key=?", scope.ProjectID, scope.UserID, sourceKey))
}

func (s *Store) GetPaper(ctx context.Context, scope domain.Scope, paperID string) (domain.Paper, error) {
	return scanPaper(s.db.QueryRowContext(ctx, "SELECT "+paperColumns+" FROM papers WHERE id=? AND project_id=? AND owner_id=?", paperID, scope.ProjectID, scope.UserID))
}

func (s *Store) ListPapers(ctx context.Context, scope domain.Scope, query domain.PaperQuery) ([]domain.Paper, error) {
	if scope.ProjectID == "" || scope.UserID == "" {
		return nil, domain.ErrInvalid
	}
	limit := query.Limit
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	conditions := []string{"project_id=?", "owner_id=?"}
	args := []any{scope.ProjectID, scope.UserID}
	if text := strings.TrimSpace(query.Text); text != "" {
		conditions = append(conditions, "(title LIKE ? OR abstract LIKE ? OR source_key LIKE ?)")
		pattern := "%" + text + "%"
		args = append(args, pattern, pattern, pattern)
	}
	if query.YearFrom > 0 {
		conditions = append(conditions, "year>=?")
		args = append(args, query.YearFrom)
	}
	if query.YearTo > 0 {
		conditions = append(conditions, "year<=?")
		args = append(args, query.YearTo)
	}
	rows, err := s.db.QueryContext(ctx, "SELECT "+paperColumns+" FROM papers WHERE "+strings.Join(conditions, " AND ")+" ORDER BY created_at DESC LIMIT ?", append(args, limit)...)
	if err != nil {
		return nil, dbError(err)
	}
	defer rows.Close()
	items := make([]domain.Paper, 0)
	for rows.Next() {
		item, scanErr := scanPaper(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, item)
	}
	return items, dbError(rows.Err())
}

func (s *Store) UpdatePaper(ctx context.Context, scope domain.Scope, paper domain.Paper, expectedRevision int64) error {
	if paper.ID == "" || paper.ProjectID != scope.ProjectID || expectedRevision < 1 {
		return domain.ErrInvalid
	}
	authors, err := json.Marshal(paper.Authors)
	if err != nil {
		return err
	}
	tags, err := json.Marshal(paper.Tags)
	if err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, `UPDATE papers SET title=?,authors_json=?,year=?,doi=?,arxiv_id=?,url=?,abstract=?,tags_json=?,note=?,reading_state=?,revision=revision+1,updated_at=CURRENT_TIMESTAMP
WHERE id=? AND project_id=? AND owner_id=? AND revision=?`, paper.Title, string(authors), paper.Year, paper.DOI, paper.ArxivID, paper.URL, paper.Abstract, string(tags), paper.Note, paper.ReadingState, paper.ID, scope.ProjectID, scope.UserID, expectedRevision)
	if err != nil {
		return dbError(err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return dbError(err)
	}
	if count == 0 {
		if _, err = s.GetPaper(ctx, scope, paper.ID); err != nil {
			return err
		}
		return domain.ErrConflict
	}
	return nil
}

var _ domain.PaperRepository = (*Store)(nil)
