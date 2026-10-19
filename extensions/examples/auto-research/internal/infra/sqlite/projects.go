package sqlite

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/goharness/autoresearch/internal/domain"
)

const projectColumns = "id,owner_id,title,question,field,settings_json,archived,revision"

type scanner interface{ Scan(...any) error }

func scanProject(row scanner) (domain.Project, error) {
	var p domain.Project
	var settings string
	if err := row.Scan(&p.ID, &p.OwnerID, &p.Title, &p.Question, &p.Field, &settings, &p.Archived, &p.Revision); err != nil {
		return p, dbError(err)
	}
	if err := json.Unmarshal([]byte(settings), &p.Settings); err != nil {
		return p, dbError(err)
	}
	return p, nil
}

func dbError(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ErrNotFound
	}
	if err != nil {
		return errors.Join(domain.ErrUnavailable, err)
	}
	return nil
}

func (s *Store) CreateProject(ctx context.Context, userID, title string) (domain.Project, error) {
	p, err := domain.NormalizeProject(domain.Project{ID: rand.Text(), OwnerID: userID, Title: title, Settings: domain.DefaultSettings(), Revision: 1})
	if err != nil {
		return p, err
	}
	settings, _ := json.Marshal(p.Settings)
	_, err = s.db.ExecContext(ctx, `INSERT INTO projects(id,owner_id,title,settings_json) VALUES (?,?,?,?)`, p.ID, userID, p.Title, string(settings))
	return p, dbError(err)
}

func (s *Store) GetProject(ctx context.Context, scope domain.Scope) (domain.Project, error) {
	return scanProject(s.db.QueryRowContext(ctx, "SELECT "+projectColumns+" FROM projects WHERE id=? AND owner_id=?", scope.ProjectID, scope.UserID))
}

func (s *Store) ListProjects(ctx context.Context, userID string) ([]domain.Project, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT "+projectColumns+" FROM projects WHERE owner_id=? ORDER BY rowid DESC", userID)
	if err != nil {
		return nil, dbError(err)
	}
	defer rows.Close()
	items := make([]domain.Project, 0)
	for rows.Next() {
		p, err := scanProject(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, p)
	}
	return items, dbError(rows.Err())
}

func (s *Store) UpdateProject(ctx context.Context, scope domain.Scope, p domain.Project, expectedRevision int64) error {
	if p.ID != scope.ProjectID {
		return domain.ErrInvalid
	}
	settings, err := json.Marshal(p.Settings)
	if err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, `UPDATE projects SET title=?,question=?,field=?,settings_json=?,archived=?,revision=revision+1 WHERE id=? AND owner_id=? AND revision=?`, p.Title, p.Question, p.Field, string(settings), p.Archived, scope.ProjectID, scope.UserID, expectedRevision)
	if err != nil {
		return dbError(err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return dbError(err)
	}
	if count == 0 {
		if _, err = s.GetProject(ctx, scope); err != nil {
			return err
		}
		return domain.ErrConflict
	}
	return nil
}

func (s *Store) DeleteProject(ctx context.Context, scope domain.Scope, confirmTitle string, expectedRevision int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return dbError(err)
	}
	defer tx.Rollback()
	var title string
	var revision int64
	if err = tx.QueryRowContext(ctx, "SELECT title,revision FROM projects WHERE id=? AND owner_id=?", scope.ProjectID, scope.UserID).Scan(&title, &revision); err != nil {
		return dbError(err)
	}
	if revision != expectedRevision {
		return domain.ErrConflict
	}
	if title != confirmTitle {
		return domain.ErrInvalid
	}
	// 仅删除数据库内的项目和会话；远程文件不属于数据库级联范围。
	if _, err = tx.ExecContext(ctx, "DELETE FROM projects WHERE id=? AND owner_id=? AND revision=?", scope.ProjectID, scope.UserID, expectedRevision); err != nil {
		return dbError(err)
	}
	return dbError(tx.Commit())
}
