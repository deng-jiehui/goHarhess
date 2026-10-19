package sqlite

import (
	"context"
	"crypto/rand"

	"github.com/goharness/autoresearch/internal/domain"
)

func (s *Store) CreateSession(ctx context.Context, scope domain.Scope, title string) (domain.Session, error) {
	title, err := domain.NormalizeTitle(title)
	if err != nil {
		return domain.Session{}, err
	}
	session := domain.Session{ID: rand.Text(), ProjectID: scope.ProjectID, Title: title}
	result, err := s.db.ExecContext(ctx, `INSERT INTO sessions(id,project_id,title) SELECT ?,id,? FROM projects WHERE id=? AND owner_id=?`, session.ID, title, scope.ProjectID, scope.UserID)
	if err != nil {
		return session, dbError(err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return session, dbError(err)
	}
	if count == 0 {
		return domain.Session{}, domain.ErrNotFound
	}
	return session, nil
}

func (s *Store) GetSession(ctx context.Context, scope domain.Scope, id string) (domain.Session, error) {
	var session domain.Session
	err := s.db.QueryRowContext(ctx, `SELECT s.id,s.project_id,s.title,s.archived FROM sessions s JOIN projects p ON p.id=s.project_id WHERE s.id=? AND p.id=? AND p.owner_id=?`, id, scope.ProjectID, scope.UserID).Scan(&session.ID, &session.ProjectID, &session.Title, &session.Archived)
	return session, dbError(err)
}

func (s *Store) ListSessions(ctx context.Context, scope domain.Scope) ([]domain.Session, error) {
	if _, err := s.GetProject(ctx, scope); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT s.id,s.project_id,s.title,s.archived FROM sessions s JOIN projects p ON p.id=s.project_id WHERE p.id=? AND p.owner_id=? ORDER BY s.rowid DESC`, scope.ProjectID, scope.UserID)
	if err != nil {
		return nil, dbError(err)
	}
	defer rows.Close()
	items := make([]domain.Session, 0)
	for rows.Next() {
		var session domain.Session
		if err = rows.Scan(&session.ID, &session.ProjectID, &session.Title, &session.Archived); err != nil {
			return nil, dbError(err)
		}
		items = append(items, session)
	}
	return items, dbError(rows.Err())
}

func (s *Store) UpdateSession(ctx context.Context, scope domain.Scope, session domain.Session) error {
	if session.ProjectID != scope.ProjectID {
		return domain.ErrNotFound
	}
	result, err := s.db.ExecContext(ctx, `UPDATE sessions SET title=?,archived=? WHERE id=? AND project_id=? AND EXISTS (SELECT 1 FROM projects WHERE id=? AND owner_id=?)`, session.Title, session.Archived, session.ID, scope.ProjectID, scope.ProjectID, scope.UserID)
	if err != nil {
		return dbError(err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return dbError(err)
	}
	if count == 0 {
		return domain.ErrNotFound
	}
	return nil
}

func (s *Store) DeleteSession(ctx context.Context, scope domain.Scope, id string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return dbError(err)
	}
	defer tx.Rollback()
	var exists int
	if err = tx.QueryRowContext(ctx, `SELECT 1 FROM sessions s JOIN projects p ON p.id=s.project_id WHERE s.id=? AND s.project_id=? AND p.owner_id=?`, id, scope.ProjectID, scope.UserID).Scan(&exists); err != nil {
		return dbError(err)
	}
	var active int
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM turns WHERE session_id=? AND state IN ('queued','running','awaiting_input','recovery_required')`, id).Scan(&active); err != nil {
		return dbError(err)
	}
	if active > 0 {
		return domain.ErrBusy
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM sessions WHERE id=? AND project_id=?`, id, scope.ProjectID)
	if err != nil {
		return dbError(err)
	}
	if count, err := result.RowsAffected(); err != nil {
		return dbError(err)
	} else if count == 0 {
		return domain.ErrNotFound
	}
	return dbError(tx.Commit())
}
