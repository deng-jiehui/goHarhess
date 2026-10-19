package app

import (
	"context"
	"strings"

	"github.com/goharness/autoresearch/internal/domain"
)

type SessionService struct{ repo domain.SessionRepository }
type SessionOption func(*SessionService)

func NewSessionService(repo domain.SessionRepository, options ...SessionOption) *SessionService {
	s := &SessionService{repo: repo}
	for _, option := range options {
		option(s)
	}
	return s
}

type SessionPatch struct {
	Title    *string `json:"title"`
	Archived *bool   `json:"archived"`
}

func (s *SessionService) Create(ctx context.Context, scope domain.Scope, title string) (domain.Session, error) {
	title, err := domain.NormalizeTitle(title)
	if err != nil {
		return domain.Session{}, err
	}
	return s.repo.CreateSession(ctx, scope, title)
}

func (s *SessionService) Get(ctx context.Context, scope domain.Scope, id string) (domain.Session, error) {
	return s.repo.GetSession(ctx, scope, id)
}

func (s *SessionService) List(ctx context.Context, scope domain.Scope, archived bool) ([]domain.Session, error) {
	all, err := s.repo.ListSessions(ctx, scope)
	if err != nil {
		return nil, err
	}
	items := make([]domain.Session, 0, len(all))
	for _, session := range all {
		if session.Archived == archived {
			items = append(items, session)
		}
	}
	return items, nil
}

func (s *SessionService) Update(ctx context.Context, scope domain.Scope, id string, in SessionPatch) (domain.Session, error) {
	if in.Title == nil && in.Archived == nil {
		return domain.Session{}, domain.ErrInvalid
	}
	session, err := s.repo.GetSession(ctx, scope, id)
	if err != nil {
		return session, err
	}
	if in.Title != nil {
		session.Title, err = domain.NormalizeTitle(*in.Title)
		if err != nil {
			return domain.Session{}, err
		}
	}
	if in.Archived != nil {
		session.Archived = *in.Archived
	}
	if err = s.repo.UpdateSession(ctx, scope, session); err != nil {
		return domain.Session{}, err
	}
	return session, nil
}

// Delete 永久删除会话及其执行记录。
func (s *SessionService) Delete(ctx context.Context, scope domain.Scope, id string) error {
	if strings.TrimSpace(id) == "" {
		return domain.ErrInvalid
	}
	return s.repo.DeleteSession(ctx, scope, id)
}
