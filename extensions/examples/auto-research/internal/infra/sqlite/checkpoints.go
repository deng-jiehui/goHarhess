package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"sort"

	"github.com/goharness/autoresearch/internal/domain"
	"github.com/goharness/goharness/pkg/state"
)

// CheckpointStore 把 goHarness 的完整检查点保存到当前项目/会话/Turn 的
// SQLite 命名空间，使用 checkpoint_heads 做乐观锁。
type CheckpointStore struct {
	store        *Store
	scope        domain.Scope
	sessionID    string
	turnID       string
	conversation string
}

func NewCheckpointStore(store *Store, scope domain.Scope, sessionID, turnID string) *CheckpointStore {
	return &CheckpointStore{store: store, scope: scope, sessionID: sessionID, turnID: turnID, conversation: "autoresearch:" + turnID}
}

func (s *CheckpointStore) Load(ctx context.Context, conversationID string) (state.Checkpoint, bool, error) {
	if conversationID != s.conversation {
		return state.Checkpoint{}, false, state.ErrInvalidCheckpoint
	}
	var id string
	if err := s.store.db.QueryRowContext(ctx, "SELECT checkpoint_id FROM checkpoint_heads WHERE conversation_key=?", conversationID).Scan(&id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return state.Checkpoint{}, false, nil
		}
		return state.Checkpoint{}, false, dbError(err)
	}
	return s.Get(ctx, conversationID, id)
}

func (s *CheckpointStore) Get(ctx context.Context, conversationID, checkpointID string) (state.Checkpoint, bool, error) {
	if conversationID != s.conversation {
		return state.Checkpoint{}, false, state.ErrInvalidCheckpoint
	}
	var raw string
	if err := s.store.db.QueryRowContext(ctx, "SELECT checkpoint_json FROM checkpoints WHERE conversation_key=? AND id=? AND owner_id=? AND project_id=? AND session_id=? AND turn_id=?", conversationID, checkpointID, s.scope.UserID, s.scope.ProjectID, s.sessionID, s.turnID).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return state.Checkpoint{}, false, nil
		}
		return state.Checkpoint{}, false, dbError(err)
	}
	checkpoint, err := state.DecodeCheckpoint([]byte(raw))
	if err != nil {
		return state.Checkpoint{}, false, err
	}
	return checkpoint, true, nil
}

func (s *CheckpointStore) List(ctx context.Context, conversationID string, options state.ListOptions) ([]state.Checkpoint, error) {
	if conversationID != s.conversation {
		return nil, state.ErrInvalidCheckpoint
	}
	limit := options.Limit
	if limit == 0 {
		limit = 100
	}
	if limit < 1 || limit > 100 {
		return nil, state.ErrInvalidCheckpoint
	}
	rows, err := s.store.db.QueryContext(ctx, "SELECT checkpoint_json FROM checkpoints WHERE conversation_key=? AND owner_id=? AND project_id=? AND session_id=? AND turn_id=?", conversationID, s.scope.UserID, s.scope.ProjectID, s.sessionID, s.turnID)
	if err != nil {
		return nil, dbError(err)
	}
	defer rows.Close()
	items := make([]state.Checkpoint, 0)
	for rows.Next() {
		var raw string
		if err = rows.Scan(&raw); err != nil {
			return nil, dbError(err)
		}
		checkpoint, decodeErr := state.DecodeCheckpoint([]byte(raw))
		if decodeErr != nil {
			return nil, decodeErr
		}
		if options.Before != "" && checkpoint.ID == options.Before {
			continue
		}
		items = append(items, checkpoint)
	}
	if err = rows.Err(); err != nil {
		return nil, dbError(err)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Version > items[j].Version })
	if len(items) > limit {
		items = items[:limit]
	}
	return items, nil
}

func (s *CheckpointStore) Save(ctx context.Context, checkpoint state.Checkpoint) (state.Checkpoint, error) {
	if checkpoint.ConversationID != s.conversation {
		return state.Checkpoint{}, state.ErrInvalidCheckpoint
	}
	prepared, err := checkpoint.PersistClone()
	if err != nil {
		return state.Checkpoint{}, err
	}
	data, err := prepared.JSON()
	if err != nil {
		return state.Checkpoint{}, err
	}
	tx, err := s.store.db.BeginTx(ctx, nil)
	if err != nil {
		return state.Checkpoint{}, dbError(err)
	}
	defer tx.Rollback()
	var headID string
	var headVersion uint64
	err = tx.QueryRowContext(ctx, "SELECT checkpoint_id,version FROM checkpoint_heads WHERE conversation_key=?", s.conversation).Scan(&headID, &headVersion)
	if errors.Is(err, sql.ErrNoRows) {
		headID, headVersion = "", 0
	} else if err != nil {
		return state.Checkpoint{}, dbError(err)
	}
	if headVersion == 0 {
		if prepared.ID != "" || prepared.Version != 0 {
			return state.Checkpoint{}, &state.ConflictError{ConversationID: s.conversation, Expected: 0, Actual: headVersion}
		}
	} else if prepared.ID != headID || prepared.Version != headVersion {
		return state.Checkpoint{}, &state.ConflictError{ConversationID: s.conversation, Expected: prepared.Version, Actual: headVersion}
	}
	prepared.ID = state.NewID()
	prepared.ParentID = headID
	prepared.Version = headVersion + 1
	prepared.SchemaVersion = state.CurrentCheckpointSchemaVersion
	data, err = prepared.JSON()
	if err != nil {
		return state.Checkpoint{}, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO checkpoints(id,owner_id,project_id,session_id,turn_id,conversation_key,version,parent_id,checkpoint_json) VALUES (?,?,?,?,?,?,?,?,?)`, prepared.ID, s.scope.UserID, s.scope.ProjectID, s.sessionID, s.turnID, s.conversation, prepared.Version, prepared.ParentID, string(data)); err != nil {
		return state.Checkpoint{}, dbError(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO checkpoint_heads(conversation_key,checkpoint_id,version) VALUES (?,?,?) ON CONFLICT(conversation_key) DO UPDATE SET checkpoint_id=excluded.checkpoint_id,version=excluded.version`, s.conversation, prepared.ID, prepared.Version); err != nil {
		return state.Checkpoint{}, dbError(err)
	}
	if err = tx.Commit(); err != nil {
		return state.Checkpoint{}, dbError(err)
	}
	return prepared, nil
}

func (s *CheckpointStore) Delete(ctx context.Context, conversationID string) error {
	if conversationID != s.conversation {
		return state.ErrInvalidCheckpoint
	}
	_, err := s.store.db.ExecContext(ctx, "DELETE FROM checkpoints WHERE conversation_key=? AND owner_id=? AND project_id=? AND session_id=? AND turn_id=?", conversationID, s.scope.UserID, s.scope.ProjectID, s.sessionID, s.turnID)
	if err != nil {
		return dbError(err)
	}
	_, err = s.store.db.ExecContext(ctx, "DELETE FROM checkpoint_heads WHERE conversation_key=?", conversationID)
	return dbError(err)
}

var _ state.Store = (*CheckpointStore)(nil)
