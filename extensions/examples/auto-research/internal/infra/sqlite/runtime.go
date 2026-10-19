package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/goharness/autoresearch/internal/domain"
)

func hashBytes(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

func scanTurn(row scanner) (domain.Turn, error) {
	var t domain.Turn
	var contextJSON, pending, canonical, progress string
	var created, updated string
	var cancel int
	if err := row.Scan(&t.ID, &t.OwnerID, &t.ProjectID, &t.SessionID, &t.State, &t.InputText,
		&t.IdempotencyKey, &t.Revision, &t.Attempt, &contextJSON, &t.DefinitionVersion, &cancel,
		&t.ErrorCode, &t.ErrorMessage, &pending, &canonical, &progress, &t.ProgressCheckpointID,
		&t.ProgressCheckpointVersion, &created, &updated); err != nil {
		return t, dbError(err)
	}
	t.CancelRequested = cancel != 0
	t.ContextJSON = []byte(contextJSON)
	t.PendingQuestions = []byte(pending)
	t.ProgressJSON = []byte(progress)
	_ = json.Unmarshal([]byte(canonical), &t.CanonicalMessageIDs)
	t.CreatedAt = parseSQLiteTime(created)
	t.UpdatedAt = parseSQLiteTime(updated)
	return t, nil
}

const turnColumns = `id,owner_id,project_id,session_id,state,input_text,idempotency_key,revision,attempt,
context_json,definition_version,cancel_requested,error_code,error_message,pending_questions_json,
canonical_message_ids_json,progress_json,progress_checkpoint_id,progress_checkpoint_version,created_at,updated_at`

func parseSQLiteTime(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	parsed, _ := time.Parse("2006-01-02 15:04:05", value)
	return parsed
}

func (s *Store) AdmitTurn(ctx context.Context, scope domain.Scope, sessionID string, draft domain.TurnDraft) (domain.AdmitResult, error) {
	if strings.TrimSpace(draft.InputText) == "" || strings.TrimSpace(draft.IdempotencyKey) == "" {
		return domain.AdmitResult{}, fmt.Errorf("%w：消息和幂等键不能为空", domain.ErrInvalid)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.AdmitResult{}, dbError(err)
	}
	defer tx.Rollback()
	row := tx.QueryRowContext(ctx, "SELECT "+turnColumns+" FROM turns WHERE session_id=? AND idempotency_key=?", sessionID, draft.IdempotencyKey)
	if value, scanErr := scanTurn(row); scanErr == nil {
		if value.InputText != draft.InputText {
			return domain.AdmitResult{}, domain.ErrConflict
		}
		return domain.AdmitResult{Turn: value, Replayed: true}, nil
	} else if !errors.Is(scanErr, domain.ErrNotFound) {
		return domain.AdmitResult{}, scanErr
	}
	var projectID string
	if err = tx.QueryRowContext(ctx, "SELECT project_id FROM sessions WHERE id=? AND project_id=? AND EXISTS (SELECT 1 FROM projects WHERE id=? AND owner_id=?)", sessionID, scope.ProjectID, scope.ProjectID, scope.UserID).Scan(&projectID); err != nil {
		return domain.AdmitResult{}, dbError(err)
	}
	var active int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM turns WHERE session_id=? AND state IN ('queued','running','awaiting_input','recovery_required')", sessionID).Scan(&active); err != nil {
		return domain.AdmitResult{}, dbError(err)
	}
	if active != 0 {
		return domain.AdmitResult{}, domain.ErrBusy
	}
	turn := domain.Turn{ID: randomID(), OwnerID: scope.UserID, ProjectID: projectID, SessionID: sessionID,
		State: domain.TurnQueued, InputText: draft.InputText, IdempotencyKey: draft.IdempotencyKey,
		Revision: 1, ContextJSON: append([]byte(nil), draft.ContextJSON...), DefinitionVersion: draft.DefinitionVersion}
	if len(turn.ContextJSON) == 0 {
		turn.ContextJSON = []byte(`{}`)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO turns(id,owner_id,project_id,session_id,state,input_text,idempotency_key,context_json,definition_version)
VALUES (?,?,?,?,?,?,?,?,?)`, turn.ID, turn.OwnerID, turn.ProjectID, turn.SessionID, turn.State, turn.InputText, turn.IdempotencyKey, turn.ContextJSON, turn.DefinitionVersion); err != nil {
		return domain.AdmitResult{}, dbError(err)
	}
	if err = tx.Commit(); err != nil {
		return domain.AdmitResult{}, dbError(err)
	}
	return domain.AdmitResult{Turn: turn}, nil
}

func randomID() string {
	return fmt.Sprintf("%d-%s", time.Now().UnixNano(), hashBytes([]byte(fmt.Sprintf("%d", time.Now().UnixNano())))[:12])
}

func (s *Store) GetTurn(ctx context.Context, scope domain.Scope, sessionID, turnID string) (domain.Turn, error) {
	return scanTurn(s.db.QueryRowContext(ctx, "SELECT "+turnColumns+" FROM turns WHERE id=? AND session_id=? AND project_id=? AND owner_id=?", turnID, sessionID, scope.ProjectID, scope.UserID))
}

func (s *Store) ListTurns(ctx context.Context, scope domain.Scope, sessionID string, limit int) ([]domain.Turn, error) {
	if limit < 1 || limit > 100 {
		return nil, domain.ErrInvalid
	}
	rows, err := s.db.QueryContext(ctx, "SELECT "+turnColumns+" FROM turns WHERE session_id=? AND project_id=? AND owner_id=? ORDER BY rowid DESC LIMIT ?", sessionID, scope.ProjectID, scope.UserID, limit)
	if err != nil {
		return nil, dbError(err)
	}
	defer rows.Close()
	items := make([]domain.Turn, 0, limit)
	for rows.Next() {
		item, scanErr := scanTurn(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		items = append(items, item)
	}
	return items, dbError(rows.Err())
}

func (s *Store) ClaimQueued(ctx context.Context) (domain.Turn, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Turn{}, false, dbError(err)
	}
	defer tx.Rollback()
	var id string
	if err = tx.QueryRowContext(ctx, "SELECT id FROM turns WHERE state='queued' ORDER BY rowid LIMIT 1").Scan(&id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.Turn{}, false, nil
		}
		return domain.Turn{}, false, dbError(err)
	}
	if _, err = tx.ExecContext(ctx, "UPDATE turns SET state='running',attempt=attempt+1,revision=revision+1,updated_at=CURRENT_TIMESTAMP WHERE id=? AND state='queued'", id); err != nil {
		return domain.Turn{}, false, dbError(err)
	}
	row := tx.QueryRowContext(ctx, "SELECT "+turnColumns+" FROM turns WHERE id=?", id)
	turn, err := scanTurn(row)
	if err != nil {
		return domain.Turn{}, false, err
	}
	if err = tx.Commit(); err != nil {
		return domain.Turn{}, false, dbError(err)
	}
	return turn, true, nil
}

func (s *Store) SetTurnState(ctx context.Context, scope domain.Scope, turnID, state, code, message string) error {
	result, err := s.db.ExecContext(ctx, `UPDATE turns SET state=?,error_code=?,error_message=?,revision=revision+1,updated_at=CURRENT_TIMESTAMP WHERE id=? AND project_id=? AND owner_id=?`, state, code, message, turnID, scope.ProjectID, scope.UserID)
	if err != nil {
		return dbError(err)
	}
	count, _ := result.RowsAffected()
	if count == 0 {
		return domain.ErrNotFound
	}
	return nil
}

func (s *Store) SetPendingQuestions(ctx context.Context, scope domain.Scope, turnID string, questions []byte) error {
	if !json.Valid(questions) {
		return domain.ErrInvalid
	}
	result, err := s.db.ExecContext(ctx, "UPDATE turns SET state='awaiting_input',pending_questions_json=?,revision=revision+1,updated_at=CURRENT_TIMESTAMP WHERE id=? AND project_id=? AND owner_id=?", questions, turnID, scope.ProjectID, scope.UserID)
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

func (s *Store) ApplyAction(ctx context.Context, scope domain.Scope, sessionID, turnID string, draft domain.ActionDraft) (domain.ActionResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.ActionResult{}, dbError(err)
	}
	defer tx.Rollback()
	var turn domain.Turn
	if turn, err = scanTurn(tx.QueryRowContext(ctx, "SELECT "+turnColumns+" FROM turns WHERE id=? AND session_id=? AND project_id=? AND owner_id=?", turnID, sessionID, scope.ProjectID, scope.UserID)); err != nil {
		return domain.ActionResult{}, err
	}
	var action domain.TurnAction
	var created string
	row := tx.QueryRowContext(ctx, "SELECT id,turn_id,kind,idempotency_key,request_hash,payload_json,state,created_at FROM turn_actions WHERE turn_id=? AND idempotency_key=?", turnID, draft.IdempotencyKey)
	if scanErr := row.Scan(&action.ID, &action.TurnID, &action.Kind, &action.IdempotencyKey, &action.RequestHash, &action.PayloadJSON, &action.State, &created); scanErr == nil {
		if action.RequestHash != draft.RequestHash {
			return domain.ActionResult{}, domain.ErrConflict
		}
		action.CreatedAt = parseSQLiteTime(created)
		return domain.ActionResult{Turn: turn, Action: action, Replayed: true}, nil
	} else if !errors.Is(scanErr, sql.ErrNoRows) {
		return domain.ActionResult{}, dbError(scanErr)
	}
	if draft.Revision > 0 && draft.Revision != turn.Revision {
		return domain.ActionResult{}, domain.ErrConflict
	}
	if turn.State == domain.TurnCompleted || turn.State == domain.TurnCancelled {
		return domain.ActionResult{Turn: turn}, nil
	}
	action = domain.TurnAction{ID: randomID(), TurnID: turnID, Kind: draft.Kind, IdempotencyKey: draft.IdempotencyKey, RequestHash: draft.RequestHash, PayloadJSON: append([]byte(nil), draft.PayloadJSON...), State: "accepted", CreatedAt: time.Now()}
	if _, err = tx.ExecContext(ctx, `INSERT INTO turn_actions(id,turn_id,kind,idempotency_key,request_hash,payload_json,state) VALUES (?,?,?,?,?,?,?)`, action.ID, action.TurnID, action.Kind, action.IdempotencyKey, action.RequestHash, action.PayloadJSON, action.State); err != nil {
		return domain.ActionResult{}, dbError(err)
	}
	nextState := domain.TurnQueued
	if draft.Kind == "cancel" {
		nextState = domain.TurnCancelled
	}
	if _, err = tx.ExecContext(ctx, "UPDATE turns SET state=?,cancel_requested=?,pending_questions_json='[]',revision=revision+1,updated_at=CURRENT_TIMESTAMP WHERE id=?", nextState, boolInt(draft.Kind == "cancel"), turnID); err != nil {
		return domain.ActionResult{}, dbError(err)
	}
	turn.State = nextState
	turn.CancelRequested = draft.Kind == "cancel"
	turn.Revision++
	if err = tx.Commit(); err != nil {
		return domain.ActionResult{}, dbError(err)
	}
	return domain.ActionResult{Turn: turn, Action: action}, nil
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func (s *Store) ListActions(ctx context.Context, scope domain.Scope, sessionID, turnID string) ([]domain.TurnAction, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT a.id,a.turn_id,a.kind,a.idempotency_key,a.request_hash,a.payload_json,a.state,a.created_at
FROM turn_actions a JOIN turns t ON t.id=a.turn_id
WHERE a.turn_id=? AND t.session_id=? AND t.project_id=? AND t.owner_id=? ORDER BY a.rowid`, turnID, sessionID, scope.ProjectID, scope.UserID)
	if err != nil {
		return nil, dbError(err)
	}
	defer rows.Close()
	items := make([]domain.TurnAction, 0)
	for rows.Next() {
		var item domain.TurnAction
		var created string
		if err = rows.Scan(&item.ID, &item.TurnID, &item.Kind, &item.IdempotencyKey, &item.RequestHash, &item.PayloadJSON, &item.State, &created); err != nil {
			return nil, dbError(err)
		}
		item.CreatedAt = parseSQLiteTime(created)
		items = append(items, item)
	}
	return items, dbError(rows.Err())
}

func (s *Store) SaveMessages(ctx context.Context, scope domain.Scope, sessionID string, attempt int, records []domain.MessageRecord) error {
	if len(records) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return dbError(err)
	}
	defer tx.Rollback()
	var next int64
	if err = tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(seq),0)+1 FROM messages WHERE session_id=?", sessionID).Scan(&next); err != nil {
		return dbError(err)
	}
	for _, record := range records {
		if record.ID == "" {
			record.ID = randomID()
		}
		if _, err = tx.ExecContext(ctx, `INSERT OR REPLACE INTO messages(id,owner_id,project_id,session_id,turn_id,attempt,seq,content_json,committed,superseded) VALUES (?,?,?,?,?,?,?,?,?,?)`, record.ID, scope.UserID, scope.ProjectID, sessionID, record.TurnID, attempt, next, record.ContentJSON, boolInt(record.Committed), boolInt(record.Superseded)); err != nil {
			return dbError(err)
		}
		next++
	}
	return dbError(tx.Commit())
}

func (s *Store) ListMessages(ctx context.Context, scope domain.Scope, sessionID string, limit int) ([]domain.MessageRecord, error) {
	if limit < 1 || limit > 100 {
		return nil, domain.ErrInvalid
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,session_id,turn_id,attempt,seq,content_json,committed,superseded FROM messages WHERE session_id=? AND project_id=? AND owner_id=? ORDER BY seq LIMIT ?`, sessionID, scope.ProjectID, scope.UserID, limit)
	if err != nil {
		return nil, dbError(err)
	}
	defer rows.Close()
	items := make([]domain.MessageRecord, 0, limit)
	for rows.Next() {
		var item domain.MessageRecord
		var committed, superseded int
		if err = rows.Scan(&item.ID, &item.SessionID, &item.TurnID, &item.Attempt, &item.Seq, &item.ContentJSON, &committed, &superseded); err != nil {
			return nil, dbError(err)
		}
		item.Committed = committed != 0
		item.Superseded = superseded != 0
		items = append(items, item)
	}
	return items, dbError(rows.Err())
}

func (s *Store) SaveNote(ctx context.Context, scope domain.Scope, note domain.Note) (domain.Note, error) {
	runeCount := len([]rune(note.Text))
	if runeCount < 1 {
		return domain.Note{}, fmt.Errorf("%w：项目笔记不能为空", domain.ErrInvalid)
	}
	if runeCount > domain.MaxNoteRunes {
		return domain.Note{}, fmt.Errorf("%w：项目笔记超过 %d 个字符（当前 %d 个）", domain.ErrInvalid, domain.MaxNoteRunes, runeCount)
	}
	if note.ToolKey == "" {
		return domain.Note{}, fmt.Errorf("%w：项目笔记幂等键不能为空", domain.ErrInvalid)
	}
	if note.ID == "" {
		note.ID = randomID()
	}
	if note.RequestHash == "" {
		note.RequestHash = hashBytes([]byte(note.Text))
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO notes(id,owner_id,project_id,source_session_id,source_turn_id,text,tool_key,request_hash) VALUES (?,?,?,?,?,?,?,?)`, note.ID, scope.UserID, scope.ProjectID, note.SourceSessionID, note.SourceTurnID, note.Text, note.ToolKey, note.RequestHash)
	if err == nil {
		_ = result
		return note, nil
	}
	var existing domain.Note
	var created string
	var deleted sql.NullString
	scanErr := s.db.QueryRowContext(ctx, `SELECT id,project_id,source_session_id,source_turn_id,text,tool_key,request_hash,created_at,deleted_at FROM notes WHERE project_id=? AND owner_id=? AND tool_key=?`, scope.ProjectID, scope.UserID, note.ToolKey).Scan(&existing.ID, &existing.ProjectID, &existing.SourceSessionID, &existing.SourceTurnID, &existing.Text, &existing.ToolKey, &existing.RequestHash, &created, &deleted)
	if scanErr != nil {
		return domain.Note{}, dbError(err)
	}
	if deleted.Valid {
		return domain.Note{}, fmt.Errorf("%w：该笔记已被删除，旧轮重试不能重新保存，请在新消息中提出保存要求", domain.ErrConflict)
	}
	existing.CreatedAt = parseSQLiteTime(created)
	if existing.Text != note.Text {
		return domain.Note{}, domain.ErrConflict
	}
	return existing, nil
}

func (s *Store) ListNotes(ctx context.Context, scope domain.Scope, limit int) ([]domain.Note, error) {
	if limit < 1 || limit > 100 {
		return nil, domain.ErrInvalid
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,project_id,source_session_id,source_turn_id,text,tool_key,request_hash,created_at FROM notes WHERE project_id=? AND owner_id=? AND deleted_at IS NULL ORDER BY rowid DESC LIMIT ?`, scope.ProjectID, scope.UserID, limit)
	if err != nil {
		return nil, dbError(err)
	}
	defer rows.Close()
	items := make([]domain.Note, 0, limit)
	for rows.Next() {
		var item domain.Note
		var created string
		if err = rows.Scan(&item.ID, &item.ProjectID, &item.SourceSessionID, &item.SourceTurnID, &item.Text, &item.ToolKey, &item.RequestHash, &created); err != nil {
			return nil, dbError(err)
		}
		item.CreatedAt = parseSQLiteTime(created)
		items = append(items, item)
	}
	return items, dbError(rows.Err())
}

func (s *Store) DeleteNote(ctx context.Context, scope domain.Scope, noteID string) error {
	result, err := s.db.ExecContext(ctx, `UPDATE notes SET deleted_at=COALESCE(deleted_at,CURRENT_TIMESTAMP) WHERE id=? AND project_id=? AND owner_id=?`, noteID, scope.ProjectID, scope.UserID)
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

func (s *Store) AppendEvent(ctx context.Context, scope domain.Scope, event domain.SessionEvent) (domain.SessionEvent, error) {
	result, err := s.db.ExecContext(ctx, `INSERT INTO events(owner_id,project_id,session_id,turn_id,attempt,kind,payload_json,stream_run_id,sequence,source_checkpoint_id) VALUES (?,?,?,?,?,?,?,?,?,?)`, scope.UserID, scope.ProjectID, event.SessionID, event.TurnID, event.Attempt, event.Kind, event.PayloadJSON, event.StreamRunID, event.Sequence, event.SourceCheckpointID)
	if err != nil {
		return event, dbError(err)
	}
	event.ID, _ = result.LastInsertId()
	return event, nil
}

func (s *Store) ListEvents(ctx context.Context, scope domain.Scope, sessionID string, after int64, limit int) ([]domain.SessionEvent, error) {
	if limit < 1 || limit > 100 {
		return nil, domain.ErrInvalid
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,session_id,turn_id,attempt,kind,payload_json,stream_run_id,sequence,source_checkpoint_id FROM events WHERE session_id=? AND project_id=? AND owner_id=? AND id>? ORDER BY id LIMIT ?`, sessionID, scope.ProjectID, scope.UserID, after, limit)
	if err != nil {
		return nil, dbError(err)
	}
	defer rows.Close()
	items := make([]domain.SessionEvent, 0, limit)
	for rows.Next() {
		var item domain.SessionEvent
		if err = rows.Scan(&item.ID, &item.SessionID, &item.TurnID, &item.Attempt, &item.Kind, &item.PayloadJSON, &item.StreamRunID, &item.Sequence, &item.SourceCheckpointID); err != nil {
			return nil, dbError(err)
		}
		items = append(items, item)
	}
	return items, dbError(rows.Err())
}

// listRecentEvents 返回最近的事件，但恢复时间顺序，供快照展示完整的收尾事件。
// 普通 ListEvents 仍按 id 递增读取，用于 SSE 的 after 游标。
func (s *Store) listRecentEvents(ctx context.Context, scope domain.Scope, sessionID string, limit int) ([]domain.SessionEvent, error) {
	if limit < 1 || limit > 100 {
		return nil, domain.ErrInvalid
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,session_id,turn_id,attempt,kind,payload_json,stream_run_id,sequence,source_checkpoint_id FROM events WHERE session_id=? AND project_id=? AND owner_id=? ORDER BY id DESC LIMIT ?`, sessionID, scope.ProjectID, scope.UserID, limit)
	if err != nil {
		return nil, dbError(err)
	}
	defer rows.Close()
	items := make([]domain.SessionEvent, 0, limit)
	for rows.Next() {
		var item domain.SessionEvent
		if err = rows.Scan(&item.ID, &item.SessionID, &item.TurnID, &item.Attempt, &item.Kind, &item.PayloadJSON, &item.StreamRunID, &item.Sequence, &item.SourceCheckpointID); err != nil {
			return nil, dbError(err)
		}
		items = append(items, item)
	}
	if err = rows.Err(); err != nil {
		return nil, dbError(err)
	}
	for left, right := 0, len(items)-1; left < right; left, right = left+1, right-1 {
		items[left], items[right] = items[right], items[left]
	}
	return items, nil
}

func (s *Store) Snapshot(ctx context.Context, scope domain.Scope, sessionID string, limit int) (domain.TurnSnapshot, error) {
	turns, err := s.ListTurns(ctx, scope, sessionID, 1)
	if err != nil {
		return domain.TurnSnapshot{}, err
	}
	if len(turns) == 0 {
		return domain.TurnSnapshot{}, domain.ErrNotFound
	}
	messages, err := s.ListMessages(ctx, scope, sessionID, limit)
	if err != nil {
		return domain.TurnSnapshot{}, err
	}
	notes, err := s.ListNotes(ctx, scope, 20)
	if err != nil {
		return domain.TurnSnapshot{}, err
	}
	events, err := s.listRecentEvents(ctx, scope, sessionID, 100)
	if err != nil {
		return domain.TurnSnapshot{}, err
	}
	return domain.TurnSnapshot{Turn: turns[0], Messages: messages, Notes: notes, Events: events}, nil
}

var _ domain.RuntimeRepository = (*Store)(nil)
