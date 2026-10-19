package domain

import "time"

// Turn 表示一次用户研究目标及其持久执行状态。
type Turn struct {
	ID                        string    `json:"id"`
	SessionID                 string    `json:"session_id"`
	ProjectID                 string    `json:"project_id"`
	OwnerID                   string    `json:"owner_id"`
	State                     string    `json:"state"`
	InputText                 string    `json:"input_text"`
	IdempotencyKey            string    `json:"idempotency_key"`
	Revision                  int64     `json:"revision"`
	Attempt                   int       `json:"attempt"`
	ContextJSON               []byte    `json:"context_json,omitempty"`
	DefinitionVersion         string    `json:"definition_version"`
	CancelRequested           bool      `json:"cancel_requested"`
	ErrorCode                 string    `json:"error_code,omitempty"`
	ErrorMessage              string    `json:"error_message,omitempty"`
	PendingQuestions          []byte    `json:"pending_questions_json,omitempty"`
	CanonicalMessageIDs       []string  `json:"canonical_message_ids,omitempty"`
	ProgressJSON              []byte    `json:"progress_json,omitempty"`
	ProgressCheckpointID      string    `json:"progress_checkpoint_id,omitempty"`
	ProgressCheckpointVersion uint64    `json:"progress_checkpoint_version,omitempty"`
	CreatedAt                 time.Time `json:"created_at"`
	UpdatedAt                 time.Time `json:"updated_at"`
}

const (
	TurnQueued           = "queued"
	TurnRunning          = "running"
	TurnAwaitingInput    = "awaiting_input"
	TurnRecoveryRequired = "recovery_required"
	TurnCompleted        = "completed"
	TurnFailed           = "failed"
	TurnCancelled        = "cancelled"
)

func (t Turn) Active() bool {
	return t.State == TurnQueued || t.State == TurnRunning || t.State == TurnAwaitingInput || t.State == TurnRecoveryRequired
}

type TurnAction struct {
	ID             string    `json:"id"`
	TurnID         string    `json:"turn_id"`
	Kind           string    `json:"kind"`
	IdempotencyKey string    `json:"idempotency_key"`
	RequestHash    string    `json:"request_hash"`
	PayloadJSON    []byte    `json:"payload_json,omitempty"`
	State          string    `json:"state"`
	CreatedAt      time.Time `json:"created_at"`
}

type MessageRecord struct {
	ID          string `json:"id"`
	SessionID   string `json:"session_id"`
	TurnID      string `json:"turn_id"`
	Attempt     int    `json:"attempt"`
	Seq         int64  `json:"seq"`
	ContentJSON []byte `json:"content_json"`
	Committed   bool   `json:"committed"`
	Superseded  bool   `json:"superseded"`
}

type Note struct {
	ID              string    `json:"id"`
	ProjectID       string    `json:"project_id"`
	Text            string    `json:"text"`
	SourceSessionID string    `json:"source_session_id"`
	SourceTurnID    string    `json:"source_turn_id"`
	ToolKey         string    `json:"tool_key"`
	RequestHash     string    `json:"request_hash"`
	CreatedAt       time.Time `json:"created_at"`
}

// MaxNoteRunes 是项目笔记允许保存的最大字符数。
const MaxNoteRunes = 4000

type SessionEvent struct {
	ID                 int64  `json:"id"`
	SessionID          string `json:"session_id"`
	TurnID             string `json:"turn_id"`
	Attempt            int    `json:"attempt"`
	Kind               string `json:"kind"`
	PayloadJSON        []byte `json:"payload_json"`
	StreamRunID        string `json:"stream_run_id,omitempty"`
	Sequence           uint64 `json:"sequence,omitempty"`
	SourceCheckpointID string `json:"source_checkpoint_id,omitempty"`
}

type TurnSnapshot struct {
	Turn     Turn            `json:"turn"`
	Messages []MessageRecord `json:"messages"`
	Notes    []Note          `json:"notes"`
	Events   []SessionEvent  `json:"events"`
}
