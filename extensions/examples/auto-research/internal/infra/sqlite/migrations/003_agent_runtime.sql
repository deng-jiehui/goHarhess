CREATE TABLE IF NOT EXISTS turns (
    id TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    state TEXT NOT NULL,
    input_text TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 1,
    attempt INTEGER NOT NULL DEFAULT 0,
    context_json TEXT NOT NULL,
    definition_version TEXT NOT NULL,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    error_code TEXT NOT NULL DEFAULT '',
    error_message TEXT NOT NULL DEFAULT '',
    pending_questions_json TEXT NOT NULL DEFAULT '[]',
    canonical_message_ids_json TEXT NOT NULL DEFAULT '[]',
    progress_json TEXT NOT NULL DEFAULT '{}',
    progress_checkpoint_id TEXT NOT NULL DEFAULT '',
    progress_checkpoint_version INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE,
    UNIQUE(session_id, idempotency_key)
);

CREATE UNIQUE INDEX IF NOT EXISTS turns_one_active
ON turns(session_id)
WHERE state IN ('queued', 'running', 'awaiting_input', 'recovery_required');

CREATE INDEX IF NOT EXISTS turns_queue ON turns(state, created_at);

CREATE TABLE IF NOT EXISTS turn_actions (
    id TEXT PRIMARY KEY,
    turn_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (turn_id) REFERENCES turns(id) ON DELETE CASCADE,
    UNIQUE(turn_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    content_json TEXT NOT NULL,
    committed INTEGER NOT NULL DEFAULT 0,
    superseded INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (turn_id) REFERENCES turns(id) ON DELETE CASCADE,
    UNIQUE(session_id, seq)
);

CREATE INDEX IF NOT EXISTS messages_session_seq ON messages(session_id, seq);

CREATE TABLE IF NOT EXISTS notes (
    id TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    source_session_id TEXT NOT NULL,
    source_turn_id TEXT NOT NULL,
    text TEXT NOT NULL,
    tool_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    UNIQUE(project_id, tool_key)
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    stream_run_id TEXT NOT NULL DEFAULT '',
    sequence INTEGER NOT NULL DEFAULT 0,
    source_checkpoint_id TEXT NOT NULL DEFAULT '',
    FOREIGN KEY (turn_id) REFERENCES turns(id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS events_source_sequence
ON events(turn_id, attempt, stream_run_id, sequence)
WHERE stream_run_id <> '';

CREATE UNIQUE INDEX IF NOT EXISTS events_progress_checkpoint
ON events(turn_id, source_checkpoint_id)
WHERE kind = 'progress' AND source_checkpoint_id <> '';

CREATE TABLE IF NOT EXISTS checkpoints (
    id TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    conversation_key TEXT NOT NULL,
    version INTEGER NOT NULL,
    parent_id TEXT NOT NULL DEFAULT '',
    checkpoint_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (turn_id) REFERENCES turns(id) ON DELETE CASCADE,
    UNIQUE(conversation_key, version)
);

CREATE TABLE IF NOT EXISTS checkpoint_heads (
    conversation_key TEXT PRIMARY KEY,
    checkpoint_id TEXT NOT NULL,
    version INTEGER NOT NULL
);
