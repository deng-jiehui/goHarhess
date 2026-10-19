CREATE TABLE IF NOT EXISTS papers (
    id TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    source_key TEXT NOT NULL,
    title TEXT NOT NULL,
    authors_json TEXT NOT NULL DEFAULT '[]',
    year INTEGER NOT NULL DEFAULT 0,
    doi TEXT NOT NULL DEFAULT '',
    arxiv_id TEXT NOT NULL DEFAULT '',
    url TEXT NOT NULL DEFAULT '',
    abstract TEXT NOT NULL DEFAULT '',
    tags_json TEXT NOT NULL DEFAULT '[]',
    note TEXT NOT NULL DEFAULT '',
    reading_state TEXT NOT NULL DEFAULT 'unread',
    revision INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    UNIQUE(project_id, source_key)
);

CREATE INDEX IF NOT EXISTS papers_project_created ON papers(project_id, owner_id, created_at DESC);
CREATE INDEX IF NOT EXISTS papers_project_year ON papers(project_id, owner_id, year);
