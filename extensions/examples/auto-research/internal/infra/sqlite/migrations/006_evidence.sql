CREATE TABLE IF NOT EXISTS source_documents (
    id TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    paper_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL DEFAULT '',
    text TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    locator TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    FOREIGN KEY (paper_id) REFERENCES papers(id) ON DELETE CASCADE,
    UNIQUE(project_id, paper_id, sha256, locator)
);

CREATE TABLE IF NOT EXISTS evidence (
    id TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    paper_id TEXT NOT NULL,
    document_id TEXT NOT NULL,
    field TEXT NOT NULL DEFAULT '',
    value TEXT NOT NULL DEFAULT '',
    quote TEXT NOT NULL,
    locator TEXT NOT NULL DEFAULT '',
    start_rune INTEGER NOT NULL,
    end_rune INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    FOREIGN KEY (paper_id) REFERENCES papers(id) ON DELETE CASCADE,
    FOREIGN KEY (document_id) REFERENCES source_documents(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS source_documents_paper ON source_documents(project_id, paper_id);
CREATE INDEX IF NOT EXISTS evidence_paper ON evidence(project_id, paper_id);
