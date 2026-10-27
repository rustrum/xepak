-- KNOWLEDGE TABLE: NOTES
-- Stores discrete pieces of text knowledge, facts, and observations.
CREATE TABLE IF NOT EXISTS notes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    project_name TEXT NOT NULL,

    title TEXT NOT NULL,
    content TEXT NOT NULL,

    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- index for tenant isolation and fast filtering
CREATE INDEX IF NOT EXISTS idx_notes_project ON notes(project_name);
CREATE INDEX IF NOT EXISTS idx_notes_project_date ON notes(project_name, created_at DESC);

-- WORKFLOW TABLE: ACTIONS
-- Tracks project actions, status updates, and temporal context.
CREATE TABLE IF NOT EXISTS actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    project_name TEXT NOT NULL,

    milestone TEXT NOT NULL,
    details TEXT,
    status TEXT NOT NULL DEFAULT 'in_progress' CHECK(status IN ('in_progress', 'completed', 'blocked', 'archived')),

    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- index for tenant isolation and status filtering
CREATE INDEX IF NOT EXISTS idx_actions_project ON actions(project_name);
CREATE INDEX IF NOT EXISTS idx_actions_project_status ON actions(project_name, status);

-- RAG RETRIEVAL LAYER: FTS5 (Full-Text Search)
-- Provides the full-text search via native SQLite capabilities

-- Virtual table for searching Notes
CREATE VIRTUAL TABLE IF NOT EXISTS notes_fts USING fts5(
    project_name UNINDEXED,
    title,
    content,
    content='notes',
    content_rowid='id',
    tokenize='porter unicode61'
);

-- Virtual table for searching Actions
CREATE VIRTUAL TABLE IF NOT EXISTS actions_fts USING fts5(
    project_name UNINDEXED,
    milestone,
    status,
    details,
    content='actions',
    content_rowid='id',
    tokenize="porter unicode61"
);

-- FTS SYNC TRIGGERS
-- Automatically keeps the FTS5 search indexes in sync with the main tables.

-- Triggers for Notes FTS
CREATE TRIGGER IF NOT EXISTS notes_ai AFTER INSERT ON notes BEGIN
    INSERT INTO notes_fts(rowid, project_name, title, content) 
    VALUES (new.id, new.project_name, new.title, new.content);
END;

CREATE TRIGGER IF NOT EXISTS notes_ad AFTER DELETE ON notes BEGIN
    INSERT INTO notes_fts(notes_fts, rowid, project_name, title, content)
    VALUES ('delete', old.id, old.project_name, old.title, old.content);
END;

CREATE TRIGGER IF NOT EXISTS notes_au AFTER UPDATE ON notes
FOR EACH ROW
-- to guard against cycle dependency with time update trigger
WHEN NEW.title        <> OLD.title
  OR NEW.content      <> OLD.content
  OR NEW.project_name <> OLD.project_name
BEGIN
    INSERT INTO notes_fts(notes_fts, rowid, project_name, title, content)
    VALUES ('delete', old.id, old.project_name, old.title, old.content);
    INSERT INTO notes_fts(rowid, project_name, title, content)
    VALUES (new.id, new.project_name, new.title, new.content);
END;

-- Triggers for Actions FTS
CREATE TRIGGER IF NOT EXISTS actions_ai AFTER INSERT ON actions BEGIN
    INSERT INTO actions_fts(rowid, project_name, milestone, details)
    VALUES (new.id, new.project_name, new.milestone, new.details);
END;

CREATE TRIGGER IF NOT EXISTS actions_ad AFTER DELETE ON actions BEGIN
    INSERT INTO actions_fts(actions_fts, rowid, project_name, milestone, details)
    VALUES ('delete', old.id, old.project_name, old.milestone, old.details);
END;

CREATE TRIGGER IF NOT EXISTS actions_au AFTER UPDATE ON actions
FOR EACH ROW
-- to guard against cycle dependency with time update trigger
WHEN NEW.milestone   <> OLD.milestone
  OR NEW.details     <> OLD.details
  OR NEW.project_name <> OLD.project_name
BEGIN
    INSERT INTO actions_fts(actions_fts, rowid, project_name, milestone, details)
    VALUES ('delete', old.id, old.project_name, old.milestone, old.details);
    INSERT INTO actions_fts(rowid, project_name, milestone, details) 
    VALUES (new.id, new.project_name, new.milestone, new.details);
END;

-- HELPER TRIGGER: Auto-update `updated_at` timestamps
CREATE TRIGGER IF NOT EXISTS trg_notes_updated_at AFTER UPDATE ON notes
FOR EACH ROW
    -- guard against cycle dependency
    WHEN NEW.updated_at = OLD.updated_at
BEGIN
    UPDATE notes SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_actions_updated_at AFTER UPDATE ON actions
FOR EACH ROW
    -- guard against cycle dependency
    WHEN NEW.updated_at = OLD.updated_at
BEGIN
    UPDATE actions SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;
