CREATE TABLE todos (
    id         INTEGER PRIMARY KEY,
    title      TEXT    NOT NULL CHECK (length(trim(title)) BETWEEN 1 AND 200),
    body       TEXT    NOT NULL DEFAULT '',
    status     TEXT    NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'in_progress', 'done')),
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    updated_at INTEGER NOT NULL DEFAULT (unixepoch())
) STRICT;

CREATE INDEX todos_status_updated_idx ON todos (status, updated_at DESC);

CREATE TRIGGER todos_touch AFTER UPDATE ON todos
WHEN NEW.updated_at = OLD.updated_at
BEGIN
    UPDATE todos SET updated_at = unixepoch() WHERE id = NEW.id;
END;

-- Keyword search (external-content FTS5)
CREATE VIRTUAL TABLE todos_fts USING fts5 (
    title, body, content='todos', content_rowid='id'
);

CREATE TRIGGER todos_ai AFTER INSERT ON todos BEGIN
    INSERT INTO todos_fts (rowid, title, body)
    VALUES (new.id, new.title, new.body);
END;

CREATE TRIGGER todos_ad AFTER DELETE ON todos BEGIN
    INSERT INTO todos_fts (todos_fts, rowid, title, body)
    VALUES ('delete', old.id, old.title, old.body);
END;

CREATE TRIGGER todos_au AFTER UPDATE ON todos BEGIN
    INSERT INTO todos_fts (todos_fts, rowid, title, body)
    VALUES ('delete', old.id, old.title, old.body);
    INSERT INTO todos_fts (rowid, title, body)
    VALUES (new.id, new.title, new.body);
END;
