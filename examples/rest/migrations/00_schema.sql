-- Core schema for the REST example.

CREATE TABLE users (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    username     TEXT NOT NULL UNIQUE,
    email        TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    created_at   INTEGER NOT NULL DEFAULT (unixepoch())
) STRICT;

CREATE TABLE roles (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL DEFAULT ''
) STRICT;

CREATE TABLE user_roles (
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role_id INTEGER NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
    PRIMARY KEY (user_id, role_id)
) STRICT;

-- API keys are stored separately so a user can have several keys and revoke
-- individual keys without changing the user account.
CREATE TABLE api_keys (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    api_key    TEXT NOT NULL UNIQUE,
    label      TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    revoked_at INTEGER
) STRICT;

CREATE INDEX api_keys_user_idx ON api_keys(user_id);

CREATE TABLE posts (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id        INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    title          TEXT NOT NULL CHECK (length(trim(title)) BETWEEN 3 AND 200),
    content        TEXT NOT NULL DEFAULT '',
    created_at     INTEGER NOT NULL DEFAULT (unixepoch()),
    updated_at     INTEGER NOT NULL DEFAULT (unixepoch()),
    -- NULL means the post has never been updated. This lets the first update
    -- succeed immediately while later updates are rate-limited.
    last_update_at INTEGER
) STRICT;

CREATE INDEX posts_user_created_idx ON posts(user_id, created_at DESC);
CREATE INDEX posts_created_idx ON posts(created_at DESC);

CREATE TABLE payments (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id      INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    amount_cents INTEGER NOT NULL CHECK (amount_cents > 0),
    currency     TEXT NOT NULL DEFAULT 'USD' CHECK (length(currency) = 3),
    status       TEXT NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending', 'paid', 'failed', 'refunded')),
    description  TEXT NOT NULL DEFAULT '',
    created_by   INTEGER NOT NULL REFERENCES users(id),
    created_at   INTEGER NOT NULL DEFAULT (unixepoch())
) STRICT;

CREATE INDEX payments_user_created_idx ON payments(user_id, created_at DESC);
CREATE INDEX payments_status_idx ON payments(status);
