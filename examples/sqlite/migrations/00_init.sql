-- SQL to initialize test SQLite database
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    password TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    content TEXT NOT NULL
);


CREATE TABLE IF NOT EXISTS typecheck (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type_text TEXT,
    type_int INTEGER,
    type_real REAL,
    type_blob BLOB
);


