-- SQL to initialize test SQLite database
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    password TEXT NOT NULL
);

INSERT INTO users (username, password)
VALUES 
('user1', 'password1'),
('user2', 'password2'),
('user3', 'password3');

CREATE TABLE IF NOT EXISTS posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    content TEXT NOT NULL
    -- user_id INTEGER NOT NULL,
    -- FOREIGN KEY (user_id) REFERENCES users(id)
);

INSERT INTO posts (title, content)
VALUES 
('Post 1', 'Content for Post 1'),
('Post 2', 'Content for Post 2'),
('Post 3', 'Content for Post 3'),
('Post 4', 'Content for Post 4'),
('Post 5', 'Content for Post 5');