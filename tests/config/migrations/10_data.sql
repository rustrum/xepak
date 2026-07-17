INSERT INTO alltypes
(type_text, type_int, type_real, type_blob)
VALUES
(NULL, NULL, NULL, NULL),
('This is TEXT',  42, 2.2, X'2a'),
('Third record',  33, 3.3, X'2b'),
('Fourth record', NULL, 4.4, NULL),
(NULL, 55, NULL, X'2d'),
('6 record', 66, 6.6, NULL),
('7 record', 77, 7.7, NULL),
('8 record', 88, 8.8, NULL);


INSERT INTO users
(username, password)
VALUES 
('user1', 'password1'),
('user2', 'password2'),
('user3', 'password3');


INSERT INTO posts
(user_id, title, content)
VALUES 
(1, 'Post from user1', 'user1 is cool agent'),
(2, 'Post from user2', 'user2 is even better AI agent'),
(3, 'Post from user3', 'user3 is better than all of you');
