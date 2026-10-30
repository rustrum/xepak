-- Seed data for the REST example.
--
-- API keys are intentionally simple and predictable so the example can be
-- tried with curl. Never use keys like these in production.

INSERT INTO roles (name, description) VALUES
    ('ADMIN',   'Full access to the example API'),
    ('MANAGER', 'Can manage payments and delete posts'),
    ('POSTER',  'Can create posts and update own posts');

INSERT INTO users (username, email, display_name) VALUES
    ('admin',   'admin@example.com',   'Ada Admin'),
    ('manager', 'manager@example.com', 'Mia Manager'),
    ('alice',   'alice@example.com',   'Alice Poster'),
    ('bob',     'bob@example.com',     'Bob Poster'),
    ('carol',   'carol@example.com',   'Carol Manager'),
    ('dave',    'dave@example.com',    'Dave Revoked');

INSERT INTO user_roles (user_id, role_id)
SELECT u.id, r.id
FROM users u
JOIN roles r ON r.name = 'ADMIN'
WHERE u.username = 'admin';

INSERT INTO user_roles (user_id, role_id)
SELECT u.id, r.id
FROM users u
JOIN roles r ON r.name = 'MANAGER'
WHERE u.username = 'manager';

INSERT INTO user_roles (user_id, role_id)
SELECT u.id, r.id
FROM users u
JOIN roles r ON r.name = 'POSTER'
WHERE u.username IN ('alice', 'bob', 'carol', 'dave');

INSERT INTO user_roles (user_id, role_id)
SELECT u.id, r.id
FROM users u
JOIN roles r ON r.name = 'MANAGER'
WHERE u.username = 'carol';

INSERT INTO api_keys (user_id, api_key, label)
SELECT id, 'admin-key-demo', 'admin demo key' FROM users WHERE username = 'admin';

INSERT INTO api_keys (user_id, api_key, label)
SELECT id, 'manager-key-demo', 'manager demo key' FROM users WHERE username = 'manager';

INSERT INTO api_keys (user_id, api_key, label)
SELECT id, 'alice-key-demo',   'alice demo key'   FROM users WHERE username = 'alice';

INSERT INTO api_keys (user_id, api_key, label)
SELECT id, 'bob-key-demo',     'bob demo key'     FROM users WHERE username = 'bob';

INSERT INTO api_keys (user_id, api_key, label)
SELECT id, 'carol-key-demo',   'carol demo key'   FROM users WHERE username = 'carol';

-- Dave's key is revoked, so requests using it must fail authentication.
INSERT INTO api_keys (user_id, api_key, label, revoked_at)
SELECT id, 'revoked-key-demo', 'revoked demo key', unixepoch() - 86400
FROM users WHERE username = 'dave';

-- Posts are owned by users. `last_update_at` is NULL so the first update is
-- allowed immediately; later updates are rate-limited to one per 30 seconds.
INSERT INTO posts (user_id, title, content, created_at, updated_at, last_update_at)
SELECT id, 'Welcome to the Xepak REST example',
       'This post is public. Try the list, search, and detail endpoints without an API key.',
       unixepoch() - 86400, unixepoch() - 86400, NULL
FROM users WHERE username = 'alice';

INSERT INTO posts (user_id, title, content, created_at, updated_at, last_update_at)
SELECT id, 'Building APIs from TOML',
       'Most endpoints in this example are static SQL; a few use Lua to build dynamic queries.',
       unixepoch() - 72000, unixepoch() - 72000, NULL
FROM users WHERE username = 'bob';

INSERT INTO posts (user_id, title, content, created_at, updated_at, last_update_at)
SELECT id, 'Manager notes: payment workflow',
       'Managers can create payments. Posters can only see payments linked to their own account.',
       unixepoch() - 54000, unixepoch() - 54000, NULL
FROM users WHERE username = 'carol';

INSERT INTO posts (user_id, title, content, created_at, updated_at, last_update_at)
SELECT id, 'How to request a payment',
       'Send a request to a manager with the amount, currency, and a short description.',
       unixepoch() - 36000, unixepoch() - 36000, NULL
FROM users WHERE username = 'manager';

INSERT INTO posts (user_id, title, content, created_at, updated_at, last_update_at)
SELECT id, 'Example roles and API keys',
       'Use the keys from the README to try the authorization rules and the revoked-key failure.',
       unixepoch() - 18000, unixepoch() - 18000, NULL
FROM users WHERE username = 'admin';

-- Payments are linked to a user. `created_by` is the manager or admin who
-- created the record.
INSERT INTO payments (user_id, amount_cents, currency, status, description, created_by, created_at)
SELECT u.id, 2500, 'USD', 'paid', 'Monthly subscription', a.id, unixepoch() - 80000
FROM users u, users a
WHERE u.username = 'alice' AND a.username = 'manager';

INSERT INTO payments (user_id, amount_cents, currency, status, description, created_by, created_at)
SELECT u.id, 1200, 'USD', 'pending', 'Seat add-on', a.id, unixepoch() - 70000
FROM users u, users a
WHERE u.username = 'alice' AND a.username = 'carol';

INSERT INTO payments (user_id, amount_cents, currency, status, description, created_by, created_at)
SELECT u.id, 9900, 'USD', 'pending', 'Annual plan', a.id, unixepoch() - 60000
FROM users u, users a
WHERE u.username = 'bob' AND a.username = 'manager';

INSERT INTO payments (user_id, amount_cents, currency, status, description, created_by, created_at)
SELECT u.id, 5000, 'EUR', 'paid', 'Consulting hours', a.id, unixepoch() - 50000
FROM users u, users a
WHERE u.username = 'carol' AND a.username = 'admin';

INSERT INTO payments (user_id, amount_cents, currency, status, description, created_by, created_at)
SELECT u.id, 300, 'USD', 'refunded', 'Small refund', a.id, unixepoch() - 40000
FROM users u, users a
WHERE u.username = 'dave' AND a.username = 'manager';

INSERT INTO payments (user_id, amount_cents, currency, status, description, created_by, created_at)
SELECT u.id, 15000, 'USD', 'paid', 'Team training', a.id, unixepoch() - 30000
FROM users u, users a
WHERE u.username = 'manager' AND a.username = 'admin';
