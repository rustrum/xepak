# Xepak REST Example: Posts, Roles, and Payments

This example shows a small REST API built entirely from Xepak TOML specs and
SQLite.

It focuses on database-backed authentication, reusable pre-processors,
role-based authorization, rate limiting, and row-level visibility.
All data lives in SQLite, including users, API keys, roles, posts, and payments.

## What it demonstrates

- Database-backed API-key authentication with `storage_authentication`.
- Advanced pre-processors usage.
- Role based access via auth expressions as `manager OR admin`.
- Static SQL and dynamic Lua-built SQL.
- `data_script` endpoints that return data directly.
- Input schemas with required fields, type coercion, and validation.
- Pagination via `limit_offset`.
- Accessing custom registry values and secrets from Lua.
- Public reads and authenticated writes in the same HTTP API.

## Data model

| Table | Purpose |
|-------|---------|
| `users` | User accounts with username, email, and display name |
| `roles` | Role names: `admin`, `manager`, `poster` |
| `user_roles` | Many-to-many link between users and roles |
| `api_keys` | API keys linked to users; supports revocation |
| `posts` | Posts owned by users |
| `payments` | Payments linked to users, created by managers/admins |


## Demo users and API keys

| User | API key | Roles | Notes |
|------|---------|-------|-------|
| `admin` | `admin-key-demo` | `ADMIN` | Full access |
| `manager` | `manager-key-demo` | `MANAGER` | Can manage payments and delete posts |
| `alice` | `alice-key-demo` | `POSTER` | Regular user; sees only her payments |
| `bob` | `bob-key-demo` | `POSTER` | Regular user |
| `carol` | `carol-key-demo` | `POSTER`, `MANAGER` | Can post and manage payments |
| `dave` | `revoked-key-demo` | `POSTER` | Key is revoked; authentication fails |

Pass the key in the `x-api-key` header.

## Justfile tool

To execute commands defined in `justfile`
you have to install [just, a command runner](https://github.com/casey/just).

Run `just -l` within directory where `justfile` is located to list all available commands.

## Launching server 

From the example directory run `just run` or `cargo run --bin xepak -- ./xepak-rest.toml`.

This will start server on `http://localhost:8080`.

## Endpoints

| Method | URI | Auth | Purpose |
|--------|-----|------|---------|
| `GET` | `/posts` | Public | List posts with optional `author`, `q`, `limit`, `offset` |
| `GET` | `/posts/search` | Public | Case-insensitive `LIKE` search (relies on `case_sensitive_like` being OFF) |
| `GET` | `/posts/{id}` | Public | Get one post |
| `POST` | `/posts/create` | Any authenticated user | Create a post |
| `PUT`/`PATCH` | `/posts/{id}/update` | Owner, manager, or admin | Update a post once per 30 seconds |
| `DELETE` | `/posts/{id}/delete` | Manager or admin | Delete a post |
| `GET` | `/users/{id}` | Public | Public user profile |
| `GET` | `/users/{id}/posts` | Public | Posts by a user |
| `GET` | `/me` | Any authenticated user | Current user id and roles |
| `GET` | `/admin/users` | Admin only | List users and their roles |
| `GET` | `/payments` | Any authenticated user | Managers see all; others see their own |
| `GET` | `/payments/{id}` | Owner, manager, or admin | Get one payment |
| `POST` | `/payments/create` | Manager or admin | Create a payment |
| `GET` | `/payments/summary` | Any authenticated user | Payment totals by status |
| `GET` | `/demo/types` | Public | Show supported value types |
| `GET` | `/demo/echo` | Public | Schema coercion and strict input example |
| `GET` | `/demo/runtime` | Any authenticated user | Registry and secret access from Lua |


## Try it

The `justfile` contains a shortcut for every endpoint. Recipe arguments are
positional; optional arguments default to the demo API keys.

```bash
# Public reads
just list_posts
just get_post 1
just search_posts xepak
just user_profile 3
just user_posts 3

# Authentication
just me
just me manager-key-demo
just revoked_key_check

# Posts
just create_post "Hello" "Created from the REST example" alice-key-demo
just update_post 1 "" "Updated content" alice-key-demo
just update_post 1 "" "Updated again too soon" alice-key-demo
just delete_post 1 manager-key-demo
just delete_post 2 alice-key-demo

# Payments
just list_payments alice-key-demo
just list_payments manager-key-demo
just get_payment 1 alice-key-demo
just get_payment 3 alice-key-demo
just create_payment 3 5000 manager-key-demo USD pending "New payment"
just payment_summary alice-key-demo
just payment_summary manager-key-demo

# Admin and demos
just admin_users
just demo_types
just demo_echo hello 7
just demo_runtime
```

## How authentication works

`00_auth.toml` defines a shared `db_auth` pre-processor and registers it as the
default pre-processor for every endpoint:

```toml
default_pre_processors = [
    { type = "ref", id = "db_auth" },
]
```

`db_auth` runs a query against the `api_keys`, `users`, `user_roles`, and `roles`
tables. The query must return one row with `id` and a comma-separated `roles`
column. The processor then stores the user id and role set in the request input.

Public endpoints set `pre_processors_ignore_default = true`. Protected endpoints
keep the default and add `authorize` pre-processors such as `manager_only` or
`admin_only`.

## Post update rate limit

The `post_update_guard` Lua pre-processor loads the post owner and checks
`last_update_at`:

- If `last_update_at` is `NULL`, the first update is allowed.
- If less than 30 seconds have passed, the request fails with `400`.
- Managers and admins can update any post, subject to the same rate limit.
- Regular posters can update only their own posts.

On a successful update, the DSL query sets both `updated_at` and
`last_update_at` explicitly:

```sql
UPDATE posts
SET title = {{title}}, updated_at = unixepoch(), last_update_at = unixepoch()
WHERE id = {{post_id}}
```

## Payment visibility

- `POST /payments/create` requires `manager` or `admin`.
- `GET /payments` returns all rows for managers/admins and only the current
  user's rows for everyone else.
- `GET /payments/{id}` uses `payment_access_guard` to return `404` for missing
  payments and `403` when a regular user requests another user's payment.
- `GET /payments/summary` aggregates only the rows the caller is allowed to see.

