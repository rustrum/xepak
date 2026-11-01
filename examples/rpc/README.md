# JSON-RPC Todo Example

This example exposes a small todo application over **JSON-RPC 2.0** 
which is served via `http://localhost:8080/rpc`.

## What it demonstrates

- JSON-RPC methods backed by static SQL and Lua-built SQL.
- Input schemas with type coercion, required fields, and validation.
- Optional filters and validated pagination via `limit_offset`.
- A shared Lua pre-processor for status validation.
- Config-based API-key authentication with role-based authorization.
- Exact-phrase search with plain-text input.

## Justfile tool

To execute commands defined in `justfile`
you have to install [just, a command runner](https://github.com/casey/just).

Run `just -l` within directory where `justfile` is located to list all available commands.

## Launching server 

From the example directory run `just run` or `cargo run --bin xepak -- ./xepak-rpc.toml`.

This will start server on `http://localhost:8080`.

## RPC Methods

| Method | Params | Result |
|--------|--------|--------|
| `list_todos` | `status?`, `limit?`, `offset?` | Array of todos |
| `get_todo` | `id` | One todo |
| `create_todo` | `title`, `body?`, `status?` | Created todo |
| `update_todo` | `id`, `title?`, `body?`, `status?` | Updated todo |
| `delete_todo` | `id` + admin API key | Deleted todo |
| `search_todos` | `query`, `status?`, `limit?`, `offset?` | Array of matching todos |

- `status` must be one of `open`, `in_progress`, or `done`.
- `limit` defaults to `50` and is capped at `50`; `offset` defaults to `0`.
- `create_todo` and `update_todo` return the row produced by `RETURNING`.
- `delete_todo` requires the `x-api-key` header with a key that has the
  `admin` role. All other methods do not require authentication.

The `justfile` provides a shortcut to call each JSON-RPC method:

```bash
just create_todo "Buy milk" "Two liters"
just list_todos
just list_todos in_progress
just get_todo 1
just update_todo 1 "" "" done
just search_todos "buy milk"
just delete_todo 1 admin-api-key
```

`delete_todo` accepts the API key as its second argument (it defaults to the
admin key):

```bash
just delete_todo 1 admin-api-key   # succeeds, "admin-api-key" has the admin role
just delete_todo 1 user-api-key    # forbidden, "user-api-key" has no admin role
```

## Authentication and authorization

The example uses configuration-based authentication. The keys are defined in
the `[registry]` section of [`xepak-rpc.toml`](xepak-rpc.toml):

| API key | User ID | Roles |
|---------|---------|-------|
| `admin-api-key` | `admin` | `admin` |
| `user-api-key` | `user` | `user` |

The keys are intentionally trivial, and they are sent in the `x-api-key` HTTP
header (or as `Authorization: Bearer <key>`):

```bash
curl -sS -X POST http://localhost:8080/rpc \
  -H "Content-Type: application/json" \
  -H "x-api-key: admin-api-key" \
  -d '{"jsonrpc":"2.0","id":1,"method":"delete_todo","params":{"id":1}}'
```

## Search semantics

`search_todos` performs an FTS5 exact-phrase search across `title` and `body`.
We just handle input as exact phrase match for simplicity to avoid complex FTS5 query syntax.

