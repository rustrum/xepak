# JSON-RPC Todo Example

This example exposes a small todo application over **JSON-RPC 2.0** using Xepak.

There is one HTTP endpoint, `POST /rpc`, and every operation is a JSON-RPC method
declared in [`specs/rpc.toml`](specs/rpc.toml). All data is stored in SQLite using
created with [`migrations/*.sql`](./migrations).

## What it demonstrates

- JSON-RPC methods backed by static SQL and Lua-built SQL.
- Input schemas with type coercion, required fields, and validation.
- Optional filters and validated pagination via `limit_offset`.
- A shared Lua pre-processor for status validation.
- Exact-phrase search with plain-text input.

## Run the example

From this directory:

```bash
just run
```

Or run the binary directly:

```bash
cargo run --bin xepak -- ./xepak-rpc.toml
```

The server listens on `http://localhost:8080` by default. The SQLite database is
created at `db/xepak-rpc.sqlite3`, and migrations are applied automatically on
startup.

## RPC Methods

| Method | Params | Result |
|--------|--------|--------|
| `list_todos` | `status?`, `limit?`, `offset?` | Array of todos |
| `get_todo` | `id` | One todo |
| `create_todo` | `title`, `body?`, `status?` | Created todo |
| `update_todo` | `id`, `title?`, `body?`, `status?` | Updated todo |
| `delete_todo` | `id` | Deleted todo |
| `search_todos` | `query`, `status?`, `limit?`, `offset?` | Array of matching todos |

- `status` must be one of `open`, `in_progress`, or `done`.
- `limit` defaults to `50` and is capped at `50`; `offset` defaults to `0`.
- `create_todo` and `update_todo` return the row produced by `RETURNING`.

The `justfile` provides a shortcut to call each JSON-RPC method:

```bash
just create_todo "Buy milk" "Two liters"
just list_todos
just list_todos in_progress
just get_todo 1
just update_todo 1 "" "" done
just search_todos "buy milk"
just delete_todo 1
```

## Search semantics

`search_todos` performs an FTS5 exact-phrase search across `title` and `body`.
We just handle input as exact phrase match for simplicity to avoid complex FTS5 query syntax.

## Notes

- The RPC endpoint accepts HTTP `POST` with a JSON body only.
- RPC method input is always schema-checked strictly: unknown params are rejected.
- A missing record from `get_todo`, `update_todo`, or `delete_todo` is returned as an empty result instead of a protocol error.
