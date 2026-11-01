# Xepak simple DSL example

A minimal REST API for notes defined entirely in TOML. It demonstrates the
core Xepak DSL concepts: endpoints, URI parameters, request schemas with
validation, SQL resources, pagination, and SQLite migrations.

The whole API is described in [`specs/notes.toml`](specs/notes.toml). Data is
stored in SQLite, and the schema is created by
[`migrations/00_init.sql`](migrations/00_init.sql).

## What it demonstrates

- Endpoint routing with URI parameters such as `/note/{note_id}`.
- HTTP method restrictions via `allow_methods`.
- Input schema validation (`required`, `not_null`, and text length ranges).
- Static SQL resources with strongly typed placeholders.
- Pagination through the `limit_offset` pre-processor.
- `single_record_response` for 404-style behavior when a note is missing.
- Applying SQLite migrations automatically on startup.

## Run the example

To run the `just` recipes, install [just, a command runner](https://github.com/casey/just).

From this directory:

```bash
just run
```

Or run the binary directly:

```bash
cargo run --bin xepak -- ./xepak.toml
```

The server listens on `http://localhost:8080` by default. The SQLite database
is created at `db/xepak.sqlite3`, and migrations are applied automatically on
startup. To start from an empty database, run `just clean` and start the
server again.

## Endpoints

| Method | URI | Params | Description |
|--------|-----|--------|-------------|
| `POST` | `/note/add` | `title`, `content` | Create a note |
| `GET` | `/note/{id}` | path `id` | Get one note |
| `GET` | `/note/list` | `limit?`, `offset?` | List notes |
| `GET` | `/note/search` | `query`, `limit?`, `offset?` | Search title/content with `LIKE` |
| `DELETE` | `/note/delete/{id}` | path `id` | Delete one note |

`limit` defaults to `50` and is capped at `50`; `offset` defaults to `0`.
`get_note` returns HTTP `404` when the note does not exist. `delete_note` is not
a single-record endpoint, so deleting a missing note returns an empty array
instead of a `404`. Request validation failures return HTTP `400` with a JSON
error object.

## Use the example

The `justfile` wraps the most common requests:

```bash
# Create notes
just add_note "Shopping list" "Milk, eggs, bread"
just add_note "Work" "Finish the report"

# Read and paginate
just get_note 1
just list_notes
just list_notes 1 1        # limit=1 offset=1

# Search title and content (case-insensitive substring match)
just search_notes milk
just search_notes report

# Delete
just delete_note 2

# Reset the local database (stop the server first)
just clean
```

The same requests with `curl`:

```bash
curl -sS -X POST http://localhost:8080/note/add \
  -H "Content-Type: application/json" \
  -d '{"title":"Shopping list","content":"Milk, eggs, bread"}'

curl -sS http://localhost:8080/note/list?limit=10&offset=0
curl -sS http://localhost:8080/note/1
curl -sS --get http://localhost:8080/note/search \
  --data-urlencode "query=milk"
curl -sS -X DELETE http://localhost:8080/note/delete/1
```

Use the `server` variable to target another host or port:

```bash
just server=http://localhost:9090 list_notes
```

## How the DSL is wired

Each `[[endpoint]]` in `specs/notes.toml` combines four building blocks:

1. **Routing** - `uri` defines the path and URI parameters. For example,
   `/note/{note_id:\d+}` captures a numeric `note_id` and makes it available to
   the SQL resource.
2. **HTTP methods** - `allow_methods` restricts which verbs are accepted. An
   endpoint without `allow_methods` accepts `GET` only.
3. **Input schema** - `schema.input` coerces and validates arguments. In this
   example the note title must be a non-null string between 1 and 200
   characters, and `content` must be present.
4. **Resource** - `resource` contains the SQL query. Values wrapped as
   `{{name}}` are bound as query parameters, not interpolated as SQL text.

The create endpoint additionally uses two endpoint features:

```toml
single_record_response = true
pre_processors = [
    { type = "parse_body_args" },
]
```

`parse_body_args` turns a JSON request body into request arguments, while
`single_record_response` makes the endpoint return one object instead of an
array.

The list and search endpoints use the built-in pagination pre-processor:

```toml
pre_processors = [
    { type = "limit_offset", limit_max_value = 50 },
]
```

It provides `limit` and `offset` arguments and substitutes them through
`{{-limit-}}` / `{{-offset-}}`. The leading and trailing dashes mark the
placeholder as safe to render directly into the SQL text (after validation and
capping).


