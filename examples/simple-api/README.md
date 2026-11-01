# Xepak simple DSL example

A minimal REST API for notes defined entirely in TOML.
It demonstrates the core Xepak DSL concepts.

## What it demonstrates

- Endpoint routing with URI parameters such as `/note/{note_id}`.
- Input schema validation.
- Static SQL resources with strongly typed placeholders.
- Pagination through the `limit_offset` pre-processor.

## Justfile tool

To execute commands defined in `justfile`
you have to install [just, a command runner](https://github.com/casey/just).

Run `just -l` within directory where `justfile` is located to list all available commands.

## Launching server 

From the example directory run `just run` or `cargo run --bin xepak -- ./xepak.toml`.

This will start server on `http://localhost:8080`.

## Endpoints

Current configuration defines next ednpoints.

| Method | URI | Params | Description |
|--------|-----|--------|-------------|
| `POST` | `/note/add` | `title`, `content` | Create a note |
| `GET` | `/note/{id}` | path `id` | Get one note |
| `GET` | `/note/list` | `limit?`, `offset?` | List notes |
| `GET` | `/note/search` | `query`, `limit?`, `offset?` | Search title/content with `LIKE` |
| `DELETE` | `/note/delete/{id}` | path `id` | Delete one note |

## Querying server

The `justfile` wraps the most common requests.

```bash
# Create notes
just add_note "Shopping list" "Milk, eggs, bread"
just add_note "Work" "Finish the report"

# Read and paginate
just get_note 1
just list_notes
just list_notes 1 1        # limit=1 offset=1

# Search title and content
just search_notes milk
just search_notes report

# Delete
just delete_note 2

# Reset the local database
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

