# Xepak — AI Agent Reference

**Version:** 0.0.10-rc | **Commit:** 338d32f

---

## Overview

Xepak is a DSL-based API server for SQLite, configured entirely via TOML. It exposes data through REST, JSON-RPC, and MCP endpoints from configuration plus optional Lua scripts — no application code.

Request flow: request → input args resolved → pre-processors run → resource (SQL or Lua) runs → output schema applied → response.

**Features:**
- Declarative endpoints: REST (`[[endpoint]]`), JSON-RPC (`[[rpc]]`), MCP (`[[mcp]]`)
- JSON + CBOR request/response (REST; negotiated via `Accept` / `Content-Type`)
- SQLite with WAL mode + sqlx migrations
- API-key or DB-backed token auth, plus role-based authorization rules
- Lua scripting for dynamic queries, validation, business logic
- Per-key app cache; schema-based input/output conversion and validation

**Unknown TOML keys are rejected** everywhere (`deny_unknown_fields`): a typo fails startup instead of being ignored.

---

## 1. Root config (`config.toml`)

| Key | Type | Default | Notes |
|-----|------|---------|-------|
| `specs_dir` | string | `./specs` | Relative to config file dir |
| `[server].listen` | string | `localhost:8080` | `HOST:PORT` |
| `[server].keep_alive` | int (u16) | `0` | Seconds; `0` disables keep-alive |
| `[server].workers` | int > 0 | none | Actix worker count |
| `[[storage]]` | array | none | At least one required, else startup error |
| `[registry]` | table | empty | Free-form values + `auth` + `secrets` |

```toml
specs_dir = "./specs"

[server]
listen = "localhost:8080"
keep_alive = 0
workers = 4

[[storage]]
type = "sqlite"                      # only supported type
id = ""                              # datasource id; referenced by resource `data_source`
file = "db/app.sqlite3"              # required; "" = shared in-memory DB; relative to config dir
create_db = true                     # default false
wal = true                           # default false
migrations_dir = "./migrations"      # default ./migrations; relative to config dir

[registry]
auth = [
    # key: literal API key, or ENV var name when from_env = true (missing ENV = startup error)
    { id = "boss", key = "BossKEY", from_env = false, roles = ["ADMIN", "MANAGER"] },
    { id = "user", key = "UserKEY" },
    { id = "", key = "", roles = ["GUEST"] },  # anonymous entry: empty key requires empty id
]
secrets = {
    secret1 = { type = "env", name = "SECRET_ENV_1" },  # missing ENV = startup error
    secret2 = { type = "text", value = "raw_value" },
}
my_config = 42                        # custom value, read via app:get_registry_value("my_config")
```

Rules:
- Multiple `[[storage]]` blocks are allowed; duplicate `id` keeps the last one (warning logged).
- `migrations_dir = ""` disables migrations. Migration files must be `<version>_<description>.sql` (sqlx rules, version = positive integer).
- `storage.file` parent directories are created automatically.
- Registry `auth` roles are **uppercased** at startup. `registry.secrets` and `auth[].key` values are trimmed.
- Auth header: `x-api-key: <key>`; `Authorization: Bearer <key>` is accepted **only for POST** and only when `x-api-key` is absent.
- Any other `[registry]` key is a custom value readable from Lua.

## 2. Specs (one or more `.toml` files in `specs_dir`)

All `.toml` files in `specs_dir` are merged as if they were one file. Root spec keys: `endpoint`, `rpc`, `mcp`, `shared_pre_processors`, `default_pre_processors`.

```toml
# Shared pre-processor definitions (referenced elsewhere by id)
[shared_pre_processors.my_auth]
type = "config_authentication"

# Applied to every endpoint unless it sets pre_processors_ignore_default = true
default_pre_processors = [
    { type = "config_authentication", anonymous_auth = false },
]

[[endpoint]]
uri = "/users/{user_id:\\d+}"         # {name} path params captured automatically; regex after ':' (escape backslash in TOML)
allow_methods = ["GET", "DELETE"]     # optional; if absent only GET is accepted
strict_schema = false                 # true = reject input args not defined in the input schema
single_record_response = true         # one record + 404 when no record, instead of an array
pre_processors_ignore_default = false # true = skip default_pre_processors for this endpoint

[endpoint.resource]
type = "query"
data_source = ""                      # storage id; "" = storage with id = ""
query = "SELECT * FROM users WHERE id = {{user_id}}"

[[endpoint.pre_processors]]           # equivalent: pre_processors = [ { ... }, ... ]
type = "config_authentication"

[[endpoint.pre_processors]]
type = "authorize"
rules = "#boss OR (ADMIN AND MANAGER)"
```

Endpoint fields: `uri` (required), `resource` (required), `allow_methods`, `strict_schema` (default `false`), `single_record_response` (default `false`), `pre_processors`, `pre_processors_ignore_default` (default `false`), `schema.input`, `schema.output`.
Duplicate `uri` across `[[endpoint]]`/`[[rpc]]` logs a warning (last registration effectively wins). A shared pre-processor of type `ref` is a config error.

### How request input becomes query args

- URI `{param}` values become text args (path args).
- Resolution priority (highest first): custom args (`ctx:set_arg`, `-limit-`, `-offset-`, `-auth-id-`) → URI path args → body args → query-string args.
- Query-string args are used only for bodyless methods (e.g. GET); they are **ignored** for POST/PUT/PATCH.
- Body exists only for POST/PUT/PATCH. A flat **object** body requires the `parse_body_args` pre-processor to become named args; a positional **array** body (from `parse_body_args`) is exposed as args `"0"`, `"1"`, ... (also for JSON-RPC params). Format from `Content-Type: application/cbor`, otherwise JSON.
- `-auth-id-` is set by authentication pre-processors, so SQL can use `{{-auth-id-}}`.
- The input schema (conversion + validation) is applied as args are parsed; the output schema is applied after the resource runs.

### Resource types

| Type | Description | Script must return |
|------|-------------|--------------------|
| `query` | Static SQL | n/a |
| `query_script_lua` | Lua builds the SQL string | a **string** (final SQL) |
| `data_script` | Lua returns data directly (no DB query); `data_source` is ignored | object or array |

REST only returns objects/arrays: a `data_script` returning a scalar/text → `520 unknown_error`; returning `null` → `404 not_found`.

### Pre-processor types

| Type | Description | Fields |
|------|-------------|--------|
| `ref` | Reference to a shared pre-processor | `id` |
| `parse_body_args` | Merge POST/PUT/PATCH body into args | `null_if_absent: [keys]` |
| `config_authentication` | API-key lookup in registry | `anonymous_auth: bool` (default `false`) |
| `storage_authentication` | DB-backed token auth | `query` (required), `data_source` (default `""`), `cache_ttl_sec: u16` |
| `limit_offset` | Pagination | `limit_max_value: usize` (required), `limit_arg_name` (default `"limit"`), `offset_arg_name` (default `"offset"`) |
| `authorize` | Role/ID authorization | `rules: string` |
| `lua_script` | Lua as a pre-processor | `script: string` |

- Default pre-processors run first (in order), then endpoint pre-processors (in order); all are stably ordered by position.
- `parse_body_args` runs only for POST/PUT/PATCH; an empty body leaves args untouched. `null_if_absent` sets missing keys to `null`.
- `config_authentication`: no key + `anonymous_auth = true` → authenticated as id `""` with the roles of the registry entry whose key is `""`. No key + `anonymous_auth = false` → `403 Authentication failed! API key is not provided.` Unknown key → `403 Authentication failed.`
- `storage_authentication`: must find a custom header/token key; absent → `403 No API key`. The query runs with arg `{{api-key}}` and must return one row with columns `id` (text) and `roles` (comma-separated text, trimmed and uppercased). No row → `403 Wrong API key or something`. `cache_ttl_sec > 0` caches the result per key.
- `limit_offset`: reads `limit`/`offset` args, clamps `limit` to `limit_max_value` (absent/unparseable → `limit_max_value`), absent/unparseable `offset` → `0`, then sets `-limit-`/`-offset-`. Setting `limit_arg_name = ""` (or `offset_arg_name = ""`) disables that arg entirely.
- `authorize` must run after an authentication pre-processor: not authenticated → `403 Not authenticated`; empty `rules` → authenticated-only; failed match → `403 Not authorized to perform request! Auth id: <id>`.

### Auth rules syntax

```
#boss OR (ADMIN AND MANAGER)
manager AND billing AND accounting
```

| Token | Meaning |
|-------|---------|
| `word` | Role name (compared uppercase; `A-Z`, `a-z`, `0-9`, `_`) |
| `#word` | Auth ID exact match (case-sensitive; `A-Z`, `a-z`, `0-9`, `_`) |
| `AND` / `OR` | Logical operators, **uppercase only** (lowercase/mixed → startup config error) |
| `( )` | Grouping |

Only one operator type per group; mix via nesting: `admin OR (manager AND (billing OR #superID))`. Operands need an operator between them; empty groups and empty `#` are config errors.

### Schema validation

`schema.input` and `schema.output` share one structure. Root must be `dict` or `tuple`. An omitted schema means "no conversion, no validation".

```toml
[endpoint.schema.input]
type = "dict"
items = {
    title = { type = "text", required = true, validate = { kind = "range", from = 5, to = 255 } },
    content = { type = "text" },
}

[endpoint.schema.output]
type = "dict"
items = { id = { type = "int" } }     # converts e.g. text/int to int
```

| `type` | Description |
|--------|-------------|
| `text`, `boolean`, `int`, `float` | Scalar values |
| `blob` | Binary data; base64 string in JSON |
| `tuple` | Ordered array; `items` = list of schemas |
| `dict` | Key-value map; `items` = name→schema map |

Per-node fields: `type` (required), `required` (default `false`; applies to nested items only), `validate` (default: pass), `items` (dict/tuple), `items_repeat` (tuple, default `false`): loop `items` over all elements.

Conversion rules (`null` stays `null` for any type):
- `text` accepts anything (numbers/bools converted to text; blob → base64 text; tuple/dict → JSON text).
- `int` accepts int, integral float within i64 range, numeric text, bool (0/1).
- `float` accepts int/float/numeric text/bool.
- `boolean` accepts bool, `0`/`1` int or float, parseable text.
- `blob` accepts blob or base64 text; `tuple`/`dict` accept only the same shape (or `null` → empty).
- Incompatible conversion → `400 bad_request`.

`strict_schema = true` (REST) rejects: unknown `dict` keys (`Unknown key <path>.<name> in the input.`) and extra `tuple` elements (unless `items_repeat`). JSON-RPC and MCP are always strict.

`validate` holds a **single** validator object; combine with `and`/`or` + an `items` array (nestable arbitrarily):

| `kind` | Fields | Applies to |
|--------|--------|------------|
| `range` | `from`, `to` (non-negative ints) | text length, int/float value, tuple length |
| `range_float` | `from`, `to` (float) | text length, int/float value, tuple length |
| `not_null` | — | rejects `null` |
| `and` / `or` | `items` (list of validators) | all / at least one must pass |

```toml
# single validator — text length 5..255
validate = { kind = "range", from = 5, to = 255 }

# not_null AND length 5..255
validate = { kind = "and", items = [
    { kind = "not_null" },
    { kind = "range", from = 5, to = 255 },
] }

# OR — an int either 0, or in 10..100
validate = { kind = "or", items = [
    { kind = "range", from = 0, to = 0 },
    { kind = "range", from = 10, to = 100 },
] }
```

Bounds are interpreted per value type (text length, number value, tuple length). Validators that do not apply to a type reject the value (e.g. `range` on `blob`, `bool`, `dict`, `null`).

### Query argument syntax

SQL uses `{{arg_name}}` placeholders:

- Name characters: `A-Z a-z 0-9 _ -` (`{{-limit-}}` is a normal name). `{{}}` is ignored.
- Placeholders **inside** single- or double-quoted SQL string literals are **not** substituted (use a placeholder outside quotes instead, e.g. `'%' || {{q}} || '%'`).
- Every occurrence is bound separately (`{{x}}` twice = two bind params).
- A placeholder with no matching arg → `400 bad_request` (`Can't bind argument 'x' - does not exist in request.`).
- Resolution priority: custom args (`ctx:set_arg`, `-limit-`, `-offset-`, `-auth-id-`) → URI path params → body args (JSON/CBOR object keys or array indexes `0`,`1`,...; JSON-RPC params; MCP tool arguments) → query string args.
- Values are bound as SQL params; `tuple`/`dict` args are serialized to a JSON text string.

### JSON-RPC endpoints (`[[rpc]]`)

Each `[[rpc]]` binds one JSON-RPC 2.0 service to a `uri`. HTTP **POST** only, JSON only (no CBOR). `rpc.methods.<name>` accepts `resource`, `schema`, `pre_processors`, `pre_processors_ignore_default`, `single_record_response`.

```toml
[[rpc]]
uri = "/rpc"

[rpc.methods.all_posts]
resource = { type = "query", query = "SELECT * FROM posts ORDER BY id" }

[rpc.methods.get]
schema.input = { type = "dict", items = { id = { type = "int", required = true } } }
single_record_response = true
resource = { type = "query", query = "SELECT * FROM posts WHERE id = {{id}}" }

[rpc.methods.get_by_pos]
schema.input = { type = "tuple", items = [ { type = "int", required = true } ] }
single_record_response = true
resource = { type = "query", query = "SELECT * FROM posts WHERE id = {{0}}" }
```

- Request must be a single JSON object (first non-space byte `{`); unknown envelope fields → parse error.
- `jsonrpc` must be exactly `"2.0"`; `params` pass through the input schema with `strict_schema` **always** enabled.
- Object `params` → named args; array `params` → positional args `0`, `1`, ...
- With `single_record_response = true`, a missing record yields `result: null` (not an error).
- All responses (including protocol errors) use HTTP **200** with a JSON-RPC envelope.

| Error code | Meaning |
|-----------|---------|
| `-32700` | Parse error (invalid JSON, unknown envelope field, non-object body) |
| `-32600` | Invalid request (bad `jsonrpc` version, non-POST method, unknown method, input error not matching other codes) |
| `-32601` | Method not found |
| `-32602` | Invalid params (input schema rejected `params`) |
| `-32603` | Internal error (`internal_error`, `unknown_error`) |
| `-32043` | Forbidden (auth/authorization) |
| `-32044` | Not found |

### MCP servers (`[[mcp]]`)

Each `[[mcp]]` binds one MCP server (JSON-RPC 2.0 over HTTP POST) to a `uri`. `mcp.tools.<name>` accepts `title`, `description` (both required strings), `annotations`, `schema`, `resource`, `pre_processors`, `pre_processors_ignore_default`.

URI path params in `uri` (e.g. `/mcp/rag/{project_name}`) are captured and exposed to tool resources as args `{{project_name}}` — useful for per-tenant servers.

```toml
[[mcp]]
uri = "/mcp"
name = "Posts server"                 # required; serverInfo.name
instructions = "Access to posts data" # required; returned by initialize
server_version = "1.0.0"              # optional
empty_response_text = "No data found" # optional; default "Empty! No data available for your request."

[mcp.tools.list_posts]
title = "List posts"
description = "List posts with pagination"
annotations = { read_only = true, destructive = false, idempotent = true, open_world = false }
schema.input = { type = "dict", items = {
    limit  = { type = "int", required = true },
    offset = { type = "int", required = true },
} }
resource = { type = "query", query = "SELECT * FROM posts ORDER BY id LIMIT {{limit}} OFFSET {{offset}}" }
```

- Methods: `initialize` (returns `protocolVersion: "2024-11-05"`, `instructions`, `serverInfo{name,version}`, `capabilities.tools.listChanged: false`), `notifications/initialized` (returns `null`), `ping` (returns `{}`), `tools/list`, `tools/call`.
- `tools/list` item: `name`, `title` (omitted when empty), `description`, `inputSchema` (JSON-Schema built from `schema.input`), `annotations` mapped to `readOnlyHint`/`destructiveHint`/`idempotentHint`/`openWorldHint` (only set flags emitted).
- `inputSchema` mapping: `dict`→`object`+`properties`+`required`, `tuple`→`array`+`items`, `text`→`string`, `int`→`integer`, `float`→`number`, `boolean`→`boolean`, `blob`→`object`; `range`/`range_float`→`minimum`/`maximum`.
- `tools/call` requires `params = { name, arguments }`; `arguments` may be an object or array and is validated against the tool input schema (always strict). Missing `params`/`name` → JSON-RPC error.
- Tool execution errors (validation, auth, SQL) are returned as a **successful** JSON-RPC result with `isError: true` and one `text` element containing the error message.
- Successful result: `{ "isError": false, "content": [ { "type": "text", "text": "<markdown>" } ] }` — always exactly one text element:
  - array result → heading `# Result details`, then `total-records-fetched: N`, optional `with-offset: N`, `with-limit: N` and a note when `N == limit`, then a `# Record <i>` section per row with `column: value` lines (blob → `BASE64: ...`, multi-line values use YAML-like `|` blocks).
  - object/scalar result → the JSON text (plain strings are passed as-is).
  - empty result → `empty_response_text`.

```jsonc
// request (POST /mcp)
{ "jsonrpc": "2.0", "id": 1, "method": "tools/call",
  "params": { "name": "list_posts", "arguments": { "limit": 2, "offset": 0 } } }

// result  (HTTP 200)
{ "jsonrpc": "2.0", "id": 1, "result": { "isError": false, "content": [
  { "type": "text", "text":
    "# Result details\ntotal-records-fetched: 2\nwith-offset: 0\nwith-limit: 2\nnote: Records count equals to limit. More data could be available to retreive.\n\n# Record 1\nid: 1\ntitle: First post\n\n# Record 2\nid: 2\ntitle: Second post\n" } ] } }
```

---

## 3. Lua scripting API

Each script runs in an isolated per-request environment and receives a `ctx` global plus helper functions.

### Context (`ctx`)

```lua
ctx:has_arg("user_id")            -- true if the arg exists in any source
local v   = ctx:get_arg("name")   -- value as a Lua type; nil when absent
ctx:set_arg("user_id", id)        -- set custom arg; schema conversion applies (strict schema rejects unknown names)
local input = ctx:load_input()    -- request input object (pass to storage_query*)
local m  = ctx:get_request_method()          -- "GET", "POST", ...
local id = ctx:get_auth_id()                 -- string or nil (after auth runs)
local roles = ctx:get_auth_roles()           -- table of strings (empty table when unauthenticated)
local is_admin = ctx:has_auth_role("admin")  -- bool, case-insensitive
```

Lua values map to/from the internal model as: `nil`→null, boolean, number (integer or float), string→text, `blob_type(...)`→blob, array-like table→tuple, map-like table→dict. Map keys must be strings, ints, or whole floats. An empty table becomes an empty tuple.

### Query builder

Parts are joined in call order with single spaces, so add `WHERE` **before** `LIMIT`. The base string must not already contain a `WHERE`. `add_joined_parts` is a no-op when its parts table is empty.

```lua
local q = query_builder("SELECT * FROM posts")
local parts = {}
if ctx:has_arg("author") then table.insert(parts, "username = {{author}}") end
q:add_joined_parts("WHERE", parts, "AND", "")   -- (prefix, parts, separator, suffix)
q:add("LIMIT {{-limit-}} OFFSET {{-offset-}}")
return q:build()
```

### Blob

```lua
local b = blob_type("\x01\x02\x03")  -- from bytes
local bytes = b:as_bytes()           -- back to a Lua string
```

### Database functions (always use the storage with `id = ""`)

```lua
storage_query("SELECT * FROM users WHERE id={{id}}", {id = 42})     -- table of rows (1-indexed)
storage_query_one("SELECT * FROM users WHERE id={{id}}", {id = 42}) -- one row (table) or nil
storage_query_value("SELECT COUNT(*) FROM users", {})               -- first column of first row
```

`args` is a Lua table or `ctx:load_input()`. `{{name}}` placeholders bind from it. Named storages cannot be targeted from Lua. `storage_query_value` errors if the result is empty or has more than one column.

### Cache, registry, secrets

```lua
app:cache_get("key")                 -- value or nil
app:cache_set("key", "value")        -- default TTL (5 min), cache holds 10k keys
app:get_registry_value("my_config")  -- custom registry value or nil
app:get_secret("secret1")            -- secret string or nil
```

### HTTP client

```lua
local resp = http_get("http://example.com/api", { ["x-api-key"] = "key" })
local resp = http_post_json("http://example.com/api", { ["Content-Type"] = "application/json" }, { message = "hi" })
-- resp:is_success(), resp:get_status_code(), resp:read_body_string(), resp:read_body_json()
```

Requests time out after 30 s. `read_body_*` consumes the response (a second call errors). HTTP failures raise Lua errors (they never panic).

### Errors & logging

```lua
error_input("...")      -- 400
error_not_found("...")  -- 404
error_forbidden("...")  -- 403
error_server("...")     -- 500

log_info("...")
log_debug("...")
```

- Use the `error_*` functions for client-visible errors. A plain Lua `error("...")` aborts the request with 520 `unknown_error` and **no message** is sent to the client.
- `http_get` / `http_post_json` never panic; a failed request raises a Lua error that can be caught with `xpcall`.

---

## 4. Use cases

### CRUD + pagination

```toml
# specs/users.toml
[[endpoint]]
uri = "/user/list"
[[endpoint.pre_processors]]
type = "limit_offset"
limit_max_value = 20
[endpoint.resource]
type = "query"
query = "SELECT * FROM users LIMIT {{-limit-}} OFFSET {{-offset-}}"

[[endpoint]]
uri = "/user/{user_id:\\d+}"
single_record_response = true
[endpoint.resource]
type = "query"
query = "SELECT * FROM users WHERE id={{user_id}}"

[[endpoint]]
uri = "/user"
allow_methods = ["POST"]
single_record_response = true
[endpoint.schema.input]
type = "dict"
items = {
    username = { type = "text", required = true, validate = { kind = "range", from = 3, to = 50 } },
    password = { type = "text", required = true },
}
[[endpoint.pre_processors]]
type = "parse_body_args"
[endpoint.resource]
type = "query"
query = "INSERT INTO users (username, password) VALUES ({{username}}, {{password}}) RETURNING id, username"
```

### Dynamic query in Lua

```toml
[[endpoint]]
uri = "/post/list"
[[endpoint.pre_processors]]
type = "limit_offset"
limit_max_value = 20
[endpoint.resource]
type = "query_script_lua"
script = """
local q = query_builder("SELECT * FROM posts")
if ctx:has_arg("author") then
    local a = ctx:get_arg("author")
    if #a < 3 then error_input("author must be at least 3 characters") end
    q:add_joined_parts("WHERE", {"username = {{author}}"}, "AND", "")
end
q:add("LIMIT {{-limit-}} OFFSET {{-offset-}}")
return q:build()
"""
```

### Authentication + authorization

```toml
# [registry] auth is defined in config.toml (see section 1)

[[endpoint]]
uri = "/admin/posts"
[[endpoint.pre_processors]]
type = "config_authentication"
[[endpoint.pre_processors]]
type = "authorize"
rules = "#boss OR (ADMIN AND MANAGER)"
[endpoint.resource]
type = "query"
query = "SELECT * FROM posts"
```

### DB-backed token auth + reading auth in Lua

```toml
[[endpoint]]
uri = "/auth/token/info"
[[endpoint.pre_processors]]
type = "storage_authentication"
query = "SELECT user_id AS id, roles FROM tokens WHERE api_key = {{api-key}}"
cache_ttl_sec = 300
[endpoint.resource]
type = "data_script"
script = """
return { id = ctx:get_auth_id(), roles = ctx:get_auth_roles(), is_admin = ctx:has_auth_role("admin") }
"""
```

### Lua pre-processor (validation / guard)

```toml
[[endpoint]]
uri = "/script/lua/pp/{token:\\w+}"
[[endpoint.pre_processors]]
type = "lua_script"
script = """
if ctx:get_arg("token") == "bad" then error_forbidden("You are BAD") end
"""
[endpoint.resource]
type = "data_script"
script = "return {}"
```

### Lua utilities (registry, secrets, cache, http)

```toml
# registry values + secrets are defined in config.toml [registry] (see section 1)

[[endpoint]]
uri = "/demo"
[endpoint.resource]
type = "data_script"
script = """
local cfg  = app:get_registry_value("my_config")  -- custom [registry] value
local sec  = app:get_secret("secret1")            -- secret string (or nil)
app:cache_set("k", "v")                           -- cache with default TTL
local resp = http_get("http://example.com/api", { ["x-api-key"] = "key" })
return { cfg = cfg, cached = app:cache_get("k"), status = resp:get_status_code() }
"""
```

### Anonymous auth & default pre-processors

```toml
# config.toml: require auth on every endpoint by default
default_pre_processors = [
    { type = "config_authentication", anonymous_auth = false },
]

# spec: skip the default for one endpoint and allow anonymous
# (a registry entry with id = "" supplies the anonymous roles)
[[endpoint]]
uri = "/auth/script/anon"
pre_processors_ignore_default = true
[[endpoint.pre_processors]]
type = "config_authentication"
anonymous_auth = true
[endpoint.resource]
type = "data_script"
script = "return { id = ctx:get_auth_id(), roles = ctx:get_auth_roles() }"
```

---

## 5. Request → response

- Data moves through one unified value type; conversions are automatic. User-visible details: `blob` is a base64 string in JSON but raw bytes in CBOR; a `tuple`/`dict` arg bound to a SQL placeholder becomes a JSON text string; CBOR is REST-only.
- REST success payloads must be objects or arrays (see [Resource types](#resource-types)).
- A REST request whose method is not in `allow_methods` → `400 bad_request` with `(๑•ᗝ•)૭ Request method <METHOD> not allowed!`.

### Error format (REST)

```json
{ "code": "bad_request", "message": "Element \"title\" length 2 is not within a range 5..=255" }
```

| Code | HTTP | Triggered by |
|------|------|--------------|
| `not_found` | 404 | No record with `single_record_response`, Lua `error_not_found()`, or `null` REST result |
| `bad_request` | 400 | Input/schema validation, method not allowed, missing SQL arg, Lua `error_input()` |
| `forbidden` | 403 | Auth/authorization failure, Lua `error_forbidden()` |
| `internal_error` | 500 | Server/storage/serialization error, Lua `error_server()` |
| `unknown_error` | 520 | Unhandled error; **no `message` field** (includes plain Lua `error()` and non-object/array REST results) |

JSON-RPC and MCP use JSON-RPC error envelopes (see [JSON-RPC endpoints](#json-rpc-endpoints-rpc) / [MCP servers](#mcp-servers-mcp)).

### Response headers

- `X-Limit` / `X-Offset` are added only when the `-limit-` / `-offset-` args exist and are > 0 (i.e. a `limit_offset` pre-processor ran).
- Response `Content-Type`: `application/json` by default; `application/cbor` when `Accept: application/cbor`.
- Request body is parsed as CBOR only when `Content-Type: application/cbor`; anything else is parsed as JSON.

### CLI

```bash
xepak [-j] [-l LOG_LEVEL] <config_file>

Options:
  -l LOG_LEVEL   Tracing filter directive (e.g. "debug", "info,sqlx=debug"); default level WARN when set, INFO otherwise
  -j             Enables log output in JSON format

Env vars (used only when the matching CLI arg/file is missing):
  XEPAK_LISTEN   HOST:PORT to listen to (overrides `[server].listen`)
  XEPAK_CONFIG   Path to config file
  XEPAK_LOG_JSON JSON log output with 1|y|yes|true|on
```

