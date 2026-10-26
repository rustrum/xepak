# Xepak — AI Agent Reference

**Version:** 0.0.8-pre-release | **Commit:** c58ec41

---

## Overview

Xepak is a DSL-based API server for databases, configured entirely via TOML. It exposes data through REST, JSON-RPC, and MCP endpoints from configuration plus optional Lua scripts — no application code. SQLite is the supported database.

Request flow: a request arrives → Xepak resolves input into query args → runs pre-processors → runs the resource (SQL or Lua) → applies the output schema → returns a response.

**Features:**
- Declarative endpoints in TOML: REST (`[[endpoint]]`), JSON-RPC (`[[rpc]]`), MCP (`[[mcp]]`)
- JSON + CBOR request/response (negotiated via `Accept` / `Content-Type`)
- SQLite with WAL mode + migrations
- API-key or DB-backed token auth, plus role-based authorization rules
- Lua scripting for dynamic queries, validation, business logic
- Per-entry caching; schema-based input validation with type coercion

---

## 1. Root config (`config.toml`)

```toml
port = 8080                          # optional, default 8080
specs_dir = "./specs"                # optional, default ./specs (relative to config dir)

[[storage]]
type = "sqlite"
id = ""                              # datasource id; must match `data_source` where referenced
file = "/path/to/db.sqlite3"         # "" = shared in-memory DB; relative paths resolve against config dir
create_db = true
wal = true
migrations_dir = "sqlite/migrations" # relative to config dir

[registry]
auth = [
    { id = "boss", key = "BossKEY", from_env = false, roles = ["ADMIN", "MANAGER"] },  # from_env=true: `key` is an ENV var name
    { id = "manager", key = "ManagerKEY", from_env = false, roles = ["MANAGER"] },
    { id = "user", key = "UserKEY", from_env = false },
    { id = "", key = "" },            # optional anonymous entry; its roles apply to anonymous requests
]

# Optional: secrets (loaded from ENV or raw text)
secrets = {
    secret1 = { type = "env", name = "SECRET_ENV_1" },
    secret2 = { type = "text", value = "raw_value" },
}

# Optional: any other keys are custom registry values, readable in Lua via app:get_registry_value()
my_config = 42
```

- `[registry]` holds arbitrary data readable from Lua; only `auth` and `secrets` have special meaning.
- A request authenticates via the `x-api-key` HTTP header (JSON-RPC/MCP also accept `Authorization: Bearer`).

## 2. Specs (one or more `.toml` files in `specs_dir`)

Each file is merged with the others (imagine all files read as one). A file may define any combination of `[[endpoint]]`, `[[rpc]]`, `[[mcp]]`, shared pre-processors, and default pre-processors.

```toml
# Shared pre-processor definitions (referenced elsewhere by id)
[shared_pre_processors.my_auth]
type = "config_authentication"

# Applied to every endpoint unless an endpoint sets pre_processors_ignore_default = true
default_pre_processors = [
    { type = "config_authentication", anonymous_auth = false },
]

[[endpoint]]
uri = "/users/{user_id:\\d+}"        # {name} path params captured automatically; regex after ':' (escape backslash in TOML)
allow_methods = ["GET", "DELETE"]    # optional whitelist; if absent only GET is accepted
strict_schema = false                # reject unknown input arg names (default: ignore them)
single_record_response = true        # return 404 when no record, instead of an empty array
pre_processors_ignore_default = false # skip default_pre_processors for this endpoint

[endpoint.resource]
type = "query"
data_source = ""                     # optional; references storage id; default "" must match storage id
query = "SELECT * FROM users WHERE id={{user_id}}"

[[endpoint.pre_processors]]
type = "config_authentication"

[[endpoint.pre_processors]]
type = "authorize"
rules = "#boss OR (ADMIN AND MANAGER)"
```

### How request input becomes query args

- URI `{param}` values become text args (path args).
- Resolution priority (highest first): args set by Lua/pre-processors (`ctx:set_arg`) → URI path args → request body args → query-string args.
- Query-string args are used only for bodyless methods (e.g. GET); they are **ignored** for POST/PUT/PATCH.
- Body exists only for POST/PUT/PATCH. A **flat map** (object) body requires the `parse_body_args` pre-processor to become named args; a **positional array** body is exposed automatically as args `"0"`, `"1"`, ... Format from `Content-Type` (`application/cbor`, otherwise JSON).
- The input schema (conversion + validation) is applied as args are parsed; the output schema is applied last, after the resource runs.

### Resource types

| Type | Description | Script must return |
|------|-------------|--------------------|
| `query` | Static SQL | n/a |
| `query_script_lua` | Lua builds the SQL query string | a **string** (the final query) |
| `data_script` | Lua returns data directly (no DB query) | any value (dict/tuple) |

### Pre-processor types

| Type | Description | Key fields |
|------|-------------|------------|
| `ref` | Reference to a shared pre-processor | `id` |
| `parse_body_args` | Merge POST/PUT/PATCH flat-map body into args | `null_if_absent: [keys]` |
| `config_authentication` | API-key lookup in registry | `anonymous_auth: bool` |
| `storage_authentication` | DB-backed token auth | `data_source`, `query`, `cache_ttl_sec` |
| `limit_offset` | Pagination: validates `limit`/`offset` args, sets `{{-limit-}}` / `{{-offset-}}` | `limit_max_value` (required), `limit_arg_name` (default `"limit"`), `offset_arg_name` (default `"offset"`) |
| `authorize` | Role-based authorization rules | `rules: string` |
| `lua_script` | Lua as a pre-processor (validation, etc.) | `script: string` |

- `parse_body_args` parses only a flat-map body. A positional-array body is already exposed as args `"0"`, `"1"`, ...
- `storage_authentication`: the query runs with an `{{api-key}}` arg and must return one row with columns `id` (text) and `roles` (comma-separated text). Non-zero `cache_ttl_sec` caches successful auth per key.
- `limit_offset`: the `limit` arg is capped at `limit_max_value`; an absent or unparseable `limit` becomes `limit_max_value`; an absent `offset` becomes `0`. The validated values are exposed to the resource as `{{-limit-}}` / `{{-offset-}}` and echoed back in the `X-Limit` / `X-Offset` response headers.
- `authorize` must run after an authentication pre-processor. An empty `rules` string means "authenticated only".

### Auth rules syntax

```
#boss OR (ADMIN AND MANAGER)
manager AND billing AND accounting
```

| Token | Meaning |
|-------|---------|
| `word` (case-insensitive) | Role name |
| `#word` | User-ID match |
| `AND` / `OR` (UPPERCASE) | Logical operators |
| `( )` | Grouping |

Only one operator type per group; mix via nesting: `admin OR (manager AND (billing OR #superID))`.

### Schema validation

Schemas are the `input` and `output` sub-tables, sharing one structure. The root is a `dict` or `tuple`.

```toml
[endpoint.schema.input]
type = "dict"
items = {
    title = { type = "text", required = true, validate = { kind = "range", from = 5, to = 255 } },
    content = { type = "text" },
}

[endpoint.schema.output]
type = "dict"
items = { id = { type = "int" } }
```

| `type` | Description |
|--------|-------------|
| `text`, `boolean`, `int`, `float` | Scalar values |
| `blob` | Binary data (base64 in JSON) |
| `tuple` | Ordered array; `items` = list of schemas, optional `items_repeat` |
| `dict` | Key-value map; `items` = name→schema map |

`validate` holds a **single** validator object. Combine with `and`/`or` + a `nested` array (nestable arbitrarily):

| `kind` | Fields | Applies to |
|--------|--------|------------|
| `range` | `from`, `to` (int) | text length, int/float value, tuple length |
| `range_float` | `from`, `to` (float) | same, float bounds |
| `not_null` | — | rejects null/undefined |
| `and` / `or` | `nested` (list of validators) | all / at least one must pass |

```toml
# single validator — text length 5..255
validate = { kind = "range", from = 5, to = 255 }

# not_null AND length 5..255
validate = { kind = "and", nested = [
    { kind = "not_null" },
    { kind = "range", from = 5, to = 255 },
] }

# OR — an int either 0, or in 10..100
validate = { kind = "or", nested = [
    { kind = "range", from = 0, to = 0 },
    { kind = "range", from = 10, to = 100 },
] }
```

`range`/`range_float` bounds are interpreted per value type (string length, number value, or tuple length). Validators that do not apply to a value type reject the value (e.g. `range` on `blob`).

### Query argument syntax

SQL uses `{{arg_name}}` placeholders, resolved in this priority order:

- custom arguments set by pre-processors (`{{-limit-}}`, `{{-offset-}}`) or Lua scripts
- URI path params
- POST/PUT/PATCH k/v arguments deserialized from JSON/CBOR payload
- JSON-RPC method k/v arguments or MCP tool arguments
- URL query string arguments

Numeric placeholders `{{0}}`, `{{1}}`, etc. are available for POST/PUT/PATCH tuple (array) payloads and JSON-RPC params passed as arrays.

### JSON-RPC endpoints (`[[rpc]]`)

Each `[[rpc]]` binds one JSON-RPC 2.0 service to a `uri`. Only HTTP **POST**, JSON body (no CBOR). The `methods` table maps a method name to a spec with the same fields as a REST endpoint (`resource`, `schema`, `pre_processors`, `single_record_response`).

```toml
[[rpc]]
uri = "/rpc"

[rpc.methods.all_posts]
resource = { type = "query", query = "SELECT * FROM posts ORDER BY id" }

[rpc.methods.get]
schema.input = { type = "tuple", items = [ { type = "int", required = true } ] }
single_record_response = true
resource = { type = "query", query = "SELECT * FROM posts WHERE id = {{0}}" }
```

- The request `params` pass through the input schema and become body args (JSON array → positional args `0`, `1`, ...). `strict_schema` is always enforced.
- Response/error format follows JSON-RPC 2.0. With `single_record_response = true`, a missing record yields `{}` (not an error).

### MCP servers (`[[mcp]]`)

Each `[[mcp]]` binds one MCP server (JSON-RPC 2.0 over HTTP POST) to a `uri`. The `tools` table maps a tool name to a spec (same fields as above). You **must** define an input schema per tool.

URI path params in the `uri` (e.g. `/mcp/rag/{project_name}`) are captured and exposed to tool resources as `{{project_name}}` args — useful for per-tenant MCP servers.

```toml
[[mcp]]
uri = "/mcp"
name = "Posts server"
instructions = "Provides access to posts data"
server_version = "1.0.0"
empty_response_text = "No data found"  # optional: text returned when a tool result is empty (default: built-in message)

[mcp.tools.list_posts]
title = "List posts"
description = "List posts with pagination"
annotations.read_only = true
schema.input = { type = "dict", items = {
    limit  = { type = "int", required = true },
    offset = { type = "int", required = true },
} }
resource = { type = "query", query = "SELECT * FROM posts ORDER BY id LIMIT {{limit}} OFFSET {{offset}}" }
```

- `annotations` (optional booleans): `read_only`, `destructive`, `idempotent`, `open_world`.
- The input schema is exposed to clients as the tool's JSON-Schema `inputSchema`.
- Supported methods: `initialize`, `notifications/initialized`, `ping`, `tools/list`, `tools/call`. `tools/call` takes `params = { name, arguments }`; `arguments` pass through the tool input schema. A tool response is a single `text` content element.

---

## 3. Lua scripting API

Each script runs in an isolated per-request environment and receives a `ctx` global plus helper functions.

### Context (`ctx`)

```lua
ctx:has_arg("user_id")            -- bool: arg exists
local v   = ctx:get_arg("name")   -- value as a Lua type
ctx:set_arg("user_id", id)        -- set arg (schema conversion applied)
local input = ctx:load_input()    -- full request input (usable as args for storage_query*)
local m  = ctx:get_request_method()          -- "GET", "POST", ...
local id = ctx:get_auth_id()                 -- string or nil (after auth runs)
local roles = ctx:get_auth_roles()           -- table of strings
local is_admin = ctx:has_auth_role("admin")  -- bool
```

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

### Database functions (run against the default data source `""` only)

```lua
storage_query("SELECT * FROM users WHERE id={{id}}", {id = 42})     -- table of rows (1-indexed)
storage_query_one("SELECT * FROM users WHERE id={{id}}", {id = 42}) -- one row (map) or nil
storage_query_value("SELECT COUNT(*) FROM users", {})               -- scalar or nil
```

`args` is a Lua table or `ctx:load_input()`. `{{name}}` placeholders bind from it. They cannot target a named storage from within Lua.

### Cache, registry, secrets

```lua
app:cache_get("key")                 -- value or nil
app:cache_set("key", "value")        -- default TTL
app:get_registry_value("my_config")  -- custom registry value or nil
app:get_secret("secret1")            -- secret string or nil
```

### HTTP client

```lua
local resp = http_get("http://example.com/api", { ["x-api-key"] = "key" })
local resp = http_post_json("http://example.com/api", { ["Content-Type"] = "application/json" }, { message = "hi" })
-- resp:is_success(), resp:get_status_code(), resp:read_body_string(), resp:read_body_json()
```

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

- Data moves through one unified value type; conversions are automatic. The only user-visible details: a `blob` is a base64 string in JSON but raw bytes in CBOR, and a `tuple`/`dict` value bound into a SQL placeholder is serialized to a JSON text string.

### Error format (REST)

```json
{ "code": "bad_request", "message": "Input argument 'name' length must be greater than 2" }
```

| Code | HTTP | Triggered by |
|------|------|--------------|
| `not_found` | 404 | Record not found, or Lua `error_not_found()` |
| `bad_request` | 400 | Input/schema validation, or Lua `error_input()` |
| `forbidden` | 403 | Auth/authorization failure, or Lua `error_forbidden()` |
| `internal_error` | 500 | Server error, or Lua `error_server()` |
| `unknown_error` | 520 | Unhandled exception (no `message` field); includes plain Lua `error()` |

JSON-RPC and MCP errors follow their respective protocol standards.

### Response headers

Paginated responses include `X-Limit` and `X-Offset` (the exact values used). The response `Content-Type` is `application/json` by default, or `application/cbor` when `Accept: application/cbor`.

### CLI

```bash
xepak [-p PORT] [-l LOG_LEVEL] <config_file>

Options:
  -p PORT        Port to listen on (env: XEPAK_PORT)
  -l LOG_LEVEL   Tracing filter directive (e.g. "debug", "info,my_crate=debug")

Env vars:
  XEPAK_PORT     Port (used if -p not provided)
  XEPAK_CONFIG   Path to config file (used if no positional arg)
```

