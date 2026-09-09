# Xepak — AI Agent Reference

**Version:** 0.0.1 | **Commit:** 37694fc

---

## Overview

Xepak is a DSL-based REST API server for SQLite databases, configured entirely via TOML files. It provides CRUD endpoints without writing application code — only configuration and optional Lua scripts for complex logic.

**Key features:**
- Declarative endpoint definitions in TOML (no Rust/Go/Haskell needed)
- JSON + CBOR response formats (negotiated via `Accept` header)
- SQLite with WAL mode support, migrations, and connection pooling
- Simple authentication via API key registry or database-backed token auth
- Authorization rules engine with role-based access control (`ADMIN AND MANAGER`, `#boss OR (ADMIN AND MANAGER)`)
- Lua scripting for dynamic query generation, data validation, and business logic
- Built-in caching layer (moka) with configurable TTL per entry
- Schema-based input validation with range checks and type coercion

---

## TOML DSL Specification

### Configuration file (`config.toml`)

```toml
port = 8080                          # optional, default: 8080
specs_dir = "./specs"                # optional, default: "./specs"

[[storage]]
type = "sqlite"
id = "default"                       # optional identifier for multi-datasource
file = "/path/to/db.sqlite3"         # empty string = in-memory DB
create_db = true                     # create if missing
wal = true                           # enable WAL mode
migrations_dir = "sqlite/migrations" # path relative to config dir

[registry]
auth = [
    { id = "boss", key = "BossKEY", from_env = false, roles = ["ADMIN", "MANAGER"] },
    { id = "manager", key = "ManagerKEY", from_env = false, roles = ["MANAGER"] },
    { id = "user", key = "UserKEY", from_env = false },
]

# Optional: secrets registry (values loaded from ENV or raw text)
secrets = {
    secret1 = { type = "env", name = "SECRET_ENV_1" },
    secret2 = { type = "text", value = "raw_value" },
}

# Optional: custom data values accessible in Lua scripts via app:get_registry_value()
my_config = 42
```

### Specs directory structure

Each `.toml` file in `specs_dir/` is loaded and merged. Files define endpoints, shared pre-processors, and default pre-processors:

```toml
# Shared pre-processor definitions (referenced by other specs)
[shared_pre_processors.my_auth]
type = "simple_authentication"

# Default pre-processors applied to all endpoints unless ignored
default_pre_processors = [
    { type = "simple_authentication", anonymous_auth = false },
]

# Endpoint definitions
[[endpoint]]
uri = "/users/{user_id:\\d+}"
single_record_response = true        # return 404 if no record, not empty array
fetch_limit = 20                     # max rows for paginated queries
limit_arg = "limit"                  # optional query param name (default: "limit")
offset_arg = "offset"                # optional query param name (default: "offset")

[endpoint.resource]
type = "query"                       # resource type (see below)
data_source = "default"              # optional, references storage id
query = "SELECT * FROM users WHERE id={{user_id}}"

[[endpoint.pre_processors]]          # endpoint-specific pre-processors
type = "simple_authentication"

[[endpoint.pre_processors]]
type = "authorize"
rules = "#boss OR (ADMIN AND MANAGER)"   # auth rules expression
```

### Resource types

| Type | Description | Script return value |
|------|-------------|---------------------|
| `query` | Static SQL query | N/A |
| `query_script_lua` | Lua script generates the SQL query string | Must return a **string** (the final query) |
| `data_script` | Lua script returns data directly (no DB query executed) | Returns any XepakValue (Map/Tuple) |

### Pre-processor types

| Type | Description | Key fields |
|------|-------------|------------|
| `ref` | Reference to a shared pre-processor | `id: string` |
| `parse_body_args` | Extracts JSON body into request arguments | — |
| `simple_authentication` | API key lookup in registry | `anonymous_auth: bool` |
| `token_authentication` | Database-backed token auth | `data_source`, `query`, `cache_ttl_sec` |
| `authorize` | Role-based authorization rules | `rules: string` |
| `lua_script` | Lua script as pre-processor (validation, etc.) | `script: string` |

### Auth rules expression syntax

```
#boss OR (ADMIN AND MANAGER)
manager AND billing AND accounting
(admin OR #superID) AND (billing OR #other_id)
```

| Token | Meaning | Example |
|-------|---------|---------|
| `lowercase_word` | Role name (case-insensitive, normalized to uppercase) | `admin`, `MANAGER` |
| `#word` | User ID match | `#boss` matches user with id `"boss"` |
| `AND` / `OR` | Logical operators (must be UPPERCASE) | — |
| `( )` | Grouping for precedence | `(A AND B) OR C` |

### Schema validation

```toml
[endpoint.schema]
title = { type = "text", scope = "input", required = true, validate = [
    { kind = "range", from = 5, to = 255 },
]}
content = { type = "text", scope = "all" }

# Schema types: text, boolean, int, float, blob, tuple, map
# Scope values: all (default), input, output
# Validators: range, range_float, not_null, and, or
```

### Query argument syntax

SQL queries use `{{arg_name}}` placeholders. Special keys for pagination:

| Placeholder | Meaning | Source |
|-------------|---------|--------|
| `{{user_id}}` | URI path parameter | `/users/{user_id}` |
| `{{-limit-}}` | Pagination limit | Query param or default |
| `{{-offset-}}` | Pagination offset | Query param or default |

---

## LUA Scripting API

Lua scripts run in an isolated VM per request. Each script receives a `ctx` global with the following methods:

### Context (`ctx`)

```lua
-- Check if argument exists (URI path params + query/body args)
if ctx:has_arg("user_id") then ... end

-- Get argument value as Lua type (auto-converted from XepakValue)
local name = ctx:get_arg("name")

-- Set argument value with schema conversion applied
ctx:set_arg("user_id", user.id)

-- Authentication data (available after auth pre-processor runs)
local auth_id = ctx:get_auth_id()       -- returns string or nil
local roles   = ctx:get_auth_roles()    -- returns table of strings
local is_admin = ctx:has_auth_role("admin")  -- returns boolean
```

### Query Builder

```lua
-- Create a query builder instance
local query = query_builder("SELECT * FROM users WHERE username={{name}}")

-- Add parts to the query
query:add("LIMIT {{-limit-}} OFFSET {{-offset-}}")

-- Add joined parts with separator (e.g., WHERE clauses)
local where_parts = {}
if ctx:has_arg("name") then
    table.insert(where_parts, "username = {{name}}")
end
if ctx:has_arg("pass") then
    table.insert(where_parts, "password = {{pass}}")
end
query:add_joined_parts("WHERE", where_parts, "OR", "")

-- Build final query string (must return this from script)
return query:build()
```

### Database functions

```lua
-- Query multiple rows → returns Lua table of tables (1-indexed)
local rows = storage_query("SELECT * FROM users WHERE id={{id}}", {id = 42})

-- Query single row → returns Lua table or nil
local user = storage_query_one("SELECT * FROM users WHERE id={{id}}", {id = 42})

-- Query single value (first column of first row) → returns number/string/nil
local count = storage_query_value("SELECT COUNT(*) FROM users")
```

### Cache API

```lua
-- Get cached value by key (returns XepakValue or nil)
local val = app:cache_get("my_key")

-- Set cached value with default TTL
app:cache_set("my_key", "value")

-- In query_script_lua resources, the script must return a string (SQL query),
-- so caching is typically done in data_script or lua_script pre-processors.
```

### Registry and secrets access

```lua
-- Get custom registry value (set in config.toml [registry])
local val = app:get_registry_value("my_config")  -- returns XepakValue or nil

-- Get secret from registry (from_env or text)
local secret = app:get_secret("secret1")  -- returns string or nil
```

### HTTP client

```lua
-- GET request → returns response object with methods: is_success(), get_status_code(), read_body_string(), read_body_json()
local resp = http_get("http://example.com/api", { ["x-api-key"] = "key123" })

-- POST JSON request
local resp = http_post_json(
    "http://example.com/api",
    { ["Content-Type"] = "application/json" },
    { message = "Hello from Xepak" }
)
```

### Error functions (throw errors that map to HTTP status codes)

```lua
error_input("Bad request message")       -- 400 Bad Request
error_not_found("Record not found")     -- 404 Not Found
error_forbidden("Access denied")        -- 403 Forbidden
error_server("Internal error")          -- 500 Internal Server Error
```

### Logging

```lua
log_info("Users count is " .. count)
log_debug(string.format("I said users count is %d", count))
```

---

## DSL Usage Examples (Use Cases)

### Use Case 1: Simple CRUD endpoints

**config.toml:**
```toml
port = 8080
[[storage]]
type = "sqlite"
file = "./db.sqlite3"
create_db = true
wal = true
migrations_dir = "migrations"
```

**specs/users.toml:**
```toml
# List users with pagination
[[endpoint]]
uri = "/user/list"
fetch_limit = 20

[endpoint.resource]
type = "query"
query = "SELECT * FROM users LIMIT {{-limit-}} OFFSET {{-offset-}}"

# Get single user by ID
[[endpoint]]
uri = "/user/{user_id:\\d+}"
single_record_response = true

[endpoint.resource]
type = "query"
query = "SELECT * FROM users WHERE id={{user_id}}"

# Create user with schema validation (use POST method)
[[endpoint]]
uri = "/user"
single_record_response = true

[endpoint.schema]
username = { type = "text", scope = "input", required = true, validate = [
    { kind = "range", from = 3, to = 50 },
]}
password = { type = "text", scope = "input", required = true }

[[endpoint.pre_processors]]
type = "parse_body_args"

[endpoint.resource]
type = "query"
query = """
INSERT INTO users (username, password) VALUES ({{username}}, {{password}})
RETURNING id, username
"""
```

### Use Case 2: Lua-generated dynamic queries

**specs/posts.toml:**
```toml
[[endpoint]]
uri = "/post/list"
fetch_limit = 20

[endpoint.resource]
type = "query_script_lua"
script = """
local query = query_builder("SELECT * FROM posts")

if ctx:has_arg("author") then
    local author = ctx:get_arg("author")
    if #author < 3 then
        error_input("Author name must be at least 3 characters")
    end
    query:add_joined_parts("WHERE", {"username = {{author}}"}, "AND", "")
end

query:add("LIMIT {{-limit-}} OFFSET {{-offset-}}")
return query:build()
"""
```

### Use Case 3: Authentication + Authorization

**config.toml:**
```toml
[registry]
auth = [
    { id = "boss", key = "BossKEY", from_env = false, roles = ["ADMIN", "MANAGER"] },
    { id = "manager", key = "ManagerKEY", from_env = false, roles = ["MANAGER"] },
    { id = "user", key = "UserKEY", from_env = false },
]
```

**specs/admin.toml:**
```toml
# Boss-only endpoint (requires ADMIN role)
[[endpoint]]
uri = "/admin/posts/boss"

[[endpoint.pre_processors]]
type = "simple_authentication"

[[endpoint.pre_processors]]
type = "authorize"
rules = "ADMIN"

[endpoint.resource]
type = "query"
query = "SELECT * FROM posts"

# Boss OR (Admin AND Manager) endpoint
[[endpoint]]
uri = "/admin/revalidate/{payment_id:\\d+}"
single_record_response = true

[[endpoint.pre_processors]]
type = "simple_authentication"

[[endpoint.pre_processors]]
type = "authorize"
rules = "#boss OR (ADMIN AND MANAGER)"

[endpoint.resource]
type = "query_script_lua"
script = """
-- Check if reprocessing is allowed (max once per 20 seconds)
local allowed = storage_query_value(
    "SELECT COALESCE(reprocess_trigger <= datetime('now', '-20 seconds'), 1) FROM payments WHERE id = {{payment_id}}",
    ctx:load_input()
)

if allowed ~= 1 then
    error_input("Can't trigger reprocessing more than once in 20 seconds")
end

return "UPDATE payments SET reprocess_trigger = current_timestamp WHERE id = {{payment_id}} RETURNING id, reprocess_trigger"
"""
```

### Use Case 4: Database-backed token authentication

**specs/token_auth.toml:**
```toml
[[endpoint]]
uri = "/auth/token/info"

[[endpoint.pre_processors]]
type = "token_authentication"
data_source = "default"
query = "SELECT user_id AS id, roles FROM tokens WHERE api_key = {{api-key}}"
cache_ttl_sec = 300

[endpoint.resource]
type = "data_script"
script = """
return {
    id = ctx:get_auth_id(),
    roles = ctx:get_auth_roles(),
    is_admin = ctx:has_auth_role("admin"),
}
"""
```

### Use Case 5: Lua pre-processor for validation

**specs/validation.toml:**
```toml
[[endpoint]]
uri = "/script/lua/pp/{token:\\w+}"

[[endpoint.pre_processors]]
type = "lua_script"
script = """
if ctx:has_arg("token") and ctx:get_arg("token") == "bad" then
    error_forbidden("You are BAD")
end
"""

[endpoint.resource]
type = "data_script"
script = """
return {}
"""
```

### Use Case 6: Registry values + secrets in Lua scripts

**config.toml:**
```toml
[registry]
auth = [ ... ]

# Custom registry data (accessible via app:get_registry_value())
int_val = 1
float_val = 2.2
string_val = "registry_val"
map_val = { host = "localhost", ["weird.key"] = "some_value" }

# Secrets (loaded from ENV or raw text)
secrets = {
    secret1 = { type = "env", name = "SECRET_ENV_1" },
    secret2 = { type = "text", value = "raw_text_value_4" },
}
```

**specs/registry_access.toml:**
```toml
[[endpoint]]
uri = "/script/lua/registry"

[endpoint.resource]
type = "data_script"
script = """
return {
    int_val = app:get_registry_value("int_val"),
    float_val = app:get_registry_value("float_val"),
    string_val = app:get_registry_value("string_val"),
    map_val = app:get_registry_value("map_val"),
}
"""

[[endpoint]]
uri = "/script/lua/secrets"

[endpoint.resource]
type = "data_script"
script = """
return {
    secret_1 = app:get_secret("secret1"),
    secret_2 = app:get_secret("secret2"),
}
"""
```

### Use Case 7: Lua cache operations

**specs/cache.toml:**
```toml
[[endpoint]]
uri = "/script/lua/cache/set/{key:\\w+}/{value:\\w+}"

[endpoint.resource]
type = "data_script"
script = """
app:cache_set(ctx:get_arg("key"), ctx:get_arg("value"))
return {}
"""

[[endpoint]]
uri = "/script/lua/cache/get/{key:\\w+}"

[endpoint.resource]
type = "data_script"
script = """
return { app:cache_get(ctx:get_arg("key")) }
"""
```

### Use Case 8: Lua HTTP client calls

**specs/http_client.toml:**
```toml
[[endpoint]]
uri = "/script/http/get"

[endpoint.resource]
type = "data_script"
script = """
local resp = http_get(
    "http://example.com/api",
    { ["x-api-key"] = "GET-API-key" }
)

return {
    success = resp:is_success(),
    status  = resp:get_status_code(),
    body    = resp:is_success() and resp:read_body_json() or {}
}
"""

[[endpoint]]
uri = "/script/http/post"

[endpoint.resource]
type = "data_script"
script = """
local resp = http_post_json(
    "http://example.com/api",
    { ["x-api-key"] = "POST-API-key" },
    { message = "Hello from Xepak" }
)

return {
    success = resp:is_success(),
    status  = resp:get_status_code(),
    body    = resp:is_success() and resp:read_body_json() or {}
}
"""
```

### Use Case 9: Anonymous authentication

**specs/anon_auth.toml:**
```toml
[[endpoint]]
uri = "/auth/script/anon"

[[endpoint.pre_processors]]
type = "simple_authentication"
anonymous_auth = true

[endpoint.resource]
type = "data_script"
script = """
return {
    id = ctx:get_auth_id(),       -- empty string for anonymous
    roles = ctx:get_auth_roles(),  -- empty table for anonymous
}
"""
```

### Use Case 10: Default pre-processors (global auth requirement)

**config.toml:**
```toml
default_pre_processors = [
    { type = "simple_authentication", anonymous_auth = false },
]
```

This makes all endpoints require authentication by default. To skip for specific endpoints:

```toml
[[endpoint]]
uri = "/public/info"
pre_processors_ignore_default = true   -- skip default pre-processors

[endpoint.resource]
type = "data_script"
script = """
return { always = "public" }
"""
```

---

## Internal Type System (XepakValue)

Xepak uses a unified dynamic value type that transcends all environments: JSON, CBOR, SQLite, Lua, and TOML.

| XepakType | JSON representation | CBOR representation | SQLite type | Lua type |
|-----------|---------------------|---------------------|-------------|----------|
| `Null` | `null` | `nil` | NULL | `nil` |
| `Boolean` | `true/false` | `true/false` | INTEGER (0/1) | `boolean` |
| `Integer` | number | integer | INTEGER | `number` (integer) |
| `Float` | number | float | REAL | `number` (float) |
| `Text` | string | byte string | TEXT | `string` |
| `Blob` | base64 string | byte string | BLOB | `string` (bytes) |
| `Tuple` | array | array | — | table (1-indexed) |
| `Map` | object | map | — | table (key-value) |

**Key properties:**
- Binary data (`Blob`) serializes as base64 in JSON, raw bytes in CBOR
- Type conversions between environments are automatic and lossless
- Planned future types: `date`, `decimal`

---

## Error Response Format

All errors return a consistent JSON/CBOR structure:

```json
{
    "code": "bad_request",
    "message": "Input argument 'name' length must be greater than 2"
}
```

| Code | HTTP Status | Triggered by |
|------|-------------|--------------|
| `not_found` | 404 | Record not found, or Lua `error_not_found()` |
| `bad_request` | 400 | Input validation failure, schema errors, Lua `error_input()` |
| `forbidden` | 403 | Auth/authorization failures, Lua `error_forbidden()` |
| `internal_error` | 500 | Server errors, Lua `error_server()` |
| `unknown_error` | 520 | Unhandled exceptions |

---

## Response Headers

Paginated responses include:

| Header | Value | When present |
|--------|-------|--------------|
| `X-Limit` | integer | When limit > 0 |
| `X-Offset` | integer | When offset > 0 |

Content-Type is set based on the `Accept` header:
- `application/json` (default)
- `application/cbor` (when `Accept: application/cbor`)

---

## CLI Usage

```bash
xepak-rest [OPTIONS] <config_file>

Options:
  -p, --port PORT        Port to listen on (env: XEPAK_PORT)
  -l, --log LEVEL        Log level filter (env: default)
```

