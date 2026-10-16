# Xepak Documentation

To get a quick overview of how things are working you should look into the [examples](./examples) directory.

Do not forget there is an [AI focused docs](./README-AI.md) built by AI for AI that could also be useful for humans.

## Configuration

All DSL is just a valid TOML file syntax.
This format was selected because TOML does not depend on space indentations,
supports nested structures and allows to split nested structures into several top-level elements.

Xepak configuration could be split among different files and folders
but in order to run the application you need to provide a path to a single root TOML file.

### Main TOML file

Main TOML file with its folder is a configuration entrypoint.
All relative file paths in configuration are based on directory where root TOML is located.

Root config quick overview.
```toml
# Application port
port = 4321 # 8080 if not defined

# Directory with specs files
specs_dir = "./specs" # default value is ./specs - relative to root config folder

storage = [] # this one requires at least one entry

registry = {} # some k-v registry
```

#### Storage

One day storage section would have multiple DB connections.
But now you should have only one for SQLite.

```toml
[[storage]]
id = ""                          # datasource identifier (do not define it use default one)
type = "sqlite"
file = "/path/to/db.sqlite3"     # empty string = shared in-memory DB; relative paths resolve against config file dir
migrations_dir = "migrations"    # path to folder with migrations *.sql files
create_db = true                 # create DB if missing
wal = true                       # enable WAL mode
```

#### Registry

Registry is a key-value dictionary of arbitrary data that you can access from Lua scripts.
Any value defined here will be loaded by key as a full object with all nested data.

```toml
[registry]
int_val = 1
float_val = 2.2
string_val = "registry_val"
map_val = {
    host = "localhost",
    "weird.key" = "some_value",
}
```

☝️But there are some system registry keys that behave differently.

**auth** key is an authentication registry and must follow exact structure:

```toml
[registry]
# It must be an array
auth = [
    # you must define id, key and optional list of roles
    { id = "boss", key = "BossKEY", roles = [ "ADMIN", "MANAGER" ] },
    { id = "manager", key = "ManagerKEY", roles = [ "MANAGER", ] },
    { id = "user", key = "UserKEY" },
    # empty id is used for anonymous auth if it is enabled
    { id = "", key = "", roles = [ "anonymous" ] },
    # no need to store actual API keys in TOML - you can load them from ENV variables
    { id = "hacker", key = "ENV_VARIABLE_WITH_THE_KEY", from_env = true },
]
```

**secrets** is a map of secret values that can be accessed from Lua.
The main point here is that secret values can be loaded from ENV variables.
```toml
secrets = {
    # secret value is loaded from ENV var
    secret1 = { type = "env", name = "SECRET_ENV_1" },
    # just a simple text in TOML
    secret2 = { type = "text", value = "raw_value" },
}

```
### Specifications directory

Defined by a `specs_dir` and could hold one or many TOML files for REST, JSON-RPC, MCP specifications.

Xepak utilizes TOML file syntax capabilities to combine multiple files together into one single specification.
Imagine that all your TOML files will be read as a single one.
```toml
# this is array definition that can't be split between files
endpoint = [
    {uri = "users/get"},
    {uri = "users/list"},
]

# but you can represent it in separate blocks in different files
[[endpoint]]
uri = "users/get"

[[endpoint]]
uri = "users/list"

# same approach works for maps
methods = {
    get = {query="select * from ..."},
    list = {query="select * from ..."}
}

# map splitted
[methods.get]
query="select * from ..."

[methods.list]
query="select * from ..."
```

### REST endpoints

Should be defined in `[[endpoint]]` block.
Each endpoint handles some `uri` which could be many different URLs if you are using URI path params.

Basic endpoint definition. 
```toml
[[endpoint]]
uri = "/users/{user_id:\\d+}"        # {name} path params captured automatically; regex allowed after ':' (escape backslash in TOML)
allow_methods = ["GET", "DELETE"]    # optional HTTP methods whitelist; if absent only GET accepted
single_record_response = true        # if true return 404 if DB returns no records
fetch_limit = 20                     # max rows for paginated queries (also the default limit)

# optional; custom names for limit/offset arguments
# if you need to have `/users/list?count=1` instead of `/users/list?limit=1`
limit_arg = "limit"
offset_arg = "offset"

# if true does not allow requests with arguments not defined in the schema
strict_schema = false
```

**Limit & offset** arguments are handled differently.
If your input has `limit` or `offset` argument (or if you defined `fetch_limit`) then the response will have `X-Limit` and `X-Offset` headers.
Response header will return exact values that were used to fetch records from DB.
These arguments will be represented in SQL like this `{{-limit-}}`, `{{-offset-}}`.

In order to use limit/offset feature you have to define `fetch_limit` upper bound
and then you can use query arguments regardless of having limit/offset in the input form client.


### Resources

Resource is the core concept to fetch data from DB.
It works the same for REST, JSON-RPC and MCP.
Resource will produce data output based on computed input with user input.


The basic resource type is `query`.
It is just an SQL string with named arguments in it.
Argument names match the ones from URI path, query string, or keys from object sent in POST/PUT payload.

```toml
[[endpoint]]
# ...
[endpoint.resource]
type = "query"
# optional param - skip it
data_source = "" # you can interact with different datasources, but please don't

# user_id could be taken from URI `/user/123` or from query string ``/user?user_id=123``
# such resource should be combined with `single_record_response = true`
query = "SELECT * FROM users WHERE id={{user_id}}"

# ANOTHER EXAMPLE
# /users/find?country=UK&offset=100
# this one require `fetch_limit = YOUR_LIMIT`
query = "SELECT * FROM users WHERE country={{country}} LIMIT {{-limit-}} OFFSET {{-offset-}}"
```

Dynamic queries can be constructed with `query_script_lua` resource type.
Lua script is using to build and return SQL query.
But you can do many fancy things before that in Lua.

```toml
[[endpoint]]
# ...
[endpoint.resource]
type = "query_script_lua"
# triple quotes is a TOML string syntax
# work well if there are not triple quotes in Lua code
script = """
local query = query_builder("SELECT * FROM users")

-- can conditionally add some SQL parts depending on the input
local parts = {};
if ctx:has_arg("name") then
    local v = ctx:get_arg("name")
    if #v < 3 then
        -- will fail immediately with proper response error
        error_input("Input argument 'name' length must be greater than 2")
    end
    -- adding SQL condition to parts array
    table.insert(parts, "username = {{name}}")
end

if ctx:has_arg("pass") then
    table.insert(parts, "password = {{pass}}")
end

-- cool function that joins parts with prefix WHERE and delimiter OR 
query:add_joined_parts("WHERE", parts, "OR", "");

-- just append new SQL portion to builder
query:add("LIMIT {{-limit-}} OFFSET {{-offset-}}")
return query:build()
"""
```

Another type of resource is a `data_script` which passes any response from Lua to client as is.
You can make API respond with any arbitrary data of any shape.

```toml
[[endpoint]]
# ...
[endpoint.resource]
type = "data_script"
script = """
-- will return object with info about user authentication
return {
    id = ctx:get_auth_id(),
    roles = ctx:get_auth_roles(),
    is_admin = ctx:has_auth_role("admin"),
    is_manager = ctx:has_auth_role("manager"),
}
"""
```

### Pre-processors

This is a shared concept of a pre-defined or a custom logic that interacts with input data, request context and runs before resource. 

Things to remember:

 - execution (definition) order matters
  - pre-processor can be reused
  - default pre-processors could be configured for all resources
  - you can drop default pre-processors for particular resource
 - Lua scripting supported

#### Pre-processor types

| Type | Description | Key fields |
|------|-------------|------------|
| `ref` | Reference to a shared pre-processor | `id: string` |
| `parse_body_args` | Merges POST/PUT/PATCH body into request context | `null_if_absent: [keys]` - set listed keys to null if they are missing in request |
| `config_authentication` | API key lookup in registry | `anonymous_auth: bool` |
| `storage_authentication` | Database-backed token auth | `data_source`, `query`, `cache_ttl_sec` |
| `authorize` | Role-based authorization rules | `rules: string` |
| `lua_script` | Lua script with custom logic | `script: string` |

#### Shared pre-processors

Are defined in the separate DSL block and can be reused later by reference.
In TOML `shared_pre_processors` is a root dictionary element where key is pre-processor ID.

```toml
[shared_pre_processors.my_pp_id]
type = "lua_script"
script = """
-- custom Lua script
"""

# somewhere in endpoint specs
[[endpoint.pre_processors]]
type = "ref"
id = "my_pp_id"

```

#### Default pre-processors

Are applied to all endpoints by default unless explicitly ignored.
Authentication is the most obvious use-case here.

```toml
# this is a root level DSL array element
# pre-processors order does matter
default_pre_processors = [
    { type = "config_authentication", anonymous_auth = false },
]
```

### Handling request body payload

By default Xepak is not parsing request body content.
There is an assumption that payload type and parsing rules could vary in future.

In order to add data from HTTP request body you have to manually add `parse_body_args` pre-processor.
It works only for POST/PUT/PATCH request types and parse data from JSON or CBOR request types.

Allowed additional property `null_if_absent` with a list of argument names that will be explicitly set to null if not present in the request body.
Could be useful with insert like queries when you are Ok with a null fields.

The main expectation for `parse_body_args` is that request body will be a key-based object.
It can be an array of objects too but dealing with such inputs is not easy now.


### JSON-RPC services

You can bind URI to a single JSON-RPC server, different URI - different JSON-RPC server.
The main DSL disadvantage here is that JSON-RPC specs for one URI cannot be split between files.
The positive thing is that with URI path arguments multiple urls will be handled by a single DSL file like `rpc/orders/uk`, `rpc/orders/by` etc.

Only POST HTTP requests are allowed for RPC endpoints.
CBOR is not supported just to follow specs not because I can't do it.

JSON-RPC is defined inside `[[rpc]]` block and most configuration resides inside 'rpc.methods' dictionary.
The `methods` maps a method name to a method specs that similar to a REST endpoint.
Has properties: `resource`, `schema`, `pre_processors`, `pre_processors_ignore_default`, `single_record_response`.


```toml
[[rpc]]
uri = "/rpc"

[rpc.methods.all_posts]
resource = { type = "query", query = "SELECT * FROM posts ORDER BY id" }

[rpc.methods.get]
schema.input = { type = "tuple", items = [ { type = "int", required = true } ] }
single_record_response = true
# NOTICE that schema is defined as a tuple (not key-value map).
# So arguments are accessed by indexes
resource = { type = "query", query = "SELECT * FROM posts WHERE id = {{0}}" }
```

### MCP services

In Xepak MCP service is just a narrowed down variant of JSON-RPC.
So the configuration is almost the same only with MCP specific properties.

Root configuration element is `[[mcp]]`, it is bound to `uri`.
The `tools` table defines MCP tool similar to REST methods.
Contains properties:`resource`, `schema`, `pre_processors`, `pre_processors_ignore_default`.

Core MCP functionality is generated automatically: `initialize`, `notifications/initialized`, `ping`, `tools/list`, `tools/call`.

Notice! 🫵 You must define input schema for your tools! MCP specs require it.

```toml
[[mcp]]
uri = "/mcp"
server_version = "1.0.0"
protocol_version = "2025-03-26"
name = "Posts server"
instructions = "Provides access to posts data"

[mcp.tools.list_posts]
title = "List posts"
description = "List posts with pagination by limit/offset"
annotations.read_only = true
schema.input = {
    type = "dict",
    items = {
        limit  = { type = "int", required = true },
        offset = { type = "int", required = true },
    }
}
resource = { type = "query", query = "SELECT * FROM posts ORDER BY id LIMIT {{limit}} OFFSET {{offset}}" }
```
Additional information:

- `annotations` property could have next (optional) flags: `read_only`, `destructive`, `idempotent`, `open_world`
- `tools/call` schema must be only in a form of a `dict`
- tool response for now is only in a form of a single content element of type `text` (should not care about it much)


## Input context

Request context is an aggregated state that holds data from the input.
It holds not only raw values but also data enriched/converted by pre-processors.

On each step CTX internal state can be updated.
You can also change it in Lua handlers before SQL query will be executed.
`HTTP -> CTX -> [pre-processor 1] -> CTX -> [pre-processor N] -> CTX -> [(resource-handler) -> CTX -> (DB query)]`

Memorize that the request context holds:

- uri template params as key-value pairs `/get/user/{user_id:\\d+}`
- querystring as a key-value pairs
- deserialized POST/PUT/PATCH payload as key-value or index based array/tuple
- any arbitrary key-value data
- auth data: client_id, roles
- limit/offset for paginated queries

Context is SQL builder focused so it access all k/v data from the same API.
Values are returned from context based on priority:

- custom arguments
- uri path arguments
- request body
- query string

So if you will set custom argument `title` it would be used in SQL `title = {{title}}` instead of a value loaded from query string.


## Lua scripting

Lua is our escape path from using Pl/SQL.
See [Lua manual](https://www.lua.org/manual) if you are not familiar with this language.
Xepak provides additional functionality for Lua environment.

I assume that Lua scripts would fit in one text block without requiring external libraries etc.

### Context (`ctx`)

Each Lua script receives context object linked with the current request.

```lua
-- returns HTTP request method
ctx:get_request_method()

-- Arguments
ctx:has_arg("user_id") -- true if arg exists
ctx:get_arg("user_id") -- return arg value
ctx:set_arg("user_id", user.id) -- set custom argument

-- Authentication data (if available)
ctx:get_auth_id()          -- string or null
ctx:get_auth_roles()       -- returns table of strings
ctx:has_auth_role("admin") -- check role ignoring case

-- This one creates clone of input context object
-- should be used to pass input ctx to other lua functions as-is
ctx:load_input()
```

### Query Builder

Handy API to easily build text queries.

```lua
-- first creating builder instance with 1st part of the query
local query = query_builder("SELECT * FROM users")

-- creating table to hold multiple query parts to join the later
local where_parts = {}
if ctx:has_arg("name") then
    table.insert(where_parts, "username = {{name}}")
end
if ctx:has_arg("pass") then
    table.insert(where_parts, "password = {{pass}}")
end

-- if where_parts table is not empty
-- adding to query string with prefix "WHERE"
-- where_parts elements joined with separator "OR"
-- also you can add suffix here too
query:add_joined_parts("WHERE", where_parts, "OR", "")

-- now add just a regular static query suffix
query:add("LIMIT {{-limit-}} OFFSET {{-offset-}}")

-- building final query and returning it from script
return query:build()
```

### Application object (`app`)

Lua also has application level context as an `app` variable.
You can use it to access app level data.

```lua
-- Accessing registry
-- Get custom registry value from the root TOML
app:get_registry_value("my_property")
-- Get single secret from registry in the root TOML
app:get_secret("secret_key")

-- Caching
app:cache_get("my_key") -- return cached value or nil
app:cache_set("my_key", "value") -- set cached value with default TTL
```

### Storage query functions

This is a big cool Lua feature. You can run sub quries from Lua before processing resource.

Query functions signature is `(query_string, args)` where:

 - query_string - query string with `{{key_name}}` placeholders
 - args - is a Lua table or input object from `ctx:load_input()`

```lua
-- Query multiple rows: returns Lua table of tables (1-indexed)
storage_query("SELECT * FROM users WHERE id={{id}}", {id = 42})

-- Query single row: returns Lua table (map) or nil
storage_query_one("SELECT * FROM users WHERE id={{id}}", {id = 42})

-- Query single value (first column of first row): returns scalar or nil
storage_query_value("SELECT COUNT(*) FROM users", {})
```
Note that `storage_query*` always runs against the **default** data source (`""`)
They cannot target a named storage from within Lua.

### Error/interrupt functions

Error functions allow you to stop request processing and return error to client from any Lua script.

Error function output should be formatted to proper output for REST/JSON-RPC/MCP.

```lua
error_input("Bad request message")    -- 400 Bad Request
error_not_found("Record not found")   -- 404 Not Found
error_forbidden("Access denied")      -- 403 Forbidden
error_server("Internal error")        -- 500 Internal Server Error
```

### Logging

You may need to send logs from Lua to application log output.

```lua
log_info("Users count is " .. count)
log_debug(string.format("I said users count is %d", count))
```

### HTTP client

What if during request processing you will need to query some external API.

```lua
-- You should not use this API if can't understand examples below

-- GET
-- returns response object with methods: is_success(), get_status_code(), read_body_string(), read_body_json()
local resp = http_get("http://example.com/api", { ["x-api-key"] = "key123" })

-- If you need to send JSON via POST
local resp = http_post_json(
    "http://example.com/api",
    { ["Content-Type"] = "application/json" },
    { message = "Hello from Xepak" }
)
```

### Blob types support

Lua works with binary types as strings.
It is not very cool.
Thus there is functions to convert Lua strings into proper blob data and back.

```lua
-- Create a blob (binary) value from a Lua string (bytes)
local b = blob_type("\x01\x02\x03")
-- Getting lua string from bytes object
-- that can be passed in the input
local bytes = b:as_bytes()
```


## Internal type system

Project is using dynamic type value wrapper `XepakValue`
which can be Map, Tuple, Integer, Float, Blob, etc.

`XepakValue` transcends all environments and data formats.
It is used to serialize/deserialize data to JSON/CBOR, represents DB output, TOML configuration records, cached values and variables inside LUA VM.

Current type system allows you to:

 - use complex toml structures in registry and read them natively in Lua
 - store arbitrary data to cache and accessing it from Lua
 - reads DB output natively in Lua and enrich it
 - convert Lua tables to JSON/CBOR

Lua type system is limited but it allows to pass through any wrapped data.
Thus output from DB travels via Lua VM unchanged.

Compared to JSON, `XepakValue`:

 - align with SQLite type system
 - support binary data (represented as base64 in JSON)
 - has schema and value validators
 - do not loose type in complex data flows: CBOR -> JSON -> SQL -> LUA -> CBOR
 - can convert JSON string representation to proper data format
 - ...
 - could have new types later: (date, decimal, etc.)

## Schema

Schema allows you to convert input data to proper internal representation
and do the same with output.

You can apply schema some input elements and accept others "as is" (non strict schema).
Strict schema means that only fields/elements that are defined in schema are allowed in the input.

Supported schema types are: `text`, `boolean`, `int`, `float`, `blob`, `tuple`, `dict`.
Each element can be marked as a `required` and have one or more validators.
Elements of type `dict` and `tuple` support nested elements in `items` property.

Schema definition is hierarchical and can be applied to any type of input.

Root level schema expected to be of type `dict` or `tuple`.

**Dict schema** handles input as key-value pairs, applies to URI, querystring and POST data where root element is a k-v object.  
**Tuple schema** is a special case for POST/PUT requests and JSON-RPC/MCP arguments where input could be a tuple/array of elements.

DSL for REST endpoints is very similar to RPC and MCP.
```toml
# ...
request_type = "POST"

# by default it is false
# if true will allow only "title" and "content" in the input.
strict_schema = true

[endpoint.schema.input]
type = "dict"
items = {
    title = {
        type = "text",
        required = true,
        validate = { kind = "range", from = 5, to = 255 },
    },
    # you can skip `content` from payload it is not marked as a required
    content = { type = "text" }
}

# Output schema is required only if DB for some reason returns differnt type like number as a string.
[endpoint.schema.output]
type = "dict"
items = {
    id = { type = "int" }
}

```

RPC method `get_post` accept arguments as a tuple
where first element is `int` (probably id) and second element is `tuple` where elements are strings.
```toml
[rpc.methods.get_post]
schema.input = {
    type = "tuple",
    items = [
        { type = "int", required = true },
        # it is not required to define nested items
        # in this case it will only validate that element is a tuple/array
        {
            type = "tuple",
            required = true,
            items = [ { type = "text" }, ],
            # if repeat=true it will apply schema definitions
            # from items to all tuple elements in sequence
            items_repeat = true,
        },
    ]
}
```

MCP schema where method arguments are key-value dictionary.
```toml
[mcp.tools.get_post]
title = "Get post"
description = "Get a single post by its ID"
schema.input = {
    type = "dict",
    items = {
        post_id = { type = "int", required = true }
    }
}
```

### Validators

Schema element has `validate` field that holds a **single** validator object.

Validator entries specs.

| `kind` | Fields | applies to |
|--------|--------|------------|
| `range` | `from`, `to` (int) | text length, int/float value, tuple length |
| `range_float` | `from`, `to` (float) | same as range but with float bounds |
| `not_null` | — | rejects null/undefined |
| `and` | `nested` (list of validators) | all must pass |
| `or` | `nested` (list of validators) | at least one must pass |

Kind `and` / `or` take a `nested` **array** of validators, which themselves may be `and` / `or` again — so you can build arbitrary trees.

```toml
# 1) Single validator — text length must be 5..255
validate = { kind = "range", from = 5, to = 255 }

# 2) AND — all nested validators must pass (not null AND length 5..255)
validate = { kind = "and", nested = [
    { kind = "not_null" },
    { kind = "range", from = 5, to = 255 },
] }
```

## Authentication & Authorization

### Authentication

Xepak utilize "x-api-key" HTTP header as a main method to authenticate client.
Fallback to "Authorize: Bearer ..." is available only for JSON-RPC and MCP endpoints.

Authentication is handled by pre-processors.

**Authentication via configuration** is performed by `config_authentication` pre-processor.
It relies only on TOML file where API keys, IDs and roles must be defined.

```toml
# ... other config directives

[registry]
# registry holds arbitrary data
# but some keys must follow exact structure
# auth data allowed only with the next format
auth = [
    { id = "boss", key = "BossKEY", from_env = false, roles = [ "ADMIN", "MANAGER" ] },
    { id = "manager", key = "ManagerKEY", from_env = false, roles = [ "MANAGER", ] },
    { id = "user", key = "UserKEY", from_env = false },
    # see how keys can be loaded from ENV variables
    { id = "hacker", key = "ENV_VARIABLE_TO_LOAD_KEY_FROM", from_env = true },
    # record with empty id will be used for anonymous auth
    { id = "", key = "", roles = [ "unknown"] },
]
```

After providing auth data in config you need to configure pre-processor.

```toml
[[endpoint.pre_processors]]
type = "config_authentication"
# this field is optional, if true - allow anonymous authentication without API key
anonymous_auth = false
```
**Authentication via storage** is performed by `storage_authentication` pre-processor.
You must store auth data in DB in such way to be able to retrieve it like this: `SELECT id,roles FROM ... WHERE ... key = {{api-key}}`. Where roles represented as a string `manager,billing,office`.

Pre-processor will query DB, extract auth data and cache it for some time.

```toml
[[endpoint.pre_processors]]
type = "storage_authentication"
query = "SELECT user_id AS id, roles FROM api_tokens WHERE api_key = {{api-key}}"
# caching auth data for N seconds to reduce queries count
cache_ttl_sec = 10 
```

### Authorization

Authorization is performed by an "authorize" pre-processor.
It should be executed after authentication processor to be able to read user id & roles from the context.
Authorize pre-processor is implementation-agnostic, it does not depend on authentication mechanism.

Access control is based on auth expression rules. 

```toml
[[endpoint.pre_processors]]
type = "authorize"
rules = "#boss OR (ADMIN AND MANAGER)"
```

Rules expressions are very simple and straightforward:

- user roles and ids must not contain any spaces
- can match by role name (ignoring case)
- user ID is prepended with "#" char
- OR, AND - reserved keywords (uppercase only)
- parentheses are combining similar conditions
- only one type of condition allowed inside parentheses (can't mix AND + OR at the same level)

Examples:

- `(manager AND billing) OR boss` - allowed for `boss` role or user that has two roles `manager` and `billing`
- `manager AND billing OR boss` - will fail - mixing operators on the same level
- `admin OR (manager AND (billing OR #45687))` - allowed for `admin` role or a `manager` but only if it has `billing` role or `id=45687`.

