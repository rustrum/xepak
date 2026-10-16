# Xepak Documentation

To get quick overview of how things are working you should look into [examples](./examples) directory.

Do not forget there is an [AI focused docs](./README-AI.md) built by AI for AI that also could be usefult for humans.

## Configuration

All DSL is just a valid TOML file syntax.
This format was selected because TOML does not depend on space identations,
supports nestes structures and allows to split nested structures into several top-level elements.

Xepak configuration could be splitted amound diferent files and folders
but in order to run application you need provide path to a single root TOML file.

### Main TOML file

Main TOML file with its folder is a configuration entrypoint.
All relative file paths in configuration are based on directory where root TOML is located.

Root config quick overview.
```toml
# Application port
port = 4321 # 8080 if not defined

# Directory with specs files
specs_dir = "./specs" # default value is ./specs - relative to root config folder

storage = [] # this one is require at least one entry

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

☝️But there are some system registry keys that behaves differently.

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

**secrets** is a map of secret values that could be accessed from Lua.
The main point here is that secret value can be loaded from ENV variable.
```toml
secrets = {
    # there core feature - secret value loaded from ENV var
    secret1 = { type = "env", name = "SECRET_ENV_1" },
    # Just a simple text in TOML
    secret2 = { type = "text", value = "raw_value" },
}

```
### Specifications directory

Defined by a `specs_dir` and could hold one or many TOML files for REST,JSON-RPC,MCP specifications.

Xepak utilize TOML file syntax capabilities to combine multiple files together into one single specification.
Imagine that all your TOML files will be read as a single one.
```toml
# this is array definition that can't be splitted between files
enpoint = [
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
Each endpoint handle some `uri` which could be many different URLs if you are using URI path params.

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

**Limit & offset** arguments are handling differently.
If your input has `limit` or `offset` argument (or if you defined `fetch_limit`) than response will have `X-Limit` and `X-Offset` headers.
Response header will return exact values that were used to fetch recrods from DB.
These arguments will be represented in SQL like this `{{-limit-}}`, `{{-offset-}}`.

In order to use limit/offset feature you have to define `fetch_limit` upper bound
and then you can use query arguments regardless of having limit/offset in the input form client.


### Resources

Resource is the core concept to fetch data from DB.
It works the same for REST,JSON-RPC and MCP.
Resource will produce data output based on computed input whith user input.


The based resource type is `query`.
It just an SQL string with named arguments in it.
Aruments names are matching the one from URI path, query string, or keys from object sent in POST/PUT payload.

```toml
[[endpoint]]
# ...
[endpoint.resource]
type = "query"
# optional param - skip it
data_source = "" # you can interact with differnt datasources, but plese don't

# user_id could be taken from URI `/user/123` or from query string ``/user?user_id=123``
# such resource should be combined with `single_record_response = true`
query = "SELECT * FROM users WHERE id={{user_id}}"

# ANOTHER EXAMPLE
# /users/find?country=UK&offset=100
# this one require `fetch_limit = YOUR_LIMIT`
query = "SELECT * FROM users WHERE country="{{country}}" LIMIT {{-limit-}} OFFSET {{-offset-}}"

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

-- can conditionnaly add some SQL parts depending on the input
local parts = {};
if ctx:has_arg("name") then
    local v = ctx:get_arg("name")
    if #v < 3 then
        -- will fail immediately wit proper response error
        error_input("Input argument 'name' length must be greater than 2")
    end
    -- adding SQL condition to parts array
    table.insert(parts, "username = {{name}}")
end

if ctx:has_arg("pass") then
    table.insert(parts, "password = {{pass}}")
end

-- cool funciton that joins parts with prefix WEHRE and delimiter OR 
query:add_joined_parts("WHERE", parts, "OR", "");

-- just append new SQL portion to builde
query:add("LIMIT {{-limit-}} OFFSET {{-offset-}}")
return query:build()
"""
```

Another type of resource is a `data_script` which pass any response from Lua to client as is.
You can make API respond with any arbigrary data of any shape.

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
 - pre-processord can be reused
 - default pre-processors could be configured for all resources
 - you can drop defalt pre-processors for particular resource
 - Lua scripting supported

Pre-processor types:

 - `ref` - Reference to a shared pre-processor
 - `parse_body_args` - Merges POST/PUT/PATCH body (flat map) into request arguments; no-op for other methods
 - `config_authentication` - API key lookup in registry
 - `storage_authentication` - Database-backed token auth
 - `authorize`  - Role-based authorization rules
 - `lua_script` - Lua script for custom validation, auth etc


**Shared pre-processors** defined in the separate DSL block and can be reused later by reference.
In TOML `shared_pre_processors` is a root dictionary element where key is pre-processor ID.

```toml
[shared_pre_processors.my_pp_id]
type = "lua_script"
script = """
-- custom Lua script
"""

# somewhere in endpint specs
[[endpoint.pre_processors]]
type = "ref"
id = "my_pp_id"

```

**Default pre-processors** are the type of pre processors that apply to all endpoints by default.
Authentication is the most obvious use-case here.

```toml
# applied to all endpoints unless explicitly ignored
# this is a root level DSL array element
# pre-processors order does matter
default_pre_processors = [
    { type = "config_authentication", anonymous_auth = false },
]
```

### JSON-RPC

### MCP


## Authentication & Authorization

### Authentication

Xepak utilize "x-api-key" HTTP header as a main method to authenticate client.
Fallback to "Authorize: Bearer ..." is available only for JSON-RPC and MCP endpoints.

Authentication is handled by pre-processors.

**Authentication via configuration** is performed by `config_authentication` pre-processor.
It is rely only on TOML file where API keys, IDs and roles must be defined.

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
# this field is optional if true - authorize without api key.
anonymous_auth = false
```
**Authentication via storage** is performed by `storage_authentication` pre-processor.
You must store auth data in DB in such way to be able to retreive it like this: `SELECT id,roles FROM ... WHERE ... key = {{api-key}}`. Where roles represented as a string `manager,billing,office`.

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
It should be executed after authentication processor to be able to read use id & roles from the context.
Authorize pre-processor is implementation-agnostic, it does not depend on authentication mechanism.

Access control is based on auth expression rules. 

```toml
[[endpoint.pre_processors]]
type = "authorize"
rules = "#boss OR (ADMIN AND MANAGER)"
```

Rules expressions are very simple and straightforward:

- user roles and ids must not contains any spaces
- can match by role name (ignoring cases)
- user ID is prepended with "#" char
- OR, AND - reserved keywords (uppercase only)
- parentheses are combining similar conditions
- only one type of condition allowed inside parentheses (can't mix AND + OR at the same level)

Examples:

- `(manager AND billing) OR boss` - allowed for `boss` role or user that has two roles `manager` and `billing`
- `manager AND billing OR boss` - will fail - mixing operators on the same level
- `admin OR (manager AND (billing OR #45687))` - allowed for `admin` role or a `manager` but only if it has `billing` role or `id=45687`.


## Internal type system

Project is using dynamic type value wrapper `XepakValue`
which can be Map, Tuple, Integer, Float, Blob, etc.

`XepakValue` transcends all environments and data formats.
It is used to serialize/deserialize data to JSON/CBOR, represents DB output, toml configuration records, cached values and variables inside LUA VM.

Current type system allows you to:

 - use complex toml structures in registry and read them natively in Lua
 - store arbitrary data to cache and accessing it from Lua
 - reads DB output natively in Lua and enrich it
 - convert Lua tables to JSON/CBOR

Lua type system is limited but it allows to pass through any wrapped data.
Thus output from DB travels via Lua VM unchanged.

Compared to JSON `XepakValue`:

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

You can apply schema only to one input element and accept others "as is" (non strict schema).
Strict schema means that only fields/elements that are defined in schema are allowed in the input.

Supported schema types are: `text`, `boolean`, `int`, `float`, `blob`, `tuple`, `dict`.
Each element can be marked as a `required` and have one ore more validators.
Elements of type `dict` and `tuple` support nested elements in `items` propery.

Schema definition is hierarchical and can be applied to any type of input.

Root level schema expected to be of type `dict` or `tuple`.

**Dict schema** handles input as key-value pairs, applies to URI, querystring and POST data where root element is an k-v object.  
**Tuple schema** is a special case for POST/PUT requests and JSON-RPC/MCP arguments where input could be a tuple/array of elements.

DSL for REST endpoins is very similar to RPC and MCP.
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

## Endpoint input/output


## Scripting
