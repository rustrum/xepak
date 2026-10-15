# Xepak Documentation

Not ready yet but you can look into curret configuration DSL in [examples](./examples/xepak/).
I hope that DSL format is self describing.

## Configuration

### Cache

### Data sources

### Registry

### Pre processors

### Auth functionality



#### Authentication

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

Pre-processor will qury DB, extract auth data and cache it for some time.

```toml
[[endpoint.pre_processors]]
type = "storage_authentication"
query = "SELECT user_id AS id, roles FROM api_tokens WHERE api_key = {{api-key}}"
# caching auth data for N seconds to reduce queries count
cache_ttl_sec = 10 
```

#### Authorization

By default authorization performed by an "authorize" pre-processor.
It should be executed after authentication processor.
Expects user id/roles tob be already in the context.
Authorize pre-processor is implementation-agnostic, it does not depend on authentication mechanism.

Access control is based on auth expression rules. 

```toml
[[endpoint.pre_processors]]
type = "authorize",
rules = "#boss OR (ADMIN AND MANAGER)"
```

Rules expressions are very simple and straightforward:

- user roles and ids must not contains any spaces
- can match by role name (ignoring cases)
- user ID is prepended with "#" char
- OR, AND - reserved keywords (uppercase only)
- parentheses groups similar conditions
- only one type of condition allowed inside parentheses (can't mix AND + OR at the same level)

Examples:

- `(manager AND billing) OR boss` - allowed for `boss` role or user that has two roles `manager` and `billing`
- `manager AND billing OR boss` - will fail - mixing operators on the same level
- `admin OR (manager AND (billing OR #45687))` - allowed for `admin` role or a `manager` but only if it has `billing` role or `id=45687`.

### Entrypoints

## Internal type system

Project is using dynamic type value wrapper `XepakValue`
which can be Map, Tuple, Integer, Float, Blob, etc.

`XepakValue` transcends all environments and data formats.
It is used to serialize/deserialize data to JSON/CBOR, represents DB output, toml configuration records, cached values and variables inside LUA VM.

Current type system allows you to:

 - use complex toml structures in registry and read them natively in Lua
 - store and read any arbitrary data to cache from Lua
 - reads DB output natively in lua and enrich it
 - convert LUA tables to JSON/CBOR

LUA type system is limited but it allows to pass through any wrapped data.
Thus output from DB goes through LUA VM unchanged.

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

By default it is allowed to apply schema only to one input element and accept others "as is" (non strict schema).
Strict schema means that only fields/elements that are defined in schema allowed.

Supported schema types are: `text`, `boolean`, `int`, `float`, `blob`, `tuple`, `dict`.
Each element can be marked as a `required`.
At leas one validator can be added to each schema element.
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
            # if repeat=true will apply schema from `items` tuple elements
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
