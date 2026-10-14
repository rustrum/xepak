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

In order to manage differenty data types and their conversion I created dynamic value wrapper `XepakValue`
which can be Map, Tuple, Integer, Float, Blob, etc.

The core idea here is that `XepakValue` transcends all environments and data formats.
It is used to serialize/deserialize data to JSON/CBOR, represents DB output, toml configuration records, cached values and scripting environment variables.

Current type system allows you to:

 - use complex toml structures in registry and read them natively in Lua
 - store and read any arbitrary data to cache from Lua
 - reads DB output natively in lua and enrich it
 - output of Lua scripts will be converted to JSON/CBOR

Lua type system has it limits.
It is barely possible to create variable of type that Lua is not supporting.
But passing such types from DB to output is not the issue.

If compared to JSON `XepakValue`:

 - align with SQLite type system
 - support binary data (represented as base64 in JSON)
 - has schema and value validators
 - do not loose type in complex data flows: CBOR -> JSON -> SQL -> LUA -> CBOR
 - can convert JSON string representation to proper data format
 - ...
 - planned to add new types later: (date, decimal, etc.)


## Endpoint input/output


## Scripting
