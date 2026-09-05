# Xepak Documentation

Not ready yet but you can look into curret configuration DSL in [examples](./examples/xepak/).
I hope that DSL format is self describing.

## Configuration

### Cache

### Data sources

### Registry

### Pre processors

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
