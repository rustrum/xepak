## Project overview

This is a rust/cargo project.
The core functionality is to provide REST/RPC/MCP functionality for DB backens based on DSL specifications.
DSL is defined in TOML files. DSL supports LUA scripts.


## Project features

### DSL configuration

All structs which names has suffix `Specs` are parts of a *.toml DSL specification.

### XepakValue type

Hierarchical multi type wrapper. XepakValue can be serialized to/from JSON and CBOR.
We deserialize input to XepakValue and provide XepakValue output from endpoint handlers.

### Schema type

Hierarchical type that defines schema for XepakValue.
Could be applied to input and outpus as well.


## Agent guidelines

- Must not edit code with bash commands like `cat` or `sed`!
- To edit files must uses existing tooling.
- Must edit code only in small semantical chunks about 100 lines of code.
- Never write whole file in one step or half or the file.

