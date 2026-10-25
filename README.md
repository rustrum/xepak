<p align="center" width="100%" style="text-align:center">
<img src="./xepak.svg" alt="Xepak REST,RPC,MCP bulder for your DB" width="60%"/>
</p>

<p align="center" width="100%" style="text-align:center">
<a href="https://crates.io/crates/xepak"><img src="https://img.shields.io/crates/v/xepak?style=for-the-badge&logo=rust" alt="Crates.io"></a>
<a href="https://docs.rs/xepak"><img src="https://img.shields.io/docsrs/xepak?style=for-the-badge&logo=docs.rs" alt="Released API docs"></a>
</p>
<p align="center" width="100%" style="text-align:center">
<a href="https://x.com/rumatoest"><img src="https://img.shields.io/twitter/follow/rumatoest?style=for-the-badge&color=blue&logo=x&label=rumatoest" alt="Follow me on X(twitter)" /></a>
<a href="https://www.threads.com/@rumatoest"><img src="https://img.shields.io/badge/rumatoest-%20-black?style=for-the-badge&logo=threads&color=black" alt="Threads" /></a>
<a href="https://www.reddit.com/user/rumatoest"><img src="https://img.shields.io/reddit/user-karma/combined/rumatoest?style=for-the-badge&color=f3562e&logo=reddit&label=u/rumatoest" alt="Follow me on reddit"/></a>
<!-- https://img.shields.io/hackernews/user-karma/rumatoest?style=for-the-badge&color=ff6600&label=Me+on+Hacker+News -->
</p>

## TL;DR

Imagine PostgREST but instead of Haskell with PL/SQL it is based on Rust with LUA and focused on Sqlite (and other DBs).


## Summary

Xepak is a DSL configurable REST,JSON-RPC,MCP service that runs against SQL DB. Support SQLite for now with other DBs planned in the future.

**Why focus on SQLite?** Because it is amazing and extremely fast DB.
Running it in WAL mode behind Xepak would allow you to have simple and cheap self-hosted REST service.

## How can I use it now?

Given its current SQLite support, Xepak is ideal for:

- 👨🏻‍💻 **rapid prototyping:** quickly buid a stateful API without writing backend boilerplate
- 🌐 **self-hosted services:** running on your own hardware or VMs with zero cloud dependency, ensuring you retain full ownership of your data
- 📟 **edge computing and IoT:** deploying a lightweight, high-performance API on a Raspberry Pi or other embedded devices
- 💻 **local-first applications:** serving as a sync layer or API for desktop applications that store data locally
- 🤖 **[local RAG for AI/LLM sandboxes](./examples/rag/README.md):** keeping AI agent knowledge local to prioritize data privacy and ownership


## Current project status

The first MVP is almost here! 🥳 No breaking changes expecting.

Need to polish the documentation and add more usage examples.


## Project Documentation

### 📕 For human beings

Available in the [README-DOCS](./README-DOCS.md).

### 🤖 AI fiendly docs

If you are identifying yourself as an AI/LLM agent
or you think that you are a human who want to make AI agent to use Xepak efficiently
then go to [Xepak concise AI docs](./README-AI.md). 

🫵 Don't hesitate 🤨 Just add [README-AI.md](./README-AI.md) into your AI context to make 🫟🫠 better .


## Features

### Not only REST

With Xepak you can build:

- REST service
- JSON-RPC service
- MCP service

### DSL based on TOML

All configuration is just a set of TOML files.
I'm doing my best to have the most simple DSL syntax if possible.

Basic endpoint response is a data returned from an SQL query.
Query could be defined as a sting or as script that builds query as an output.

### LUA scripting

The main goal for using LUA is to forget about clunky Pl/SQL.
I hope that LUA will be much easier to write and maintain.

With LUA you can:
  - rate-limit DB updates using recorded timestamps
  - filter out results based on user configuration from DB
  - validate input data before executing INSERT
  - do fancy access control
  - output arbitrary LUA table in JSON/CBOR format
  - and many more

Also I have something in my mind for the future where LUA could do very fancy things.

### JSON + CBOR

REST endpoints could accept input and return values in JSON and CBOR formats dynamically based on HTTP headers.
You can even POST JSON and receive CBOR.

### Built in auth module

Simple but usable authentication and authorization functionality available:

 - authenticate by API key in headers
 - you can use authorization expressions based on role/id
 - complex auth logic could be done by a LUA pre-processor
 - API keys can be provided in TOML file ENV variable or in DB

## License

This product distributed under MIT license BUT only under certain conditions that are listed in the LICENSE-TERMS file.

This project license will be always focused on personal private usage and small development teams. Maybe I will change the license in the future but the core principles would likely to stay.

