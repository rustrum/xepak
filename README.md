<p align="center" width="100%" style="text-align:center">
<img src="./xepak-rest.png" alt="REST service for your DB" />
</p>

<p align="center" width="100%" style="text-align:center">
<a href="https://crates.io/crates/xepak"><img src="https://img.shields.io/crates/v/xepak?style=for-the-badge&logo=rust" alt="Crates.io"></a>
<a href="https://docs.rs/xepak"><img src="https://img.shields.io/docsrs/xepak?style=for-the-badge&logo=docs.rs" alt="Released API docs"></a>
<a href="https://x.com/rumatoest"><img src="https://img.shields.io/twitter/follow/rumatoest?style=for-the-badge&color=blue&logo=x&label=rumatoest" alt="Follow me on X(twitter)" /></a>
<a href="https://www.threads.com/@rumatoest"><img src="https://img.shields.io/badge/rumatoest-%20-black?style=for-the-badge&logo=threads&color=black" alt="Threads" /></a>
<a href="https://www.reddit.com/user/rumatoest"><img src="https://img.shields.io/reddit/user-karma/combined/rumatoest?style=for-the-badge&color=f3562e&logo=reddit&label=u/rumatoest" alt="Follow me on reddit"/></a>
<!-- https://img.shields.io/hackernews/user-karma/rumatoest?style=for-the-badge&color=ff6600&label=Me+on+Hacker+News -->
</p>

## TL;DR

Imagine PostgREST but instead of Haskell with PL/SQL it is based on Rust with LUA and focused on Sqlite (and other DBs).


## What is this?

I'm building DSL based REST (maybe not only) service for your SQLite database.
Will add other DBs support, only after polishing SQLite functionality.

**Why focus on SQLite?** Because it is amazing and extremely fast DB.
Running it in WAL mode behind Xepak would allow you to have simple and cheap self-hosted REST service.


## Current project status

I'm aiming for a first MVP release in the next month.
But there is a lot work to do and architecture desisions to consider.

**I encourage you to bookmark and visit this project later**.


## Project Documentation

Will be available in the [separate file](./README-DOCS.md)

### 🤖 AI fiendly docs

If you are identifying yourself as an AI/LLM agent
or you think that you are a human who need to make it's AI use Xepak efficiently
then go to [Xepak concise AI docs](./README-AI.md). 

🫵 Don't hesitate 🤨 Just add [README-AI.md](./README-AI.md) into your AI context to make 🫟🫠 better .


## Features

### Not only REST

With Xepak you can build:

- REST service
- JSON-RPC service
- MCP service (will be available soon)

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
  - output arbitraty LUA table in JSON/CBOR format
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
 - API keys can be provided in TOML file or load from ENV variable
 - API keys could be loaded from DB itself

## License

This product distributed under MIT license BUT only under certain conditions that listed in the LICENSE-TERMS file.

I know it's kina silly but I'm not in the mood right now to write my own license. Will do it later.

