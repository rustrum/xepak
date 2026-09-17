<p align="center" width="100%" style="text-align:center">
<img src="./xepak-rest.png" alt="REST service for your DB" />
</p>

<p align="center" width="100%" style="text-align:center">
<a href="https://crates.io/crates/xepak"><img src="https://img.shields.io/crates/v/xepak?style=for-the-badge" alt="Crates.io"></a>
<a href="https://docs.rs/xepak"><img src="https://img.shields.io/docsrs/xepak?style=for-the-badge" alt="Released API docs"></a>
<a href=""><img src="https://img.shields.io/twitter/follow/rumatoest?style=for-the-badge&color=blue" alt="Follow me on X(twitter)" /></a>
<a href="https://www.reddit.com/user/rumatoest"><img src="https://img.shields.io/reddit/user-karma/combined/rumatoest?style=for-the-badge&color=f3562e" alt="Follow me on reddit"/></a>
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

### JSON + CBOR

Support input and output in JSON and CBOR formats.

### SQL oriended

Each endpoint response is basically a data returned from an SQL query
that could be defined as a string or could be generated dynamically via script.

### Auth

Right now aut module is very simple but usable:
 - in DSL you can define identifier with roles and API key
 - for each non public endpoint you should add authentication processor
 - if you need fine grained access - add authorization processor with auth expression string

### Scripting support

Scripting support is needed to build complex queries and perfom data integrity and access control.

The main goal here is to have maintainable, universal logic that can be applied to any DB instead of relying on clunky SQL scripting. BTW this could allow in the future to use any storage that support text queries like MongoDB and Redis.

With scripting you can:
  - rate-limit DB updates using recorded timestamps
  - filter out results based on user configuration from DB
  - validate input data before executing INSERT
  - etc.

Right now Xepak supports two scripting languages.

- **LUA** is the main scripting language.
- **Rhai** is also a scripting lanuage.

**Performance considerations**

Now it is impossible to determine will Rhai be slower in real world scenarios or not.
There are many factors that would affect final performance expect script execution speed.
But if LUA will outperform Rhai in tests then Rhai will be dismissed.


## License

This product distributed under MIT license BUT only under certain conditions that listed in the LICENSE-TERMS file.

I know it's kina silly but I'm not in the mood right now to write my own license. Will do it later.

