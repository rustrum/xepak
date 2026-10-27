# RAG Example: Multi-Project Knowledge Base

This example shows how to build a simple MCP-based RAG knowledge base on top of SQLite using Xepak.

It exposes one logical MCP service as many isolated project endpoints `/mcp/rag/<PROJECT_NAME>`

Each `project_name` behaves like a separate knowledge base.
An agent connected to `http://localhost:8080/mcp/rag/alpha`
can only see and modify knowledge belonging to the `alpha` project.
Another agent connected to `http://localhost:8080/mcp/rag/beta`
gets a completely separate knowledge space.


## What this example does

The RAG example provides a project-scoped knowledge base with the following capabilities:

1. **Store knowledge notes**

   Agents can save durable facts, decisions, observations, or context.

2. **Track project actions**

    Agents can log milestones, status updates, and completed work.

3. **List stored knowledge**

    Agents can inspect recent notes and actions entries.

4. **Search stored knowledge**

   Agents can perform full-text search across note titles and contents.
   But here is the issue, `query` is not a regular string but FTS5 expression.
   So LLMs are struggle to provide queries in the proper format.

5. **Isolate data per project**

   All data lives in shared SQLite tables, but every row is scoped by a `project_name` discriminator.


## Using this example with an AI agent

This example is designed to be used through an MCP-compatible agent or client.

The general flow is:

1. Start the Xepak server with this example loaded.
2. Pick a project name (letters/numbers/undescore) for example `my_1st_demo`.
3. Connect your agent to the project-specific MCP URL.
4. Ask the agent to remember, retrieve, or search project knowledge.

Example endpoint: `http://localhost:8080/mcp/rag/my_1st_demo`

For multiple projects, add multiple MCP servers with different project names.


## How to launch example

If you have xepak binary/docker you must run it and provide with path to xepak-rag.toml

Keep in mind that docker must have access to whole directory where root TOML located.


## How to run/test from sources

You should have `cargo` installed with [just a command runner](https://github.com/casey/just)
so you can use `justfile` scripts.

Clone this repo `cd` to the current example directory and execute `just -l` to list available commands.

Execute `just run` to build/launch Xepak server with the current configuration example.

Open another terminal session to execute other `just` commands to query MCP tools.

