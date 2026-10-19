# AI friendly documentation generation

Project name is "Xepak".
It is a server that generates REST API, JSON-RPC/MCP server for database backend based only on TOML DSL configuration. When additional logic needed it used Lua scripts.


# Action steps

## Must use memory file to track progress and store findings

Do not rely on your memory store all meaningful information in memory file.

Docs generation progress must be tracked in the memory file `agents_work/ai-docs-progress.md`.

Must create this file before code investigation.
CRITICAL! If file already exists must ask user what to do: read existing file and continue appending data to it or replacing it with an empty one.

During code investigation all findings must be appended to the file as a sections with appropriate headers.

Must update memory file after each discovery immediately.
All discrepancies between current docs and actual code state must be memorized.
Preffered to make several small updated after each finding and combine them later if needed.
Can have dynamic section at the end of the files with a plans to do next.
NOT good doing several steps without memory file updates in order to make big update later.
Must update the memory file as often as possible.

After processing all project files `agents_work/ai-docs-progress.md` must contains all data to update existing documentation.

Important! Must re-read `agents_work/ai-docs-progress.md` after each context compression step. Must add all memory file rules to context compression.

### Read existing docs

Must read `README-AI.md` which contains existing documentation.

Human docs available in `README.md` and `README-DOCS.md`.

Important! All documentation files could have errors and must not be treated as source of truth.

### Read existing code

Must read only files from the next directories:
- directory `src` contains all project code
- directory `tests` contains test with valid DSL 
- directory `examples` has example projects configuration

## Validate existing docs

Will validate current docs to check if there any discrepancy with an actual project functionality.

Shold also check what files changed since commit provided in the `README-AI.md`
by executing `git diff --name-only GIT_COMMIT_HASH` 

## Update existing documentation

Will align `README-AI.md` with existing project features.
Will reformat docs style and structure according to rules provided below.
When adding features, keep the user-perspective scope and compression — do not let architecture, internal type model, or redundant examples creep in.

If `README-AI.md` address all project functionality then no need to update it
but I can ask user to improve document style if needed.

## Final check for AI-docs

I will validate that updated `README-AI.md`:
- contains no new errors
- stays user-perspective (no architecture, no internal type/value model)
- is compressed (no duplicated info, token-efficient)

## Fix errors in `README-DOCS.md` (human-oriented docs)

Must fix significant errors in `README-DOCS.md`.
Do not perform big changes or rewrites for human-oriented docs.

Write report what errors were discovered in `README-DOCS.md` and what was fixed.


# Documentation structure rules

Docs header must contain project version from `Cargo.toml` and latest git commit hash.

`README-AI.md` is for AI agents and must be written from the **user/operator perspective**:

 - must omit architecture, internal design and implementation details
 - all info must be in one place must not require to read other project docs & examples
 - optimize for low token count: prefer tables and terse bullets over prose.
 - do not duplicate information (define once, reference elsewhere).
 - do not copy project examples as-is; generalize into the smallest set of complete, composable examples.
 - keep exactly what a model needs to write valid configuration and interpret responses without reading other docs.

`README-AI.md` should have struture similar to `README-DOCS.md` with compact DSL usage examples (use-cases).


# Important

I will not update `README-AI.md` in a single batch.
I will prefer small step by step updates.

