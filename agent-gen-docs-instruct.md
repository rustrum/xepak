# AI friendly documentation generation

Project name is "Xepak".
It is a server that generates REST API, JSON-RPC/MCP server for database backend based only on TOML DSL configuration.
For complex logic insted of PlSQL it is using LUA scripting.


# Action steps

## Must use memory file to track progress and store findings

Do not rely on your memory store all meaningful information in memory file.

Docs genenration progress must be tracked in the memory file `agents_work/ai-docs-progress.md`.

Must create this file before code investigation.
CRITICAL! If file already exists must ask user what to do: read exiting file and contiue appending data to it or replacing it with an empty one.

During code investigation all findings must be appended to the file as a sections with appropriate headers.

Must update memory file after each discovery immediately.
All discrepances between current docs and actual code state must be memorized.
Preffered to make several small updated after each finding and combine them later if needed.
Can have dynamic section at the endo of the files with a plans to do next.
NOT good doing several steps without memory file updates in order to make big update later.
Must update the memory file as often as possible.

After processing all project files `agents_work/ai-docs-progress.md` must contains all data to update existing documentation.

Important! Must re-read `agents_work/ai-docs-progress.md` after each context compression step. Must add all memory file rules to context compression.

### Read existing docs and code

Must read `README-AI.md` which contains existing documentation.

I must read and analyze all project related files to get understanding how to update docs:
- human docs available in `README.md` and `README-DOCS.md`.
- directory `src` contains all project code
- directory `tests` contains test with valid DSL 
- directory `examples` has example projects configuration

Must ignore all content from other directories including: `behind`, `.xepak-stubs`, `target`.

## Validate existing docs

Will validate current docs to check if there any discrepancy with an actual project functionality.

Shold also check what files changed since commit provided in the `README-AI.md`
by executing `git diff --name-only GIT_COMMIT_HASH` 

## Update existing documentation

Will align `README-AI.md` with existing project features.
Will reformat docs style and structure according to rules provided below.

If `README-AI.md` address all project functionality then no need to update it
but I can ask user to improve document style if needed.

## Final check

I will validate that updated `README-AI.md` is not containing any new errors.


# Documentation structure rules

Documentation must be in AI-friendly format:

 - all info must be in one place must not require to read other project docs & examples
 - only concise straightforward notions
 - do not duplicate information
 - only AI-friendly explanation and formatting allowed
 - I must not copy examples as-is I must generalize things do avoid dummy duplication

Docs header must contains project version from `Cargo.toml` and latest git commit hash.

`README-AI.md` should have sections related to next topics at least:

- Brief Xepak features overview
- TOML DSL specification
- LUA scripting API
- DSL usage examples (use-cases)


# Important

I will not update `README-AI.md` in a single batch.
I will prefer small step by step updates.

