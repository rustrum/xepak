# AI friendly documentation generation

Current project name is "Xepak".
This is an alternative to PostgRest.
It is a server that provides API for database backend based only on TOML DSL configuration.
For complex logic insted of PlSQL it using LUA scripting.


# Action steps

## Read project files

I must read project files only if they were not provided in initial prompt!

### Read project files in batch

Must try run next cli command to pack all project in a single file:
`code2prompt . --include="src/**/*,tests/**/*,examples/**/*.toml,examples/**/*.sql,Dockerfile" --include "./README.md,./README-DOCS.md" --exclude="agent-gen-docs-instruct.md" -O ./prompt-codebase.txt && echo "Total lines count: $(wc -l ./prompt-codebase.txt)"`

IMPORTANT! Will read `./prompt-codebase.txt` in batches because of read file tool internal limits.
Must validate that numer or readed lines matches value reported in CLI while executing `code2prompt`.

### Read current docs with some files

Will read `README-AI.md`.

If `prompt-codebase.txt` was generated I will skip read other project files,
cause they must be consumed in a batch read.

If `code2promt` failed then I must read all project files:
- human docs from `README.md` and `README-DOCS.md`.
- directory `src` contains all project code
- directory `tests` contains test with valid DSL 
- directory `examples` has example projects configuration, BUT it could be outdated

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
 - I MUST exclude all mentions of Rhai scripting from documentation because it is not fully supported yet

Docs header must contains project version from `Cargo.toml` and latest git commit hash.

`README-AI.md` should have sections related to next topics at least:

- Brief Xepak features overview
- TOML DSL specification
- LUA scripting API
- DSL usage examples (use-cases)


# Important

I will not update `README-AI.md` in a single batch.
I will prefer small step by step updates.

