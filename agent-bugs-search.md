# Bug search instructions

Use `agents_work/bug-search-progress.md` as the only persistent memory:

 - if it exists ask APPEND/RESET/ABORT
 - create it before analysis
 - update it immediately after every step/finding
 - re-read it after every context compression and resume from STATE

Search shallowly for Rust bugs likely missed by clippy, such as panics, logic errors, error-handling misuse, unsafe/FFI issues, concurrency/state bugs, and cross-module contract mismatches.

Avoid deep call-chain wandering unless direct evidence exists.
Record each finding with id, file, line, category, severity, confidence, evidence, issue, and next step.
Keep LOG append-only and STATE/PLAN/SUMMARY current.
Finish by updating SUMMARY and reporting concise results.
