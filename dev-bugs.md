# Bug Findings Report: xepak Rust Project

> Last updated: 2026-10-30. This file merges the previous findings report with a
> fresh full-repo scan (21 new findings, ids `B01`–`B21`) and includes a
> cross-validation of all previously listed bugs against the current code.
> Detailed evidence, line numbers and next steps: `agents_work/bug-report-progress.md`.

## Cross-Validation of Previously Listed Bugs

Status of every bug from the earlier report, re-checked against the current code:

| Old finding | Status | Evidence in current code |
|---|---|---|
| Lua HTTP client (no pooling, no timeout) | **PARTIALLY FIXED** | Timeout added: `.timeout(Duration::from_secs(HTTP_REQUEST_TIMEOUT))` = 30s (`script_lua.rs:767,786`). Still `reqwest::Client::new()` per call (`script_lua.rs:763,780`) — no connection pooling. |
| Method Not Allowed returns 400 instead of 405 | **CONFIRMED** | `rest.rs:57-70` returns `XepakError::Input` → 400 (`server/mod.rs:203`), never 405. |
| RPC `NotFound` mapped to -32601 | **CONFIRMED** | `rpc.rs:253`. |
| Unused `RPC_ERROR_INVALID_PARAMS` | **CONFIRMED** | Defined at `rpc.rs:29`, no usages in codebase. |
| Malformed query string drops all args | **CONFIRMED** | `processor.rs:202-208` — warn + `Default::default()`. |
| Content-Type exact `eq()` check | **CONFIRMED** | `processor.rs:282-283` — `accept.eq(CONTENT_TYPE_CBOR)`. |
| Duplicate AND operators accepted | **CONFIRMED** | `auth/rules.rs:307` — `(And \| AndExpecting, And) => Ok(AndExpecting)`. |

**Cross-validation result:** 2 fixed, 1 stale, 2 merged into new ids (B10, B20), 14 still open.

## New Findings (2026-10-30 scan, B01–B21)

### Medium (9)
- **B03 — `strict_schema` trailing-element check is dead** — `src/xepak_data/schema.rs:287-292`
  - Strict extra-element check compares `items.len() > idx`, but after the loop `idx == items.len()` always → trailing tuple elements beyond the schema are NOT rejected. Contradicts documented strict semantics.
  - **Fix**: compare against the actual value tuple length.
- **B18 — NaN bypasses range validators** — `src/xepak_data/schema.rs:379-433`
  - All `<`/`>` comparisons against NaN are false, so Range/RangeFloat validators accept NaN.
  - **Fix**: explicitly reject non-finite floats in range validation.

### Low (8)
- **B07 — Configured keep-alive seconds ignored** — `src/server/mod.rs:160-165`
  - `server_keep_alive` is only tested for zero; any non-zero value maps to the fixed `KEEP_ALIVE_TIMEOUT` (15s). E.g. configured 5 → 15.
  - **Fix**: use `Duration::from_secs(config.server_keep_alive)`.


## Previously Listed Bugs Still Open (not covered by B-ids)


### Low (8)
- **Lua HTTP client: no connection pooling** (partially fixed — timeout now present) — `src/script_lua.rs:763,780`
  - `reqwest::Client::new()` per HTTP call: no pooling; 30s timeout was added since the last report.
  - **Fix**: share a single `reqwest::Client` (e.g. via app data).
- **Method Not Allowed returns 400 instead of 405** — `src/server/handlers/rest.rs:57-70`
  - `validate_method_allowed` returns `XepakError::Input` → 400 (`server/mod.rs:203`); HTTP semantics call for 405.
  - **Fix**: add a dedicated error variant mapped to 405.
- **RPC `NotFound` mapped to -32601** — `src/server/handlers/rpc.rs:253`
  - `XepakError::NotFound` → `RPC_ERROR_METHOD_NOT_FOUND`, conflating missing resources with missing methods.
  - **Fix**: map to a resource-specific error code or -32603.
- **Unused `RPC_ERROR_INVALID_PARAMS`** — `src/server/handlers/rpc.rs:29`
  - Defined but never used; invalid params fall into other codes.
- **Auth cache key contains raw API key** — `src/auth/storage.rs:43-45`
  - `format!("storage-token-auth-key-{}", api_key)` — plaintext credential in cache keys (exposure risk if keys are logged/dumped).
  - **Fix**: hash the key (e.g. SHA-256) before building the cache key.
- **Malformed query string drops ALL query args** — `src/server/processor.rs:202-208`
  - Decode failure → `tracing::warn!` + empty args; silent data loss instead of a 400 error.
  - **Fix**: return an `Input` error on decode failure.
- **Content-Type check uses exact `eq()`** — `src/server/processor.rs:282-283`
  - `application/cbor; charset=...` is misparsed as JSON.
  - **Fix**: compare the media-type prefix (e.g. `starts_with` on the token before `;`).
