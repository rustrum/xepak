# Bugs found at 10.12.2026


## Not important to the point that it is not a bug at all :)

- **Lua HTTP client: no connection pooling** (partially fixed — timeout now present) — `src/script_lua.rs:763,780`
  - `reqwest::Client::new()` per HTTP call: no pooling. Timeout was added (`script_lua.rs:767,786`): `.timeout(Duration::from_secs(HTTP_REQUEST_TIMEOUT))` = 30s, since the last report.
  - **Fix**: share a single `reqwest::Client` (e.g. via app data).

- **Method Not Allowed returns 400 instead of 405** — `src/server/handlers/rest.rs:57-70`
  - `validate_method_allowed` returns `XepakError::Input` → 400 (`server/mod.rs:203`), never 405. HTTP semantics call for 405.
  - **Fix**: maybe add a dedicated error variant mapped to 405. Or add additional error code to error type

- **Auth cache key contains raw API key** — `src/auth/storage.rs:43-45`
  - `format!("storage-token-auth-key-{}", api_key)` — plaintext credential in cache keys (exposure risk if keys are logged/dumped).
  - **Fix**: hash the key (e.g. SHA-256) before building the cache key.
  - **Concerns**: We already know unhashed keys so there is no point to hash it for a key

- **Malformed query string drops ALL query args** — `src/server/processor.rs:202-208`
  - Decode failure → `tracing::warn!` + empty args; silent data loss instead of a 400 error.
  - **Fix**: return an `Input` error on decode failure.
  - **Concerns**: Malformed request must not expect server to provide valid processing, silend drop with warning is better than nothing 

- **Content-Type check uses exact `eq()`** — `src/server/processor.rs:282-283`
  - `application/cbor; charset=...` is misparsed as JSON.
  - **Fix**: compare the media-type prefix (e.g. `starts_with` on the token before `;`).

- **Duplicate AND operators accepted** — `src/auth/rules.rs:307`
  - `(And | AndExpecting, And) => Ok(AndExpecting)` — a second AND operator in the same clause is accepted instead of being rejected.
  - **Concerns**: Need to make parser more complex to track it. Why should I? `manager AND AND AND admin` still understandable expression.
