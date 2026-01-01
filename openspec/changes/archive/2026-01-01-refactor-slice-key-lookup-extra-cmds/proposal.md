# Change: Extend slice-key lookup to STRLEN/TTL/EXPIRE (fast path)

## Why
`refactor-slice-key-lookup` removed key materialization for `GET`, `TYPE`, and `EXISTS` by allowing the DB keyspace to
probe keys using a request-scoped slice/len view.

The same remaining per-command key copy is still present for common small commands:
- `STRLEN`
- `TTL`
- `EXPIRE`

These commands are on the same hot request → DB boundary and benefit similarly from “ptr+len” key probing.
Extending the slice-key path to them further aligns Yierdis with Redis’ internal `(ptr,len)` model and reduces
allocation on read-heavy workloads.

## What Changes
- Add slice-key overloads in `YierdisDb` for:
  - `strlen`
  - `ttlSeconds`
  - `expire`
- Update `YierdisFastCommandProcessor` so `STRLEN`, `TTL`, `EXPIRE` use slice-key DB APIs and do not materialize the
  lookup key `byte[]`.
- Add fast pipeline tests covering these commands (including binary-safe keys).

External command semantics MUST remain unchanged.

## Scope
### In scope
- Fast path only: request-backed slice-key probing for `STRLEN`, `TTL`, `EXPIRE`.
- Tests for correctness and binary safety.

### Out of scope (non-goals)
- Write-path key materialization removal for commands that create keys (e.g. `SET`).
- Any change to expiration cleanup policy or TTL rounding behavior.
- Netty buffer lifetime changes (ownership remains in `YierdisFastCommandHandler`).

## Impact
Expected affected areas:
- `yierdis-server/src/main/java/yier/bubu/redis/db/YierdisDb.java`
- `yierdis-server/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`
- `yierdis-server/src/test/java/yier/bubu/redis/FastPipelineTest.java`

## Relationship to other changes
- Follow-up to `refactor-slice-key-lookup` (reuses `YierdisBytesView` + `ByteArrayKeyspace` slice probing).
- Complements `refactor-resp-decode-zero-copy` and `refactor-resp-slice-io` by removing another remaining “copy the key”
  boundary on the hot path.

