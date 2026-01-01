# Change: Refactor DB key lookup to accept slice/len keys (zero-copy read path)

## Why
After `refactor-resp-decode-zero-copy`, request decode is already “ptr + len” style: each command argument is a slice view
over a single retained Netty `ByteBuf` frame.

However, the current fast command processor still materializes `byte[]` for keys even for read-heavy commands like
`GET`, `TYPE`, and `EXISTS`. That means:
- request decode avoids per-arg copies, but read path still pays a key copy per command (often the dominant remaining
  allocation for small commands),
- it diverges from Redis’ internal model, where key lookups operate on `(ptr,len)` and only copy when the data must be
  stored beyond the request.

Adding a slice-based lookup path for the DB keyspace is the most direct way to further align Yierdis with Redis’ model
and reduce allocations on common read workloads.

## What Changes
- Introduce a request-scoped key view abstraction that can represent a key as a slice/len over the decoded request frame.
- Extend the internal keyspace (`ByteArrayKeyspace`) to support:
  - hashing a slice using the same algorithm as `byte[]` hashing, and
  - comparing a stored canonical `byte[]` key to a slice without materializing a new array.
- Wire slice-based lookup into `YierdisDb` for read-path operations so that commands can:
  - probe the keyspace using a slice key,
  - resolve the canonical stored `byte[]` key reference when present, and
  - reuse that canonical key for expiration checks where needed.
- Update the fast command processor to use the new APIs for common commands (at minimum: `GET`, `TYPE`, `EXISTS`).

External command semantics MUST remain unchanged.

## Scope
### In scope
- Slice-based key lookup for the fast path for:
  - `GET`
  - `TYPE`
  - `EXISTS` (including multi-key form)
  - (Optional if low-cost once the primitive exists) `STRLEN`, `TTL`, `EXPIRE`
- Unit tests validating behavior remains correct through the fast pipeline under the new lookup path.

### Out of scope (non-goals)
- Storing keys off-heap or representing stored keys as slices (stored keys remain canonical `byte[]`).
- Full write-path refactor to avoid key materialization for commands that create new keys (e.g. `SET`, `HSET`, `LPUSH`).
- Cross-command slice lifetime beyond the request (no retaining request buffers in DB).

## Impact
Expected affected areas:
- `yierdis-server/src/main/java/yier/bubu/redis/db/ByteArrayKeyspace.java`
- `yierdis-server/src/main/java/yier/bubu/redis/db/YierdisDb.java`
- `yierdis-server/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`
- New/updated tests under `yierdis-server/src/test/java/yier/bubu/redis/**`

## Relationship to other changes
- Builds directly on `refactor-resp-decode-zero-copy` (request argv is already slice-based).
- Complements `refactor-resp-slice-io` (reply path slice/len) by also removing key copies on the request→DB boundary
  for read commands.

## Risks & Mitigations
- **Hash/equality mismatch between slice keys and stored `byte[]` keys**:
  - Reuse the exact same hash function semantics as the existing `byte[]` key path.
  - Add tests that exercise lookups with binary keys (including non-UTF8 bytes).
- **Lifecycle bugs (use-after-release)**:
  - The DB MUST NOT retain request-backed slices; slice keys are request-scoped and only valid during command execution.
  - Keep ownership boundary unchanged: `YierdisFastCommandHandler` recycles commands in `finally`.

