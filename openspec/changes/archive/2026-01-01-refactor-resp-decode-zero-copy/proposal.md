# Change: Refactor RESP2 request decode to zero-copy slices (Netty refCnt lifecycle)

## Why
Redis’ request path is fundamentally “ptr + len”: the server parses RESP and then works with argument pointers into the
input buffer, copying only when the data must outlive the request.

Yierdis’ current fast-path request decoder (`RespCommandDecoder`) allocates a new `byte[]` for every bulk string argument
and copies bytes out of Netty’s `ByteBuf`. This has a few downsides:
- High allocation rate on pipelined workloads (many small commands).
- Expensive copies for large payload commands (big bulk strings).
- It blocks future “true off-heap” work, because the request path always materializes on-heap `byte[]`.

If the goal is to align implementation style with Redis, the request decoder needs a first-class slice/len model with
explicit lifetime management under Netty’s reference counting.

## What Changes
- Refactor the fast-path request decoder to represent bulk string arguments as **zero-copy slices** backed by Netty’s
  input `ByteBuf` (RESP2 array-of-bulk-strings form).
- Introduce explicit lifecycle management:
  - retained slices MUST be released exactly once when the decoded command is finished (or abandoned due to partial frame).
  - the fast pipeline handler becomes the clear ownership boundary for releasing/recycling decoded commands.
- Provide a minimal “escape hatch” API to materialize `byte[]` when required (e.g., when storing keys/values in the DB),
  while keeping parsing/routing on slices where possible.

External semantics MUST remain unchanged (binary-safe bytes, `$-1` null bulk, etc.).

## Scope
### In scope
- Request decode path only (Netty `ByteBuf` → decoded argv representation), for the fast pipeline:
  - `RespCommandDecoder`
  - `RespCommand`
  - `YierdisFastCommandHandler` ownership/release
  - fast processor parsing changes needed to consume slice-based argv
- Unit tests covering correctness and lifecycle safety (no leaks on partial frames / errors).

### Out of scope (non-goals)
- Storage layer accepting slices directly (DB currently uses `byte[]` keys/values; slice-based key lookup is separate).
- RESP3, inline protocol, or new external behavior.
- Changing hard DoS bounds (`maxArgs`, `maxBulkBytes`, `maxLineBytes`) beyond keeping them consistent with current logic.

## Impact
Expected affected areas:
- `yierdis-server/src/main/java/yier/bubu/redis/protocol/RespCommandDecoder.java`
- `yierdis-server/src/main/java/yier/bubu/redis/protocol/RespCommand.java`
- `yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java`
- `yierdis-server/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`
- New protocol tests under `yierdis-server/src/test/java/yier/bubu/redis/protocol/**`

## Relationship to other changes
- Complements `refactor-resp-slice-io` (reply path slice/len). Together they move Yierdis closer to Redis’ ptr+len model.
- Enables future work in `refactor-full-offheap-storage` (off-heap values benefit from not forcing on-heap decode).

## Risks & Mitigations
- **Netty refCnt lifetime bugs (leaks / use-after-release)**:
  - Keep ownership simple: handler releases in `finally`.
  - Decoder retains exactly once per decoded command (preferred design in `design.md`).
  - Add tests that exercise partial frames and error paths.
- **Complexity creep**:
  - Keep “slice argv” API minimal and only wire what the fast path needs.
  - Defer slice-based DB lookup / hash-on-slice to a separate change.

