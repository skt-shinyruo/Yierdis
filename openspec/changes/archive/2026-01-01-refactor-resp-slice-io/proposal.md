# Change: Refactor RESP2 I/O to support slice/len (reduce heap copies)

## Why
Redis’ internal I/O model is fundamentally “ptr + len”: values are stored as contiguous buffers (SDS/listpack/quicklist)
and replies write bytes directly from storage to the network buffer without materializing trimmed copies.

Yierdis currently allocates and copies on several hot reply paths:
- `YierdisObject.stringBytesView()` copies when the backing array capacity is larger than the logical length
  (e.g. after APPEND growth).
- Packed encodings (listpack/quicklist-node-like) often expose elements as `byte[]` by copying slices out of a larger
  buffer (e.g. listpack cursor to `byte[]`).
- The fast command path (`YierdisFastCommandProcessor`) relies on `RespWriter.bulkString(byte[])` and `bulkStringArray(List<byte[]>)`,
  which forces upstream callers to produce fully materialized `byte[]` values even when they already have `(buf, off, len)`.

If the goal is implementation alignment with Redis, the server needs a first-class slice/len reply path so values can be
written directly from their storage buffers.

## What Changes
- Extend the RESP2 writer to support writing bulk strings from:
  - `(byte[] buf, int off, int len)` (heap slice),
  - off-heap slices (`YierdisOffHeapSlice`) when available,
  - integer-encoded values as bulk strings without allocating an intermediate `byte[]`.
- Add internal “byte view”/slice utilities so DB/value encodings can expose read-only slices for immediate reply output.
- Update the fast command processor to use slice/len output for hot read commands so the reply path avoids per-command
  heap copies where possible.

External RESP2 semantics MUST remain unchanged.

## Scope
### In scope
- Reply-path copy reduction (“storage → RESP2 reply”) for the fast path.
- Slice-based access for:
  - string values (RAW/EMBSTR/INT)
  - packed encodings that already store contiguous buffers (e.g. listpack-like buffers)
- Unit/integration tests validating correctness (binary-safe behavior, boundaries, null vs empty behavior where supported).

### Out of scope (non-goals)
- Request decode zero-copy (Netty `ByteBuf` slicing / retain semantics). The request decoder may continue to copy args.
- Full off-heap storage migration (separate proposal already exists).
- New Redis commands or external behavior changes.

## Impact
- Affected code (expected):
  - `yierdis-server/src/main/java/yier/bubu/redis/protocol/RespWriter.java`
  - `yierdis-server/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java`
  - `yierdis-server/src/main/java/yier/bubu/redis/db/YierdisDb.java`
  - value containers and packed encodings under `yierdis-server/src/main/java/yier/bubu/redis/db/**`
- Tests:
  - add protocol-level tests for the new slice APIs
  - add DB/command tests for correctness on grown strings and packed collections

## Relationship to existing changes
This change is a prerequisite / enabler for:
- `refactor-full-offheap-storage` (off-heap values need a slice-based reply path to avoid round-tripping through heap arrays).

## Risks & Mitigations
- **Lifetime safety of slices**: restrict slice usage to immediate write-out in the command execution thread; avoid storing
  slice objects beyond a single command.
- **Complexity creep**: keep the API surface small (bulk string slice + minimal streaming for arrays); leave request-side
  zero-copy for a separate change.
- **Correctness regressions**: add targeted tests for binary-safe edge cases and for strings with extra capacity.

