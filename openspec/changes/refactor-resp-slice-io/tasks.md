## 1. Proposal Acceptance
- [x] Confirm scope: reply-path copy reduction only (no request decoder zero-copy).
- [x] Confirm target commands for fast-path optimization (at minimum `GET`, `LRANGE`, `HGETALL`, `SMEMBERS`, `ZRANGE`/`ZREVRANGE`/`ZRANGEBYSCORE`).

## 2. Slice API (Protocol)
- [x] Extend `RespWriter` to support bulk strings from `(byte[] buf, int off, int len)` without copying.
- [x] Add a bulk-string write path for `YierdisOffHeapSlice` (write length then `slice.writeTo(out)`).
- [x] Add a bulk-string write path for integer-encoded values without allocating a `byte[]` (length computed + writeLongAscii).
- [x] Add protocol unit tests verifying output bytes are correct for:
  - null bulk, empty bulk, non-empty bulk slices, and offheap slices.

## 3. Slice API (DB / Values)
- [x] Add minimal “read-only byte view” support for string values so GET can write `(payload, 0, rawLen)` without trimming copies.
- [x] Add minimal “entry write” support for listpack-like buffers so LRANGE/HGETALL/SMEMBERS/ZRANGE can write elements without `byte[]` materialization.
- [x] Keep existing allocation-based APIs for the non-fast path unchanged (unless a small refactor is clearly safer).

## 4. Fast Command Processor Wiring
- [x] Update `YierdisFastCommandProcessor` to use the new slice/len APIs for the target commands.
- [x] Preserve existing error handling and RESP formatting (array headers, null bulk semantics, etc).

## 5. Tests (DB + Integration)
- [x] Add/extend tests that cover:
  - `GET` after `APPEND` growth still returns correct bytes (no trimmed-copy requirement; correctness is mandatory).
  - Range commands over packed encodings return correct bytes and ordering (binary-safe).
  - Off-heap slice bulk write path correctness (using the existing offheap allocator tests as reference).

## 6. Verification
- [x] Run `openspec validate refactor-resp-slice-io --strict`.
- [x] Run `mvn test -pl :yierdis -am`.
