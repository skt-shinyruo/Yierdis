# Design: Slice/len reply path (RESP2)

## Goals
- Align replies with Redis’ “ptr + len” approach: write bytes directly from storage buffers.
- Reduce or eliminate `byte[]` trimming copies on hot read replies.
- Keep changes minimal and localized to the fast path.

## Current pain points (examples)
- Strings: `YierdisObject` can have `payload` capacity > `rawLen` after growth; GET currently has to materialize a trimmed
  `byte[]` if it relies on a “byte[] view” API.
- Packed encodings: listpack-like buffers store data contiguously, but reply APIs often return `byte[]` per element,
  forcing allocations that Redis avoids.

## Proposed API surface

### Writer
Add bulk string overloads:
- `bulkString(byte[] buf, int off, int len)`
- `bulkString(YierdisOffHeapSlice slice)`
- `bulkStringLongAscii(long value)` (writes `:$len\r\n<digits>\r\n` as bulk string without allocating)

All existing `RespWriter` methods remain and delegate where possible.

### Value-to-reply bridge
Avoid long-lived slice objects:
- Use ephemeral views while traversing a listpack buffer and write each element immediately.
- For strings, write directly using `(payload,0,rawLen)` for RAW/EMBSTR and `bulkStringLongAscii(intValue)` for INT.

This preserves safety without introducing ref-counting or ownership hazards.

## Array replies (count known upfront)
RESP arrays require the element count before writing. For Yierdis’ supported commands:
- `LRANGE`: element count can be computed from list length + clamped indices.
- `HGETALL`: count is `hlen * 2` (field/value pairs).
- `SMEMBERS`: count is set cardinality.
- `ZRANGE`: count is deterministic for range + optional WITHSCORES.

DB/value code should compute the count (O(1)) and then stream elements (O(n)) into the writer without allocations.

## Off-heap compatibility
Even if the DB is still heap-based, supporting `YierdisOffHeapSlice` in `RespWriter` is an important step toward:
- future off-heap value storage,
- avoiding forced heap copies when off-heap is enabled.

## Non-goals / deferrals
- Request-side zero-copy decoding is deferred because it introduces Netty reference counting and lifetime management.
- Tree-based `CommandProcessor`/`RespObject` path remains unchanged in this change for scope control.

