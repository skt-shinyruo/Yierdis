# Design: Full off-heap storage (Unsafe-based)

## Overview
This change replaces heap-resident storage (`byte[]`, Java collections/arrays) with off-heap memory managed explicitly via
`sun.misc.Unsafe`. The goal is to approximate Redis' explicit-memory model while staying compatible with the project’s
current RESP2 command subset and single-threaded DB ownership semantics.

## Principles
- **Explicit ownership**: every off-heap allocation has a single owner that is responsible for freeing it.
- **No implicit copies**: API surfaces should prefer `(ptr, len)` / slice-style reads over allocating `byte[]` for replies.
- **Hard limits**: all user-controlled allocations must respect a configured max off-heap bytes limit.
- **Safe-by-default APIs**: prevent use-after-free and out-of-bounds access with guard rails in debug/test mode.

## Memory Model
### Handles vs raw addresses
Use `long` handles representing absolute addresses for hot paths. For improved safety, optionally wrap in small structs:
- `OffHeapSlice { long addr; int len; }` (read-only view)
- `OffHeapBuf { long addr; int len; int cap; }` (mutable SDS-like buffer)

Only minimal Java objects remain on-heap as “roots” holding base addresses and metadata.

### Alignment & Endianness
- Store integers/longs in little-endian to match typical native layouts (Redis is CPU-endian; we standardize for portability).
- Align allocations to 8 bytes.

## Allocator
### Requirements
- Allocate variable-sized blocks for keys/values/struct nodes.
- Reuse freed memory quickly (free lists).
- Enforce a global `maxOffHeapBytes`.
- Provide `close()` to free all blocks deterministically.

### Proposed design
- **Size classes** for small blocks (e.g., 16..64KB, powers of two) with per-class free lists.
- **Large blocks** allocated directly via `Unsafe.allocateMemory`.
- **Accounting**: track total allocated bytes + peak; refuse allocations that exceed max.
- **Debug mode (tests)**: poison freed memory and keep a small quarantine to catch UAF patterns.

## Off-heap Hash Table
### Slot layout (off-heap)
Store the hash table arrays off-heap:
- `states[]` (byte)
- `hashes[]` (int)
- `keyPtr[]` (long) -> pointer to key bytes block
- `keyLen[]` (int)
- `valueHandle[]` (long) -> pointer/handle to the value object

Open addressing + tombstones mirrors the current `ByteArrayKeyspace` approach, but implemented in off-heap arrays.
Incremental rehashing uses two tables (table0/table1) and migrates a bounded number of slots per operation.

## Off-heap Values
### String
Implement SDS-like buffer:
- `len` and `cap` stored in a small off-heap header, followed by bytes.
- Growth uses the existing “<1MB double, >=1MB linear” policy.
- INT encoding can remain as a tagged immediate (`long`) without allocation.

### Composite types
Hashes/Sets/List/ZSet need packed encodings that avoid per-element Java objects:
- Packed variants stored as contiguous off-heap arrays of length-prefixed byte strings (listpack-like).
- Upgraded variants use off-heap hash tables or skiplist nodes.

## Reply Path (RESP2)
Extend the writer to accept `OffHeapSlice` and write directly into Netty `ByteBuf` without creating heap `byte[]`.
This is critical to avoid “read-time copies” and to keep latency stable.

## Lifecycle
- DB owns the allocator; `YierdisDb.shutdown()` must free all off-heap allocations.
- Overwrite/DEL/expire must free replaced blocks eagerly to avoid leaks.

## Compatibility Notes
- External behavior stays within the currently supported command subset and RESP2 protocol framing.
- Internal representations/encodings become off-heap and are not expected to match Redis byte-for-byte.

