# Design: Listpack-like packed encodings (heap-based)

## Goal
Make Yierdis’ “packed” encodings closer to Redis by storing small composite values in a **single contiguous buffer**
(listpack/quicklist-node-like) rather than as many small Java objects (`byte[]` per element).

The design must remain:
- binary-safe (`byte[]` semantics, not `String`),
- minimal (learning/demo codebase),
- compatible with existing command behavior.

## Key Idea
Introduce a small reusable primitive in `db/`:

`YierdisListpack` (name TBD) storing entries as:
- a single `byte[] data` backing buffer
- an entry count
- a small amount of metadata needed to support the current command subset

### Encoding format (approximate)
Each entry is stored as:
- `varint length` (1–5 bytes)
- `length` raw bytes

This is not required to match Redis listpack’s encoding. The important property is **contiguity** and
**no per-entry heap objects**.

### Operations (minimal set)
The packed composite types in this repo need:
- append/prepend of raw bytes
- pop from head/tail
- index-based access (LRANGE/ZRANGE in packed form)
- linear search by bytes (HGET field scan, SISMEMBER scan, ZSET member scan in packed form)
- delete at index / delete matching entry

Given the packed thresholds are small (tens to hundreds), O(n) scan costs are acceptable and match Redis’ choice:
packed encodings are only used while N is small.

## Integration by value type

### HashValue
Store pairs as `[field][value][field][value]...` in listpack.
- `hget(field)`: scan pairs, compare bytes on field entries
- `hset(field,value)`: scan to update existing; append new pair if absent
- `hdel(fields...)`: scan and delete matching pairs
- Upgrade to `ByteArrayHashMap` when thresholds are exceeded (same as today)

### SetValue
Keep the existing intset path for canonical integers; change the listpack path to use `YierdisListpack`.
- contains/add/remove are linear scans while listpack is in use
- Upgrade to `ByteArrayHashSet` when thresholds are exceeded (same as today)

### ListValue
Align with Redis quicklist shape:
- Packed list uses a single listpack (small N)
- Quicklist nodes store listpack segments (bounded by entry count and raw bytes)
- Push/pop operate on the first/last node and convert/merge nodes based on thresholds

### ZSetValue
Packed zset stores member/score pairs in listpack while small.
To preserve ordering:
- insertion in packed form uses a linear/binary search over entries (depending on how we expose “scoreAt”),
  keeping pairs sorted by `(score, memberLex)`
- upgrade to dict + skiplist when thresholds are exceeded (same as today)

## Compatibility & Constraints
- External RESP responses must not change.
- The packed encodings remain on-heap; off-heap integration is intentionally deferred to a separate change.
- Avoid introducing new public APIs unless needed; keep listpack package-private inside `db/`.

