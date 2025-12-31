# Design: Maxmemory, approximate accounting, and eviction

This design intentionally prioritizes **teaching value + predictable performance** over “perfect Redis parity”.

## Goals
- Provide a Redis-like **maxmemory** story that is:
  - small to implement,
  - easy to reason about,
  - and demonstrable via `redis-cli`.
- Keep single-threaded DB execution semantics (no new concurrency).
- Avoid global per-key linked lists (full LRU) and instead use Redis-like **sampling**.

## Non-goals
- Exact byte-for-byte memory usage parity with Redis (jemalloc, fragmentation, RSS, etc.).
- Fully accurate JVM object sizing (object headers/alignment).

## Memory Accounting Model (approximate)

### Principles
- Prefer accounting **capacity** (reserved bytes) for packed buffers (`byte[].length`) since it matches “allocator view”.
- Avoid per-operation full scans; maintain totals incrementally on mutation.
- Accept approximate overhead; keep it stable and documented.

### Proposed accounting units
- **Keys**: optionally count `key.length` (and a small constant overhead if desired).
- **STRING**
  - `STRING_INT`: digits length (ASCII) as payload size.
  - `STRING_EMBSTR/RAW`: `byte[].length` (capacity) and/or `rawLen` (used). Prefer capacity for stability.
- **HASH/LIST/ZSET packed forms**: count internal packed buffer capacity (e.g., listpack `data.length`).
- **Hashtable forms** (`ByteArrayHashMap/ByteArrayHashSet`): count backing array sizes (`keys[]`, `values[]`, `states[]`, etc.).
- **Quicklist**: sum per-node listpack capacities.
- **ZSET skiplist**: count hash map storage + skiplist node arrays (approximate).
- **Expire dictionary**: small, but optionally include `expires` key entries.
- **Off-heap**: add `offHeapAllocator.usedBytes()` when enabled. This is exact and low-cost.

### API shape
- A DB-internal estimator like:
  - `long estimateEntryBytes(byte[] key, YierdisObject obj)` (or value-only)
  - `long estimateValueBytes(YierdisObject obj)`
- Store the last computed estimate in the object so updates can do:
  - `usedBytes += (newEstimate - oldEstimate)`

## LRU Sampling

### Metadata
- Use a cheap monotonically increasing **clock** (e.g., `int lruClock`) on the DB.
- Each access sets `obj.lru = lruClock` (or stores `lastAccessMillis` if preferred).

### Sampling algorithm (allkeys-lru)
- For one eviction step:
  - sample N keys using `store.randomKey()` (N = `maxmemorySamples`)
  - pick the candidate with the oldest LRU clock (smallest or largest depending on encoding)
  - delete that key
- This matches Redis’ “approximate LRU” philosophy and keeps overhead bounded.

## Eviction Loop

### Trigger points
- Run eviction only on **write commands** (mutations).
- Optionally call `cleanupExpired()` first to reclaim obviously freeable entries.

### Loop shape
- While `usedBytes > maxmemoryBytes`:
  - choose a victim key according to policy
  - delete it (and update `usedBytes`)
  - stop if:
    - no keys can be evicted (DB empty), or
    - attempts exceed a small bound to avoid long pauses

### noeviction behavior
To be Redis-like, `noeviction` should reject writes **without mutating state** when the write would exceed maxmemory.
If exact delta estimation is difficult for some commands, prefer a conservative estimate (may reject earlier) and
document that memory accounting is approximate.

## Introspection Commands
- `OBJECT ENCODING <key>`:
  - Map internal `ValueEncoding` to stable Redis-like strings:
    - `STRING_INT` → `int`
    - `STRING_EMBSTR` → `embstr`
    - `STRING_RAW` → `raw`
    - `HASH_PACKED` / `ZSET_PACKED` / `LIST_PACKED` → `listpack`
    - `HASH_HT` / `SET_HT` → `hashtable`
    - `SET_INTSET` → `intset`
    - `LIST_QUICKLIST` → `quicklist`
    - `ZSET_SKIPLIST` → `skiplist`
- `MEMORY USAGE <key>`:
  - Return the estimated bytes (value-only or key+overhead per acceptance decision).

