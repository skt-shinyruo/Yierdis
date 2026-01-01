## 1. Proposal Acceptance
- [x] Confirm the intended meaning of “full off-heap”: all data + index arrays off-heap, with only minimal metadata objects on-heap.
- [x] Confirm allocator choice: `sun.misc.Unsafe` (no incubator modules) and explicit max memory cap.

## 2. Storage Engine (Core)
- [x] Add `yier.bubu.redis.db.offheap` package with allocator + primitives (`OffHeapAllocator`, `OffHeapSlice`, bounds checks).
- [x] Implement slab/free-list allocator with size classes + fallback for large blocks.
- [x] Add deterministic `close()` / `shutdown()` wiring to free all allocations.
- [x] Add memory accounting + hard limit enforcement (fail command with RESP error on OOM).

## 3. Off-heap Dictionaries / Indexes
- [x] Implement off-heap open-addressing hash table (keys stored off-heap, slots array off-heap).
- [x] Implement off-heap keyspace store (key -> object handle) with incremental rehash equivalent.
- [x] Implement off-heap expires index (key -> expireAtMillis) and active-expire sampling.

## 4. Off-heap Values (Redis-like encodings)
- [x] Implement off-heap string value with (ptr,len,cap) semantics and growth/shrink APIs.
- [x] Migrate string commands (`GET/SET/APPEND/STRLEN/INCR/DECR`) to off-heap payloads.
- [x] Implement off-heap hash/list/set/zset internal structures (packed + upgraded forms).
- [x] Implement off-heap zset member dict + skiplist nodes stored off-heap.

## 5. Protocol / Reply Path
- [x] Extend `RespWriter` to write bulk strings from off-heap slices without copying to heap.
- [x] Ensure `YierdisFastCommandProcessor` uses the new write APIs.

## 6. Safety / Validation
- [x] Add unit tests for allocator: allocate/free/reuse, bounds checks, leak detection on shutdown.
- [x] Update existing DB tests to use the new storage engine.
- [x] Add targeted fuzz-ish tests for hash table (random ops + invariants).
- [x] Run `mvn test` and document results.
  - Latest: BUILD SUCCESS (137 tests)

## 7. Documentation
- [x] Update `README.md` with off-heap configuration (max memory, shutdown semantics) and caveats.
