## 1. Proposal Acceptance
- [x] Confirm the intended meaning of “full off-heap”: all data + index arrays off-heap, with only minimal metadata objects on-heap.
- [x] Confirm allocator choice: `sun.misc.Unsafe` (no incubator modules) and explicit max memory cap.

## 2. Storage Engine (Core)
- [ ] Add `yier.bubu.redis.db.offheap` package with allocator + primitives (`OffHeapAllocator`, `OffHeapSlice`, bounds checks).
- [ ] Implement slab/free-list allocator with size classes + fallback for large blocks.
- [ ] Add deterministic `close()` / `shutdown()` wiring to free all allocations.
- [ ] Add memory accounting + hard limit enforcement (fail command with RESP error on OOM).

## 3. Off-heap Dictionaries / Indexes
- [x] Implement off-heap open-addressing hash table (keys stored off-heap, slots array off-heap).
- [x] Implement off-heap keyspace store (key -> object handle) with incremental rehash equivalent.
- [x] Implement off-heap expires index (key -> expireAtMillis) and active-expire sampling.

## 4. Off-heap Values (Redis-like encodings)
- [ ] Implement off-heap string value with (ptr,len,cap) semantics and growth/shrink APIs.
- [ ] Migrate string commands (`GET/SET/APPEND/STRLEN/INCR/DECR`) to off-heap payloads.
- [ ] Implement off-heap hash/list/set/zset internal structures (packed + upgraded forms).
- [ ] Implement off-heap zset member dict + skiplist nodes stored off-heap.

## 5. Protocol / Reply Path
- [x] Extend `RespWriter` to write bulk strings from off-heap slices without copying to heap.
- [x] Ensure `YierdisFastCommandProcessor` uses the new write APIs.

## 6. Safety / Validation
- [x] Add unit tests for allocator: allocate/free/reuse, bounds checks, leak detection on shutdown.
- [ ] Update existing DB tests to use the new storage engine.
- [ ] Add targeted fuzz-ish tests for hash table (random ops + invariants).
- [x] Run `mvn test` and document results.

## 7. Documentation
- [x] Update `README.md` with off-heap configuration (max memory, shutdown semantics) and caveats.
