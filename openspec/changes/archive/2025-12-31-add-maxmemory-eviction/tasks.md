## 1. Proposal Acceptance
- [x] Confirm maxmemory applies to (on-heap estimate only) vs (on-heap estimate + off-heap used bytes). (Approved: on-heap estimate + off-heap used bytes)
- [x] Confirm initial policy set: `noeviction`, `allkeys-random`, `allkeys-lru` (sampling LRU). (Approved: `noeviction`, `allkeys-random`, `allkeys-lru`)
- [x] Confirm whether LRU metadata updates on reads + writes (recommended) or writes only. (Approved: reads + writes; only maintained when `allkeys-lru` is active)
- [x] Confirm `MEMORY USAGE` semantics: value-only vs include key+overhead. (Approved: include key + overhead; must match maxmemory accounting model)

## 2. Server Flags & Wiring
- [x] Add CLI flags to `ServerConfig`:
  - [x] `--maxmemoryBytes`
  - [x] `--maxmemoryPolicy`
  - [x] `--maxmemorySamples`
- [x] Thread the config into `YierdisDb` construction and/or command processor.
- [x] Update `README.md` with flags and examples.

## 3. Memory Accounting Primitives
- [x] Define a stable “approximate bytes” model for:
  - [x] `STRING` (int/embstr/raw)
  - [x] `LIST` (listpack/quicklist nodes)
  - [x] `HASH` (listpack/hashtable)
  - [x] `SET` (intset/hashtable)
  - [x] `ZSET` (packed/skiplist)
- [x] Implement per-object memory estimation with minimal allocations.
- [x] Maintain a DB-level `usedBytes` counter updated on mutations (set/overwrite/delete/upgrade).
- [x] Include off-heap allocator bytes if enabled (per Proposal Acceptance).

## 4. LRU Metadata (for eviction)
- [x] Add per-key access metadata (e.g., LRU clock) stored in the value container (`YierdisObject`) or a parallel structure.
- [x] Update access metadata from read/write paths (per Proposal Acceptance).

## 5. Eviction Engine
- [x] Add a DB method like `enforceMaxMemory()` that:
  - [x] runs expiration cleanup first (best-effort),
  - [x] evicts keys until `usedBytes <= maxmemoryBytes`,
  - [x] stops after a bounded number of attempts to avoid long stalls.
- [x] Implement policies:
  - [x] `noeviction`: fail writes with OOM-style error before mutating state (or with a clearly documented behavior).
  - [x] `allkeys-random`: random-key eviction using keyspace sampling.
  - [x] `allkeys-lru`: sampling-based approximate LRU (pick oldest from N random candidates).

## 6. Introspection Commands
- [x] Implement `OBJECT ENCODING <key>` (maps internal encoding to Redis-like names).
- [x] Implement `MEMORY USAGE <key>` returning the chosen accounting model.
- [x] Add basic negative tests (`wrong number of arguments`, missing key, wrong type where applicable).

## 7. Tests & Validation
- [x] Add deterministic unit tests for eviction:
  - [x] `noeviction` rejects writes when full
  - [x] `allkeys-random` evicts until within limit
  - [x] `allkeys-lru` evicts least-recently-used among sampled keys
- [x] Add tests for encoding introspection (`OBJECT ENCODING`) around upgrade thresholds.
- [x] Run `mvn test` and record results. (Done: BUILD SUCCESS, 129 tests on 2025-12-31)
