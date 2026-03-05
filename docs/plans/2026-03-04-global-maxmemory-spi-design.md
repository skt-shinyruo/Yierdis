# Global Maxmemory SPI & Governor Refactor — Design

**Date:** 2026-03-04  
**Branch:** `refactor/maxmemory-spi`

## Goal

Decouple “maxmemory / eviction policy” from `YierdisDb` internal data structures so that:

1. Runtime can assemble different storage engines (multiple keyspace implementations, or a future persistent/external engine) without modifying runtime code.
2. The **global maxmemory governor** can coordinate eviction across multiple DBs/engines without directly reading engine internals (no `db.usedBytes`, no `db.store`, no `YierdisObject.lruClock` access).
3. The **maxmemory semantics** remain Redis-like and stable:
   - `maxmemoryBytes` is a *logical dataset estimate* budget
   - when over budget, the system must evict/delete keys to get under budget (if policy permits)
   - for `noeviction`, writes that would grow the dataset must be rejected with a stable OOM error

Non-goals:
- Change wire protocol or command semantics.
- Add persistence/AOF/replication in this refactor (we only make the boundaries ready).
- Make memory accounting JVM-precise (we keep “best-effort explainable estimate” semantics).

---

## Current Problems (from code)

### 1) Global maxmemory coordinator is coupled to `YierdisDb` internals

`YierdisGlobalMaxmemoryCoordinator` currently:

- reads `YierdisDb` internal fields (e.g. `db.usedBytes`)
- reads the internal keyspace implementation directly (`db.store.size()`, `db.store.randomKey()`, `db.store.forEach(...)`)
- reads `YierdisObject` internal eviction metadata (`e.lruClock`)

This means any change to store/ledger/LRU metadata forces changes in the coordinator.

### 2) Runtime assembly is not a true boundary

`YierdisInstance.create(...)` directly constructs the concrete DB class via `new YierdisDb(...)`.

This blocks:
- swapping to a different in-memory keyspace implementation (sharded/off-heap variants)
- introducing a persistent/external engine (RocksDB-like, remote KV, etc.)

---

## Proposed Architecture

### A) Introduce a minimal “maxmemory SPI” in `yierdis-core-api`

Add SPI types in `yierdis-core/yierdis-core-api` under package `yier.bubu.redis.ops` (co-located with `DbEngine` and existing ops boundaries).

Key design principles:
- **SPI is policy-oriented**, not data-structure-oriented.
- Avoid exposing internal store types or `YierdisObject`.
- Keep SPI small enough that external engines can implement it without importing core-db.

#### Proposed SPI types

1) `MaxmemoryPolicy` (enum)

Shared policy enum used by both runtime governor and engines:
- `NOEVICTION`
- `ALLKEYS_RANDOM`
- `ALLKEYS_LRU`

2) `MaxmemoryErrors` (constants)

Move the stable OOM error string to a neutral place so all engines/governors share it without depending on `YierdisDb`:
- `OOM_ERR = "OOM command not allowed when used memory > 'maxmemory'."`

3) `MaxmemoryCoordinator`

An engine-facing coordinator interface used at reservation time and (optionally) as the global LRU clock source.

Required methods:
- `void prepareWrite(long estimatedExtraBytes)`  
  Best-effort “preflight” that evicts/cleans up so the write can proceed under the global budget (or throws OOM).
- `long nextLruClock()`  
  Single global monotonically increasing clock for cross-engine LRU comparability.

4) `MaxmemoryCoordinatorAware`

Allows runtime to attach/detach a global coordinator without casting to concrete engine classes.
- `void attachMaxmemoryCoordinator(MaxmemoryCoordinator coordinator)`

5) `MaxmemoryParticipant` + `MaxmemoryCandidate`

The governor interacts with each engine via a participant view:
- `long usedBytesForMaxmemory()` — dataset estimate excluding shared usage sources
- `int keyCountEstimate()`
- `void cleanupExpired(long nowMillis)`
- `MaxmemoryCandidate sampleCandidate(MaxmemoryPolicy policy, long nowMillis)`
- `default MaxmemoryCandidate scanBestCandidate(MaxmemoryPolicy policy, long nowMillis)` (optional deterministic full scan)
- `boolean evict(MaxmemoryCandidate candidate, long nowMillis)`

`MaxmemoryCandidate` is a small immutable value:
- `MaxmemoryParticipant owner`
- `byte[] key`
- `long lruClock` (only meaningful for LRU policies; can be 0 for RANDOM)

6) `MaxmemoryUsageSource` (shared usage)

Encodes instance-wide shared memory usage that must be counted once (e.g. a shared off-heap allocator):
- `long usedBytes()`

Contract:
- Participant `usedBytesForMaxmemory()` must **not** include these shared sources (avoid double-counting).

### B) Implement the global governor in `yierdis-core-runtime`

Add a runtime component (e.g. `YierdisGlobalMaxmemoryGovernor`) that:

- implements `MaxmemoryCoordinator`
- maintains a single global LRU clock (`AtomicLong`)
- coordinates eviction across `MaxmemoryParticipant[]`
- sums global used bytes:
  - `sum(participant.usedBytesForMaxmemory())`
  - `+ sum(sharedUsageSource.usedBytes())`

Behavior requirements (matches existing semantics):
- If `maxmemoryBytes <= 0`: no-op.
- Always attempt `cleanupExpired` across participants before eviction (best-effort).
- `NOEVICTION`:
  - if `estimatedExtraBytes > 0` and global used would exceed budget → throw OOM
  - if `estimatedExtraBytes == 0` (“no growth”) → allow even when already over budget
- Evicting policies:
  - compute `limit = maxmemoryBytes - estimatedExtraBytes`
  - evict until global used <= limit or attempts/time limit exhausted
  - if still above limit and `estimatedExtraBytes > 0` → throw OOM

Victim selection:
- RANDOM: sample candidates and evict directly
- LRU:
  - if `samples >= totalKeys`: deterministic full scan via `scanBestCandidate` when supported, else fall back to sampling
  - else: pick min `lruClock` among `samples` candidates

### C) Adapt `YierdisDb` as an engine implementation (in `yierdis-core-db`)

`YierdisDb` remains free to use its internal store (`YierdisKeyspace<YierdisObject>`) and internal metadata, but those details stay inside the engine.

Changes:
- Replace the existing `YierdisGlobalMaxmemoryCoordinator` usage with `MaxmemoryCoordinator` SPI:
  - `DbMemoryLedger.reserve(...)` calls `coordinator.prepareWrite(...)` in GLOBAL mode
  - `touch(...)` uses `coordinator.nextLruClock()` when LRU is enabled in GLOBAL mode
- Implement `MaxmemoryParticipant` logic for:
  - `usedBytesForMaxmemory()` (excluding shared sources)
  - candidate sampling / deterministic scan
  - eviction execution (delete key, release payload, adjust ledger)

The net effect:
- governor never reads `db.store` or `YierdisObject`
- only the engine touches its internals

### D) Make runtime assembly pluggable (engine factory in core-api; default impl in core-db)

To support future engines, introduce a factory SPI so runtime does not call `new YierdisDb(...)` directly.

1) `DbEngineFactory` (core-api)

Creates an engine instance for a given DB index and config.

2) Default `YierdisDbEngineFactory` (core-db)

Implements `DbEngineFactory` and builds `YierdisDb` engines using the current constructor/options.

3) `YierdisInstance` (core-runtime)

`YierdisInstance.create(config)` selects a factory:
- default to `YierdisDbEngineFactory` (internal default; no behavior change)
- if config provides an alternative factory, runtime uses it

In GLOBAL maxmemory scope:
- runtime creates the governor using the participants produced by engines
- runtime attaches the governor to all engines via `MaxmemoryCoordinatorAware`

This makes runtime a true “composition root” without hard-coding engine implementation classes.

---

## Testing & Verification

Maintain existing tests, and add at least one new test to reduce regression risk:

1) **Existing:** global maxmemory counts shared off-heap once across DBs  
`yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/runtime/YierdisInstanceTest.java`

2) **New:** global LRU eviction across DBs is consistent  
Add a test that:
- creates an instance with 2 DBs, `maxmemoryScope=GLOBAL`, policy `ALLKEYS_LRU`
- uses a small budget and deterministic scan (samples >= total global keys)
- verifies the globally least-recently-used key (across both DB indices) is evicted

Verification commands (after implementation):
- `mvn -q -pl yierdis-core/yierdis-core-api test -Dmaven.repo.local=/tmp/m2repo-yierdis`
- `mvn -q -pl yierdis-core/yierdis-core-db test -Dmaven.repo.local=/tmp/m2repo-yierdis`
- `mvn -q -pl yierdis-core/yierdis-core-runtime test -Dmaven.repo.local=/tmp/m2repo-yierdis`
- `mvn -q test -Dmaven.repo.local=/tmp/m2repo-yierdis`

---

## Risks & Mitigations

- **SPI bloat:** keep SPI policy-focused; avoid exposing store concepts.
- **Performance regressions:** keep governor allocation-free on hot paths (candidate reuse can be a follow-up if needed).
- **Semantics drift:** keep OOM message and no-growth exceptions stable via shared constants and regression tests.
- **Engine implementor burden:** offer a small default governor + clear participant contracts; external engines only implement required hooks.

