# DB Native Allocator Unification Design

## Status

Draft design spec for migrating DB storage objects onto one DB-level `NativeAllocator` namespace.

This spec builds on `docs/project-docs/native-allocator-and-handles.md`. That document defines the handle ABI, object table, realloc, pin, epoch, quarantine, and defrag semantics. This document defines how the DB layer should adopt that allocator consistently across entry records, string payloads, key bytes, collection roots, maintenance, scan/snapshot safety, stress tests, and performance validation.

## Problem

The DB currently has several native-memory ownership models:

- `EntryTable` allocates `ENTRY_RECORD` objects through `YierdisStableNativeAllocator`.
- `StringRoot` allocates payload bytes through `OffHeapAllocator` and keeps a heap `Map<Long, Slot>`.
- `NativeKeyDirectory` stores key bytes in `YierdisFfmBlobStore`.
- `ListRoot`, `HashRoot`, `SetRoot`, and `ZSetRoot` keep root identity in heap maps and use a mix of heap control structures plus FFM payload adapters.

Those models can coexist during migration, but they must not be treated as one coherent stable-handle system until they share one DB allocator namespace. Otherwise memory stats, active defrag, stale-handle checks, and epoch/quarantine guarantees apply to only part of the DB.

## Goals

- Create exactly one DB-owned `NativeAllocator` namespace for allocator-backed DB objects.
- Ensure `EntryTable` and `StringRoot` use the same allocator instance.
- Migrate `StringRoot` to allocator-backed `STRING_BYTES` handles first.
- Keep key bytes explicitly isolated in the short term, then migrate them to `KEY_BYTES` after string migration is proven.
- Migrate collection roots in phases, starting with allocator-backed root objects before nativeizing internal nodes.
- Add DB maintenance integration for allocator defrag.
- Add scan/snapshot epoch rules that make free, realloc, and defrag safety testable.
- Add deterministic and randomized churn tests at allocator and DB levels.
- Add benchmarks that quantify stable-handle overhead before expanding the model to every collection node.

## Non-Goals

- Do not rewrite all collection internals in one change.
- Do not migrate key bytes in the same phase as strings.
- Do not remove `YierdisFfmBlobStore` immediately; it remains a transitional owner for key bytes and some collection payloads.
- Do not expose allocator-private addresses, page ids, blob offsets, or `MemorySegment` references to command code.
- Do not make DB operations multi-threaded. The design assumes the existing single-owner-thread shard model and uses epochs to model long-running scans/snapshots and maintenance interleavings.

## Design Principles

### One DB Allocator Namespace

Each `YierdisDb` owns one `NativeAllocator` instance for DB-scoped stable objects. The allocator is created in the DB component assembly layer and passed into storage components that allocate stable objects.

Initial allocator-backed kinds:

- `ENTRY_RECORD`
- `STRING_BYTES`

Later allocator-backed kinds:

- `KEY_BYTES`
- collection root objects using `LIST_NODE`, `HASH_NODE`, `SET_NODE`, and `ZSET_NODE`
- future internal collection nodes

Transitional native structures that are not allocator-backed must be documented as outside the stable allocator namespace and excluded from allocator defrag.

### Stable Handles, Short-Lived Views

DB structures store only `NativeHandle`-backed wrapper types such as `EntryHandle` and `ValueHandle`. They resolve handles through `NativeAllocator.resolve(...)` only inside a bounded operation.

Rules:

- Do not cache physical addresses.
- Do not keep `NativeObjectView` in root maps or entry records.
- Copy bytes out for APIs that need a durable heap result.
- Close every resolved view in the same method that opened it.

### Migration by Semantic Boundary

The migration order is based on ownership risk:

1. `EntryTable` allocator ownership cleanup, because entry records already use the stable allocator.
2. `StringRoot`, because it is self-contained and exercises allocate, realloc, resolve, free, stale-handle detection, and memory accounting.
3. Key bytes, because they affect key equality, scan, expire, eviction, and entry lifecycle.
4. Collection roots, because root identity and payload ownership are separate concerns.
5. DB maintenance defrag and scan/snapshot epochs once enough DB objects are allocator-backed to make the behavior meaningful.

## Phase 1: DB Allocator Ownership

### Current Issue

`EntryTable` can construct its own `YierdisStableNativeAllocator`, while `StringRoot` receives an `OffHeapAllocator`. This creates separate allocation domains inside one DB.

### Required Design

Introduce DB-level ownership for `NativeAllocator` in the storage component assembly path:

- `YierdisDbStorageComponents` creates or receives the DB `NativeAllocator`.
- `YierdisDbOwnedResources` records whether the DB owns the `NativeAllocator` and closes it exactly once when it does.
- `EntryTable` receives the allocator and does not create a private stable allocator in production assembly.
- Compatibility constructors may remain for unit tests, but production paths must call the shared-allocator constructor.
- `YierdisDbKeyLifecycle`, memory reporting, and maintenance code can access allocator stats through a DB-level accessor rather than through an arbitrary root.

Resource close order must make ownership explicit:

- roots release their live handles before the shared allocator is closed
- `EntryTable.close()` frees entry records but must not close a shared allocator it does not own
- legacy test constructors that create private allocators may keep owning behavior
- DB-owned resources close transitional blob stores and off-heap allocators separately from the stable allocator

### Accounting

Allocator-backed bytes come from `NativeAllocator.stats()` and direct native bytes should not double-count those objects. Transitional paths stay separately reported:

- stable allocator bytes: entry and string after Phase 2
- blob store bytes: key bytes until Phase 3
- root adapter native bytes: collection internals until Phase 4/5
- heap ledger bytes: Java metadata and command-visible logical accounting

### Acceptance Criteria

- A DB instance has one shared `NativeAllocator` for allocator-backed objects.
- `EntryTable` and `StringRoot` can be constructed with that same allocator.
- Closing DB-owned resources closes the shared allocator once.
- Existing off-heap/blob-store bytes are still visible in memory stats and are not counted as stable allocator objects.
- Tests assert that production component assembly does not allocate a private `YierdisStableNativeAllocator` inside `EntryTable`.

## Phase 2: StringRoot Migration

### Target Model

`StringRoot` stores each string/HLL payload as one allocator object:

```text
ValueHandle
  -> NativeHandle(kind = STRING_BYTES)
  -> NativeAllocator object table slot
  -> current native payload bytes
```

`StringRoot` no longer owns a heap `Map<Long, Slot>` for live string payloads. The allocator object table is the source of truth for liveness, size, generation, and physical location.

### API Behavior

`store(byte[])` and `store(BytesSlice)`:

- allocate `NativeObjectKind.STRING_BYTES` with the exact logical length
- copy the payload through a short-lived `NativeObjectView`
- return `ValueHandle` wrapping the allocator handle

`append(handle, suffix)`:

- resolve the existing length through allocator metadata or object view size
- call `allocator.realloc(handle.nativeHandle(), newLen, PRESERVE_PREFIX)`
- assert or rely on the contract that the returned handle raw value is unchanged
- write the suffix at the old length through a short-lived write view

`overwrite(handle, value)`:

- use `realloc(..., PRESERVE_PREFIX)` or a replace policy chosen by implementation
- write the full new payload
- preserve the same handle when possible; if a future replace path changes the handle, the entry record update must be transactional with the payload operation

`slice`, `copy`, `getByte`, and `setByte`:

- resolve the handle only for the duration of the method
- return heap copies for durable results
- for `slice`, either return a copy-backed `OffHeapSlice` equivalent or introduce a scoped slice API; it must not expose a view that can outlive the allocator resolve scope unless it pins and closes explicitly

`release(handle)`:

- call `allocator.free(handle.nativeHandle())`
- stale/double-free behavior comes from the allocator

### HLL Compatibility

HyperLogLog uses `StringRoot` as its byte store. The string migration must preserve:

- sparse and dense HLL byte layouts
- `isHllString`, `isDense`, `pfAdd`, `pfCount`, and merge helpers
- byte-level register reads and writes

Any change to `slice` must keep HLL callers from retaining an unsafe native view.

### Acceptance Criteria

- `APPEND` preserves the raw `ValueHandle` across in-place and moved realloc paths.
- Resolving or reading a freed string handle fails with stale-handle semantics.
- Reusing an allocator slot increments generation and does not allow an old handle to observe a new string.
- `release` returns allocator logical used bytes for that string to zero.
- DB shutdown after string churn returns runtime used bytes to zero.
- Existing string and HLL behavior tests pass unchanged or with only API-safe updates.

## Phase 3: Key Bytes Decision and Migration

### Short-Term Isolation

Until key bytes migrate, `NativeKeyDirectory` remains backed by `YierdisFfmBlobStore`. This must be documented as a separate native namespace:

- key byte refs are not allocator handles
- allocator defrag must not attempt to move key blob-store objects
- allocator stats do not include key blob-store payload bytes
- memory reporter continues to add key-directory native bytes separately

### Migration Target

When migrated, each key byte payload becomes:

```text
KeyHandle
  -> NativeHandle(kind = KEY_BYTES)
  -> NativeAllocator object table slot
  -> key bytes
```

`NativeKeyDirectory` still owns hashing, probing, key equality, and mapping from key bytes to `EntryHandle`, but payload storage moves from blob refs to allocator handles.

### Required Semantics

- Key equality resolves key handles only for the comparison operation.
- Insert copies the key into `KEY_BYTES`.
- Delete frees the key handle after removing the directory entry.
- Scan copies key bytes for the scan batch and does not expose allocator views to callers.
- Eviction, expiration, rename-like lifecycle operations, and DB shutdown free key handles exactly once.

### Acceptance Criteria

- Before migration, docs and stats clearly identify key bytes as blob-store-owned.
- After migration, key insert/delete/overwrite churn returns allocator key bytes to zero.
- Scan output is unchanged.
- Expire and eviction do not leak key bytes.
- Stale key handle injection fails through allocator checks.

## Phase 4: Collection Root Migration

### Current Issue

Collection roots currently store root identity in heap maps:

```text
Map<Long, ListValue>
Map<Long, HashValue>
Map<Long, SetValue>
Map<Long, ZSetValue>
```

Wrapping those map keys in `ValueHandle` is not enough. A stable handle is only meaningful when the root object is allocator-owned.

### Phase A: Allocator-Backed Root Objects

Each collection root allocates a small root metadata object:

- `LIST_NODE`
- `HASH_NODE`
- `SET_NODE`
- `ZSET_NODE`

The root object stores enough metadata to locate or validate the existing adapter object. During this phase, the value implementation may remain heap/FFM hybrid, but root liveness and generation are allocator-backed.

The heap map can temporarily become an adapter table keyed by allocator handle raw value. The allocator object remains the liveness authority; the adapter table must not resurrect freed roots.

### Phase B: Native Internal Nodes

Internal listpack, intset, hashtable, skiplist, and zset nodes move behind allocator handles incrementally. Each migrated node type must define:

- object kind
- binary layout
- owner root link
- free order
- defrag move validation
- stats ownership

### Phase C: Handle Graph Visitor

Add a visitor that walks from entries to roots to internal node handles. Uses:

- integrity checks
- leak detection
- defrag candidate discovery
- future snapshot/RDB/AOF traversal
- migration verification

### Acceptance Criteria

- Root handles become stale after collection deletion.
- Adapter tables do not own liveness.
- Collection operations behave identically before and after root-object migration.
- Native bytes and allocator stats agree after mixed collection churn.
- Graph visitor can enumerate all allocator-backed objects reachable from live entries.

## Phase 5: DB Defrag Maintenance

### Maintenance Entry Point

Add a DB-level maintenance hook that calls:

```text
allocator.defragCycle(options)
```

The hook runs on the DB owner thread and shares the same scheduling model as expiration and maxmemory maintenance.

### Configuration

Expose bounded controls:

- enabled/disabled flag
- max bytes per cycle
- max objects per cycle
- max time per cycle
- minimum object size or fragmentation threshold, if needed

Defaults should be conservative and safe for tests:

- disabled or tiny budget by default until string migration and DB-level safety tests are stable
- explicit test configuration enables deterministic defrag

### Defrag Rules

- Pinned objects are skipped.
- Quarantined/freed objects are not moved.
- Transitional blob-store objects are ignored.
- Moved objects retain the same stable handle.
- Old physical blocks are retained until epoch/quarantine rules allow reclamation.
- DB read/write results before and after defrag must be identical.

### Metrics

Expose allocator metrics through DB memory/introspection stats:

- moved object count
- moved bytes
- skipped pinned objects
- skipped over-budget objects
- quarantined objects
- quarantine bytes
- stale handle detections
- retained moved blocks, if available

### Acceptance Criteria

- Maintenance can run defrag with deterministic budgets in tests.
- Reads and writes after defrag see the same logical values.
- Pinned objects are skipped and recorded.
- Defrag does not touch non-allocator blob-store objects.
- Metrics change predictably for moved/skipped/quarantined cases.

## Phase 6: Scan and Snapshot Safety

### Epoch Model

Use allocator epochs to make long reads safe:

- command operations may rely on short-lived views only
- scan batch opens a `SCAN` epoch for the batch
- snapshot opens a `SNAPSHOT` epoch for the snapshot operation or for bounded chunks if the implementation streams
- defrag uses `DEFRAG` epoch or equivalent internal protection

### Scan Rules

- Scan walks key directory state using the DB owner-thread model.
- Within each batch, resolve allocator-backed key/value handles only long enough to copy bytes or metadata into batch output.
- Do not return allocator views from scan APIs.
- If an entry is deleted before its batch copy, it is skipped or treated according to existing scan semantics.

### Snapshot Rules

- Snapshot materializes heap-owned `YierdisSnapshotEntry` values.
- String bytes are copied while the relevant snapshot epoch is active.
- Collection snapshot support must use the future graph visitor or type-specific copy routines; it must not persist native addresses.

### Acceptance Criteria

- Freed or moved blocks are not physically reclaimed while an active scan/snapshot epoch can still observe them.
- Scan with interleaved delete, overwrite, append, and defrag follows documented single-thread interleaving semantics.
- Snapshot output remains stable after later writes and frees.
- Tests simulate free/defrag while epochs are open and verify quarantine/retained bytes are released after epoch close.

## Phase 7: Random Churn and Stress Tests

### Allocator-Level Stress

Extend allocator tests with deterministic random sequences:

- allocate
- free
- realloc with `PRESERVE_PREFIX`
- resolve read/write
- pin/unpin
- begin/close epochs
- defrag one object
- defrag cycle
- stale handle injection
- generation reuse

Assertions:

- model bytes match allocator bytes
- stale handles fail
- no object is readable after final free
- runtime used bytes returns to zero after allocator close

### DB-Level Stress

Add mixed DB churn:

- SET, GET, APPEND, DEL
- overwrite existing values
- TTL expire and explicit cleanup
- eviction paths if maxmemory is enabled
- PFADD/PFCOUNT because HLL shares `StringRoot`
- list/hash/set/zset add/remove operations once root migration starts
- defrag maintenance interleaved between operations
- scan/snapshot batches interleaved with writes and deletes

Assertions:

- heap reference model matches DB command results
- memory reporter and allocator stats do not diverge
- shutdown returns runtime used bytes to zero
- no stale handle is accepted after delete or generation reuse

## Phase 8: Performance and Memory Evaluation

### Benchmarks

Measure before and after each major migration:

- SET throughput and p50/p95/p99
- GET throughput and p50/p95/p99
- APPEND throughput and p50/p95/p99
- small-object churn
- HLL sparse/dense update cost
- defrag enabled versus disabled p99 impact

### Allocator Microbenchmarks

Measure:

- allocate/free per object size class
- resolve/close cost
- realloc in-place versus moved
- pin/unpin overhead
- object table metadata bytes per live object
- quarantine/epoch retained bytes under churn

### Acceptance Criteria

- Stable-handle migration has quantified overhead.
- Object table metadata percentage is reported for small and medium values.
- Defrag p99 impact is bounded by configured budgets.
- Benchmark output is comparable with the existing benchmark harness.

## Implementation Order

1. Add DB-owned shared `NativeAllocator` to component assembly and resource cleanup.
2. Pass the shared allocator into `EntryTable`.
3. Convert `StringRoot` to `NativeAllocator` while preserving public behavior.
4. Add string migration tests for append handle stability, stale handle failure, generation reuse, and shutdown accounting.
5. Update docs and memory reporter to distinguish stable allocator bytes from transitional blob-store bytes.
6. Add scan/snapshot epoch tests around allocator-backed strings.
7. Add DB maintenance defrag hook behind conservative configuration.
8. Add deterministic DB churn tests with defrag disabled and enabled.
9. Decide key migration gate based on string results; either keep documented isolation or migrate key bytes to `KEY_BYTES`.
10. Migrate collection roots in Phase A before nativeizing internal nodes.
11. Add graph visitor after at least one collection root is allocator-backed.
12. Run benchmark suite and record before/after data.

## Open Decisions

- Whether `StringRoot.slice(...)` should return a heap-copy slice or introduce an explicit closeable scoped slice. A heap-copy slice is safer and preserves the no-escaping-view rule, but may cost more for HLL paths.
- Whether `overwrite` must always preserve the handle. Preserving it simplifies entry records; replacing it requires transactional entry update and rollback logic.
- Whether DB memory stats should expose raw `NativeAllocatorStats` or a DB-shaped projection. A projection keeps storage API stable, while raw stats help debugging.
- Whether key bytes should be migrated before DB defrag is enabled by default. Conservative answer: no; enable defrag for allocator-backed entry/string objects first, leave key blob store untouched.

## Definition of Done

This migration is complete when:

- DB production assembly creates one native allocator namespace for allocator-backed objects.
- Entry records, strings, key bytes, and collection roots are allocator-backed or explicitly documented as transitional non-allocator objects.
- Stable handles survive defrag and realloc without physical-address exposure.
- Stale handles fail at allocator boundaries.
- Scan and snapshot tests prove epoch/quarantine behavior.
- DB-level churn tests prove no native leaks after mixed operations and shutdown.
- Maintenance defrag is budgeted, observable, and disabled/enabled by configuration.
- Benchmarks quantify the throughput, latency, and metadata cost of the stable-handle model.
