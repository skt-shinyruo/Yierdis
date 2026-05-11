# YierdisObject Removal Design

## Goal

Remove `YierdisObject` from the `yierdis-db-memory` implementation instead of keeping it as a compatibility adapter.

After this refactor, the single DB storage model should be:

```text
YierdisFfmMemoryRuntime
  -> slab allocator
  -> EntryTable
  -> NativeKeyDirectory
  -> EntryRecord
  -> ValueHandle
  -> TypeRoot
```

Production hot paths must not instantiate, store, mutate, scan, introspect, or release values through `YierdisObject`.

## Current State

The native storage migration already introduced:

- `EntryTable`
- `EntryRecord`
- `EntryHandle`
- `NativeKeyDirectory`
- `ValueHandle`
- `StringRoot`
- `ListRoot`
- `HashRoot`
- `SetRoot`
- `ZSetRoot`

However, `YierdisObject` is still present as a compatibility adapter. It still appears in several critical paths:

- `YierdisDb` and `YierdisDbStorageComponents` still carry `YierdisKeyspace<YierdisObject>`.
- `YierdisDbKeyLifecycle` still exposes `getLiveObject`, `getStoredObject`, `computeObjectWithHandle`, `computeObjectIfPresentWithHandle`, `removeObject`, and object-based `touch`.
- string/list/hash/set/zset/HLL ops still use object fields such as type, encoding, estimated bytes, LRU clock, and value handle.
- TTL, delete, scan, maxmemory eviction, memory reporter, introspection, and resource cleanup still accept or release object instances.
- docs and tests still mention `YierdisObject` as an implementation detail.

This keeps a second value model alive and makes future changes easy to route through the wrong abstraction.

## Non-Goals

- Do not change the public DB API, command API, protocol layer, server runtime, or wire behavior.
- Do not introduce a new heap value object to replace `YierdisObject`.
- Do not remove the existing `HashValue`, `ListValue`, `SetValue`, or `ZSetValue` internals if they are still used behind roots.
- Do not redesign the FFM slab allocator.

## Target Architecture

### Key Ownership

`NativeKeyDirectory` becomes the authoritative key dictionary for live DB values.

The compatibility `YierdisFfmKeyspace<YierdisObject>` store should be removed from the DB state. If key scanning still needs a cursor implementation that only exists in `YierdisFfmKeyspace`, that capability should move to `NativeKeyDirectory` or a native key iterator without storing values.

### Entry Metadata

`EntryRecord` becomes the authoritative source for:

- logical type
- value encoding
- `ValueHandle`
- expire time
- estimated bytes used for ledger subtraction
- LRU/LFU metadata

The current `EntryRecord.version()` field is being used as estimated bytes. This should be renamed or wrapped in a clearer API during the refactor if the blast radius is reasonable. If not, the refactor must at least hide that meaning behind lifecycle methods so callers no longer read `version()` directly for memory semantics.

### Value Payload

Each type op should resolve:

```text
key bytes / BytesView
  -> KeyHandle
  -> EntryHandle
  -> EntryRecord
  -> ValueHandle
  -> TypeRoot
```

The roots own all payload lifecycle:

- `StringRoot.release(handle)`
- `ListRoot.release(handle)`
- `HashRoot.release(handle)`
- `SetRoot.release(handle)`
- `ZSetRoot.release(handle)`

There should be one central release helper that switches on `EntryRecord.type()` and releases the record's `ValueHandle` through the matching root.

### Lifecycle API

`YierdisDbKeyLifecycle` should become native-entry-only. The final API should center on methods like:

- `keyHandle(...)`
- `entryHandle(...)`
- `entryRecord(...)`
- `liveEntryRecord(...)`
- `computeEntryWithHandle(...)`
- `computeEntryIfPresentWithHandle(...)`
- `removeEntry(...)`
- `removeIfExpired(...)`
- `touchEntry(...)`
- `setExpireAtMillis(...)`
- `removeExpire(...)`

Object-shaped methods should be deleted, not deprecated.

### Type Ops

Type ops should stop creating `YierdisObject` instances and instead construct or update records directly.

For writes:

1. Resolve live record and handle.
2. If missing, create a new value in the matching root.
3. If present and wrong type, throw `WrongTypeException`.
4. Mutate through root.
5. Recompute encoding and estimated bytes.
6. Replace the `EntryRecord` atomically through lifecycle.
7. Release old root payload only after the entry replacement/removal path is known to be committed.

For reads:

1. Resolve `liveEntryRecord`.
2. Check type from record.
3. Read from matching root by `ValueHandle`.
4. Touch entry metadata for LRU/LFU.

### HLL

HLL should become a string-root helper. `YierdisHyperLogLog` must no longer accept `YierdisObject`.

Its public helper surface inside db-memory should operate on either:

- `StringRoot + ValueHandle`, for in-place sparse/dense mutation; or
- `byte[]` plus an explicit write-back call for operations that materialize and rewrite.

### Memory Reporting And Introspection

`YierdisDbMemoryReporter` and `YierdisDbIntrospection` should read only:

- `EntryRecord`
- `KeyHandle`
- root state
- allocator/runtime counters
- expires index counters

No fallback to object estimates should remain.

### Cleanup And Eviction

The delete, TTL cleanup, flush, shutdown, and maxmemory eviction paths should all remove entries by handle:

```text
remove expire metadata
unlink NativeKeyDirectory entry
release EntryTable slot
release TypeRoot payload
subtract EntryRecord estimated bytes
```

If release fails, cleanup should preserve the existing failure aggregation behavior from `YierdisDbOwnedResources`.

## Migration Order

### Phase 1: Guard Against Compatibility Regressions

Add an architecture or unit guard that fails while production source still references `YierdisObject`.

The guard should allow references only in:

- the test that proves it has been removed, if needed during RED
- deleted-file history, which is not scanned

Expected RED: the guard fails because production code still imports or references `YierdisObject`.

### Phase 2: Native Key Directory Becomes The Only Live Dictionary

Remove `YierdisKeyspace<YierdisObject>` from:

- `YierdisDb`
- `YierdisDbStorageComponents`
- `YierdisDbComponents`
- `YierdisDbComponentFactory`
- `YierdisDbMemoryReporter`
- `YierdisDbOwnedResources`
- `YierdisDbKeyLifecycle`

`NativeKeyDirectory` must provide any missing iteration/random-key operations required by `KEYS`, `SCAN`, `RANDOMKEY`, expiration sampling, and maxmemory sampling.

### Phase 3: Rewrite Lifecycle Around Entry Records

Replace object lifecycle methods with entry lifecycle methods.

This phase should centralize:

- live lookup
- expiration checks
- LRU/LFU touch
- entry replacement
- entry removal
- value release
- estimated-byte lookup

### Phase 4: Migrate Type Ops

Move type ops to direct record/root operations in a risk-ordered sequence:

1. string
2. HLL
3. list
4. hash
5. set
6. zset

Each type should get focused tests for create, update, wrong type, delete, expiration, memory usage, and root release.

### Phase 5: Remove `YierdisObject`

Delete:

- `YierdisObject.java`
- object-specific tests
- object-specific estimator APIs
- docs that describe the compatibility layer

Run an exact source search for `YierdisObject` and leave no production references.

### Phase 6: Update Documentation And PR Description

Update docs to describe only the native model:

```text
FFM slabs
+ 64-bit handles
+ EntryTable
+ NativeKeyDirectory
+ TypeRoot per data type
```

Remove compatibility-adapter wording from PR-facing documentation.

## Testing Strategy

Use TDD for each phase.

Minimum focused suites:

- `NativeStorageRegressionTest`
- `YierdisDbMemoryEstimatorTest`
- `OffHeapKeysToggleTest`
- `UnsafeOffHeapDbSmokeTest`
- `UnsafeOffHeapKeyspaceTest`
- `ExpireIndexTest`
- `MemoryStatsAccountingConsistencyTest`
- `YierdisFfmRehashConsistencyTest`
- `StringRootTest`
- `ListRootTest`
- `CollectionRootTest`
- `HashValueTest`
- `SetValueTest`
- `ZSetValueTest`
- `OffHeapCollectionReadStreamingTest`

Full regressions before final commit:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -pl yierdis-memory/yierdis-memory-ffm,yierdis-db/yierdis-db-memory -am test
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -pl yierdis-tests/yierdis-architecture-tests -am test -Dsurefire.failIfNoSpecifiedTests=false
```

## Risks

- Removing the compatibility store can break scan or random-key behavior if `NativeKeyDirectory` does not provide equivalent cursor and sampling semantics.
- Replacing object touch with entry touch can alter LRU eviction unless `EntryRecord.lruOrLfu` is updated consistently.
- Root payload release ordering can leak native memory or release live handles if entry replacement is not atomic enough.
- HLL has dense/sparse in-place mutation logic that currently uses string-like object helpers; this is likely the most subtle string-related migration.
- Memory accounting can drift if estimated bytes are not refreshed after every root mutation.

## Success Criteria

- `rg "YierdisObject" yierdis-db/yierdis-db-memory/src/main/java` returns no results.
- `YierdisObject.java` is deleted.
- DB construction no longer creates `YierdisKeyspace<YierdisObject>`.
- Type ops work directly from `EntryRecord.valueHandle()` and the corresponding root.
- TTL, delete, scan, maxmemory, memory reporter, introspection, flush, and shutdown use entry/root state only.
- All focused and full regression commands pass with JDK 25.
- Internal docs no longer describe a compatibility `YierdisObject` layer as part of the current implementation.
