# Production Allocator Spec Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the production allocator spec beyond the stable-handle core: page/size-class allocation, native object metadata, DB handle migration, epoch-safe reads, active defrag, metrics, and proof tests.

**Architecture:** Build the remaining work as a sequence of independently testable layers. The page allocator and object table become the physical and metadata base, `YierdisStableNativeAllocator` moves onto that base, DB handles migrate to `NativeHandle`, epochs protect scan/snapshot readers, and active defrag moves unpinned physical blocks by updating object metadata while handles remain stable.

**Tech Stack:** Java 25, Maven, JUnit 4, JDK FFM API, existing `YierdisFfmMemoryRuntime`, existing DB memory modules.

---

## Scope And Completion Audit Baseline

The completed stable-handle core already provides:

- `NativeHandle` ABI fields and validation.
- `NativeAllocator` contracts.
- `YierdisStableNativeAllocator` with generation checks, stable `realloc`, pin/quarantine basics, and stats.
- API/FFM tests for the core allocator milestone.

This plan completes the remaining requirements from `docs/superpowers/specs/2026-05-14-production-allocator-handle-design.md`.

Completion must be audited against these concrete success criteria:

| Spec requirement | Required artifact evidence | Required verification |
| --- | --- | --- |
| Page and size-class allocator | New FFM allocator implementation with 64 KiB pages, small size classes through 32 KiB, medium spans, large spans, page ownership metadata, and fragmentation stats | Unit tests for class selection, page ownership, reuse, medium/large spans, accounting, and no page crossing |
| Native object metadata table | Native object-table storage for address, logical size, capacity, generation, domain, kind, pin count, state, owner shard, and epochs | Unit tests that inspect state transitions, stale generation rejection, slot retirement, quarantine, and metadata accounting |
| Stable allocator on production metadata | `YierdisStableNativeAllocator` backed by the object table and page allocator, not per-object regions or heap-only metadata | Existing stable allocator tests plus movement/accounting tests passing |
| DB handle migration | `EntryHandle` and `ValueHandle` wrap production `NativeHandle`; `EntryTable`, key directory, and type roots use allocator-backed handles | DB memory tests updated so stale/wrong-kind entry and value handles fail fast |
| Storage integration | String, HLL, list, hash, set, zset, key bytes, entry records, expire and eviction references use stable handles, not raw physical addresses | Integration tests for overwrite, append, collection mutation, TTL deletion, eviction unlink, scan, and memory reporter |
| Epoch safety | Shard read epochs and bounded pin/copy protocol protect scan and snapshot-style readers | Tests showing freed/moved objects remain quarantined until epoch close, and no raw resolved view escapes an epoch |
| Active defrag | Defrag planner and maintenance cycle move eligible unpinned objects by updating metadata while handle values stay stable | Unit and integration tests for moved bytes, skipped pinned objects, validation hook failure rollback, budget stop, and stable handles |
| Metrics | Stats include logical/reserved/committed bytes, free bytes by class, fragmentation, object counts, page counts, quarantine, pins, stale/double-free, defrag, realloc, and allocation latency | Unit tests for allocator metrics plus DB memory reporter coverage |
| Stress/fuzz confidence | Random allocator operation tests and DB churn tests cover allocation churn, stale attempts, forced OOM, defrag, and close/leak behavior | Maven test modules pass and stress test command has deterministic bounded runtime |

Do not mark the production allocator spec complete until every row above has direct evidence.

## Execution Order

Implement in this order. Each phase must pass its tests and commit before the next phase starts.

1. Page and size-class allocator.
2. Native object metadata table.
3. Stable allocator rebuilt on page allocator plus object table.
4. DB handle wrappers and entry table migration.
5. Key bytes and string/HLL value migration.
6. Collection root migration and handle graph traversal.
7. Epoch manager and safe scan/snapshot read protocol.
8. Active defrag movement.
9. Metrics, reporter integration, and stress/fuzz tests.
10. Completion audit.

## File Structure

Create production allocator internals in `yierdis-memory-ffm`:

- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeSizeClass.java`
  Defines the fixed small-object size classes from 16 B through 32 KiB.
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativePageClass.java`
  Names page ownership classes: small size class, medium span, large span, metadata, quarantine.
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeBlock.java`
  Carries allocator-private physical block identity: region/span, offset, capacity, page class.
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativePageAllocator.java`
  Allocates and frees physical blocks from 64 KiB pages, medium spans, and large spans.
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativePageAllocatorStats.java`
  Reports committed bytes, used bytes, free bytes by class, live page counts, and fragmentation.
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeObjectTable.java`
  Stores object metadata in FFM memory and manages slot state transitions.
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeObjectMeta.java`
  Value object snapshot of one metadata entry for tests, metrics, and validation.
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeEpochManager.java`
  Tracks command, scan, snapshot, and defrag epochs.
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeDefragPlanner.java`
  Selects eligible objects and enforces byte/time budgets.
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeDefragReport.java`
  Reports moved bytes, skipped pinned objects, reclaimed pages, and failed moves.

Update API contracts in `yierdis-memory-api`:

- `NativeAllocator` gains allocator-owned epoch and defrag methods only after object-table/page allocator tests are green.
- `NativeAllocatorStats` gains committed bytes, free bytes, fragmentation, object counts, page counts, quarantine bytes, double-free detections, defrag metrics, and allocation latency snapshot fields.
- `NativeObjectKind` gains any missing production kinds needed by DB roots.

Update DB memory code:

- `EntryHandle` and `ValueHandle` become typed wrappers around `NativeHandle`.
- `EntryTable` stores entry records as `NativeObjectKind.ENTRY_RECORD`.
- `NativeKeyDirectory` stores key bytes as `NativeObjectKind.KEY_BYTES` and entry handles as `EntryHandle`.
- `StringRoot` stores string bytes as `NativeObjectKind.STRING_BYTES` and uses stable `realloc`.
- `ListRoot`, `HashRoot`, `SetRoot`, `ZSetRoot`, and HLL paths store root or node objects through `ValueHandle`.
- Expire and eviction references keep entry handles and remove index references before entry reuse.

## Phase 1: Page And Size-Class Allocator

**Files:**

- Create: `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeSizeClass.java`
- Create: `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativePageClass.java`
- Create: `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeBlock.java`
- Create: `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativePageAllocator.java`
- Create: `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativePageAllocatorStats.java`
- Test: `yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisNativePageAllocatorTest.java`

- [ ] **Step 1: Write failing size-class tests**

Create `YierdisNativePageAllocatorTest` with tests named:

- `choosesSmallSizeClasses`
- `smallAllocationsNeverCrossPageBoundary`
- `smallPagesBelongToOneSizeClass`
- `freesAndReusesSmallBlocks`
- `allocatesMediumSpansForObjectsAboveSmallLimit`
- `allocatesLargeSpansForObjectsAboveOneMiB`
- `reportsCommittedUsedAndFreeBytes`

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-ffm -am -Dtest=YierdisNativePageAllocatorTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because the allocator classes do not exist.

- [ ] **Step 2: Implement size-class definitions**

`YierdisNativeSizeClass` must expose:

```java
static YierdisNativeSizeClass forSize(int requestedBytes)
int bytes()
boolean supports(int requestedBytes)
```

The supported small classes are exactly:

```text
16, 24, 32, 48, 64, 96, 128, 192, 256, 384, 512, 768,
1024, 1536, 2048, 3072, 4096, 6144, 8192, 12288, 16384, 24576, 32768
```

- [ ] **Step 3: Implement physical block allocation**

`YierdisNativePageAllocator` must:

- Use 64 KiB pages for small classes.
- Assign each small page to exactly one `YierdisNativeSizeClass`.
- Allocate medium objects `> 32 KiB` and `<= 1 MiB` as contiguous page spans.
- Allocate large objects `> 1 MiB` as dedicated spans.
- Return `YierdisNativeBlock` objects with bounds-checked byte access.
- Preserve accounting on allocation failure.
- Reuse freed small blocks before committing a new page.

- [ ] **Step 4: Run allocator tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-ffm -am -Dtest=YierdisNativePageAllocatorTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeSizeClass.java \
        yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativePageClass.java \
        yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeBlock.java \
        yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativePageAllocator.java \
        yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativePageAllocatorStats.java \
        yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisNativePageAllocatorTest.java
git commit -m "feat(memory): add native page size-class allocator"
```

## Phase 2: Native Object Metadata Table

**Files:**

- Create: `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeObjectTable.java`
- Create: `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeObjectMeta.java`
- Test: `yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisNativeObjectTableTest.java`

- [ ] **Step 1: Write failing object-table tests**

Tests must cover:

- slot allocation returns generation-bearing handles
- metadata is stored in FFM-backed memory, not `HashMap<Long, Slot>`
- free increments generation before reuse
- generation wrap retires the slot
- pinned free enters quarantine
- stale, wrong-kind, wrong-domain, double-free, and quarantined resolve attempts fail
- metadata reports owner shard id, alloc epoch, and free epoch

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-ffm -am -Dtest=YierdisNativeObjectTableTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because the object table does not exist.

- [ ] **Step 2: Implement object metadata records**

Each metadata record must preserve these fields:

```text
address
size
capacity
segmentId
pageClass
generation
domain
kind
flags
pinCount
ownerShardId
allocEpoch
freeEpoch
state
```

- [ ] **Step 3: Run object-table tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-ffm -am -Dtest=YierdisNativeObjectTableTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeObjectTable.java \
        yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeObjectMeta.java \
        yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisNativeObjectTableTest.java
git commit -m "feat(memory): add native object metadata table"
```

## Phase 3: Stable Allocator On Production Metadata

**Files:**

- Modify: `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocator.java`
- Modify: `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeAllocatorStats.java`
- Test: `yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocatorTest.java`

- [ ] **Step 1: Add failing tests that prove the implementation is no longer heap-slot-only**

Tests must prove:

- allocations reserve blocks from `YierdisNativePageAllocator`
- metadata state changes are visible through `YierdisNativeObjectTable`
- `realloc` updates metadata address while handle stays equal
- old physical blocks enter quarantine after move until no resolved view/epoch can reference them
- stats include committed bytes and fragmentation fields

- [ ] **Step 2: Rebuild allocator internals**

`YierdisStableNativeAllocator` must delegate:

- physical allocation/free/reuse to `YierdisNativePageAllocator`
- handle slot lifecycle to `YierdisNativeObjectTable`
- stale checks to object-table generation and state checks
- movement publication to object-table address updates

- [ ] **Step 3: Run API and FFM tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-api,yierdis-memory/yierdis-memory-ffm -am test
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeAllocatorStats.java \
        yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocator.java \
        yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocatorTest.java
git commit -m "feat(memory): back stable allocator with production metadata"
```

## Phase 4: DB Entry And Value Handle Migration

**Files:**

- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/EntryHandle.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/ValueHandle.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/EntryRecord.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/EntryTable.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbStorageComponents.java`
- Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/entry/EntryTableContractTest.java`
- Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/entry/ValueHandleContractTest.java`

- [ ] **Step 1: Write failing DB handle tests**

Tests must prove:

- `EntryHandle.raw()` is a production `NativeHandle` with `ENTRY_OBJECT` domain and `ENTRY_RECORD` kind.
- `ValueHandle.raw()` is a production `NativeHandle` with a kind owned by its type root.
- stale entry handles fail fast after release and slot reuse.
- wrong-kind value handles fail in the owning root before mutation.

- [ ] **Step 2: Migrate wrappers**

`EntryHandle` and `ValueHandle` must keep `raw()` for call-site compatibility and add:

```java
NativeHandle nativeHandle()
static EntryHandle fromNativeHandle(NativeHandle handle)
static ValueHandle fromNativeHandle(NativeHandle handle)
```

- [ ] **Step 3: Migrate `EntryTable`**

`EntryTable` must store 56-byte `EntryRecord` objects through `NativeAllocator` with `NativeObjectKind.ENTRY_RECORD`. It must stop using `HashMap<Long, Slot>` as the production handle-to-record map.

- [ ] **Step 4: Run DB entry tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=EntryTableContractTest,ValueHandleContractTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/EntryHandle.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/ValueHandle.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/EntryRecord.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/EntryTable.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbStorageComponents.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/entry/EntryTableContractTest.java \
        yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/entry/ValueHandleContractTest.java
git commit -m "feat(db): migrate entry handles to native handles"
```

## Phase 5: Key Bytes And String/HLL Migration

**Files:**

- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/keyspace/NativeKeyDirectory.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/StringRoot.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/YierdisHyperLogLog.java`
- Test: string, HLL, keyspace, scan, TTL, and native storage regression tests.

- [ ] **Step 1: Write failing migration tests**

Tests must prove:

- key bytes are allocated as `NativeObjectKind.KEY_BYTES`
- string payloads are allocated as `NativeObjectKind.STRING_BYTES`
- string append uses stable `realloc`
- HLL sparse/dense rewrite preserves stable handles or explicitly updates the owning entry before freeing old handles
- scan copies encoded key bytes without exposing raw allocator addresses

- [ ] **Step 2: Implement key and string migration**

Use `NativeAllocator.resolve()` for bounded reads and writes. Resolved views must close before the shard operation returns.

- [ ] **Step 3: Run focused DB tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am test
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/keyspace/NativeKeyDirectory.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/StringRoot.java \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/YierdisHyperLogLog.java \
        yierdis-db/yierdis-db-memory/src/test/java
git commit -m "feat(db): migrate keys and strings to native handles"
```

## Phase 6: Collection Roots And Handle Graph

**Files:**

- Modify: `ListRoot`, `HashRoot`, `SetRoot`, `ZSetRoot`, collection value classes, and collection tests.
- Create API or DB-internal traversal contracts for handle graph visiting and rewriting.

- [ ] **Step 1: Write failing collection migration tests**

Tests must cover list, hash, set, zset allocation, mutation, release, stale handle rejection, and memory accounting.

- [ ] **Step 2: Add handle graph traversal contract**

Every type root that stores child handles must expose:

```java
void visitChildHandles(ValueHandle root, HandleVisitor visitor)
void rewriteChildHandle(ValueHandle root, NativeHandle oldHandle, NativeHandle newHandle)
```

- [ ] **Step 3: Migrate collection roots**

Collection roots may keep compact logical encodings where appropriate, but every durable reference must be a `NativeHandle`-backed wrapper.

- [ ] **Step 4: Run DB tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am test
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry \
        yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value \
        yierdis-db/yierdis-db-memory/src/test/java
git commit -m "feat(db): migrate collection roots to native handles"
```

## Phase 7: Epoch Safety

**Files:**

- Create: `YierdisNativeEpochManager`
- Modify: allocator resolve/free/realloc/defrag paths
- Modify: keyspace scan and snapshot-like read paths
- Test: epoch, scan, and quarantine tests

- [ ] **Step 1: Write failing epoch tests**

Tests must prove:

- an active epoch delays reuse of freed slots and moved blocks
- scan reads copy or pin bounded batches before yielding
- unclosed resolved views are rejected by lifecycle checks
- pinned objects are skipped by defrag

- [ ] **Step 2: Implement epoch manager**

Epoch manager must track command, scan, snapshot, and defrag epoch ids. Allocator quarantine release must require both zero pins and no active epoch at or before the free/move epoch.

- [ ] **Step 3: Run focused tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-ffm,yierdis-db/yierdis-db-memory -am test
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign \
        yierdis-db/yierdis-db-memory/src/main/java \
        yierdis-memory/yierdis-memory-ffm/src/test/java \
        yierdis-db/yierdis-db-memory/src/test/java
git commit -m "feat(memory): add epoch-safe native reads"
```

## Phase 8: Active Defrag

**Files:**

- Create: `YierdisNativeDefragPlanner`
- Create: `YierdisNativeDefragReport`
- Modify: `NativeAllocator` and `YierdisStableNativeAllocator`
- Modify: DB maintenance integration point
- Test: allocator defrag and DB scan-under-defrag tests

- [ ] **Step 1: Write failing defrag tests**

Tests must prove:

- unpinned object movement keeps handle value unchanged
- resolved bytes are preserved after movement
- pinned objects are skipped and counted
- validation hook failure rolls back the move
- byte and time budgets stop a cycle
- old blocks are quarantined until epochs clear

- [ ] **Step 2: Implement defrag planner and report**

Planner must select only allocated, unpinned, non-moving objects whose kind permits movement and whose size fits the configured budget.

- [ ] **Step 3: Implement movement protocol**

Movement must:

1. Mark metadata `MOVING`.
2. Allocate target block.
3. Copy bytes.
4. Run validation hook.
5. Publish new metadata address on the shard owner path.
6. Mark metadata `ALLOCATED`.
7. Quarantine the old block.

- [ ] **Step 4: Run defrag tests**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-api,yierdis-memory/yierdis-memory-ffm,yierdis-db/yierdis-db-memory -am test
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add yierdis-memory/yierdis-memory-api/src/main/java \
        yierdis-memory/yierdis-memory-ffm/src/main/java \
        yierdis-memory/yierdis-memory-ffm/src/test/java \
        yierdis-db/yierdis-db-memory/src/main/java \
        yierdis-db/yierdis-db-memory/src/test/java
git commit -m "feat(memory): add active native defrag"
```

## Phase 9: Metrics And Stress Coverage

**Files:**

- Modify: `NativeAllocatorStats`
- Modify: `YierdisDbMemoryReporter`
- Add allocator fuzz/stress tests under memory and DB test modules.

- [ ] **Step 1: Write failing metric tests**

Tests must assert every required metric from the spec:

- logical used bytes
- reserved native bytes
- committed segment bytes
- free bytes by size class
- internal fragmentation
- external fragmentation
- object count by kind
- live page count by class
- free page count
- quarantine bytes
- pinned object count
- stale handle detection count
- double free detection count
- defrag moved bytes
- defrag reclaimed pages
- defrag skipped pinned objects
- realloc in-place count
- realloc moved count
- allocation latency histogram

- [ ] **Step 2: Write bounded fuzz/stress tests**

Tests must generate deterministic random sequences of allocate, free, realloc, resolve, pin, unpin, epoch, and defrag operations with forced stale-handle attempts and forced OOM.

- [ ] **Step 3: Run final verification suite**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-api,yierdis-memory/yierdis-memory-ffm,yierdis-db/yierdis-db-memory,yierdis-tests/yierdis-architecture-tests -am test
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add yierdis-memory yierdis-db yierdis-tests
git commit -m "test(memory): cover production allocator metrics and stress"
```

## Phase 10: Completion Audit

**Files:**

- Inspect all files changed by this plan.
- Modify docs only if implementation behavior differs from the spec.

- [ ] **Step 1: Check unresolved markers**

Run:

```bash
rg -n "T[B]D|T[O]DO|F[I]XME" yierdis-memory yierdis-db docs/project-docs/ffm-usage.md docs/superpowers/specs/2026-05-14-production-allocator-handle-design.md
```

Expected: no matches introduced by this work.

- [ ] **Step 2: Audit spec success criteria**

Create a checklist from the Success Criteria section of `2026-05-14-production-allocator-handle-design.md` and map each item to code files and tests. Every item must have direct artifact evidence.

- [ ] **Step 3: Run final verification**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-api,yierdis-memory/yierdis-memory-ffm,yierdis-db/yierdis-db-memory,yierdis-tests/yierdis-architecture-tests -am test
git status --short
```

Expected: Maven PASS and clean worktree.

- [ ] **Step 4: Update documentation**

Update `docs/project-docs/ffm-usage.md` to remove the current milestone wording and document the completed production allocator behavior.

- [ ] **Step 5: Commit audit documentation**

```bash
git add docs/project-docs/ffm-usage.md
git commit -m "docs: document completed production allocator"
```

## Self-Review Notes

This plan intentionally treats the current stable-handle allocator as the base, not the finish line. A passing test suite is not enough for completion unless it directly covers DB handle migration, page/size-class allocation, epoch safety, active defrag movement, and the spec success criteria audit.
