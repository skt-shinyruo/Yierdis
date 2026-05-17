# DB Native Allocator List Quicklist Node Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add allocator-backed native records for FFM list quicklist internal nodes while preserving current list behavior and root-handle semantics.

**Architecture:** Keep `NativeObjectKind.LIST_NODE` as the collection root record used by `ListRoot` and `NativeCollectionRootTable`. Add a distinct `LIST_QUICKLIST_NODE` object kind and let `ListValue` own one small native metadata record for each FFM quicklist node; payload bytes remain owned by `YierdisFfmListpack` and `YierdisFfmBlobStore`. Graph traversal is a bounded decision: add only a small list-node hook if it fits cleanly, otherwise keep graph traversal root-only in this split and document/test that boundary.

**Tech Stack:** Java 25, Maven, JUnit 4, JDK FFM-backed memory runtime, `NativeAllocator`, `YierdisStableNativeAllocator`.

---

## Scope

This plan implements only Track 2.1 from `docs/superpowers/specs/2026-05-16-db-native-allocator-list-quicklist-node-design.md`.

In scope:

- `LIST_QUICKLIST_NODE` object kind and separate stats/count accounting.
- Native metadata records for FFM quicklist nodes in `ListValue`.
- Allocation, free, stale-handle, release, and defrag-style stable-handle tests focused on lists.
- A bounded graph traversal decision for list internals.

Explicitly out of scope:

- hash, set, and zset nativeization.
- key byte migration.
- DB defrag maintenance scheduling or policy.
- scan/snapshot epoch policy.
- benchmarks or benchmark acceptance gates.
- migrating listpack/blob payload bytes into the stable allocator.
- changing existing `LIST_NODE` root record behavior.

All implementation tasks must use fresh subagents per task when using subagent-driven execution. The main agent must review the subagent result, run the task verification commands, and commit only after each task is reviewed and green. This plan creation task does not make those commits.

All Maven commands must use JDK 25:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn ...
```

## File Map

- Modify: `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeObjectKind.java`
  - Add `LIST_QUICKLIST_NODE` as a distinct list-internal kind. Do not replace or alias `LIST_NODE`.
- Modify: `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeObjectKindCounts.java`
  - Add `listQuicklistNodeObjects` and expose it through `count(NativeObjectKind.LIST_QUICKLIST_NODE)`.
- Modify: `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocator.java`
  - Count `LIST_QUICKLIST_NODE` separately in allocator stats.
- Test: `yierdis-memory/yierdis-memory-api/src/test/java/yier/bubu/redis/memory/api/NativeAllocatorContractTest.java`
  - Cover the new stats record field and `objectCount` mapping.
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/ListRoot.java`
  - Pass the root native handle and allocator into FFM-backed `ListValue` instances.
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/ListValue.java`
  - Add native quicklist node metadata records and free-order handling.
- Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/value/ListValueTest.java`
  - Add list-focused quicklist allocation/free/stale/defrag behavior tests where package-private hooks are enough.
- Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/entry/ListRootTest.java`
  - Add root release tests that prove internal nodes are closed before the root `LIST_NODE` record is freed.
- Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/entry/CollectionRootTest.java` or a dedicated list root test
  - Keep root-kind coverage for `LIST_NODE` versus `LIST_QUICKLIST_NODE` without broadening hash/set/zset scope.
- Optional bounded decision: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbNativeHandleGraph.java`
  - Add a minimal list-internal traversal hook only if it stays small and does not expose payload internals.
- Optional bounded test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/YierdisDbNativeHandleGraphTest.java`
  - Test graph traversal only if the hook is added; otherwise assert/document root-only traversal for this split.

---

### Task 1: Native Kind And Stats API

**Files:**
- Modify: `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeObjectKind.java`
- Modify: `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeObjectKindCounts.java`
- Modify: `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocator.java`
- Test: `yierdis-memory/yierdis-memory-api/src/test/java/yier/bubu/redis/memory/api/NativeAllocatorContractTest.java`

- [x] **Step 1: Write the failing API test first**
  - Extend `NativeAllocatorContractTest.statsRecordExposesProductionAllocatorCounters` or add a focused test.
  - Assert `NativeObjectKindCounts` has a separate `listQuicklistNodeObjects` value.
  - Assert `stats.objectCount(NativeObjectKind.LIST_NODE)` still returns the root count.
  - Assert `stats.objectCount(NativeObjectKind.LIST_QUICKLIST_NODE)` returns only the quicklist internal count.
  - Do not alter hash/set/zset expectations except for constructor argument position updates caused by the new record field.

- [x] **Step 2: Run the focused failing API test**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-api -Dtest=NativeAllocatorContractTest test
```

Expected: FAIL before implementation because `LIST_QUICKLIST_NODE` and/or `listQuicklistNodeObjects` do not exist.

- [x] **Step 3: Implement the object kind and stats record changes**
  - Add `LIST_QUICKLIST_NODE` to `NativeObjectKind`.
  - Use the `TYPE_ROOT` domain unless the implementation first introduces a narrower compatible domain; do not reuse the `LIST_NODE` code/domain tuple.
  - Add `listQuicklistNodeObjects` to `NativeObjectKindCounts`.
  - Update `NativeObjectKindCounts.empty()`, compact constructor validation, and `count(...)`.
  - Update all direct `NativeObjectKindCounts` constructor call sites.

- [x] **Step 4: Implement allocator-side counting**
  - In `YierdisStableNativeAllocator.objectKindCounts()`, add a separate local counter for `LIST_QUICKLIST_NODE`.
  - Increment it only when the live allocation kind is `LIST_QUICKLIST_NODE`.
  - Keep `LIST_NODE` counting unchanged and root-only.

- [x] **Step 5: Run the API and allocator stats checks**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-api,yierdis-memory/yierdis-memory-ffm -am -Dtest=NativeAllocatorContractTest,YierdisStableNativeAllocatorTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [x] **Step 6: Main-agent review and commit**
  - Main agent reviews the diff for accidental `LIST_NODE` behavior changes.
  - Commit only this task's changes if execution mode includes commits.

### Task 2: Native Quicklist Node Records In `ListRoot` And `ListValue`

**Files:**
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/ListRoot.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/ListValue.java`
- Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/value/ListValueTest.java`
- Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/entry/ListRootTest.java`

- [x] **Step 1: Write failing list allocation tests first**
  - Add a focused test proving packed FFM mode does not allocate `LIST_QUICKLIST_NODE`.
  - Add a focused test proving conversion to FFM quicklist mode allocates one `LIST_QUICKLIST_NODE` per `FfmListNode`.
  - Add a focused test proving adding a new FFM quicklist node increments `LIST_QUICKLIST_NODE`, not `LIST_NODE`.
  - Prefer existing reflection helpers in `ListValueTest` for node count only; expose package-private diagnostics only if reflection becomes brittle.

- [x] **Step 2: Run the focused failing list tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=ListValueTest,ListRootTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL before implementation because FFM quicklist nodes do not allocate native records.

- [x] **Step 3: Wire allocator and root handle into FFM list values**
  - Update `ListRoot` so `NativeCollectionRootTable.create(...)` can create a `ListValue` with the just-allocated root handle.
  - Keep `LIST_NODE` root allocation in `NativeCollectionRootTable` unchanged.
  - If needed, extend `NativeCollectionRootTable` with a handle-aware adapter factory instead of leaking root table internals.
  - Keep the no-runtime `ListValue()` path unchanged for heap-only list behavior.

- [x] **Step 4: Add the native node record layout in `ListValue`**
  - Use a fixed-size 48-byte little-endian record matching the spec:
    - `ownerRootRawHandle` at offset `0`.
    - `prevNodeRawHandle` at offset `8`, or `0`.
    - `nextNodeRawHandle` at offset `16`, or `0`.
    - `payloadRefRaw` at offset `24`, transitional diagnostic value or `0`.
    - `entryCount` at offset `32`.
    - `encodedBytes` at offset `36`.
    - `flags` at offset `40`.
    - `reserved` at offset `44`.
  - Store only metadata and links here. Do not move or count listpack/blob payload bytes as `LIST_QUICKLIST_NODE` bytes.

- [x] **Step 5: Allocate one native record per FFM quicklist node**
  - Update `FfmListNode` to cache its `NativeHandle` and payload `YierdisFfmListpack`.
  - Allocate `NativeObjectKind.LIST_QUICKLIST_NODE` when creating an FFM quicklist node.
  - If native allocation succeeds but payload creation fails, free the node record before rethrowing.
  - Write owner root handle and current metadata after node creation and after mutations.
  - Resolve the native record per operation; do not cache `NativeObjectView`, spans, physical addresses, or payload addresses.

- [x] **Step 6: Maintain links and metadata during list mutations**
  - Update previous/next raw handle fields whenever nodes are added, removed, restored after a failed merge, or merged.
  - Update `entryCount` and `encodedBytes` after `addFirst`, `addLast`, `removeFirst`, `removeLast`, and `appendAll`.
  - Validate owner root as `LIST_NODE` before trusting an internal node when practical within current locking.
  - Treat Java `ArrayDeque<FfmListNode>` as an adapter index only; allocator liveness remains authoritative.

- [x] **Step 7: Run focused list tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=ListValueTest,ListRootTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [x] **Step 8: Main-agent review and commit**
  - Main agent reviews the diff for root-kind preservation, payload ownership boundaries, and stale `NativeObjectView` caching.
  - Commit only this task's changes if execution mode includes commits.

### Task 3: List-Focused Free, Stale, Defrag, And Release Tests

**Files:**
- Modify/Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/value/ListValueTest.java`
- Modify/Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/entry/ListRootTest.java`
- Modify/Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/entry/CollectionRootTest.java` or a dedicated list root test
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/ListValue.java`
- Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/ListRoot.java`

- [x] **Step 1: Write failing free-order and release tests first**
  - Test popping an element that empties an FFM quicklist node frees exactly that node's `LIST_QUICKLIST_NODE` record.
  - Test first-node and last-node merge cases free the discarded node record and preserve element order.
  - Test `ListRoot.release(handle)` frees all internal `LIST_QUICKLIST_NODE` records before freeing the root `LIST_NODE` record.
  - Test close/clear paths leave both `LIST_NODE` and `LIST_QUICKLIST_NODE` counts at zero.

- [x] **Step 2: Write failing stale-handle and slot-reuse tests**
  - Capture an internal quicklist node handle through a package-private diagnostic method or test-only reflection.
  - Remove the node and assert allocator resolution fails with stale-handle semantics.
  - Allocate another node after removal and assert the old handle cannot observe the new node.
  - Keep tests list-focused; do not add hash/set/zset coverage in this task.

- [x] **Step 3: Write failing defrag-style movement test**
  - Create an FFM quicklist with at least one native node record.
  - Capture the internal node handle.
  - Call `NativeAllocator.defragOne(handle, maxMoveBytes)` with enough move budget.
  - Assert the handle raw value is still valid and list operations still read/write correctly after resolving the moved record again.
  - Assert payload bytes are not treated as allocator objects in this split.

- [x] **Step 4: Run the focused failing tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=ListValueTest,ListRootTest,CollectionRootTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL until free order, stale handling, and defrag-safe resolution are complete.

- [x] **Step 5: Implement free-order hardening**
  - When removing a node, unlink it from the Java quicklist structure first.
  - Update neighboring native link fields while they are still live.
  - Close/free the node payload through `YierdisFfmListpack.close()`.
  - Clear adapter fields that reference the payload and native handle.
  - Free the `LIST_QUICKLIST_NODE` record through `NativeAllocator.free(...)`.
  - If payload close fails during list release, still attempt to free the native node record and attach suppressed failures consistently with `NativeCollectionRootTable.release(...)`.

- [x] **Step 6: Implement stale-handle authority checks**
  - Resolve the internal node handle before reading or mutating native metadata.
  - Do not allow an old `FfmListNode` adapter reference to make a freed node usable.
  - Ensure wrong-generation, freed, wrong-domain, and wrong-kind internal handles fail through allocator stale-handle validation.

- [x] **Step 7: Run focused and regression list tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=ListValueTest,ListRootTest,CollectionRootTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [x] **Step 8: Main-agent review and commit**
  - Main agent reviews the diff for double-free risks, suppressed failure behavior, and accidental payload accounting.
  - Commit only this task's changes if execution mode includes commits.

### Task 4: Graph Traversal Decision And Minimal Hook

**Files:**
- Optional Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbNativeHandleGraph.java`
- Optional Test: `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/YierdisDbNativeHandleGraphTest.java`
- Optional Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/ListRoot.java`
- Optional Modify: `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/ListValue.java`

- [x] **Step 1: Make the traversal decision before editing graph code**
  - Add the traversal hook only if `YierdisDbNativeHandleGraph` can reach live list internal node handles with a small, package-private API that does not expose payload bytes, memory segments, physical addresses, or blob offsets.
  - If the hook requires broad DB lifecycle changes, keep graph traversal root-only for this split.
  - Record the decision in test names/comments only where necessary; do not edit the parent roadmap or spec.

- [ ] **Step 2A: If adding the minimal hook, write the failing graph test first**
  - Add a `LIST_QUICKLIST_NODE` role to `YierdisDbNativeHandleGraph.Role`.
  - Create a DB list large enough to enter FFM quicklist mode with multiple quicklist nodes.
  - Assert traversal shape includes `ENTRY_RECORD -> LIST_NODE root -> LIST_QUICKLIST_NODE internal nodes`.
  - Assert graph traversal does not visit FFM blob/listpack payload bytes as allocator objects.

- [ ] **Step 3A: Implement the minimal hook**
  - Add a package-private method from `ListRoot` and/or `ListValue` that enumerates internal quicklist node `NativeHandle`s for a validated live list root.
  - In `YierdisDbNativeHandleGraph`, after visiting a live list collection root, visit each resolved internal node with role `LIST_QUICKLIST_NODE`.
  - Keep hash/set/zset traversal unchanged.

- [ ] **Step 4A: Run the graph hook test**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=YierdisDbNativeHandleGraphTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS if the hook was added.

- [x] **Step 2B: If deferring traversal, write explicit root-only tests/docs in tests**
  - Keep `YierdisDbNativeHandleGraph.Role` unchanged.
  - Add or adjust `YierdisDbNativeHandleGraphTest` to assert list roots are still visited as `COLLECTION_ROOT`.
  - Add a concise test comment explaining that internal quicklist node traversal is intentionally deferred because this split only added allocator-visible internal records.
  - Ensure list-focused tests from Task 3 cover internal handle allocation, liveness, and release despite graph deferral.

- [x] **Step 3B: Run the root-only graph test**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=YierdisDbNativeHandleGraphTest,ListValueTest,ListRootTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS if traversal is deferred.

- [x] **Step 5: Main-agent review and commit**
  - Main agent verifies the graph decision stayed bounded.
  - Commit only this task's changes if execution mode includes commits.

### Task 5: Final Verification

**Files:**
- No source changes expected.

- [ ] **Step 1: Run memory API focused tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-api -Dtest=NativeAllocatorContractTest test
```

Expected: PASS.

- [ ] **Step 2: Run memory FFM allocator tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-ffm -am -Dtest=YierdisStableNativeAllocatorTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 3: Run DB list and graph focused tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=ListValueTest,ListRootTest,CollectionRootTest,YierdisDbNativeHandleGraphTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 4: Run a broader DB memory module verification**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am test
```

Expected: PASS.

- [ ] **Step 5: Main-agent final review**
  - Confirm the diff does not edit source/tests outside this plan's scope.
  - Confirm no hash/set/zset internals, key bytes, DB defrag maintenance policy, scan/snapshot epoch policy, benchmarks, parent roadmap, spec, or `yierdis.md` were changed.
  - Confirm `LIST_NODE` remains root-only and `LIST_QUICKLIST_NODE` remains internal quicklist-node-only.
  - Confirm allocator stats do not double count payload bytes.
  - Confirm each implementation task was done by a fresh subagent and reviewed/tested/committed by the main agent between tasks.
