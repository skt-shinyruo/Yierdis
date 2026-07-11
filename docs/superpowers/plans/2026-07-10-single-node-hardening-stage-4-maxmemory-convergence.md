# Maxmemory Convergence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Use one physical-aware component snapshot for global and per-DB maxmemory, reclaim empty pages before eviction/OOM, and make MEMORY STATS and INFO report the same model even when enforcement is disabled.

**Architecture:** Every DB component reports the memory it owns; the DB aggregates those snapshots once, and global scope sums DB snapshots without injecting an FFM runtime total. Maxmemory reservations add only prospective growth to the same effective physical usage. Both local and global governors pressure-trim participating allocators before choosing victims and again before returning OOM.

**Tech Stack:** Java 25, Maven, JUnit 4, Stage 1 common-memory records and page trim, Stage 2 prepared reservations, Stage 3 hash metrics, existing maxmemory policies and command observability.

## Global Constraints

- Execute after Stage 3 full verification passes.
- `effectiveBytesForMaxmemory = heapEstimatedBytes + nativeMetadataCommittedBytes + nativeDataCommittedBytes` with saturating arithmetic.
- Global scope sums each DB/component snapshot exactly once; FFM runtime totals remain diagnostics, not a separate maxmemory term.
- A DB snapshot is O(1) in key/collection count and never traverses adapters or hash slots during admission.
- Per-DB scope uses the identical snapshot shape and differs only in budget ownership.
- `maxmemoryBytes=0` disables enforcement and eviction but never suppresses complete memory statistics.
- `offheap_included_in_maxmemory` is true because the accounting model includes committed native memory, regardless of governor attachment.
- Pressure trim runs before victim selection and immediately before OOM.
- `estimatedExtraBytes` is Stage 2's conservative peak physical-growth estimate; a failed prepared write must release its provisional native scope and restore the prior physical snapshot.
- Eviction is considered successful for budget purposes only when the post-delete physical snapshot decreases or trim subsequently releases committed bytes.
- Preserve all existing CLI meanings and exact Redis OOM text.
- Every Maven/Java command uses the explicit JDK 25 prefix.

---

## File Structure

Create:

- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/MemoryUsageParticipant.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/DbComponentMemoryUsage.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/PhysicalMemoryAccountingTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/MaxmemoryPageTrimTest.java`
- `yierdis-server/yierdis-server-runtime/src/test/java/yier/bubu/redis/runtime/embedded/GlobalPhysicalMemoryAccountingTest.java`
- `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/runtime/MaxmemoryPhysicalProgressTest.java`

Modify:

- `yierdis-db/yierdis-db-api/pom.xml`
- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/RuntimeDbEngine.java`
- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/MaxmemoryParticipant.java`
- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/MaxmemoryCoordinator.java`
- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/YierdisMemoryStats.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMemoryReporter.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbComponentFactory.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbStorageComponents.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/DbMemoryAccounting.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ledger/YierdisDbMemoryLedger.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbMaxmemorySupport.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbDataMaintenance.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDb.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/hash/HashTableMaintenanceRegistry.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/keyspace/NativeKeyDirectory.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/ffm/YierdisFfmExpireIndex.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/NativeCollectionRootTable.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/ListRoot.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/HashRoot.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/SetRoot.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/ZSetRoot.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/NativeListpack.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/ListValue.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/HashValue.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/SetValue.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/ZSetValue.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/value/ZSkipList.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/YierdisDbMemoryReporterTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/MemoryStatsAccountingConsistencyTest.java`
- `yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisGlobalMaxmemoryGovernor.java`
- `yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstance.java`
- `yierdis-server/yierdis-server-runtime/src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstanceObservability.java`
- `yierdis-server/yierdis-server-runtime/src/test/java/yier/bubu/redis/runtime/embedded/YierdisGlobalMaxmemoryGovernorTest.java`
- `yierdis-server/yierdis-server-runtime/src/test/java/yier/bubu/redis/runtime/embedded/DbEngineFactoryInjectionTest.java`
- `yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyServerInfoProvider.java`
- `yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/keyspace/KeyCommands.java`
- `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/runtime/YierdisInstanceTest.java`
- `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/runtime/GlobalMaxmemoryLruAcrossDbsTest.java`
- `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/MemoryStatsCommandTest.java`
- `yierdis-cli/src/test/java/yier/bubu/redis/app/client/MaxmemoryScopeTest.java`

Delete:

- `yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/MaxmemoryUsageSource.java`

## Stable Interfaces Produced By This Stage

```java
public interface MemoryUsageParticipant {
    MemoryUsageSnapshot memoryUsage();
    MemoryReclaimResult trimMemory(MemoryPressureBudget budget);
}
```

`DbComponentMemoryUsage` is an owner-thread-confined O(1) aggregator. It sums
the allocator snapshot, expiry-region snapshot, and a fixed set of top-level
retained-heap counters. Collection roots update aggregate adapter bytes when a
value adapter is created/closed or when its persistent arrays/nodes change;
active/old hash-table and maintenance-registry estimates come from Stage 3's
constant-time methods. Snapshot code never calls `forEachEntry`,
`adapterBytes`, SCAN, or any collection iterator.

`MaxmemoryParticipant` extends `MemoryUsageParticipant`; `usedBytesForMaxmemory()` remains as a compatibility default returning `memoryUsage().effectiveBytesForMaxmemory()` and is deleted only after all repository consumers migrate.

`MaxmemoryCoordinator` changes reservation admission to:

```java
void prepareWrite(MaxmemoryParticipant requester, long estimatedExtraBytes);
long nextLruClock();
```

This identifies the per-DB owner while allowing global aggregation. Stage 2 ledger passes its DB participant through a bound callback.

`YierdisMemoryStats` retains existing accessors and adds:

```java
long nativeMetadataCommittedBytes();
long nativeDataCommittedBytes();
long nativeDataLiveBytes();
long nativeReclaimableBytes();
```

The existing `offHeapUsedBytes()` becomes native metadata plus native data committed; `totalEstimatedBytes()` equals the physical effective value; `usedBytesForMaxmemory()` is identical to `totalEstimatedBytes()`; and `effectiveUsedBytesForMaxmemory()` is their saturating sum with outstanding reservations.

---

### Task 1: Aggregate DB-Owned Component Snapshots

**Interfaces:** Produces `YierdisDb.memoryUsage()` and `DbComponentMemoryUsage.snapshot()`.

- [ ] **Step 1: Add a failing physical accounting test**

```java
@Test
public void dbSnapshotCountsAllocatorAndFfmExpiryRegionsExactlyOnce() {
    try (Fixture f = fixture()) {
        f.db().bindToCurrentThread();
        f.set("key", "value");
        f.expire("key", 60_000);
        MemoryUsageSnapshot usage = f.db().memoryUsage();
        NativeAllocatorStats allocator = f.allocator().stats();
        long expiryBytes = f.expiry().nativeBytes();

        Assert.assertEquals(allocator.metadataCommittedBytes(), usage.nativeMetadataCommittedBytes());
        Assert.assertEquals(allocator.committedBytes() + expiryBytes, usage.nativeDataCommittedBytes());
        Assert.assertTrue(usage.heapEstimatedBytes() >= f.allocator().memoryUsage().heapEstimatedBytes());
        Assert.assertEquals(MemoryUsageSnapshot.addSaturating(
                usage.heapEstimatedBytes(),
                MemoryUsageSnapshot.addSaturating(usage.nativeMetadataCommittedBytes(),
                        usage.nativeDataCommittedBytes())), usage.effectiveBytesForMaxmemory());
    }
}
```

Add `snapshotUsesRetainedHeapCountersWithoutWalkingCollections`: create 10,000
Hash/Set/ZSet adapters, force active/old tables, listpack/intset growth, and
skiplist nodes, then arm test-only iteration traps on the key directory and
collection adapter directory. `db.memoryUsage()` must succeed without touching
either trap and must equal the sum of their already-maintained counters. Delete
all values, finish rehash, and assert structural heap bytes return to the empty
DB baseline. Include Stage 1 adapter-directory segments and Stage 3 registry
participant fields in the exact expected formula.

- [ ] **Step 2: Run and observe logical/native double-count ambiguity**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=PhysicalMemoryAccountingTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because DB components do not expose a common snapshot.

- [ ] **Step 3: Assign ownership once**

The shared allocator owns every object-table metadata segment, native data page, primitive page/segment directory, page/segment and FFM wrapper object, free stack, quarantine array, and defrag cursor. Consume its Stage 1 `memoryUsage()` once. Therefore key bytes, entry records, string values, and collection nodes do not add logical native lengths or allocator control structures again. `YierdisFfmExpireIndex` owns its separate FFM table regions and their wrapper estimates. Storage owns persistent Java adapters, quicklist/listpack handle arrays, intsets, skiplist nodes/level arrays, active/old hash arrays, Stage 1 adapter-directory references, root/keyspace objects, and Stage 3 maintenance participant fields. Count only retained structures, not temporary command result lists or fault-test objects, using saturating arithmetic.

- [ ] **Step 4: Implement `DbComponentMemoryUsage`**

Make every retained component expose or feed a constant-time heap counter. `NativeCollectionRootTable` maintains the sum of its adapter directory plus every live adapter's current retained heap bytes; each adapter updates that sum on allocation-free publication/retirement of listpack, intset, quicklist, skiplist, and hash arrays. The key directory, expiry index, and maintenance registry report active/old/fixed control bytes directly. `DbComponentMemoryUsage.snapshot()` adds this fixed component set to allocator `memoryUsage()` and expiry native regions; it never traverses keys, roots, adapters, or slots. Precompute counter replacements during Stage 2 preparation so the visible pointer swap and counter assignment cannot allocate. Reclaimable native bytes include allocator empty pages only; heap table capacity remains counted until shrink/retirement actually releases the old array.

- [ ] **Step 5: Run DB accounting tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=PhysicalMemoryAccountingTest,YierdisDbMemoryReporterTest,MemoryStatsAccountingConsistencyTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 6: Commit DB snapshots**

```bash
git add yierdis-db
git commit -m "refactor: report database memory by owned components"
```

Expected: PASS and the DB component-snapshot commit succeeds.

### Task 2: Make Stats A Projection Of The Snapshot

**Interfaces:** Produces the `YierdisMemoryStats` field semantics above.

- [ ] **Step 1: Add disabled-maxmemory and saturation tests**

```java
@Test
public void disabledMaxmemoryStillReportsPhysicalUsageAndIncludedFlag() {
    try (YierdisDb db = createDb(0)) {
        db.bindToCurrentThread();
        db.writes().strings().setString(bytes("k"), bytes("v"), SetMode.NORMAL, null);
        YierdisMemoryStats stats = db.memory().memoryStats();
        Assert.assertEquals(0L, stats.maxmemoryBytes());
        Assert.assertTrue(stats.offHeapIncludedInMaxmemory());
        Assert.assertTrue(stats.nativeDataCommittedBytes() > 0);
        Assert.assertEquals(stats.totalEstimatedBytes(), stats.usedBytesForMaxmemory());
    }
}
```

Use a fake component with `Long.MAX_VALUE` heap plus one native byte and assert all aggregate fields saturate rather than wrap.

- [ ] **Step 2: Run and observe the governor-dependent included flag**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory,yierdis-tests/yierdis-integration-tests -am -Dtest=YierdisDbMemoryReporterTest,MemoryStatsAccountingConsistencyTest,MemoryStatsCommandTest,YierdisInstanceTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL in the current reporter/observability model.

- [ ] **Step 3: Replace `DbMemoryAccounting.snapshot` inputs**

Accept a `MemoryUsageSnapshot`, reserved bytes, maxmemory setting, key/expiry/hash metrics, defrag/maintenance metrics, and health. Derive legacy fields from the snapshot. Remove conditional off-heap inclusion and TTL logical-byte addition. Ensure every addition uses `MemoryUsageSnapshot.addSaturating`.

- [ ] **Step 4: Update MEMORY STATS and INFO projections**

Keep existing fields and add stable names:

```text
native_metadata_committed_bytes
native_data_committed_bytes
native_data_live_bytes
native_reclaimable_bytes
```

`yierdis_offheap_included_in_maxmemory` is always `1` for this engine. Preserve RESP array structure and existing field spellings.

- [ ] **Step 5: Run command and observability tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory,yierdis-tests/yierdis-integration-tests -am -Dtest=YierdisDbMemoryReporterTest,MemoryStatsAccountingConsistencyTest,MemoryStatsCommandTest,YierdisInstanceTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 6: Commit stats convergence**

```bash
git add yierdis-db yierdis-command yierdis-server
git commit -m "fix: align memory stats with physical maxmemory usage"
```

Expected: PASS and the snapshot-projection commit succeeds.

### Task 3: Replace Global Shared-Runtime Accounting

**Interfaces:** Runtime sums `RuntimeDbEngine.memoryUsage()`; no `MaxmemoryUsageSource` or identity-dedup path remains.

- [ ] **Step 1: Add a two-DB exact aggregation test**

Use two fake participants with snapshots `(heap=10, metadata=20, data=30)` and `(heap=1, metadata=2, data=3)`. Assert global used is 66, not 66 plus any runtime diagnostic total. Add a fake shared runtime counter of 1,000 to prove it is ignored by maxmemory.

- [ ] **Step 2: Run and observe shared-source injection**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-runtime -am -Dtest=GlobalPhysicalMemoryAccountingTest,YierdisGlobalMaxmemoryGovernorTest,DbEngineFactoryInjectionTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because `YierdisInstance` constructs `MaxmemoryUsageSource[]{ sharedOffHeapUsedBytes }`.

- [ ] **Step 3: Remove global shared usage APIs**

Delete `MaxmemoryUsageSource`, `globalSharedOffHeapUsageIdentity()`, `globalSharedOffHeapUsedBytes()`, `sharedOffHeapUsedBytes`, and the governor usage-source constructor. Update runtime test doubles to return component snapshots. `YierdisInstanceObservability.memoryStats()` aggregates the same snapshots for both scopes; only maxmemory budget allocation differs.

- [ ] **Step 4: Run runtime tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-runtime,yierdis-tests/yierdis-integration-tests -am -Dtest=GlobalPhysicalMemoryAccountingTest,YierdisGlobalMaxmemoryGovernorTest,DbEngineFactoryInjectionTest,YierdisInstanceTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 5: Commit global convergence**

```bash
git add yierdis-db yierdis-server
git commit -m "refactor: sum owned memory snapshots globally"
```

Expected: PASS and the global physical-accounting commit succeeds.

### Task 4: Trim Pages Before Local Eviction And OOM

**Interfaces:** Local governor calls `participant.trimMemory(MemoryPressureBudget)` using the remaining eviction deadline and an inspected-page limit derived from `maxmemorySamples` with a minimum of 16.

- [ ] **Step 1: Add a trim-without-eviction test**

Fill enough small pages, delete all values so pages are empty but one warm page remains, choose `NOEVICTION`, and request growth that fits only after trim. Assert admission succeeds, no key eviction occurs, committed bytes fall by the trim result, and `nativeReclaimableBytes` reaches zero.

Also start from an empty DB and calculate a first SET's `MutationMemoryEstimator` value, including its first metadata segment and small page. Set maxmemory to one byte below `before.effectiveBytesForMaxmemory() + estimate`; assert exact OOM and the complete snapshot returns to `before`. Set the limit to that sum and assert the same SET succeeds without exceeding the admitted peak.

- [ ] **Step 2: Run and observe OOM before physical trim**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=MaxmemoryPageTrimTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL.

- [ ] **Step 3: Implement the local pressure order**

For a positive peak-physical reservation:

```text
snapshot + reservation <= limit -> admit
cleanup expired -> resnapshot
pressure trim -> resnapshot
if eviction policy: select one victim -> delete -> pressure trim -> repeat
final pressure trim -> resnapshot
still above limit -> OOM
```

Never call trim for `maxmemoryBytes=0`. A zero-growth write remains allowed even if currently above limit, preserving existing semantics.

After invisible preparation, assert `NativeAllocationScope.growth().effectiveBytes()` plus all prepared non-allocator FFM/heap growth does not exceed the admitted estimate. An underestimate is an internal invariant failure that aborts before visibility and degrades the DB; it is never silently admitted beyond maxmemory.

- [ ] **Step 4: Run local maxmemory and page tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory,yierdis-tests/yierdis-integration-tests -am -Dtest=MaxmemoryPageTrimTest,StringDirectOpsTest,MaxmemoryEvictionTest,TtlMaxmemoryTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 5: Commit local trim**

```bash
git add yierdis-db
git commit -m "fix: trim native pages before local eviction"
```

Expected: PASS and the local trim-before-eviction commit succeeds.

### Task 5: Trim Across Databases In The Global Governor

**Interfaces:** `prepareWrite(requester, estimatedExtraBytes)` sums all participants plus requester's outstanding reservation and rotates trim work across DBs.

- [ ] **Step 1: Add cross-DB trim and saturation tests**

One DB exposes 64 KiB reclaimable committed memory and no keys; another requests growth. Assert the global governor trims the first DB and admits the second without eviction. Add `Long.MAX_VALUE` participant usage plus growth one and assert OOM rather than overflow admission.

- [ ] **Step 2: Run and observe no global trim path**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-runtime -am -Dtest=YierdisGlobalMaxmemoryGovernorTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL.

- [ ] **Step 3: Update the coordinator and ledger binding**

The DB ledger calls `coordinator.prepareWrite(publicDbParticipant, estimatedExtraBytes)`. Global trim rotates from the last participant index, respects remaining elapsed-time and page-inspection budgets, and resnapshots after each trim. Eviction candidate selection still spans all DBs and uses the global LRU clock.

- [ ] **Step 4: Run governor and cross-DB LRU tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-server/yierdis-server-runtime,yierdis-tests/yierdis-integration-tests -am -Dtest=YierdisGlobalMaxmemoryGovernorTest,GlobalMaxmemoryLruAcrossDbsTest,MaxmemoryPhysicalProgressTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 5: Commit global trim**

```bash
git add yierdis-db yierdis-server yierdis-tests/yierdis-integration-tests
git commit -m "fix: reclaim native pages before global eviction"
```

Expected: PASS and the global pressure-trim commit succeeds.

### Task 6: Make Eviction Demonstrate Physical Progress

**Interfaces:** Both governors compare `before.effectiveBytesForMaxmemory()` and `after.effectiveBytesForMaxmemory()` and expose stalled-attempt metrics.

- [ ] **Step 1: Add retained-page progress tests**

Create multiple small keys sharing a page. Evict one key and assert logical live bytes decrease while committed bytes may not; the governor must continue until trim closes an empty page or the budget expires. Assert it never loops past key-count plus fixed retry bounds when no candidate changes physical usage.

- [ ] **Step 2: Run and observe logical-success premature stop**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-integration-tests -am -Dtest=MaxmemoryPhysicalProgressTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL in current `evictUntilUnder` loops.

- [ ] **Step 3: Track progress and bounded stalls**

Count a victim deletion separately from physical reclaimed bytes. Continue resnapshotting and trimming. Stop with OOM if a full candidate pass yields no physical progress, the time budget is exhausted, or no candidates remain. Expose deleted victims, physically reclaimed bytes, trimmed pages, and stalled attempts in memory/maintenance metrics.

- [ ] **Step 4: Run maxmemory suites**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory,yierdis-server/yierdis-server-runtime,yierdis-tests/yierdis-integration-tests -am '-Dtest=*Maxmemory*Test,GlobalMaxmemoryLruAcrossDbsTest,YierdisInstanceTest' -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 5: Commit progress semantics**

```bash
git add yierdis-db yierdis-server yierdis-tests
git commit -m "fix: require physical progress during maxmemory eviction"
```

Expected: PASS and the physical-progress eviction commit succeeds.

### Task 7: Fix The Two Existing `MaxmemoryScopeTest` Failures

**Interfaces:** This task changes production behavior only; do not weaken the existing test assertions.

- [ ] **Step 1: Run the two tests before any test edit**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-cli -am -Dtest=MaxmemoryScopeTest#globalMemoryStatsIncludesDefaultFfmNativeMemoryOnce+globalScopeEvictsAcrossDbsUsingLru -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: before Stage 4 fixes, FAIL on both methods, reproducing the known baseline.

- [ ] **Step 2: Verify the tests pass unchanged**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-cli -am -Dtest=MaxmemoryScopeTest#globalMemoryStatsIncludesDefaultFfmNativeMemoryOnce+globalScopeEvictsAcrossDbsUsingLru -Dsurefire.failIfNoSpecifiedTests=false test
git diff -- yierdis-cli/src/test/java/yier/bubu/redis/app/client/MaxmemoryScopeTest.java
```

Expected: after Tasks 1-6, PASS. The diff must be empty except imports or accessor updates mechanically required by the new stats record, with all behavioral assertions intact.

- [ ] **Step 3: Run all scope tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-cli -am -Dtest=MaxmemoryScopeTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 4: Commit the regression fix**

```bash
git add yierdis-cli yierdis-db yierdis-server
git commit -m "fix: converge global maxmemory scope behavior"
```

Expected: PASS and the focused maxmemory regression commit succeeds.

### Task 8: Verify Stage 4 End To End

**Interfaces:** Completes the maxmemory acceptance boundary consumed by ingress and commit-stream stages.

- [ ] **Step 1: Run focused memory and maxmemory reactors**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory,yierdis-db/yierdis-db-memory,yierdis-server/yierdis-server-runtime,yierdis-tests/yierdis-integration-tests -am test
```

Expected: PASS.

- [ ] **Step 2: Run architecture and the full suite**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-architecture-tests -am test
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn test
```

Expected: PASS, including both formerly failing `MaxmemoryScopeTest` methods.

- [ ] **Step 3: Commit Stage 4 acceptance**

```bash
git add yierdis-db yierdis-server yierdis-command yierdis-cli yierdis-tests
git commit -m "test: verify physical maxmemory convergence"
```
