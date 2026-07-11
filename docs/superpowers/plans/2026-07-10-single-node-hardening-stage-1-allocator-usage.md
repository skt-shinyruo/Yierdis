# Allocator And Usage Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make native metadata lazy, remove production allocator and root-adapter heap mirrors, eliminate append-only page tracking, reclaim empty data pages, and expose authoritative heap/native memory snapshots without changing stable-handle behavior.

**Architecture:** Add a neutral common-memory contract below both allocator and storage APIs. Replace the monolithic object table with 4096-slot FFM segments and primitive slot bookkeeping, make page locations resolvable through a reusable-id segmented page directory and intrusive live/empty lists, and replace boxed collection-root adapter maps with a lazy slot directory. Report Java control structures together with native regions. Allocation estimation simulates both heap-directory growth and native segment/page commitments. Production access is owner-thread confined; an explicit synchronized adapter remains available for standalone concurrent tools.

**Tech Stack:** Java 25, Maven, JUnit 4, Java FFM API, existing `NativeHandle` encoding, existing `YierdisFfmMemoryRuntime`, explicit JDK 25 command prefix.

## Global Constraints

- Preserve RESP behavior, command semantics, all existing CLI option names, and the 40-bit native-handle slot encoding.
- Retain one command-owner thread; this stage does not introduce DB sharding or parallel mutation.
- `nativeSlotCapacity` is an admission ceiling, never a startup allocation size.
- An empty object table commits zero metadata bytes, independent of `nativeSlotCapacity`.
- A metadata segment contains exactly 4096 slots and remains committed after first use so generations and retired-slot state survive reuse.
- Normal allocation retains at most one empty warm page per size class; pressure trim closes every empty data page.
- Page/span indexes grow with peak concurrent pages, not lifetime allocation count; closed page ids are reused only after all live/pin/quarantine references are gone.
- The FFM runtime tracks live regions with counters, not a per-region `Set`, and collection roots use no boxed handle map.
- Allocator-owned segment objects, page objects, primitive stacks, directories, quarantine storage, and defrag state contribute to `heapEstimatedBytes` and to pre-allocation growth estimates.
- Every Maven, Java, script, smoke, or benchmark command uses `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH`.
- Every production edit follows red-green-refactor: add one focused failing test, observe the expected failure, make the smallest implementation, then rerun the focused reactor.

---

## File Structure

Create:

- `yierdis-common/yierdis-common-memory/pom.xml`
- `yierdis-common/yierdis-common-memory/src/main/java/yier/bubu/redis/common/memory/MemoryUsageSnapshot.java`
- `yierdis-common/yierdis-common-memory/src/main/java/yier/bubu/redis/common/memory/MemoryPressureBudget.java`
- `yierdis-common/yierdis-common-memory/src/main/java/yier/bubu/redis/common/memory/MemoryReclaimResult.java`
- `yierdis-common/yierdis-common-memory/src/test/java/yier/bubu/redis/common/memory/MemoryUsageSnapshotTest.java`
- `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeAllocationGrowth.java`
- `yierdis-memory/yierdis-memory-api/src/test/java/yier/bubu/redis/memory/api/NativeAllocationGrowthTest.java`
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeObjectSegment.java`
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeObjectTableStats.java`
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativePageDirectory.java`
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisAllocatorThreadGuard.java`
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/SynchronizedNativeAllocator.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/YierdisNativeAdapterDirectory.java`
- `yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/SynchronizedNativeAllocatorTest.java`
- `yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/runtime/EmptyDatabaseFootprintTest.java`

Modify:

- `pom.xml`
- `yierdis-common/pom.xml`
- `yierdis-memory/yierdis-memory-api/pom.xml`
- `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeAllocator.java`
- `yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeAllocatorStats.java`
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativeObjectTable.java`
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisFfmMemoryRuntime.java`
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativePageAllocator.java`
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisNativePageAllocatorStats.java`
- `yierdis-memory/yierdis-memory-ffm/src/main/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocator.java`
- `yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisNativeObjectTableTest.java`
- `yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisNativePageAllocatorTest.java`
- `yierdis-memory/yierdis-memory-ffm/src/test/java/yier/bubu/redis/memory/foreign/YierdisStableNativeAllocatorTest.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/StringRoot.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/EntryTable.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/entry/NativeCollectionRootTable.java`
- `yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDbRuntimeState.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/entry/StringRootTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/entry/EntryTableContractTest.java`
- `yierdis-db/yierdis-db-memory/src/test/java/yier/bubu/redis/storage/memory/internal/entry/CollectionRootTest.java`
- `yierdis-tests/yierdis-architecture-tests/src/test/resources/architecture-policy.yml`
- `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`

## Stable Interfaces Produced By This Stage

```java
public record MemoryUsageSnapshot(
        long heapEstimatedBytes,
        long nativeMetadataCommittedBytes,
        long nativeDataCommittedBytes,
        long nativeDataLiveBytes,
        long nativeReclaimableBytes
) {
    public long effectiveBytesForMaxmemory() {
        return addSaturating(addSaturating(heapEstimatedBytes, nativeMetadataCommittedBytes),
                nativeDataCommittedBytes);
    }

    public static MemoryUsageSnapshot zero();
    public MemoryUsageSnapshot plus(MemoryUsageSnapshot other);
    public static long addSaturating(long left, long right);
}
```

```java
public record MemoryPressureBudget(long maxInspectedUnits, long maxReclaimedBytes, long timeLimitNanos) {
    public static MemoryPressureBudget unlimited();
}

public record MemoryReclaimResult(
        long inspectedUnits,
        long reclaimedUnits,
        long reclaimedBytes,
        StopReason stopReason
) {
    public enum StopReason { COMPLETE, INSPECTION_LIMIT, BYTE_LIMIT, TIME_LIMIT }
    public static MemoryReclaimResult empty();
}
```

Allocator planning uses one non-negative growth value that separates the three
components included by maxmemory:

```java
public record NativeAllocationGrowth(
        long heapEstimatedBytes,
        long nativeMetadataCommittedBytes,
        long nativeDataCommittedBytes
) {
    public long effectiveBytes();
    public NativeAllocationGrowth plus(NativeAllocationGrowth other);
    public static NativeAllocationGrowth zero();
}
```

`NativeAllocator` gains these methods; later stages consume these exact signatures:

```java
void bindToCurrentThread();
MemoryUsageSnapshot memoryUsage();
MemoryReclaimResult trimEmptyPages(MemoryPressureBudget budget);
NativeAllocationGrowth estimateAdditionalGrowth(int... requestedBytes);
```

`NativeAllocatorStats` appends, in this order, `metadataCommittedBytes`, `activeMetadataSegments`, `freeSlots`, `retiredSlots`, and `peakLiveSlots`. Preserve the existing shorter constructors by delegating the new fields to zero.

---

### Task 1: Add Neutral Saturating Memory Contracts

**Interfaces:** Produces the three common-memory records above for every later task and stage.

- [ ] **Step 1: Write the failing common-memory unit test**

Create `MemoryUsageSnapshotTest` with these tests:

```java
@Test
public void effectiveBytesUsePhysicalCommittedMemory() {
    MemoryUsageSnapshot usage = new MemoryUsageSnapshot(7, 11, 13, 5, 8);
    Assert.assertEquals(31L, usage.effectiveBytesForMaxmemory());
}

@Test
public void aggregationSaturatesInsteadOfWrapping() {
    MemoryUsageSnapshot left = new MemoryUsageSnapshot(Long.MAX_VALUE, 0, 0, 0, 0);
    MemoryUsageSnapshot right = new MemoryUsageSnapshot(1, 2, 3, 4, 5);
    MemoryUsageSnapshot total = left.plus(right);
    Assert.assertEquals(Long.MAX_VALUE, total.heapEstimatedBytes());
    Assert.assertEquals(Long.MAX_VALUE, total.effectiveBytesForMaxmemory());
}

@Test(expected = IllegalArgumentException.class)
public void negativeComponentsAreRejected() {
    new MemoryUsageSnapshot(-1, 0, 0, 0, 0);
}
```

- [ ] **Step 2: Run the test and verify the module is missing**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-common/yierdis-common-memory -am -Dtest=MemoryUsageSnapshotTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because `yierdis-common-memory` and `MemoryUsageSnapshot` do not exist.

- [ ] **Step 3: Add the module and exact record implementations**

Add `yierdis-common-memory` to `yierdis-common/pom.xml`, dependency management to the root `pom.xml`, and JUnit as a test dependency. Implement canonical-constructor validation, derived `effectiveBytesForMaxmemory()`, `zero()`, and component-wise saturating `plus()`. There is no constructor or stored field that accepts an independent effective value. `MemoryPressureBudget` rejects negative limits; zero means no work, while `unlimited()` uses `Long.MAX_VALUE` for all limits. `MemoryReclaimResult` rejects negative counters and requires a non-null stop reason.

- [ ] **Step 4: Run the focused test**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-common/yierdis-common-memory -am -Dtest=MemoryUsageSnapshotTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS with 3 tests.

- [ ] **Step 5: Commit the neutral contracts**

```bash
git add pom.xml yierdis-common yierdis-tests/yierdis-architecture-tests/src/test/resources/architecture-policy.yml yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java
git commit -m "feat: add neutral memory usage contracts"
```

Expected: PASS and the neutral memory contracts commit succeeds.

### Task 2: Make Object Metadata Lazy And Segmented

**Interfaces:** Consumes `MemoryUsageSnapshot`; produces `YierdisNativeObjectTable.stats()` returning `YierdisNativeObjectTableStats` and preserves all current allocate/resolve/free/pin/move signatures.

- [ ] **Step 1: Add failing footprint and segment-boundary tests**

Add to `YierdisNativeObjectTableTest`:

```java
@Test
public void emptyTableCommitsNoMetadataRegardlessOfMaximumSlots() {
    for (int maxSlots : new int[]{1, 4_096, 262_144}) {
        try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("lazy-table-" + maxSlots);
             YierdisNativeObjectTable table = new YierdisNativeObjectTable(runtime, maxSlots, 0)) {
            Assert.assertEquals(0L, runtime.usedBytes());
            Assert.assertEquals(0L, table.stats().metadataCommittedBytes());
            Assert.assertEquals(0, table.stats().activeSegments());
        }
    }
}

@Test
public void allocationCrossing4096SlotsCommitsExactlyTwoSegments() {
    try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("segment-boundary");
         YierdisNativeObjectTable table = new YierdisNativeObjectTable(runtime, 4_097, 0)) {
        List<NativeHandle> handles = new ArrayList<>();
        for (int i = 0; i < 4_097; i++) {
            handles.add(table.allocate(NativeObjectKind.STRING_BYTES, 1, 1, i + 1L, 1, 0));
        }
        Assert.assertEquals(2, table.stats().activeSegments());
        Assert.assertEquals(4_097L, table.stats().liveSlots());
        Assert.assertEquals(2L * 4_096L * YierdisNativeObjectTable.META_BYTES,
                table.stats().metadataCommittedBytes());
    }
}
```

- [ ] **Step 2: Verify the eager-allocation regression**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-ffm -am -Dtest=YierdisNativeObjectTableTest#emptyTableCommitsNoMetadataRegardlessOfMaximumSlots+allocationCrossing4096SlotsCommitsExactlyTwoSegments -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because construction commits `maxSlots * 72` bytes and `stats()` is absent.

- [ ] **Step 3: Implement `YierdisNativeObjectSegment`**

Use one FFM region of `4096 * META_BYTES`, an `int[4096]` free stack, and a `long[64]` retired bitmap. Initialize only the valid slots in the last partial segment. Expose allocation/release and metadata field access by segment-local offset; do not use `Integer`, `ArrayDeque<Integer>`, or `boolean[maxSlots]`. Map `slotId` with:

```java
int zeroBased = Math.toIntExact(slotId - 1L);
int segmentIndex = zeroBased >>> 12;
int segmentOffset = zeroBased & 0x0fff;
```

- [ ] **Step 4: Replace the monolithic table**

Keep `YierdisNativeObjectTable.META_BYTES = 72` and every state transition. Allocate a segment only when no committed segment has a free non-retired slot. Maintain `liveSlots`, `freeSlots`, `retiredSlots`, `peakLiveSlots`, per-state counts, and a primitive stack of segment indexes with available slots. `close()` closes only committed segments.

- [ ] **Step 5: Run all object-table and stable-handle tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-ffm -am -Dtest=YierdisNativeObjectTableTest,YierdisStableNativeAllocatorTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS; stale-generation, retirement, pin, quarantine, move, and defrag tests remain green.

- [ ] **Step 6: Commit segmented metadata**

```bash
git add yierdis-memory/yierdis-memory-ffm
git commit -m "feat: allocate native object metadata lazily"
```

Expected: PASS and the lazy segmented object-table commit succeeds.

### Task 3: Add Accounted Warm-Page Retention And Bounded Pressure Trim

**Interfaces:** Produces `NativeAllocator.trimEmptyPages(MemoryPressureBudget)`, `estimateAdditionalGrowth(int...)`, and `MemoryReclaimResult` with inspected page count and physically closed bytes.

- [ ] **Step 1: Add failing trim tests**

Add to `YierdisNativePageAllocatorTest`:

```java
@Test
public void normalFreeRetainsOneWarmPageAndPressureTrimClosesIt() {
    try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("page-trim");
         YierdisNativePageAllocator allocator = new YierdisNativePageAllocator(runtime)) {
        YierdisNativeBlock block = allocator.allocate(32);
        block.close();
        Assert.assertEquals(1L, allocator.stats().emptySmallPages());
        Assert.assertEquals(YierdisNativePageAllocator.PAGE_BYTES, runtime.usedBytes());

        MemoryReclaimResult result = allocator.trimEmptyPages(MemoryPressureBudget.unlimited());
        Assert.assertEquals(1L, result.reclaimedUnits());
        Assert.assertEquals(YierdisNativePageAllocator.PAGE_BYTES, result.reclaimedBytes());
        Assert.assertEquals(0L, runtime.usedBytes());
    }
}

@Test
public void trimUsesTheEmptyPageIndexAndHonorsInspectionBudget() {
    try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("bounded-trim");
         YierdisNativePageAllocator allocator = new YierdisNativePageAllocator(runtime)) {
        YierdisNativeBlock first = allocator.allocate(16);
        YierdisNativeBlock second = allocator.allocate(32);
        first.close();
        second.close();
        MemoryReclaimResult result = allocator.trimEmptyPages(new MemoryPressureBudget(1, Long.MAX_VALUE, Long.MAX_VALUE));
        Assert.assertEquals(1L, result.inspectedUnits());
        Assert.assertEquals(YierdisNativePageAllocator.PAGE_BYTES, result.reclaimedBytes());
        Assert.assertEquals(MemoryReclaimResult.StopReason.INSPECTION_LIMIT, result.stopReason());
    }
}

@Test
public void allocationEstimateIncludesOnlyNewSegmentsAndPages() {
    try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("allocation-estimate");
         YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 8_192)) {
        NativeAllocationGrowth first = allocator.estimateAdditionalGrowth(32);
        Assert.assertEquals(4_096L * YierdisNativeObjectTable.META_BYTES,
                first.nativeMetadataCommittedBytes());
        Assert.assertEquals(YierdisNativePageAllocator.PAGE_BYTES,
                first.nativeDataCommittedBytes());
        Assert.assertTrue(first.heapEstimatedBytes() > 0L);
        NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 32);
        Assert.assertEquals(NativeAllocationGrowth.zero(), allocator.estimateAdditionalGrowth(32));
        allocator.free(handle);
    }
}
```

Add a 100,000-iteration medium/large span churn case. After every allocation is
closed, assert live page-directory entries, span descriptors, live-region
count, and page-id-directory heap bytes return to the fixture baseline. Add a
reflection guard that `YierdisNativePageAllocator` owns no `List`/`Map` field
whose size can follow historical allocations, and that `YierdisFfmMemoryRuntime`
owns no `Set`/`Map` of regions.

Add `NativeAllocationGrowthTest` cases that reject each negative component,
saturate `effectiveBytes()` and component-wise `plus(...)` at `Long.MAX_VALUE`,
and verify `zero()` is the additive identity.

- [ ] **Step 2: Run and observe missing trim behavior**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-ffm -am -Dtest=NativeAllocationGrowthTest,YierdisNativePageAllocatorTest#normalFreeRetainsOneWarmPageAndPressureTrimClosesIt+trimUsesTheEmptyPageIndexAndHonorsInspectionBudget+allocationEstimateIncludesOnlyNewSegmentsAndPages -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL with missing trim, estimate, empty-page, and common-memory APIs.

- [ ] **Step 3: Implement empty-page indexing and physical close**

Replace each `SmallPage.freeOffsets` boxed deque with a fixed primitive `int[]` LIFO stack sized to that page's block count. Replace the append-only `smallPages` and `spans` arrays with intrusive per-size-class non-full/empty lists, one intrusive live-span list, exact counters, and `YierdisNativePageDirectory`. Track one warm empty page per `YierdisNativeSizeClass`; when a second page of the same class becomes empty, close the older page after O(1) unlinking. `trimEmptyPages` consumes only the indexed empty-page list, removes and closes candidates within all three limits, updates committed bytes before region close is reported complete, and returns the exact stop reason. Closing any page/span removes every strong reference so churn cannot retain historical descriptors.

Allocate one reusable primitive page id per small page or whole span. Return it to a primitive free-id stack only after the directory entry is removed and no metadata, pin, defrag source, or quarantine record can refer to the page. Empty directory segments are released. `estimateAdditionalGrowth` simulates free-id reuse and directory-segment growth; id exhaustion is possible only at peak concurrent page-id capacity, not after a lifetime count of allocations.

- [ ] **Step 4: Expose trim through `NativeAllocator`**

Implement the exact `NativeAllocationGrowth` record above and add the four Stage 1 methods to `NativeAllocator`. `YierdisStableNativeAllocator.memoryUsage()` includes conservative heap bytes for the object-segment directory, segment objects/free stacks/retired bitmaps, page directory and free-id stack, intrusive list references, page/span and FFM region/arena/segment wrappers, primitive free-offset stacks, quarantine arrays, and defrag state; it also reports object-table metadata committed bytes and page allocator committed/live/reclaimable bytes. `estimateAdditionalGrowth` validates every positive requested size and simulates the entire batch without mutation: it consumes current non-retired metadata slots, reusable page ids, and size-class free blocks first, then adds heap directory/array growth together with whole 4096-slot metadata segments, 64 KiB small pages, and rounded medium/large spans using saturating arithmetic. Replace the runtime's concurrent live-region set with exact atomic count/byte counters. Delegate trim to the page allocator and add reclaimed pages to the existing cumulative defrag-reclaimed metric.

- [ ] **Step 5: Run page, runtime, and allocator tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-ffm -am -Dtest=NativeAllocationGrowthTest,YierdisNativePageAllocatorTest,YierdisFfmMemoryRuntimeTest,YierdisStableNativeAllocatorTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS with zero live runtime bytes after close.

- [ ] **Step 6: Commit page reclamation**

```bash
git add yierdis-memory
git commit -m "feat: trim empty native pages under pressure"
```

Expected: PASS and the warm-page/pressure-trim commit succeeds.

### Task 4: Remove The Stable Allocator Object Mirror

**Interfaces:** `YierdisNativeObjectMeta.segmentId()` stores the page id and `address()` stores the page offset; `YierdisNativePageDirectory` resolves pages without boxed keys and reports predictable heap capacity.

- [ ] **Step 1: Add failing mirror and location tests**

Add to `YierdisStableNativeAllocatorTest`:

```java
@Test
public void allocatorHasNoPerObjectAllocationMap() {
    Assert.assertFalse(Arrays.stream(YierdisStableNativeAllocator.class.getDeclaredFields())
            .anyMatch(field -> Map.class.isAssignableFrom(field.getType())));
}

@Test
public void objectCanResolveReallocateDefragAndFreeFromMetadataLocation() {
    try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("metadata-location");
         YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 32)) {
        NativeHandle handle = allocator.allocate(NativeObjectKind.STRING_BYTES, 32);
        try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_WRITE)) {
            view.setByte(0, (byte) 42);
        }
        allocator.realloc(handle, 128, NativeReallocPolicy.PRESERVE_PREFIX);
        allocator.defragOne(handle, 1_024);
        try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
            Assert.assertEquals(42, view.getByte(0));
        }
        allocator.free(handle);
        Assert.assertEquals(0L, allocator.stats().liveObjects());
    }
}
```

- [ ] **Step 2: Run and verify the mirror assertion fails**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-ffm -am -Dtest=YierdisStableNativeAllocatorTest#allocatorHasNoPerObjectAllocationMap+objectCanResolveReallocateDefragAndFreeFromMetadataLocation -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because `allocations` is a `HashMap<Long, Allocation>`.

- [ ] **Step 3: Add page-id location resolution**

Maintain page-level lookup in `YierdisNativePageDirectory`, using lazily grown primitive page-id segments and reference arrays rather than `Map<Long, ...>` or boxed keys. Add package-private `view`, `free`, and `moveSource` operations that validate page class, offset, capacity, and closed state from `YierdisNativeObjectMeta`. Directory growth and the primitive free-id stack are included in `memoryUsage()` and `estimateAdditionalGrowth`. Metadata publication writes page id and page offset before a handle becomes visible. Reuse a closed page id only after the old directory entry and every quarantine/defrag reference are gone; checked exhaustion of concurrently live ids throws the existing `OffHeapOutOfMemoryException`. Stage 2 converts this boundary to the unified capacity subtype.

- [ ] **Step 4: Remove `Allocation` and `allocations`**

Resolve all live, quarantined, and defrag source locations through metadata and page indexes. Store retained moved-block quarantine as primitive parallel arrays of page id, offset, capacity, and retirement epoch. Iterate live defrag candidates directly through segmented object-table slot cursors in slot order; do not build boxed handle/slot lists. Preserve `NativeObjectView` pin/unpin and stale-generation validation.

- [ ] **Step 5: Run the complete allocator contract**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-ffm -am -Dtest=NativeAllocatorContractTest,YierdisStableNativeAllocatorTest,YierdisNativeObjectTableTest,YierdisNativePageAllocatorTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 6: Commit metadata-owned locations**

```bash
git add yierdis-memory
git commit -m "refactor: resolve native objects from table metadata"
```

Expected: PASS and the allocator mirror-removal commit succeeds.

### Task 5: Enforce Owner-Thread Access And Provide A Concurrent Adapter

**Interfaces:** Production allocator methods are not `synchronized`; `bindToCurrentThread()` explicitly establishes ownership. `SynchronizedNativeAllocator` is the only supported multi-thread adapter.

- [ ] **Step 1: Add failing confinement tests**

```java
@Test
public void productionAllocatorMethodsAreNotSynchronized() {
    for (String name : List.of("allocate", "realloc", "free", "pin", "unpin", "resolve", "stats")) {
        Assert.assertFalse(Modifier.isSynchronized(Arrays.stream(YierdisStableNativeAllocator.class.getMethods())
                .filter(method -> method.getName().equals(name)).findFirst().orElseThrow().getModifiers()));
    }
}

@Test
public void crossThreadProductionAccessFailsFast() throws Exception {
    try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("owner-guard");
         YierdisStableNativeAllocator allocator = new YierdisStableNativeAllocator(runtime, 16)) {
        allocator.bindToCurrentThread();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread thread = Thread.ofPlatform().start(() -> {
            try { allocator.stats(); } catch (Throwable t) { failure.set(t); }
        });
        thread.join();
        Assert.assertTrue(failure.get() instanceof IllegalStateException);
    }
}
```

- [ ] **Step 2: Verify synchronized production methods fail the test**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-memory/yierdis-memory-ffm -am -Dtest=YierdisStableNativeAllocatorTest#productionAllocatorMethodsAreNotSynchronized+crossThreadProductionAccessFailsFast -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL on synchronized modifiers and absent binding.

- [ ] **Step 3: Implement the guard and adapter**

`YierdisAllocatorThreadGuard` starts unbound. `bindToCurrentThread()` or the first stateful standalone operation claims ownership; once claimed, ownership never silently moves to another thread. Server startup binds explicitly before use. Remove method-wide synchronization from the object table, page allocator, stable allocator, and object views. `SynchronizedNativeAllocator` serializes the complete `NativeAllocator` interface around an internal guard-disabled stable allocator; add a two-thread allocation/free test that ends with zero live objects.

- [ ] **Step 4: Bind from DB ownership setup**

In `YierdisDbRuntimeState.bindToCurrentThread()`, bind the DB guard and then call `keyLifecycle().nativeAllocator().bindToCurrentThread()`. Keep cross-thread DB access fail-fast behavior.

- [ ] **Step 5: Run memory and DB construction tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=YierdisStableNativeAllocatorTest,SynchronizedNativeAllocatorTest,YierdisDbConstructionTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 6: Commit confinement**

```bash
git add yierdis-memory yierdis-db/yierdis-db-memory
git commit -m "refactor: confine production allocator to owner thread"
```

Expected: PASS and the owner-thread/concurrent-adapter commit succeeds.

### Task 6: Remove Root, Entry, And Collection Adapter Mirroring

**Interfaces:** Keyspace lifecycle is the owner of entry and string handles; `YierdisNativeAdapterDirectory<T>` resolves collection adapters by validated native slot without boxed keys; allocator kind counts provide diagnostics, and allocator close reports leaks before forced cleanup.

- [ ] **Step 1: Add a failing structural and lifecycle test**

Add to `StringRootTest`:

```java
@Test
public void stringRootDoesNotMirrorEveryLiveHandle() {
    Assert.assertFalse(Arrays.stream(StringRoot.class.getDeclaredFields())
            .anyMatch(field -> Set.class.isAssignableFrom(field.getType())));
}

@Test
public void entryTableDoesNotMirrorEveryLiveHandle() {
    Assert.assertFalse(Arrays.stream(EntryTable.class.getDeclaredFields())
            .anyMatch(field -> Set.class.isAssignableFrom(field.getType())));
}
```

Add to `CollectionRootTest` a reflection assertion that
`NativeCollectionRootTable` owns no `Map`/`Set`, then create/release Hash, Set,
and ZSet roots across at least two native object-table segments. Verify adapter
lookup rejects stale generations, clear/close visits each live adapter exactly
once without allocating a boxed handle snapshot, and completely empty adapter
directory segments are released.

Extend existing create/store/release tests to delete entries and strings through `YierdisDbKeyLifecycle`, close the DB, and assert `allocator.stats().objectCount(ENTRY_RECORD)`, total live objects, and `runtime.usedBytes()` are zero.

- [ ] **Step 2: Run and verify `liveHandles` is detected**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=StringRootTest,EntryTableContractTest,CollectionRootTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because `StringRoot`/`EntryTable` own boxed `liveHandles` sets and collection roots own a boxed adapter map.

- [ ] **Step 3: Make lifecycle ownership authoritative**

Remove both `liveHandles` sets, synchronized modifiers, and root/table-level enumeration. Validate handles by native kind/domain and object-table metadata. Derive entry count/native bytes from allocator `ENTRY_RECORD` kind counts. Replace `NativeCollectionRootTable.adapters` with lazily allocated 4096-slot `Object[]` directory segments plus primitive live counts; validate the native handle before slot lookup, compare the adapter's recorded raw handle/generation, and release an empty directory segment because generation authority remains in the native object table. Iteration walks only committed adapter segments and non-null references without `Long[]`, boxed keys, or snapshots. Make keyspace clear/shutdown iterate directory entries, release each value exactly once, then release entry and key handles. Standalone owning tables rely on allocator close after their explicit handles are released; shared tables never attempt hidden enumeration. On allocator close, capture the live count as a leak diagnostic, perform bounded forced page/table cleanup, and throw the first invariant failure after cleanup.

- [ ] **Step 4: Run string, key lifecycle, and shutdown tests**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-db/yierdis-db-memory -am -Dtest=StringRootTest,EntryTableContractTest,CollectionRootTest,OffHeapStringStorageTest,NativeStorageRegressionTest,TtlLifecycleDirectOpsTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS and no runtime-region leaks.

- [ ] **Step 5: Commit mirror removal**

```bash
git add yierdis-db/yierdis-db-memory yierdis-memory/yierdis-memory-ffm
git commit -m "refactor: remove boxed root handle mirrors"
```

Expected: PASS and the root/entry/collection mirror-removal commit succeeds.

### Task 7: Prove Sixteen Empty Databases Have Zero Metadata Footprint

**Interfaces:** This is the Stage 1 acceptance test consumed by later maxmemory work.

- [ ] **Step 1: Add the failing integration regression**

Create `EmptyDatabaseFootprintTest`:

```java
@Test
public void sixteenEmptyDatabasesCommitNoObjectMetadata() {
    try (YierdisFfmMemoryRuntime runtime = new YierdisFfmMemoryRuntime("empty-sixteen")) {
        YierdisDbEngineFactory factory = new YierdisDbEngineFactory(
                runtime, NativeDefragOptions.disabled(), 262_144);
        try (YierdisInstance instance = YierdisInstance.create(YierdisInstanceConfig.builder()
                .databases(16)
                .engineFactory(factory)
                .build())) {
            instance.bindToCurrentThread();
            for (int db = 0; db < 16; db++) {
                YierdisMemoryStats stats = instance.engine(db).memory().memoryStats();
                Assert.assertEquals(0L, stats.nativeMetadataCommittedBytes());
            }
        }
        Assert.assertEquals(0L, runtime.usedBytes());
    }
}
```

- [ ] **Step 2: Run the acceptance test**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-integration-tests -am -Dtest=EmptyDatabaseFootprintTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: initially FAIL because `YierdisMemoryStats` does not expose metadata committed bytes. Add the field as a compatibility extension populated from `NativeAllocator.memoryUsage()`; Stage 4 will replace the remaining accounting fields.

- [ ] **Step 3: Verify architecture and full suite**

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn -pl yierdis-tests/yierdis-architecture-tests -am test
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn test
```

Expected: all tests PASS except no pre-existing failure is waived; specifically record the current `MaxmemoryScopeTest` status for Stage 4 rather than weakening its assertions.

- [ ] **Step 4: Commit Stage 1 acceptance**

```bash
git add yierdis-tests/yierdis-integration-tests yierdis-db/yierdis-db-api yierdis-db/yierdis-db-memory
git commit -m "test: lock empty database native footprint"
```

Expected: PASS for architecture and the full suite with the empty-database footprint commit present.

Stage 1 is complete only when `runtime.usedBytes()` returns to zero after the test and every existing stable-handle safety test remains green.
