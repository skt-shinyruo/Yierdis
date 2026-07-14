package yier.bubu.redis.memory.foreign;

import java.util.Arrays;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;
import yier.bubu.redis.memory.api.NativeCapacityExceededException;

final class YierdisNativePageDirectory {
    private static final int ENTRIES_PER_SEGMENT = 1_024;
    private static final int INITIAL_DIRECTORY_SEGMENTS = 16;
    private static final int INITIAL_FREE_IDS = 16;
    private static final long ARRAY_HEADER_BYTES = 16L;
    private static final long CHECKPOINT_OBJECT_BYTES = 56L;
    private static final long REFERENCE_BYTES = 8L;
    private static final long INT_BYTES = 4L;

    private Object[][] segments = new Object[INITIAL_DIRECTORY_SEGMENTS][];
    private int[] segmentCounts = new int[INITIAL_DIRECTORY_SEGMENTS];
    private int[] freeIds = new int[INITIAL_FREE_IDS];
    private int freeIdCount;
    private int nextId = 1;
    private int liveEntries;
    private long retainedHeapBytes;
    private AllocationScopeCheckpoint activeAllocationScope;
    private boolean allocationScopeAbortInProgress;
    private boolean heapIterationTrapForTesting;
    private boolean allocationScopeAbortAllocationTracking;
    private boolean allocationScopeAbortAllocated;

    YierdisNativePageDirectory() {
        retainedHeapBytes = baseHeapBytes();
    }

    int add(Object entry) {
        if (entry == null) {
            throw new NullPointerException("entry");
        }
        int pageId;
        if (freeIdCount > 0) {
            pageId = freeIds[--freeIdCount];
        } else {
            if (nextId <= 0) {
                throw new NativeCapacityExceededException("native page id space exhausted");
            }
            pageId = nextId;
            nextId = nextId == Integer.MAX_VALUE ? -1 : nextId + 1;
        }
        int segmentIndex = segmentIndex(pageId);
        ensureDirectoryCapacity(segmentIndex + 1);
        Object[] segment = segments[segmentIndex];
        if (segment == null) {
            detachSegmentsForActiveAllocationScope();
            segment = new Object[ENTRIES_PER_SEGMENT];
            segments[segmentIndex] = segment;
            retainedHeapBytes = MemoryUsageSnapshot.addSaturating(retainedHeapBytes, segmentHeapBytes());
        }
        int offset = segmentOffset(pageId);
        if (segment[offset] != null) {
            throw new IllegalStateException("page id is already live: " + pageId);
        }
        detachSegmentCountsForActiveAllocationScope();
        segment[offset] = entry;
        segmentCounts[segmentIndex]++;
        liveEntries++;
        return pageId;
    }

    Object get(int pageId) {
        if (pageId <= 0) {
            return null;
        }
        int segmentIndex = segmentIndex(pageId);
        if (segmentIndex >= segments.length || segments[segmentIndex] == null) {
            return null;
        }
        return segments[segmentIndex][segmentOffset(pageId)];
    }

    void remove(int pageId, Object expected) {
        remove(pageId, expected, true);
    }

    void remove(int pageId, Object expected, boolean recycleId) {
        int segmentIndex = segmentIndex(pageId);
        if (segmentIndex >= segments.length || segments[segmentIndex] == null) {
            throw new IllegalStateException("unknown page id: " + pageId);
        }
        int offset = segmentOffset(pageId);
        if (segments[segmentIndex][offset] != expected) {
            throw new IllegalStateException("page id owner mismatch: " + pageId);
        }
        detachSegmentCountsForActiveAllocationScope();
        segments[segmentIndex][offset] = null;
        segmentCounts[segmentIndex]--;
        liveEntries--;
        if (segmentCounts[segmentIndex] == 0) {
            detachSegmentsForActiveAllocationScope();
            segments[segmentIndex] = null;
            subtractRetainedHeapBytes(segmentHeapBytes());
        }
        if (!recycleId) {
            return;
        }
        detachFreeIdsForActiveAllocationScope();
        ensureFreeIdCapacity(freeIdCount + 1);
        freeIds[freeIdCount++] = pageId;
    }

    int liveEntries() {
        return liveEntries;
    }

    int reusableIdCount() {
        return freeIdCount;
    }

    long heapEstimatedBytes() {
        return retainedHeapBytes;
    }

    void armHeapIterationTrapForTesting() {
        heapIterationTrapForTesting = true;
    }

    void disarmHeapIterationTrapForTesting() {
        heapIterationTrapForTesting = false;
    }

    void armAllocationScopeAbortAllocationTrackingForTesting() {
        allocationScopeAbortAllocationTracking = true;
        allocationScopeAbortAllocated = false;
    }

    void disarmAllocationScopeAbortAllocationTrackingForTesting() {
        allocationScopeAbortAllocationTracking = false;
    }

    boolean allocationScopeAbortAllocatedForTesting() {
        return allocationScopeAbortAllocated;
    }

    long estimateAdditionalHeapBytes(int additionalEntries) {
        if (additionalEntries <= 0) {
            return 0L;
        }
        long bytes = 0L;
        int idsBeyondReusable = Math.max(0, additionalEntries - freeIdCount);
        int highestId = idsBeyondReusable == 0 ? 0 : nextId + idsBeyondReusable - 1;
        if (highestId > 0) {
            int requiredDirectoryLength = segmentIndex(highestId) + 1;
            if (requiredDirectoryLength > segments.length) {
                int grown = grownCapacity(segments.length, requiredDirectoryLength);
                bytes += (long) (grown - segments.length) * (REFERENCE_BYTES + INT_BYTES);
            }
        }

        int simulatedDirectoryLength = highestId == 0
                ? segments.length
                : Math.max(segments.length, segmentIndex(highestId) + 1);
        boolean[] materialized = new boolean[simulatedDirectoryLength];
        for (int i = 0; i < segments.length; i++) {
            materialized[i] = segments[i] != null;
        }
        for (int i = 0; i < additionalEntries; i++) {
            int pageId = i < freeIdCount ? freeIds[freeIdCount - 1 - i] : nextId + i - freeIdCount;
            int segmentIndex = segmentIndex(pageId);
            if (!materialized[segmentIndex]) {
                materialized[segmentIndex] = true;
                bytes += ARRAY_HEADER_BYTES + ENTRIES_PER_SEGMENT * REFERENCE_BYTES;
            }
        }
        return bytes;
    }

    long worstCaseSegmentRematerializationHeapBytes(int additionalEntries) {
        if (additionalEntries <= 0) {
            return 0L;
        }
        long segmentBytes = ARRAY_HEADER_BYTES + ENTRIES_PER_SEGMENT * REFERENCE_BYTES;
        return Long.MAX_VALUE / segmentBytes < additionalEntries
                ? Long.MAX_VALUE
                : segmentBytes * additionalEntries;
    }

    AllocationScopeCheckpoint allocationScopeCheckpoint() {
        if (activeAllocationScope != null) {
            throw new IllegalStateException("native page-directory allocation scope is already active");
        }
        AllocationScopeCheckpoint checkpoint = new AllocationScopeCheckpoint(
                segments,
                segmentCounts,
                freeIds,
                freeIdCount,
                nextId,
                liveEntries,
                retainedHeapBytes
        );
        activeAllocationScope = checkpoint;
        return checkpoint;
    }

    long allocationScopeCheckpointHeapEstimatedBytes() {
        return allocationScopeCheckpointHeapEstimatedBytes(
                segments.length,
                segmentCounts.length,
                freeIds.length
        );
    }

    void promoteAllocationScope(AllocationScopeCheckpoint checkpoint) {
        if (activeAllocationScope == checkpoint) {
            activeAllocationScope = null;
            allocationScopeAbortInProgress = false;
            checkpoint.releaseReferences();
        }
    }

    void beginAllocationScopeAbort(AllocationScopeCheckpoint checkpoint) {
        if (activeAllocationScope != checkpoint) {
            throw new IllegalStateException("native page-directory allocation scope is not active");
        }
        allocationScopeAbortInProgress = true;
    }

    void discardAllocationScope(AllocationScopeCheckpoint checkpoint) {
        if (activeAllocationScope == checkpoint) {
            allocationScopeAbortInProgress = false;
            activeAllocationScope = null;
        }
        checkpoint.releaseReferences();
    }

    void restoreAllocationScopeCheckpoint(AllocationScopeCheckpoint checkpoint) {
        if (checkpoint == null) {
            throw new NullPointerException("checkpoint");
        }
        if (activeAllocationScope != checkpoint) {
            throw new IllegalStateException("native page-directory allocation scope is not active");
        }
        try {
            if (liveEntries != checkpoint.liveEntries) {
                throw new IllegalStateException("allocation scope left a live native page");
            }
            segments = checkpoint.segments;
            segmentCounts = checkpoint.segmentCounts;
            freeIds = checkpoint.freeIds;
            freeIdCount = checkpoint.freeIdCount;
            nextId = checkpoint.nextId;
            retainedHeapBytes = checkpoint.retainedHeapBytes;
        } finally {
            allocationScopeAbortInProgress = false;
            activeAllocationScope = null;
            checkpoint.releaseReferences();
        }
    }

    void clear() {
        segments = new Object[INITIAL_DIRECTORY_SEGMENTS][];
        segmentCounts = new int[INITIAL_DIRECTORY_SEGMENTS];
        freeIds = new int[INITIAL_FREE_IDS];
        freeIdCount = 0;
        nextId = 1;
        liveEntries = 0;
        retainedHeapBytes = baseHeapBytes();
        activeAllocationScope = null;
        allocationScopeAbortInProgress = false;
    }

    private void ensureDirectoryCapacity(int required) {
        if (required <= segments.length) {
            return;
        }
        failIfAllocationScopeAbortWouldAllocate();
        retainDirectoryArraysForActiveAllocationScope();
        long previousHeapBytes = directoryArrayHeapBytes();
        int capacity = grownCapacity(segments.length, required);
        segments = Arrays.copyOf(segments, capacity);
        segmentCounts = Arrays.copyOf(segmentCounts, capacity);
        replaceRetainedHeapBytes(previousHeapBytes, directoryArrayHeapBytes());
    }

    private void ensureFreeIdCapacity(int required) {
        if (required <= freeIds.length) {
            return;
        }
        failIfAllocationScopeAbortWouldAllocate();
        if (allocationScopeAbortAllocationTracking) {
            allocationScopeAbortAllocated = true;
        }
        retainFreeIdsForActiveAllocationScope();
        long previousHeapBytes = arrayHeapBytes(freeIds.length, INT_BYTES);
        freeIds = Arrays.copyOf(freeIds, grownCapacity(freeIds.length, required));
        replaceRetainedHeapBytes(previousHeapBytes, arrayHeapBytes(freeIds.length, INT_BYTES));
    }

    private void detachSegmentsForActiveAllocationScope() {
        if (activeAllocationScope == null || segments != activeAllocationScope.segments) {
            return;
        }
        failIfAllocationScopeAbortWouldAllocate();
        segments = segments.clone();
        activeAllocationScope.retainSegments();
    }

    private void detachSegmentCountsForActiveAllocationScope() {
        if (activeAllocationScope == null || segmentCounts != activeAllocationScope.segmentCounts) {
            return;
        }
        failIfAllocationScopeAbortWouldAllocate();
        segmentCounts = segmentCounts.clone();
        activeAllocationScope.retainSegmentCounts();
    }

    private void detachFreeIdsForActiveAllocationScope() {
        if (activeAllocationScope == null || freeIds != activeAllocationScope.freeIds) {
            return;
        }
        failIfAllocationScopeAbortWouldAllocate();
        freeIds = freeIds.clone();
        activeAllocationScope.retainFreeIds();
    }

    private void retainDirectoryArraysForActiveAllocationScope() {
        if (activeAllocationScope == null) {
            return;
        }
        if (segments == activeAllocationScope.segments) {
            activeAllocationScope.retainSegments();
        }
        if (segmentCounts == activeAllocationScope.segmentCounts) {
            activeAllocationScope.retainSegmentCounts();
        }
    }

    private void retainFreeIdsForActiveAllocationScope() {
        if (activeAllocationScope != null && freeIds == activeAllocationScope.freeIds) {
            activeAllocationScope.retainFreeIds();
        }
    }

    private void failIfAllocationScopeAbortWouldAllocate() {
        if (allocationScopeAbortInProgress) {
            throw new IllegalStateException("native page-directory allocation scope abort must not allocate");
        }
    }

    private static int grownCapacity(int current, int required) {
        int capacity = current;
        while (capacity < required) {
            int next = capacity + Math.max(1, capacity >>> 1);
            if (next < 0) {
                return required;
            }
            capacity = next;
        }
        return capacity;
    }

    private static int segmentIndex(int pageId) {
        return (pageId - 1) / ENTRIES_PER_SEGMENT;
    }

    private long baseHeapBytes() {
        return MemoryUsageSnapshot.addSaturating(
                directoryArrayHeapBytes(),
                arrayHeapBytes(freeIds.length, INT_BYTES)
        );
    }

    private long directoryArrayHeapBytes() {
        return MemoryUsageSnapshot.addSaturating(
                arrayHeapBytes(segments.length, REFERENCE_BYTES),
                arrayHeapBytes(segmentCounts.length, INT_BYTES)
        );
    }

    private static long segmentHeapBytes() {
        return arrayHeapBytes(ENTRIES_PER_SEGMENT, REFERENCE_BYTES);
    }

    private void replaceRetainedHeapBytes(long previousHeapBytes, long nextHeapBytes) {
        subtractRetainedHeapBytes(previousHeapBytes);
        retainedHeapBytes = MemoryUsageSnapshot.addSaturating(retainedHeapBytes, nextHeapBytes);
    }

    private void subtractRetainedHeapBytes(long bytes) {
        if (bytes < 0L || retainedHeapBytes < bytes) {
            throw new IllegalStateException("native page-directory heap accounting underflow");
        }
        retainedHeapBytes -= bytes;
    }

    private static int segmentOffset(int pageId) {
        return (pageId - 1) % ENTRIES_PER_SEGMENT;
    }

    static final class AllocationScopeCheckpoint {
        private Object[][] segments;
        private int[] segmentCounts;
        private int[] freeIds;
        private final int freeIdCount;
        private final int nextId;
        private final int liveEntries;
        private final long retainedHeapBytes;
        private boolean retainsSegments;
        private boolean retainsSegmentCounts;
        private boolean retainsFreeIds;

        private AllocationScopeCheckpoint(
                Object[][] segments,
                int[] segmentCounts,
                int[] freeIds,
                int freeIdCount,
                int nextId,
                int liveEntries,
                long retainedHeapBytes
        ) {
            this.segments = segments;
            this.segmentCounts = segmentCounts;
            this.freeIds = freeIds;
            this.freeIdCount = freeIdCount;
            this.nextId = nextId;
            this.liveEntries = liveEntries;
            this.retainedHeapBytes = retainedHeapBytes;
        }

        private void retainSegments() {
            retainsSegments = true;
        }

        private void retainSegmentCounts() {
            retainsSegmentCounts = true;
        }

        private void retainFreeIds() {
            retainsFreeIds = true;
        }

        private void releaseReferences() {
            segments = null;
            segmentCounts = null;
            freeIds = null;
            retainsSegments = false;
            retainsSegmentCounts = false;
            retainsFreeIds = false;
        }

        long heapEstimatedBytes() {
            long bytes = CHECKPOINT_OBJECT_BYTES;
            if (retainsSegments) {
                bytes = MemoryUsageSnapshot.addSaturating(bytes, arrayHeapBytes(segments.length, REFERENCE_BYTES));
            }
            if (retainsSegmentCounts) {
                bytes = MemoryUsageSnapshot.addSaturating(bytes, arrayHeapBytes(segmentCounts.length, INT_BYTES));
            }
            if (retainsFreeIds) {
                bytes = MemoryUsageSnapshot.addSaturating(bytes, arrayHeapBytes(freeIds.length, INT_BYTES));
            }
            return bytes;
        }
    }

    private static long arrayHeapBytes(int length, long elementBytes) {
        return ARRAY_HEADER_BYTES + (long) length * elementBytes;
    }

    private static long allocationScopeCheckpointHeapEstimatedBytes(
            int segmentsLength,
            int segmentCountsLength,
            int freeIdsLength
    ) {
        return CHECKPOINT_OBJECT_BYTES
                + arrayHeapBytes(segmentsLength, REFERENCE_BYTES)
                + arrayHeapBytes(segmentCountsLength, INT_BYTES)
                + arrayHeapBytes(freeIdsLength, INT_BYTES);
    }
}
