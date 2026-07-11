package yier.bubu.redis.memory.foreign;

import java.util.Arrays;
import yier.bubu.redis.memory.api.OffHeapOutOfMemoryException;

final class YierdisNativePageDirectory {
    private static final int ENTRIES_PER_SEGMENT = 1_024;
    private static final int INITIAL_DIRECTORY_SEGMENTS = 16;
    private static final int INITIAL_FREE_IDS = 16;
    private static final long ARRAY_HEADER_BYTES = 16L;
    private static final long REFERENCE_BYTES = 8L;
    private static final long INT_BYTES = 4L;

    private Object[][] segments = new Object[INITIAL_DIRECTORY_SEGMENTS][];
    private int[] segmentCounts = new int[INITIAL_DIRECTORY_SEGMENTS];
    private int[] freeIds = new int[INITIAL_FREE_IDS];
    private int freeIdCount;
    private int nextId = 1;
    private int liveEntries;

    int add(Object entry) {
        if (entry == null) {
            throw new NullPointerException("entry");
        }
        int pageId;
        if (freeIdCount > 0) {
            pageId = freeIds[--freeIdCount];
        } else {
            if (nextId <= 0) {
                throw new OffHeapOutOfMemoryException("native page id space exhausted");
            }
            pageId = nextId;
            nextId = nextId == Integer.MAX_VALUE ? -1 : nextId + 1;
        }
        int segmentIndex = segmentIndex(pageId);
        ensureDirectoryCapacity(segmentIndex + 1);
        Object[] segment = segments[segmentIndex];
        if (segment == null) {
            segment = new Object[ENTRIES_PER_SEGMENT];
            segments[segmentIndex] = segment;
        }
        int offset = segmentOffset(pageId);
        if (segment[offset] != null) {
            throw new IllegalStateException("page id is already live: " + pageId);
        }
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
        int segmentIndex = segmentIndex(pageId);
        if (segmentIndex >= segments.length || segments[segmentIndex] == null) {
            throw new IllegalStateException("unknown page id: " + pageId);
        }
        int offset = segmentOffset(pageId);
        if (segments[segmentIndex][offset] != expected) {
            throw new IllegalStateException("page id owner mismatch: " + pageId);
        }
        segments[segmentIndex][offset] = null;
        segmentCounts[segmentIndex]--;
        liveEntries--;
        if (segmentCounts[segmentIndex] == 0) {
            segments[segmentIndex] = null;
        }
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
        long bytes = ARRAY_HEADER_BYTES + (long) segments.length * REFERENCE_BYTES;
        bytes += ARRAY_HEADER_BYTES + (long) segmentCounts.length * INT_BYTES;
        bytes += ARRAY_HEADER_BYTES + (long) freeIds.length * INT_BYTES;
        for (Object[] segment : segments) {
            if (segment != null) {
                bytes += ARRAY_HEADER_BYTES + (long) segment.length * REFERENCE_BYTES;
            }
        }
        return bytes;
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

    void clear() {
        segments = new Object[INITIAL_DIRECTORY_SEGMENTS][];
        segmentCounts = new int[INITIAL_DIRECTORY_SEGMENTS];
        freeIds = new int[INITIAL_FREE_IDS];
        freeIdCount = 0;
        nextId = 1;
        liveEntries = 0;
    }

    private void ensureDirectoryCapacity(int required) {
        if (required <= segments.length) {
            return;
        }
        int capacity = grownCapacity(segments.length, required);
        segments = Arrays.copyOf(segments, capacity);
        segmentCounts = Arrays.copyOf(segmentCounts, capacity);
    }

    private void ensureFreeIdCapacity(int required) {
        if (required <= freeIds.length) {
            return;
        }
        freeIds = Arrays.copyOf(freeIds, grownCapacity(freeIds.length, required));
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

    private static int segmentOffset(int pageId) {
        return (pageId - 1) % ENTRIES_PER_SEGMENT;
    }
}
